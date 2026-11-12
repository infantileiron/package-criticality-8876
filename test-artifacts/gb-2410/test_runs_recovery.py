"""冻结情景单一活动运行、队列去重、中断恢复与迟到回写归属。"""
import fakeredis,pytest,threading
from sqlalchemy import select
from src.config.database import Session
from src.models import ScheduleRun
from src.services import runs
import src.worker as worker
from src.utils.time import later

@pytest.fixture(autouse=True)
def fake_queue(monkeypatch):
 fake=fakeredis.FakeRedis(decode_responses=True)
 monkeypatch.setattr(runs,'queue_client',lambda:fake)
 monkeypatch.setattr(worker,'client',fake)
 monkeypatch.setattr(worker,'LEASE_SECONDS',5)
 return fake

def jobs(fake):return fake.lrange(runs.QUEUE_KEY,0,-1)

def test_concurrent_requests_reuse_one_active_run(scenario,fake_queue):
 results=[]

 def request():
  with Session() as session:results.append(runs.create(session,'s1'))

 threads=[threading.Thread(target=request) for _ in range(8)]
 for t in threads:t.start()
 for t in threads:t.join()
 ids={r['id'] for r in results}
 assert len(ids)==1,ids
 assert sorted(r['reused'] for r in results)==[False]+[True]*7
 assert len(jobs(fake_queue))==1
 with Session() as session:
  assert len(list(session.scalars(select(ScheduleRun))))==1
  assert session.scalar(select(ScheduleRun)).active_key=='s1'

def test_queued_duplicate_delivery_does_not_publish_twice(db,scenario,fake_queue):
 first=runs.create(db,'s1')
 assert runs.create(db,'s1')['id']==first['id']
 # 队列重复投递：同一运行两份消息，只有第一次领取生效。
 fake_queue.delete(runs.QUEUE_KEY)
 fake_queue.lpush(runs.QUEUE_KEY,first['id'],first['id'])
 db.expire_all();db.get(ScheduleRun,first['id']).lease_until=later(-5);db.commit()
 assert worker.execute(first['id']) is True
 assert worker.execute(first['id']) is False
 assert worker.execute(first['id']) is False
 db.expire_all()
 row=db.get(ScheduleRun,first['id'])
 assert row.status=='completed' and row.active_key is None and row.result['checksum']
 assert len(list(db.scalars(select(ScheduleRun))))==1

def test_retry_failed_run_keeps_history_and_new_attempt(db,scenario,fake_queue):
 source=runs.create(db,'s1')['id']
 row=db.get(ScheduleRun,source);row.status='failed';row.error='worker 分析异常';row.active_key=None;db.commit()
 attempt=runs.retry(db,source)
 assert attempt['id']!=source and attempt['attempt_count']==2 and attempt['retry_of']==source
 db.expire_all()
 failed=db.get(ScheduleRun,source)
 assert failed.status=='failed' and failed.error=='worker 分析异常'
 with pytest.raises(Exception):runs.retry(db,attempt['id'])

def test_retry_rejects_completed_run(db,scenario,fake_queue):
 run_id=runs.create(db,'s1')['id']
 worker.execute(run_id)
 db.expire_all()
 with pytest.raises(Exception):runs.retry(db,run_id)
 assert db.get(ScheduleRun,run_id).status=='completed'

def test_interrupted_run_recovers_and_finishes(db,scenario,fake_queue):
 run_id=runs.create(db,'s1')['id']
 assert worker.execute(run_id,block=lambda:False) is False  # 中断：无回写
 db.expire_all()
 row=db.get(ScheduleRun,run_id)
 assert row.status=='running' and not row.result
 row.lease_until=later(-5);db.commit()
 db.expire_all()
 assert worker.recover(db)==[run_id]
 db.expire_all()
 assert db.get(ScheduleRun,run_id).recovery_count==1
 assert worker.execute(run_id) is True
 db.expire_all()
 row=db.get(ScheduleRun,run_id)
 assert row.status=='completed' and row.active_key is None
 assert row.attempt_count==1 and row.recovery_count==1 and row.result['checksum']

def test_late_result_from_old_execution_cannot_overwrite(db,scenario,fake_queue):
 run_id=runs.create(db,'s1')['id']
 old=worker.claim(run_id)
 db.expire_all()
 db.get(ScheduleRun,run_id).lease_until=later(-5);db.commit()
 db.expire_all()
 assert worker.recover(db)==[run_id]
 tokens=[];real_claim=worker.claim
 worker.claim=lambda run,token=None: tokens.append(real_claim(run,token)) or tokens[-1]
 assert worker.execute(run_id) is True
 assert tokens and tokens[0]!=old
 worker.claim=real_claim
 db.expire_all()
 good=db.get(ScheduleRun,run_id).result['checksum']
 assert worker.terminate(run_id,old,result={'checksum':'late','stale':True}) is False
 db.expire_all()
 row=db.get(ScheduleRun,run_id)
 assert row.status=='completed' and row.result['checksum']==good
 assert worker.terminate(run_id,old,error='迟到的失败') is False
 db.expire_all()
 assert db.get(ScheduleRun,run_id).status=='completed'

def test_recovery_requeues_stale_queued_once(db,scenario,fake_queue):
 run_id=runs.create(db,'s1')['id']
 assert worker.recover(db)==[]  # 创建后处于派发窗口，不重复投递
 fake_queue.delete(runs.QUEUE_KEY)
 db.expire_all()
 db.get(ScheduleRun,run_id).lease_until=later(-5);db.commit()
 db.expire_all()
 assert worker.recover(db)==[run_id]
 assert jobs(fake_queue)==[run_id]
 assert worker.recover(db)==[]
