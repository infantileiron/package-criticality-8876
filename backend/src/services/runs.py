"""单一活动运行：并发申请复用、失败重试、队列去重。

- 每个冻结情景最多一个 queued/running 运行，由 schedule_runs.active_key 唯一键保证。
- 完成/失败时释放 active_key；失败行原样保留，重试创建新的尝试记录并指向 retry_of。
- 队列投递失败时运行落为 failed，仍可在页面重试。
- lease_until 同时是“领取租约截止”与“排队派发窗口”：窗口内未被领取由恢复扫描重新投递。
"""
import uuid
import redis
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from src.config.settings import REDIS_URL
from src.models import ScheduleRun, DurationScenario
from src.utils.serialization import serialize
from src.utils.time import now,later

QUEUE_KEY='package-criticality:jobs'
DISPATCH_SECONDS=30

def queue_client(): return redis.Redis.from_url(REDIS_URL)

def enqueue(client,run_id):
 try:client.lpush(QUEUE_KEY,run_id);return True
 except redis.RedisError:return False

def serialize_run(run,reused=None):
 data=serialize(run);data.pop('claim_token',None)
 if reused is not None:data['reused']=reused
 return data

def active_for(db,scenario_id):
 return db.scalar(select(ScheduleRun).where(ScheduleRun.active_key==scenario_id))

def _create(db,scenario_id,attempt_count=1,retry_of=None):
 item=ScheduleRun(id=str(uuid.uuid4()),scenario_id=scenario_id,status='queued',result={},error='',created_at=now(),active_key=scenario_id,attempt_count=attempt_count,recovery_count=0,retry_of=retry_of,claim_token='',lease_until=later(DISPATCH_SECONDS),started_at=None,finished_at=None)
 db.add(item)
 try:
  db.commit()
 except IntegrityError:
  # 并发申请在唯一键上冲突：复用先到的活动运行，不重复建任务、不重复投递。
  db.rollback()
  existing=active_for(db,scenario_id)
  if existing:return existing,False
  raise HTTPException(409,'同一情景的活动运行创建冲突，请重试')
 return item,True

def _publish(db,item):
 try:
  client=queue_client();sent=enqueue(client,item.id)
 except Exception:
  sent=False
 if not sent:
  item.status='failed';item.active_key=None;item.claim_token='';item.lease_until=None;item.finished_at=now();item.error='后台队列不可用，请重试';db.commit()
  raise HTTPException(503,item.error)
 return item

def create(db,scenario_id):
 if not db.get(DurationScenario,scenario_id):raise HTTPException(404,'风险情景不存在')
 existing=active_for(db,scenario_id)
 if existing:return serialize_run(existing,reused=True)
 item,created=_create(db,scenario_id)
 if not created:return serialize_run(item,reused=True)
 _publish(db,item)
 return serialize_run(item,reused=False)

def retry(db,run_id):
 source=db.get(ScheduleRun,run_id)
 if not source:raise HTTPException(404,'运行不存在')
 if source.status!='failed':raise HTTPException(409,'仅失败运行可从页面重试；成功结果保留原记录')
 if not db.get(DurationScenario,source.scenario_id):raise HTTPException(409,'原风险情景已不存在，不能重试')
 existing=active_for(db,source.scenario_id)
 if existing:return serialize_run(existing,reused=True)
 item,created=_create(db,source.scenario_id,attempt_count=(source.attempt_count or 1)+1,retry_of=source.id)
 if not created:return serialize_run(item,reused=True)
 _publish(db,item)
 return serialize_run(item,reused=False)

def require_completed(db,id):
 run=db.get(ScheduleRun,id)
 if not run:raise HTTPException(404,'运行不存在')
 if run.status!='completed':raise HTTPException(409,'风险计算尚未完成')
 return run
