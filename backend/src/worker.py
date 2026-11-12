"""后台worker：租约领取、心跳续租、中断恢复与迟到回写丢弃。

同一个运行可能被重复投递；领取时校验状态与租约，已领取或已终态的消息直接丢弃。
计算过程中由心跳线程续租。worker 崩溃后租约到期，恢复扫描把运行重新入队并累加恢复次数。
回写使用 claim_token 条件更新：租约被接管或运行已终态时，旧执行的迟到结果不会覆盖新结果。
"""
import threading,traceback,uuid,redis
from pathlib import Path
from sqlalchemy import select,update
from src.config.settings import ARTIFACTS_DIR,REDIS_URL
from src.config.database import Session
from src.models import ScheduleRun,DurationScenario
from src.services.analysis import analyze
from src.services.runs import QUEUE_KEY,DISPATCH_SECONDS,enqueue
from src.utils.time import now,later,expired

LEASE_SECONDS=30
HEARTBEAT_SECONDS=10
client=redis.Redis.from_url(REDIS_URL)
heartbeat=Path(ARTIFACTS_DIR)/'worker-heartbeat'

def claim(id,token=None):
 """领取运行：queued、租约过期的 running 或恢复扫描已重新派发的运行可领取。"""
 token=token or uuid.uuid4().hex
 with Session() as db:
  run=db.scalar(select(ScheduleRun).where(ScheduleRun.id==id).with_for_update())
  if not run or run.active_key is None or run.status not in ('queued','running'):return None
  if run.status=='running' and run.claim_token and not expired(run.lease_until):return None
  # claim_token 为空表示已由恢复扫描重新派发，可直接领取。
  run.status='running';run.claim_token=token;run.lease_until=later(LEASE_SECONDS)
  if not run.started_at:run.started_at=now()
  db.commit()
  return token

def renew(run_id,token):
 with Session() as db:
  changed=db.execute(update(ScheduleRun).where(ScheduleRun.id==run_id,ScheduleRun.claim_token==token,ScheduleRun.status=='running').values(lease_until=later(LEASE_SECONDS)))
  db.commit();return changed.rowcount==1

def terminate(run_id,token,result=None,error=None):
 """条件回写；token 不再匹配或运行已终态时返回False，迟到执行不覆盖新结果。"""
 values={'lease_until':None,'finished_at':now()}
 if error is None:values.update(status='completed',result=result,error='',active_key=None)
 else:values.update(status='failed',error=error,active_key=None)
 with Session() as db:
  changed=db.execute(update(ScheduleRun).where(ScheduleRun.id==run_id,ScheduleRun.claim_token==token,ScheduleRun.status=='running').values(**values))
  db.commit();return changed.rowcount==1

def _compute(run_id,token,pause=None):
 with Session() as db:
  run=db.get(ScheduleRun,run_id)
  if not run:return False
  scenario=db.get(DurationScenario,run.scenario_id)
 if not scenario:return terminate(run_id,token,error='风险情景不存在，无法恢复计算')
 try:
  if pause:pause()
  data=analyze(scenario.snapshot,scenario)
 except Exception as e:
  traceback.print_exc();return terminate(run_id,token,error=str(e))
 return terminate(run_id,token,result=data)

def execute(id,block=None,pause=None):
 """领取并计算。block 返回False表示模拟中断：不写回结果，租约保留到恢复扫描接管。"""
 token=claim(id)
 if not token:return False
 stop=threading.Event()
 def beat():
  while not stop.wait(HEARTBEAT_SECONDS):
   try:
    if not renew(id,token):stop.set();return
   except Exception:traceback.print_exc()
 threading.Thread(target=beat,daemon=True).start()
 try:
  if block is not None and not block():return False
  return _compute(id,token,pause)
 except Exception:
  traceback.print_exc();return terminate(id,token,error='worker 执行异常，可重试')
 finally:
  stop.set()

def _requeue(run,db):
 if run.status=='running':
  # 中断：旧租约作废并计入恢复次数，claim_token 清空使任何迟到回写都失效。
  run.recovery_count=(run.recovery_count or 0)+1;run.claim_token=''
 # 新一轮派发窗口；未被领取则下轮扫描再次投递。
 run.lease_until=later(DISPATCH_SECONDS)
 db.commit()
 return enqueue(client,run.id)

def recover(db):
 """恢复扫描：租约过期的 running 与派发窗口已过且未被领取的 queued 重新入队。"""
 requeued=[]
 for run in db.scalars(select(ScheduleRun).where(ScheduleRun.active_key.is_not(None),ScheduleRun.status.in_(('queued','running')))):
  if not expired(run.lease_until):continue
  if _requeue(run,db):requeued.append(run.id)
 return requeued

def run_forever(heartbeat_path=heartbeat):
 while True:
  heartbeat_path.touch()
  try:
   with Session() as db:recover(db)
  except Exception:traceback.print_exc()
  job=client.brpop(QUEUE_KEY,timeout=1)
  if job:execute(job[1].decode())

def main():run_forever()

if __name__=='__main__':main()
