from typing import Optional
from sqlalchemy import String,Integer,JSON,ForeignKey,Text
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class ScheduleRun(Base):
 __tablename__='schedule_runs'
 id:Mapped[str]=mapped_column(String(80),primary_key=True)
 scenario_id:Mapped[str]=mapped_column(ForeignKey('duration_scenarios.id'))
 status:Mapped[str]=mapped_column(String(32))
 result:Mapped[dict]=mapped_column(JSON,default=dict)
 error:Mapped[str]=mapped_column(Text,default='')
 created_at:Mapped[str]=mapped_column(String(64))
 # 同一冻结情景只允许一个活动运行（queued/running）；完成或失败后置空，释放唯一键。
 active_key:Mapped[Optional[str]]=mapped_column(String(80),unique=True,nullable=True,default=None)
 attempt_count:Mapped[int]=mapped_column(Integer,nullable=False,default=1,server_default='1')
 recovery_count:Mapped[int]=mapped_column(Integer,nullable=False,default=0,server_default='0')
 retry_of:Mapped[Optional[str]]=mapped_column(String(80),nullable=True,default=None)
 claim_token:Mapped[str]=mapped_column(String(120),nullable=False,default='',server_default='')
 lease_until:Mapped[Optional[str]]=mapped_column(String(64),nullable=True,default=None)
 started_at:Mapped[Optional[str]]=mapped_column(String(64),nullable=True,default=None)
 finished_at:Mapped[Optional[str]]=mapped_column(String(64),nullable=True,default=None)
