import os,sys,tempfile
sys.path.insert(0,os.environ.get('BACKEND_ROOT','/workspace/backend'))
_DB=tempfile.NamedTemporaryFile(suffix='.sqlite3',delete=False).name
os.environ.setdefault('DATABASE_URL','sqlite+pysqlite:///'+_DB)
os.environ.setdefault('REDIS_URL','redis://localhost:6379/15')
os.environ.setdefault('SEED_DEMO','false')
import pytest
from src.config.database import Base,engine,Session
from src.models import User,WorkPackage,TaskDependency,DurationScenario,ScheduleRun,DeliveryReview,Report
from src.utils.security import hash_password
from src.utils.time import now

@pytest.fixture(autouse=True)
def fresh_db():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
 yield
 Base.metadata.drop_all(engine)

@pytest.fixture
def db():
 with Session() as session:yield session

@pytest.fixture
def scenario(db):
 tasks=[{'task_id':'T01','name':'交接','a':1.,'m':2.,'b':3.},{'task_id':'T02','name':'加固','a':2.,'m':4.,'b':6.}]
 pkg=WorkPackage(id='p1',name='模拟包',source='sim.json',tasks=tasks,created_at=now());db.add(pkg);db.flush()
 db.add(TaskDependency(id='e1',package_id='p1',predecessor='T01',successor='T02',created_at=now()));db.flush()
 snap={'package':{'id':'p1','name':'模拟包','source':'sim.json','tasks':tasks,'created_at':now()},'dependencies':[{'id':'e1','package_id':'p1','predecessor':'T01','successor':'T02','created_at':now()}]}
 item=DurationScenario(id='s1',name='情景一',package_id='p1',version=1,deadline=10.,samples=120,seed=7,snapshot=snap,created_at=now());db.add(item);db.commit()
 return item
