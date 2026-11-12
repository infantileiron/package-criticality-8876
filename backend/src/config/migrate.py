"""增量表结构修复：已存在的数据库卷升级到“单一活动运行”字段。

只做加法：新增列、回填活动运行标记并建立唯一索引，不删除历史运行。
同一情景若已有多个活动运行，保留最早者继续执行，其余标记失败并保留原因。
"""
from sqlalchemy import inspect,text
from src.config.database import engine

RUN_COLUMNS={
 'active_key':'VARCHAR(80) NULL',
 'attempt_count':'INTEGER NOT NULL DEFAULT 1',
 'recovery_count':'INTEGER NOT NULL DEFAULT 0',
 'retry_of':'VARCHAR(80) NULL',
 'claim_token':"VARCHAR(120) NOT NULL DEFAULT ''",
 'lease_until':'VARCHAR(64) NULL',
 'started_at':'VARCHAR(64) NULL',
 'finished_at':'VARCHAR(64) NULL',
}
INDEX='uq_schedule_runs_active_key'

def ensure_schema():
 inspector=inspect(engine)
 if 'schedule_runs' not in inspector.get_table_names():return
 existing={c['name'] for c in inspector.get_columns('schedule_runs')}
 with engine.begin() as conn:
  for name,ddl in RUN_COLUMNS.items():
   if name not in existing:conn.execute(text(f'ALTER TABLE schedule_runs ADD COLUMN {name} {ddl}'))
  indexes={i['name'] for i in inspect(engine).get_indexes('schedule_runs')}
  covered=INDEX in indexes or any(set(c.get('column_names') or [])=={'active_key'} for c in inspect(engine).get_unique_constraints('schedule_runs'))
  if covered:return
  rows=conn.execute(text("SELECT id,scenario_id,created_at FROM schedule_runs WHERE status IN ('queued','running') ORDER BY scenario_id,created_at,id")).fetchall()
  kept=set()
  for run_id,scenario_id,created_at in rows:
   if scenario_id in kept:
    conn.execute(text("UPDATE schedule_runs SET status='failed',error='升级时发现同一情景的重复活动运行，保留最早记录',finished_at=COALESCE(finished_at,created_at),claim_token='',lease_until=NULL,recovery_count=COALESCE(recovery_count,0) WHERE id=:id"),{'id':run_id})
   else:
    kept.add(scenario_id)
    conn.execute(text('UPDATE schedule_runs SET active_key=:sid WHERE id=:id'),{'sid':scenario_id,'id':run_id})
  conn.execute(text(f'CREATE UNIQUE INDEX {INDEX} ON schedule_runs(active_key)'))
