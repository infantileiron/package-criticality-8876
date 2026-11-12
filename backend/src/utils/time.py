from datetime import datetime,timezone,timedelta
def now(): return datetime.now(timezone.utc).isoformat()
# 固定宽度UTC时间戳；租约与恢复窗口按同格式字符串比较。
def stamp(value=None): return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
def later(seconds): return stamp(datetime.now(timezone.utc)+timedelta(seconds=seconds))
def expired(value,current=None): return value is None or value<(current or stamp())
