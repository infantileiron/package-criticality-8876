"""页面字段与后端归属信号的契约测试（不依赖浏览器）。"""
import os
ROOT=os.environ.get('REPO_ROOT','/workspace')

def read(path):return open(os.path.join(ROOT,path),encoding='utf-8').read()

def test_results_page_exposes_attempts_recovery_and_failure():
 html=read('frontend/src/pages/Results.vue'),read('frontend/src/components/RunAttempts.vue')
 joined=''.join(html)
 for token in ('attemptLabel','recoveryLabel','failureReason','从失败记录重试','失败原因'):
  assert token in joined,token
 assert 'RunAttempts' in read('frontend/src/pages/Results.vue')
 assert 'export const retry' in read('frontend/src/api/runs.ts')

def test_results_page_does_not_tell_users_to_create_new_scenario():
 page=read('frontend/src/pages/Results.vue')
 assert '调整输入后新建情景重试' not in page
 assert "run.status==='failed'" in page

def test_enums_and_types_cover_attempt_metadata():
 assert "retry_of" in read('frontend/src/types/domain.ts')
 assert "recovery_count" in read('frontend/src/types/domain.ts')
 assert 'failed' in read('frontend/src/constants/enums.ts')
