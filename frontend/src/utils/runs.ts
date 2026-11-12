import type {Run} from '../types/domain';
export function attemptLabel(run:Run){return `尝试 ${run.attempt_count||1}${run.retry_of?' · 第2次起为重试':' · 首次'}`}
export function recoveryLabel(run:Run){
 if(run.recovery_count>0)return `已中断恢复 ${run.recovery_count} 次`
 if(run.status==='running')return '执行中 · 心跳续租'
 if(run.status==='queued')return '等待worker领取 · 中断后自动恢复'
 if(run.status==='completed'||run.status==='failed')return '未发生中断恢复'
 return '未发生中断恢复'
}
export function failureReason(run:Run){return run.status==='failed'?(run.error||'后台计算失败，原因未记录'):''}
export function canRetry(run:Run){return run.status==='failed'}
