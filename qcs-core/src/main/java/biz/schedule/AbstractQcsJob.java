package biz.schedule;

import com.greatwall.component.ccyl.redis.template.RedisRepository;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

@Slf4j
public abstract class AbstractQcsJob extends QuartzJobBean {

    private static final String JOB_STATUS_KEY_PREFIX = "JOB_STATUS_";

    private static final String JOB_STATUS_RUNNING = "RUNNING";

    private static final String JOB_STATUS_STOPPED = "STOPPED";

    @Autowired
    protected RedisRepository redisRepository;

    protected abstract void executeJob(JobExecutionContext context) throws Exception;

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException{
        String statusKey = JOB_STATUS_KEY_PREFIX + jobExecutionContext.getJobDetail().getKey();
        String jobStatus = (String) redisRepository.get(statusKey);
        log.info("任务:{}, 当前状态:{}", statusKey, jobStatus);
        if (JOB_STATUS_RUNNING.equals(jobStatus)) {
            log.info("任务:{}, 正在运行中，忽略本次调度...", statusKey);
            return;
        }
        redisRepository.set(statusKey, JOB_STATUS_RUNNING);
        try{
            this.executeJob(jobExecutionContext);
        }catch (Exception e){
            log.error("执行任务失败： ", e);
        }finally {
            redisRepository.del(statusKey);
            log.info("任务:{}, 执行结束", statusKey);
        }
    };
}
