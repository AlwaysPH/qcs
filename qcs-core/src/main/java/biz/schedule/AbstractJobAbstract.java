package biz.schedule;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.core.biz.service.TaskService;
import com.gwi.qcs.model.domain.mysql.Task;
import com.gwi.qcs.model.entity.JobDefinition;
import com.gwi.qcs.model.entity.RedisSaveEntiy;
import com.gwi.qcs.model.entity.ScoreJobDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractJobAbstract extends AbstractQcsJob {

    private static final String SCORE_TASK_DATA = "scoreTaskData";

    private static final String REDIS_RUNNING_TIME = "running_time_";

    private static final String REDIS_GROUP = "dataquality:";

    /**
     * 质控任务分组名称
     */
    public static final String QCS_JOB_GROUP = "qcsTask";

    /**
     * 评分任务分组名称
     */
    public static final String SCORE_JOB_GROUP = "scoreTask";

    /**
     * 数据集线程池长度
     */
    @Value("${data-quality.thread-pool.dataset-pool-size}")
    protected Integer datasetThreadpoolSize;
    /**
     * 规则线程池长度
     */
    @Value("${data-quality.thread-pool.rule-pool-size}")
    protected Integer ruleThreadpoolSize;

    /**
     * 评分规则线程池长度
     */
    @Value("${data-quality.thread-pool.score_rule-pool-size}")
    protected Integer scoreRuleThreadpoolSize;

    /**
     * 批量查询最大查询数量
     */
    @Value("${data-quality.db-opeartion.max-query-size}")
    protected Integer maxRuleQuerySize;
    /**
     * ES插入批次数
     */
    @Value("${data-quality.db-opeartion.max-es-exec-size}")
    protected Integer maxRuleEsExecSize;

    protected RedisRepository redisRepository;

    @Autowired
    private TaskService taskService;

    @Autowired
    public void setRedisRepository(RedisRepository redisRepository) {
        this.redisRepository = redisRepository;
    }

    protected final void executeJob(JobExecutionContext context) throws JobExecutionException {
        try {
            this.executeTaskJob(context);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            //异常设置任务失败状态，并删除quartz任务
            setTaskStatusException(context);
            //redis设置质控任务失败数据
            setRedisFailData(context);
        } finally {
            //临时任务跑完设置任务结束时间
            setTempTaskEndTime(context);
        }
    }

    private void setRedisFailData(JobExecutionContext context) {
        JobDefinition jobDefinition = JSON.parseObject(JSON.toJSONString(context.getJobDetail().getJobDataMap()), JobDefinition.class);
        //质控任务
        if (null != jobDefinition.getJobId()) {
            Map<String, String> map = jobDefinition.getExtras();
            String standardId = map.get(JobDefinition.STANDARD_ID);
            String jobId = String.valueOf(jobDefinition.getJobId());
            RedisSaveEntiy entiy = getRedisEntity(standardId, jobId);
            if (null != entiy) {
                entiy.setSuccess(false);
                setRedisEntity(standardId, jobId, entiy);
            }
        }
    }

    private void setRedisEntity(String standardId, String jobId, RedisSaveEntiy entiy) {
        redisRepository.set(REDIS_GROUP + standardId + "_" + jobId, entiy);
    }

    private RedisSaveEntiy getRedisEntity(String standardId, String jobId) {
        return redisRepository.get(REDIS_GROUP + standardId + "_" + jobId) == null ? null : (RedisSaveEntiy) redisRepository.get(REDIS_GROUP + standardId + "_" + jobId);
    }


    private void setTaskStatusException(JobExecutionContext context) throws JobExecutionException {
        String jobId = "";
        JobDefinition jobDefinition = JSON.parseObject(JSON.toJSONString(context.getJobDetail().getJobDataMap()), JobDefinition.class);
        try {
            if (null == jobDefinition.getJobId()) {
                //评分任务获取数据
                JobDetail detail = context.getJobDetail();
                JobDataMap dataMap = detail.getJobDataMap();
                List<ScoreJobDefinition> scoreJobList = (List<ScoreJobDefinition>) dataMap.get(SCORE_TASK_DATA);
                ScoreJobDefinition scoreJobDefinition = scoreJobList.get(0);
                jobId = scoreJobDefinition.getScoreTaskId();
                //取消任务
                QuartzJobManager.getInstance().deleteJob(jobId, SCORE_JOB_GROUP);
                //中断正在执行的任务
                QuartzJobManager.getInstance().interruptJob(jobId, SCORE_JOB_GROUP);
            } else {
                //质控任务
                jobId = String.valueOf(jobDefinition.getJobId());
                //取消任务
                QuartzJobManager.getInstance().deleteJob(jobId, QCS_JOB_GROUP);
                //中断正在执行的任务
                QuartzJobManager.getInstance().interruptJob(jobId, QCS_JOB_GROUP);
            }
            Task task = new Task();
            task.setStatus(String.valueOf(EnumConstant.TaskStatus.EXEC_FAILED.getValue()));
            task.setTaskEndtime(DateUtil.now());
            task.setId(jobId);
            taskService.updateById(task);
        } catch (Exception e) {
            throw new JobExecutionException(e);
        }
    }

    private void setTempTaskEndTime(JobExecutionContext context) {
        String jobId = "";
        JobDefinition jobDefinition = JSON.parseObject(JSON.toJSONString(context.getJobDetail().getJobDataMap()), JobDefinition.class);
        if (null == jobDefinition.getJobId()) {
            //评分任务获取数据
            JobDetail detail = context.getJobDetail();
            JobDataMap dataMap = detail.getJobDataMap();
            List<ScoreJobDefinition> scoreJobList = (List<ScoreJobDefinition>) dataMap.get(SCORE_TASK_DATA);
            ScoreJobDefinition scoreJobDefinition = scoreJobList.get(0);
            jobId = scoreJobDefinition.getScoreTaskId();
            //评分任务通过redis存储当前任务执行完成时间比较任务结束时间，判断该任务是否挂起，从而是否执行修改任务结束时间操作
            if (!scoreJobDefinition.isUnbounded()) {
                setTaskEndTime(jobId);
            }
        }
    }

    private void setTaskEndTime(String jobId) {
        Task task = new Task();
        task.setTaskEndtime(DateUtil.now());
        task.setId(jobId);
        taskService.updateById(task);
    }

    /**
     * 执行任务抽象方法
     *
     * @param context Job信息
     * @throws JobExecutionException
     */
    protected abstract void executeTaskJob(JobExecutionContext context) throws JobExecutionException;
}
