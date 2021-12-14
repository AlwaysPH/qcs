package biz.schedule;

import cn.hutool.core.date.DateUtil;
import com.gwi.qcs.core.biz.service.dao.mysql.ParameterService;
import com.gwi.qcs.model.domain.mysql.Task;
import com.gwi.qcs.model.mapper.mysql.TaskMapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

@Slf4j
@Component
public class QuartzJobManager {

    /**
     * 持续任务
     */
    private static final String TASK_CONTINUE_TYPE = "0";

    private static QuartzJobManager jobManager;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    private ParameterService parameterService;

    private final SchedulerListener schedulerListener = new SchedulerListener();

    public QuartzJobManager() {
        jobManager = this;
    }

    public static QuartzJobManager getInstance() {
        return QuartzJobManager.jobManager;
    }

    @PostConstruct
    private void init() {
        try{
            log.info("...............初始化程序定时任务...............");
            parameterService.initSchedule();
        }catch (Exception e){
            log.error("初始化定时任务失败： ", e);
        }
    }

    /**
     * 创建job
     *
     * @param clazz          任务类
     * @param jobName        任务名称
     * @param jobGroupName   任务所在组名称
     * @param cronExpression cron表达式
     * @throws Exception
     */
    public void addJob(Class clazz, String jobName, String jobGroupName, String cronExpression) throws Exception {

        // 启动调度器
        scheduler.start();

        //构建job信息
        JobDetail jobDetail = JobBuilder.newJob(((AbstractQcsJob) clazz.newInstance()).getClass()).withIdentity(jobName, jobGroupName).build();

        //表达式调度构建器(即任务执行的时间)
        CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

        //按新的cronExpression表达式构建一个新的trigger
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroupName)
                .withSchedule(scheduleBuilder).build();
        scheduler.scheduleJob(jobDetail, trigger);
        schedulerListener.jobAdded(jobDetail);
    }

    /**
     * 创建job，可传参
     *
     * @param clazz          任务类
     * @param task        任务
     * @param jobGroupName   任务所在组名称
     * @param cronExpression cron表达式
     * @param argMap         map形式参数
     * @throws Exception
     */
    public void addJob(Class clazz, Task task, String jobGroupName, String cronExpression, Map<String, Object> argMap) throws Exception {
        String jobName = task.getId();

        // 启动调度器
        scheduler.start();

        //构建job信息
        JobDetail jobDetail = JobBuilder.newJob(((AbstractJobAbstract) clazz.newInstance()).getClass()).withIdentity(jobName, jobGroupName).build();

        //获得JobDataMap，写入数据
        jobDetail.getJobDataMap().putAll(argMap);
        Trigger trigger;
        if(TASK_CONTINUE_TYPE.equals(task.getTaskType())){
            //持续任务按cron表达式执行
            CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
            trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroupName)
                .withSchedule(scheduleBuilder).build();
        }else{
            trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroupName)
                .startAt(new Date()).build();
        }

        scheduler.scheduleJob(jobDetail, trigger);
        schedulerListener.jobAdded(jobDetail);
    }

    /**
     * 创建job并立即执行
     *
     * @param clazz        任务类
     * @param jobName      任务名称
     * @param jobGroupName 任务所在组名称
     * @param argMap       map形式参数
     * @throws Exception
     */
    public void addAndRunNowJob(Class clazz, String jobName, String jobGroupName, Map<String, Object> argMap) throws Exception {

        // 启动调度器
        scheduler.start();

        //构建job信息
        JobDetail jobDetail = JobBuilder.newJob(((AbstractJobAbstract) clazz.newInstance()).getClass()).withIdentity(jobName, jobGroupName).build();

        //获得JobDataMap，写入数据
        jobDetail.getJobDataMap().putAll(argMap);

        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroupName).startNow().build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

    /**
     * 暂停job
     *
     * @param jobName      任务名称
     * @param jobGroupName 任务所在组名称
     * @throws SchedulerException
     */
    public void pauseJob(String jobName, String jobGroupName) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobName, jobGroupName);
        scheduler.pauseJob(jobKey);
        schedulerListener.jobPaused(jobKey);
    }

    /**
     * 恢复job
     *
     * @param jobName      任务名称
     * @param jobGroupName 任务所在组名称
     * @throws SchedulerException
     */
    public void resumeJob(String jobName, String jobGroupName) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobName, jobGroupName);
        scheduler.resumeJob(jobKey);
        schedulerListener.jobResumed(jobKey);
    }

    /***
     * 立即执行某个job
     * @param jobName
     * @param jobGroupName
     * @throws SchedulerException
     */
    public void runJobNow(String jobName, String jobGroupName) throws SchedulerException {
        scheduler.triggerJob(JobKey.jobKey(jobName, jobGroupName));
    }

    /**
     * job 更新,只更新频率
     *
     * @param jobName        任务名称
     * @param jobGroupName   任务所在组名称
     * @param cronExpression cron表达式
     * @throws Exception
     */
    public void updateJob(String jobName, String jobGroupName, String cronExpression) throws Exception {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);
        // 表达式调度构建器
        CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

        // 按新的cronExpression表达式重新构建trigger
        trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();

        // 按新的trigger重新设置job执行
        scheduler.rescheduleJob(triggerKey, trigger);

    }

    /**
     * job 更新,更新频率和参数
     *
     * @param jobName        任务名称
     * @param jobGroupName   任务所在组名称
     * @param cronExpression cron表达式
     * @param argMap         参数
     * @throws Exception
     */
    public void updateJob(String jobName, String jobGroupName, String cronExpression, Map<String, Object> argMap) throws Exception {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);
        // 表达式调度构建器
        CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

        // 按新的cronExpression表达式重新构建trigger
        trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();

        //修改map
        trigger.getJobDataMap().putAll(argMap);

        // 按新的trigger重新设置job执行
        scheduler.rescheduleJob(triggerKey, trigger);

    }

    /**
     * job 更新,只更新更新参数
     *
     * @param jobName      任务名称
     * @param jobGroupName 任务所在组名称
     * @param argMap       参数
     * @throws Exception
     */
    public void updateJob(String jobName, String jobGroupName, Map<String, Object> argMap) throws Exception {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);

        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

        //修改map
        trigger.getJobDataMap().putAll(argMap);

        // 按新的trigger重新设置job执行
        scheduler.rescheduleJob(triggerKey, trigger);

    }


    /**
     * job 删除
     *
     * @param jobName      任务名称
     * @param jobGroupName 任务所在组名称
     * @throws Exception
     */
    public void deleteJob(String jobName, String jobGroupName) throws Exception {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);
        if(scheduler.checkExists(triggerKey)){
            JobKey jobKey = JobKey.jobKey(jobName, jobGroupName);
            scheduler.deleteJob(jobKey);
            schedulerListener.jobDeleted(jobKey);
        }
    }

    /**
     * 启动所有定时任务
     */
    public void startAllJobs() {
        try {
            scheduler.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭所有定时任务
     */
    public void shutdownAllJobs() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * 判断任务是否存在
     * @param triggerName
     * @param triggerGroupName
     * @return
     */
    public Boolean notExists(String triggerName, String triggerGroupName) {
        try {
            return scheduler.getTriggerState(TriggerKey.triggerKey(triggerName, triggerGroupName)) == Trigger.TriggerState.NONE;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 获取所有任务列表
     *
     * @return
     * @throws SchedulerException
     */
    public List<Map<String, Object>> getAllJob() throws SchedulerException {
        GroupMatcher<JobKey> matcher = GroupMatcher.anyJobGroup();
        Set<JobKey> jobKeys = scheduler.getJobKeys(matcher);
        List<Map<String, Object>> jobList = new ArrayList<>();
        for (JobKey jobKey : jobKeys) {
            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            for (Trigger trigger : triggers) {
                Map<String, Object> job = new HashMap<>();
                job.put("jobName", jobKey.getName());
                job.put("jobGroupName", jobKey.getGroup());
                job.put("trigger", trigger.getKey());
                Trigger.TriggerState triggerState = scheduler.getTriggerState(trigger.getKey());
                job.put("jobStatus", triggerState.name());
                if (trigger instanceof CronTrigger) {
                    CronTrigger cronTrigger = (CronTrigger) trigger;
                    String cronExpression = cronTrigger.getCronExpression();
                    job.put("cronExpression", cronExpression);
                }
                jobList.add(job);
            }
        }
        return jobList;
    }

    /***
     * 任务执行完成之后删除任务，并修改任务状态
     * @param jobName
     * @param jobGroupName
     * @param jobId
     * @throws Exception
     */
    public void updateTaskStatusAndCancelJob(String jobName, String jobGroupName, Long jobId, int jobStatus) throws Exception {
        deleteJob(jobName, jobGroupName);
        Task task = new Task();
        task.setStatus(String.valueOf(jobStatus));
        task.setId(String.valueOf(jobId));
        task.setTaskEndtime(DateUtil.now());
        taskMapper.updateById(task);
    }

    /***
     * 中断正在执行的任务
     * @param jobName
     * @param jobGroupName
     * @throws UnableToInterruptJobException
     */
    public void interruptJob(String jobName, String jobGroupName) throws UnableToInterruptJobException {
        scheduler.interrupt(JobKey.jobKey(jobName, jobGroupName));
    }
}
