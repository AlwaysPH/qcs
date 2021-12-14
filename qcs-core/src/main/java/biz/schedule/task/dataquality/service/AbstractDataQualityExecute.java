package biz.schedule.task.dataquality.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Lists;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.RetryUtils;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.properties.SparkLivyProperties;
import com.gwi.qcs.core.biz.schedule.QuartzJobManager;
import com.gwi.qcs.core.biz.schedule.task.dataquality.function.FunctionFactory;
import com.gwi.qcs.core.biz.service.SparkLivyService;
import com.gwi.qcs.core.biz.service.TaskService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetFiledService;
import com.gwi.qcs.core.biz.service.dao.mysql.SqlExecHistoryService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.core.biz.service.impl.SparkLivyServiceImpl;
import com.gwi.qcs.livy.bean.YarnState;
import com.gwi.qcs.model.domain.mysql.DatasetField;
import com.gwi.qcs.model.domain.mysql.SqlExecHistory;
import com.gwi.qcs.model.domain.mysql.Task;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import com.gwi.qcs.model.entity.DataSourceDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.gwi.qcs.core.biz.schedule.task.DataQualityJobAbstract.QCS_JOB_GROUP;

/**
 * 质控任务执行器抽象类
 *
 * @author ljl
 * @date 2021/2/2 14:49
 **/
@Slf4j
public abstract class AbstractDataQualityExecute implements DataQualityExecute {

    public static final String DATA_STARTTIME = "conditions.value.0";
    public static final String DATA_ENDTIME = "conditions.value.1";

    /**
     * 数据库
     */
    protected String schema;
    /**
     * 标准版本ID
     */
    protected String standardId;

    protected JobDefinition jobDefinition;
    protected DateTime startDate;
    protected DateTime endDate;

    /**
     * 任务的所有日期
     */
    protected List<DateTime> execDates = Lists.newArrayList();

    private SparkLivyProperties properties;

    private DateTime sparkStartTime;

    @Override
    public void executeJob(JobDefinition jobDefinition) throws JobExecutionException {
        long jobStartTime = System.currentTimeMillis();

        this.properties = SpringContextUtil.getBean(SparkLivyProperties.class);
        this.jobDefinition = jobDefinition;

        this.startDate = DateUtil.parseDate(jobDefinition.getExtras().get(DATA_STARTTIME));

        //临时任务才有endDate
        if (!jobDefinition.isUnbounded()) {
            this.endDate = DateUtil.parseDate(jobDefinition.getExtras().get(DATA_ENDTIME));
        }

        this.schema = jobDefinition.getInput().getSettings().get(DataSourceDescriptor.SCHEMA);
        this.standardId = jobDefinition.getExtras().get(JobDefinition.STANDARD_ID);

        if (!checkJobTime(jobDefinition)) {
            log.error("校验质控任务时间失败，Job信息:{}", jobDefinition);
            return;
        }

        //调用子类实现
        boolean finish = execute();

        if (finish) {
            log.info("质控完成，任务ID：{}，日期：{}，总耗时:[{}毫秒]", jobDefinition.getJobId(),
                    execDates.toString(), ((System.currentTimeMillis() - jobStartTime)));
        }
    }

    /**
     * 任务执行
     *
     * @throws JobExecutionException 执行异常
     */
    protected abstract boolean execute() throws JobExecutionException;

    /**
     * 调用spark模块
     *
     * @return 是否继续执行，false 结束任务
     */
    protected boolean sparkBridge(DateTime cycleDay) throws JobExecutionException {
        boolean result = context(SparkLivyService.class).startJob(String.valueOf(jobDefinition.getJobId()), cycleDay.toDateStr(),
                jobDefinition.isUnbounded() ? CommonConstant.TASK_CONTINUE_TYPE : CommonConstant.TASK_TEMP_TYPE, standardId);
        if (!result) {
            log.error("启动spark失败：jobId :{}, cycleDay:{}", jobDefinition.getJobId(), cycleDay);
            //启动失败，结束任务
            throw new JobExecutionException("启动spark失败");
        } else {
            return queryJobStatusOnSpark(cycleDay);
        }
    }

    /**
     * 查看Spark任务执行情况
     *
     * @return 是否继续执行，false 结束任务
     */
    protected boolean queryJobStatusOnSpark(DateTime cycleDay) throws JobExecutionException {
        sparkStartTime = new DateTime();
        for (; ; ) {
            //休眠1分钟，在查表，spark执行情况
            ThreadUtil.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));

            //如果前端取消掉整个质控任务，结束当前轮训，结束线程
            Task task = new Task();
            task.setId(String.valueOf(jobDefinition.getJobId()));
            task.setBizType(CommonConstant.TASK_QUALITY);
            Task currentTask = taskService().queryTaskById(task);

            EnumConstant.TaskStatus taskStatus = EnumConstant.TaskStatus.getEnum(Integer.parseInt(currentTask.getStatus()));
            switch (taskStatus) {
                case EXEC_FAILED:
                case EXEC_CANCEL:
                    log.info("任务被前端结束，结束查询任务轮训，任务ID：{}，日期：{}~{}，任务状态：{}", jobDefinition.getJobId(),
                            CollUtil.getFirst(execDates), CollUtil.getLast(execDates), taskStatus.getName());
                    updateTaskStatus(taskStatus.getValue());
                    return false;
                case EXEC_PAUSE:
                    log.info("任务被暂停，结束查询任务轮训，任务ID：{}，日期：{}~{}，任务状态：{}", jobDefinition.getJobId(),
                            CollUtil.getFirst(execDates), CollUtil.getLast(execDates), taskStatus.getName());
                    return false;
                default:
                    break;
            }

            //根据任务ID和标准版本查询
            QueryWrapper<TaskProgress> wrapper = new QueryWrapper<>();
            wrapper.eq("TASK_ID", jobDefinition.getJobId());
            wrapper.eq("STANDARD_ID", standardId);
            wrapper.eq("CYCLE_DAY", cycleDay);
            List<TaskProgress> taskProgresses = taskProgressService().list(wrapper);

            for (TaskProgress progress : taskProgresses) {
                EnumConstant.TaskStatus status = EnumConstant.TaskStatus.getEnum(progress.getStatus());
                switch (status) {
                    case NOT_EXEC:
                    case EXECUTING:
                    case EXEC_PAUSE:
                        log.info("查询Spark任务执行情况，任务ID：{}，日期：{}~{}，状态：{}", jobDefinition.getJobId(),
                                CollUtil.getFirst(execDates), CollUtil.getLast(execDates), status.getName());

                        //增加两层校验任务状态，如果任务还是执行中场景下，查询livy接口，判断任务最终状态。
                        YarnState taskYarnState = context(SparkLivyService.class).status(progress.getSparkTaskId());
                        if (taskYarnState != null && YarnState.isFail(taskYarnState)) {
                            log.info("查询Livy接口,任务失败，结束当前流程，停止任务。任务ID：{}", jobDefinition.getJobId());
                            updateTaskStatus(EnumConstant.TaskStatus.EXEC_FAILED.getValue());
                            return false;
                        }

                        break;
                    case EXEC_SUCCEED:
                        log.info("任务ID：{}，完成{}质控任务", jobDefinition.getJobId(), cycleDay);
                        return true;
                    default:
                        log.info("结束查询，任务ID：{}，日期：{}~{}，状态：{}", jobDefinition.getJobId(),
                                CollUtil.getFirst(execDates), CollUtil.getLast(execDates), status.getName());
                        updateTaskStatus(taskStatus.getValue());
                        return false;
                }
            }

            //超过spark任务生命周期时间，结束任务
            if (DateUtil.between(sparkStartTime, DateUtil.date(), DateUnit.HOUR) >= properties.getLifeCycle()) {
                Optional<String> sparkTaskId = taskProgresses.stream().map(TaskProgress::getSparkTaskId).findAny();

                sparkTaskId.ifPresent(id -> RetryUtils.retryEveryOnException(properties.getRetry(),
                        () -> new SparkLivyServiceImpl().killJob(id), result -> result));

                //执行超时，任务失败
                updateTaskStatus(EnumConstant.TaskStatus.EXEC_FAILED.getValue());
                return false;
            }
        }
    }

    public static <T> T context(Class<T> requiredType) {
        return SpringContextUtil.getBean(requiredType);
    }

    /**
     * 更新任务状态
     *
     * @param status 任务状态
     * @throws JobExecutionException
     */
    private void updateTaskStatus(int status) throws JobExecutionException {
        try {
            QuartzJobManager.getInstance().updateTaskStatusAndCancelJob(jobDefinition.getJobId().toString(), QCS_JOB_GROUP,
                    jobDefinition.getJobId(), status);
        } catch (Exception e) {
            throw new JobExecutionException(e);
        }
    }

    /**
     * 构造质控进度表实体
     *
     * @param execDate 日期
     * @return 进度表实体对象
     */
    protected TaskProgress buildDqTaskProgress(DateTime execDate) {
        TaskProgress progress = new TaskProgress();
        progress.setStatus(EnumConstant.TaskStatus.NOT_EXEC.getValue());
        progress.setTaskId(jobDefinition.getJobId());
        progress.setJobType(CommonConstant.TASK_NEW_ORG_TYPE.equals(jobDefinition.getTaskType())
                ? DqTaskConstant.Task.ORG_TEMP_DQ : DqTaskConstant.Task.DQ);
        progress.setStandardId(standardId);
        progress.setCycleDay(execDate);
        return progress;
    }

    /**
     * 构造执行校验SQL
     *
     * @param execDate      校验日期
     * @param jobDefinition 任务信息
     */
    protected List<SqlExecHistory> buildSqlOfExecData(JobDefinition jobDefinition, DateTime execDate) {
        String standardName = jobDefinition.getExtras().get(JobDefinition.STANDARD_NAME);

        List<SqlExecHistory> sqlList = Lists.newArrayList();
        jobDefinition.getDatasets().forEach(dataset -> {
            dataset.getEvaluations().forEach(rule -> {
                try {
                    String ruleSql = new FunctionFactory(rule, dataset, jobDefinition, execDate).getFunctionSql();

                    SqlExecHistory sqlExecHistory = new SqlExecHistory();
                    sqlExecHistory.setTaskId(jobDefinition.getJobId());
                    sqlExecHistory.setDatasetId(dataset.getId());
                    sqlExecHistory.setDatasetCode(dataset.getCode());
                    sqlExecHistory.setRuleId(rule.getRuleId());
                    sqlExecHistory.setRuleName(rule.getRuleName());
                    sqlExecHistory.setCategoryId(rule.getCategoryId());
                    sqlExecHistory.setCategoryName(rule.getCategoryName());
                    sqlExecHistory.setDatasetDatabases(schema);

                    sqlExecHistory.setStatus(EnumConstant.TaskStatus.NOT_EXEC.getValue());
                    sqlExecHistory.setCycleDay(execDate);
                    sqlExecHistory.setExecSql(ruleSql);

                    // 如果是规则已经合并执行，无法确定字段属性，这里不进行设置，在spark模块中去获取
                    if (!rule.isMergeSql() && rule.getDataSetItemId() != null) {
                        DatasetField field = context(DatasetFiledService.class).getByField(rule.getDataSetItemId());
                        if (field != null) {
                            sqlExecHistory.setDatasetItemId(rule.getDataSetItemId());
                            sqlExecHistory.setDatasetItemCode(field.getFieldName());
                        }
                    }
                    sqlExecHistory.setItemType(rule.isMergeSql() ? DqTaskConstant.ExecHistory.MERGE_SQL : DqTaskConstant.ExecHistory.IN_TABLE);

                    sqlExecHistory.setStandardId(standardId);
                    sqlExecHistory.setStandardName(standardName);
                    sqlExecHistory.setCreateAt(DateTime.now());
                    sqlList.add(sqlExecHistory);
                } catch (Exception e) {
                    log.error("生成规则SQL失败：", e);
                }
            });
        });
        return sqlList;
    }

    /**
     * 是否执行（检查任务时间范围）
     *
     * @param jobDefinition 任务信息
     * @throws JobExecutionException
     */
    private Boolean checkJobTime(JobDefinition jobDefinition) throws JobExecutionException {

        //先判断数据采集时间是否大于T-1
        if (DateUtil.yesterday().getTime() < startDate.getTime()) {
            log.error("数据采集开始时间大于等于T-1，数据无法进行采集操作");
            return false;
        }

        //任务执行到哪天
        TaskProgress execDateProgress = taskProgressService().getBaseMapper().selectLastCycleDayByTaskId(jobDefinition.getJobId());

        //没有查到执行记录，可执行
        if (execDateProgress == null) {
            return true;
        }

        //当前执行日期大于结束日期
        if (endDate != null && execDateProgress.getCycleDay().getTime() > endDate.getTime()) {
            log.info(String.format("数据采集时间范围已完成[%s-%s]，不进行采集操作！", startDate.toDateStr(), endDate.toDateStr()));
            if (!jobDefinition.isUnbounded()) {
                updateTaskStatus(EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
            }
            return false;
        }

        //判断是否是持续任务并且执行时间是否已到达T-1
        if (DateUtil.yesterday().getTime() <= execDateProgress.getCycleDay().getTime()) {
            log.error("数据采集时间已经到今天，不进行采集操作！");
            if (!jobDefinition.isUnbounded()) {
                updateTaskStatus(EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
            }
            return false;
        }

        return true;
    }

    /**
     * 获取任务进度表操作service
     *
     * @return TaskProgressService
     */
    protected TaskProgressService taskProgressService() {
        return SpringContextUtil.getBean(TaskProgressService.class);
    }

    /**
     * 获取任务操作service
     *
     * @return TaskProgressService
     */
    protected TaskService taskService() {
        return SpringContextUtil.getBean(TaskService.class);
    }

    /**
     * 获取SQL执行进度表操作service
     *
     * @return SqlExecHistoryService
     */
    protected SqlExecHistoryService sqlExecHistoryService() {
        return SpringContextUtil.getBean(SqlExecHistoryService.class);
    }

}
