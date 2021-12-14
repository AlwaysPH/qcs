package biz.schedule.task.score;

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.core.biz.schedule.AbstractJobAbstract;
import com.gwi.qcs.core.biz.schedule.QuartzJobManager;
import com.gwi.qcs.core.biz.schedule.task.JobExecutor;
import com.gwi.qcs.core.biz.schedule.task.score.callable.RuleScoreCallable;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.InstanceQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.RuleCategoryQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.model.domain.mysql.InstanceQuality;
import com.gwi.qcs.model.domain.mysql.ScoreInstanceRule;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import com.gwi.qcs.model.entity.ScoreEntity;
import com.gwi.qcs.model.entity.ScoreJobDefinition;
import com.gwi.qcs.model.entity.StandardDataSet;
import com.gwi.qcs.model.entity.StandardRuleEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 评分任务
 */
@Slf4j
@Component
public class ScoringJobAbstract extends AbstractJobAbstract implements InterruptableJob {

    private static final String SCORE_TASK_DATA = "scoreTaskData";

    private static final String REDIS_GROUP = "dataquality:";

    private final String DATASET_POOL_NAME = "TaskPool-for-jobId-[%s]";

    private static final Integer SCORE_INSTANCE_THREADPOOL_SIZE = 10;

    private static final String REDIS_RUNNING_TIME = "running_time_";

    /**
     * 评分任务分组名称
     */
    public static final String SCORE_JOB_GROUP = "scoreTask";

    /**
     * job 是否中断
     */
    private boolean interrupted = false;

    @Autowired
    TaskProgressService taskProgressService;

    @Autowired
    InstanceQualityService instanceQualityService;

    @Autowired
    RuleCategoryQualityService ruleCategoryQualityService;

    @Autowired
    DatasetQualityService datasetQualityService;

    @Override
    protected void executeTaskJob(JobExecutionContext context) throws JobExecutionException {
        JobDetail detail = context.getJobDetail();
        JobDataMap dataMap = detail.getJobDataMap();
        List<ScoreJobDefinition> scoreJobList = (List<ScoreJobDefinition>) dataMap.get(SCORE_TASK_DATA);

        if (CollUtil.isEmpty(scoreJobList)) {
            throw new JobExecutionException("not job definition");
        }

        //获取当前评分对象所有标准版本流水号
        List<ScoreEntity> standardList = getAllStandard(scoreJobList);

        //评分执行对象
        ScoreJobDefinition scoreJobDefinition = scoreJobList.get(0);
        Long jobId = Long.parseLong(scoreJobDefinition.getScoreTaskId());
        TaskProgress query = new TaskProgress();
        query.setTaskId(jobId);
        query.setJobType(DqTaskConstant.Task.SCORE);
        TaskProgress taskProgress = taskProgressService.getOne(new QueryWrapper<>(query));
        Date startDate;
        if(taskProgress == null){
            taskProgress = new TaskProgress();
            startDate = DateUtils.string2Date(scoreJobDefinition.getStartTime(), DateUtils.DEFINE_YYYY_MM_DD);
        }else{
            startDate = DateUtils.addDays(taskProgress.getCycleDay(), 1);
        }
        taskProgress.setJobType(DqTaskConstant.Task.SCORE);
        taskProgress.setStatus(EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
        taskProgress.setTaskId(jobId);

        //判断任务执行时间是否小于质控任务结束时间
        try{
            while (getTaskCompleted(standardList, DateUtils.date2String(startDate, DateUtils.DEFINE_YYYY_MM_DD), scoreJobDefinition)
                && !interrupted) {
                taskProgress.setCycleDay(startDate);
                String scoreJobStartTime = scoreJobDefinition.getStartTime();
                String resultDate = DateUtils.date2String(startDate, DateUtils.DEFINE_YYYY_MM_DD);
                for(ScoreEntity scoreEntity : standardList){
                    log.info("开始执行评分任务......" + scoreJobStartTime + " JobId:" + scoreEntity.getScoreTaskId() + ", instanceId:" + scoreEntity.getScoreInstance().getId());
                    // 删除评分数据
                    Long start = System.currentTimeMillis();
                    String scoreInstanceId = scoreEntity.getScoreInstance().getId().toString();
                    datasetQualityService.del(scoreInstanceId, resultDate);
                    ruleCategoryQualityService.del(scoreInstanceId, resultDate);
                    instanceQualityService.del(scoreInstanceId, resultDate);
                    log.info("删除评分数据结束，耗时：{} ms", System.currentTimeMillis() - start);
                    List<StandardRuleEntity> result;
                    //按天执行
                    scoreEntity.setStartTime(resultDate);
                    scoreEntity.setExecuteTime(resultDate);
                    JobExecutor excutor = new JobExecutor(DATASET_POOL_NAME.replace("%s", scoreEntity.getScoreTaskId())
                        .replace("%d", String.valueOf(scoreEntity.getScoreInstance().getId())), SCORE_INSTANCE_THREADPOOL_SIZE);
                    try {
                        Future<List<StandardRuleEntity>> future = ruleScore(scoreEntity, excutor, scoreJobStartTime);
                        result = future.get();
                        log.info("插入评分对象质量表（INSTANCE_QUALITY）开始......" + " JobId:" + scoreEntity.getScoreTaskId()
                            + ", instanceId:" + scoreEntity.getScoreInstance().getId() + ", data 共{}条", result.size());
                        insertInstanceQuality(result);
                        log.info("插入评分对象质量表（INSTANCE_QUALITY）结束......" + " JobId:" + scoreEntity.getScoreTaskId()
                            + ", instanceId:" + scoreEntity.getScoreInstance().getId());
                    } catch (Exception ex) {
                        log.error("评分任务线程执行中断" + " JobId:" + scoreEntity.getScoreTaskId() + ", instanceId:" + scoreEntity.getScoreInstance().getId(), ex);
                        throw new JobExecutionException("执行评分失败： " + ex.getMessage());
                    } finally {
                        excutor.shutdown();
                    }
                    log.info("评分任务执行结束......" + " JobId:" + scoreEntity.getScoreTaskId() + ", instanceId:"
                        + scoreEntity.getScoreInstance().getId() + " 执行时间： "+ resultDate);
                }
                taskProgressService.saveOrUpdate(taskProgress);
                startDate = DateUtils.addDays(startDate, 1);
            }
        }catch (Exception e){
            throw new JobExecutionException(StringUtils.isEmpty(e.getMessage()) ? "执行评分失败" : e.getMessage());
        }
    }

    /***
     * 判断评分对象中是否配置规则
     * @param standardList
     * @return
     */
    private boolean ruleIsNull(List<ScoreEntity> standardList) {
        if (null == standardList || standardList.size() <= 0) {
            return true;
        }
        List<String> list = new ArrayList<>();
        List<ScoreInstanceRule> scoreInstanceRuleList = new ArrayList<>();
        standardList.stream().forEach(e -> {
            scoreInstanceRuleList.addAll(e.getStandardRuleList());
            scoreInstanceRuleList.addAll(e.getConsistencyRuleList());
            scoreInstanceRuleList.addAll(e.getCompleteRuleList());
            scoreInstanceRuleList.addAll(e.getStabilityRuleList());
            scoreInstanceRuleList.addAll(e.getRelevanceRuleList());
            scoreInstanceRuleList.addAll(e.getTimelinesRuleList());
        });
        if (null != scoreInstanceRuleList && scoreInstanceRuleList.size() > 0) {
            list = scoreInstanceRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
            if (null != list && list.size() > 0) {
                return false;
            }
        }
        return true;
    }

    private boolean getTaskCompleted(List<ScoreEntity> standardList, String startTime, ScoreJobDefinition scoreJobDefinition) throws JobExecutionException {
        //判断评分任务是否存在任务结束时间,若存在，则为临时任务
        String endTime = scoreJobDefinition.getEndTime();
        if (StringUtils.isNotEmpty(endTime)) {
            //任务完成时间大于评分结束时间，任务执行结束
            if (DateUtils.dataToStamp(startTime) > DateUtils.dataToStamp(endTime)) {
                try {
                    QuartzJobManager.getInstance().updateTaskStatusAndCancelJob(scoreJobDefinition.getScoreTaskId(), SCORE_JOB_GROUP,
                            Long.valueOf(scoreJobDefinition.getScoreTaskId()), EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
                } catch (Exception e) {
                    throw new JobExecutionException(e);
                }
                return false;
            }
        }
        Set<String> standardIdSet = new HashSet<>();
        for (ScoreEntity scoreEntity : standardList) {
            standardIdSet.addAll(scoreEntity.getStandardDataSetList().stream()
                .map(e -> String.valueOf(e.getStandardId())).collect(Collectors.toSet()));
        }
        boolean isFinish = taskProgressService.checkDataQualityIsFinish(standardIdSet, DateUtils.string2Date(startTime, DateUtils.DEFINE_YYYY_MM_DD));
        if(!isFinish && CommonConstant.TASK_TEMP_TYPE.equals(scoreJobDefinition.getTask().getTaskType())){
            throw new JobExecutionException("临时评分任务的" + startTime + "没有质控，评分失败");
        }
        return isFinish;
    }

    private void insertInstanceQuality(List<StandardRuleEntity> result) {
        if (CollectionUtils.isEmpty(result)) {
            return;
        }
        List<InstanceQuality> qualityList = new ArrayList<>();
        result.stream().forEach(e -> {
            InstanceQuality quality = new InstanceQuality();
            quality.setScoreInstanceId(String.valueOf(e.getScoreInstance().getId()));
            quality.setCycle(e.getCycle());
            quality.setStartDay(e.getStartTime().split("T")[0]);
            quality.setEndDay(e.getEndTime().split("T")[0]);
            quality.setScore(Double.valueOf(String.format("%.2f", null2Zero(e.getScore()))));
            quality.setRecordAmount(null2Zero(e.getRecordAmount()));
            quality.setNoUploadDayAmount(null2Zero(e.getNoUploadDayAmount()));
            quality.setFailAmount(null2Zero(e.getProblemNum()));
            quality.setFailAmountRate(Double.valueOf(String.format("%.2f", (divide(quality.getFailAmount(), quality.getRecordAmount())) * 100)));
            quality.setCheckTimes(null2Zero(e.getCheckTotal()));
            quality.setCheckFailTimes(null2Zero(e.getFailNum()));
            quality.setCheckFailRate(Double.valueOf(String.format("%.2f", (divide(quality.getCheckFailTimes(), quality.getCheckTimes())) * 100)));
            quality.setHopeDataSetAmount(null2Zero(e.getHopeDataSetAmount()));
            quality.setRealityDataSetAmount(null2Zero(e.getRealityDataSetAmount()));
            quality.setMissDataSetAmount(null2Zero(e.getMissDataSetAmount()));
            quality.setCycleDay(DateUtils.string2Date(quality.getStartDay(), DateUtils.DEFINE_YYYY_MM_DD));
            quality.setCreateTime(DateUtils.date2String(new Date(), DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS));
            qualityList.add(quality);
        });
        instanceQualityService.saveBatch(qualityList);
    }

    private Future<List<StandardRuleEntity>> ruleScore(ScoreEntity scoreEntity, JobExecutor excutor, String scoreJobStartTime) {
        scoreEntity.setScoreRuleThreadpoolSize(scoreRuleThreadpoolSize);
        scoreEntity.setMaxRuleEsExecSize(maxRuleEsExecSize);
        scoreEntity.setRedisRepository(redisRepository);
        scoreEntity.setScoreJobStartTime(scoreJobStartTime);
        return excutor.startRun(new RuleScoreCallable(scoreEntity));
    }

    private List<ScoreEntity> getAllStandard(List<ScoreJobDefinition> scoreJobList) {
        List<ScoreEntity> result = new ArrayList<>();

        List<StandardDataSet> dataSetList = new ArrayList<>();
        if (CollUtil.isEmpty(scoreJobList)) {
            return result;
        }

        for (ScoreJobDefinition jobDefinition : scoreJobList) {
            ScoreEntity scoreEntity = new ScoreEntity();
            List<StandardDataSet> standardDataSetList = jobDefinition.getStandardDataSetList();
            for (StandardDataSet standardDataSet : standardDataSetList) {
                if (!dataSetList.contains(standardDataSet)) {
                    dataSetList.add(standardDataSet);
                }
            }
            scoreEntity.setScoreTaskId(jobDefinition.getScoreTaskId());
            scoreEntity.setScoreInstance(jobDefinition.getScoreInstance());
            scoreEntity.setStandardRuleList(jobDefinition.getStandardRuleList());
            scoreEntity.setStandardWeight(jobDefinition.getStandardWeight());
            scoreEntity.setStandardScoreType(jobDefinition.getStandardScoreType());
            scoreEntity.setConsistencyRuleList(jobDefinition.getConsistencyRuleList());
            scoreEntity.setConsistencyWeight(jobDefinition.getConsistencyWeight());
            scoreEntity.setConsistencyScoreType(jobDefinition.getConsistencyScoreType());
            scoreEntity.setCompleteRuleList(jobDefinition.getCompleteRuleList());
            scoreEntity.setCompleteWeight(jobDefinition.getCompleteWeight());
            scoreEntity.setCompleteScoreType(jobDefinition.getCompleteScoreType());
            scoreEntity.setStabilityRuleList(jobDefinition.getStabilityRuleList());
            scoreEntity.setStabilityWeight(jobDefinition.getStabilityWeight());
            scoreEntity.setStabilityScoreType(jobDefinition.getStabilityScoreType());
            scoreEntity.setRelevanceRuleList(jobDefinition.getRelevanceRuleList());
            scoreEntity.setRelevanceWeight(jobDefinition.getRelevanceWeight());
            scoreEntity.setRelevanceScoreType(jobDefinition.getRelevanceScoreType());
            scoreEntity.setTimelinesRuleList(jobDefinition.getTimelinesRuleList());
            scoreEntity.setTimelinesWeight(jobDefinition.getTimelinesWeight());
            scoreEntity.setTimelinesScoreType(jobDefinition.getTimelinesScoreType());
            scoreEntity.setStandardDataSetList(dataSetList);
            scoreEntity.setStartTime(jobDefinition.getStartTime());
            scoreEntity.setIsParent(jobDefinition.getIsParent());
            scoreEntity.setCompleteAllRuleList(jobDefinition.getCompleteAllRuleList());
            scoreEntity.setStandardCate(jobDefinition.getStandardCate());
            scoreEntity.setConsistencyCate(jobDefinition.getConsistencyCate());
            scoreEntity.setCompleteCate(jobDefinition.getCompleteCate());
            scoreEntity.setStabilityCate(jobDefinition.getStabilityCate());
            scoreEntity.setRelevanceCate(jobDefinition.getRelevanceCate());
            scoreEntity.setTimelinesCate(jobDefinition.getTimelinesCate());
            scoreEntity.setEsLimit(jobDefinition.getEsLimit());
            result.add(scoreEntity);
        }
        return result;
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interrupted = true;
    }

    private Double null2Zero(Double num) {
        if (null == num) {
            return 0.00;
        }
        return num;
    }

    private Long null2Zero(Long num) {
        if (null == num) {
            return 0L;
        }
        return num;
    }

    private Integer null2Zero(Integer num) {
        if (null == num) {
            return 0;
        }
        return num;
    }

    private Double divide(Long divisor, Long dividend) {
        if (divisor == null || dividend == null || dividend == 0) {
            return 0.0;
        } else {
            return (divisor * 1.0000 / dividend);
        }
    }
}
