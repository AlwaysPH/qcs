package biz.schedule.task.score.callable;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataRuleService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataScoreService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetService;
import com.gwi.qcs.core.biz.service.dao.mysql.InstanceQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.RuleCategoryQualityService;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.clickhouse.PreDataRule;
import com.gwi.qcs.model.domain.clickhouse.PreDataScore;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapenum.RecordDataEnum;
import com.gwi.qcs.model.mapenum.ScoreTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Slf4j
public class ParentScoreCallable implements Callable<Map<String, Object>> {

    private static final int WEEK = 6;
    public static final int ARRAY_INDEX_AT2 = 2;
    public static final String DATA_SET_QUALITY_STANDARD = "dataSet_quality_standard_";
    public static final String DATA_SET_QUALITY_CONSISTENCY = "dataSet_quality_consistency_";
    public static final String DATA_SET_QUALITY_COMPLETE = "dataSet_quality_complete_";
    public static final String DATA_SET_QUALITY_STABILITY = "dataSet_quality_stability_";
    public static final String DATA_SET_QUALITY_RELEVANCE = "dataSet_quality_relevance_";
    public static final String DATA_SET_QUALITY_TIMELINES = "dataSet_quality_timelines_";

    protected RuleCategoryQualityService ruleCategoryQualityService;
    protected PreDataRuleService preDataRuleService;
    protected PreDataScoreService preDataScoreService;
    protected DatasetService datasetService;
    protected DataVolumeStatisticsService dataVolumeStatisticsService;
    protected InstanceQualityService instanceQualityService;
    protected ScoreEntity scoreEntity;
    protected String redisDatasetQuality;
    protected RecordDataEnum recordDataEnum;

    protected List<StandardRuleEntity> standardRuleEntityList = new ArrayList<>();
    protected List<DataSetQuality> dataSetQualityList = new ArrayList<>();
    protected List<RuleCategoryQuality> qualityList = new ArrayList<>();
    protected List<String> standardIdList = new ArrayList<>();
    protected List<String> dataSetIdList = new ArrayList<>();
    protected List<String> dataSetCodeList = new ArrayList<>();
    protected List<String> orgCodeList = new ArrayList<>();
    protected List<String> dataSourceList = new ArrayList<>();
    protected List<ScoreSumListEntity> recordList = new ArrayList<>();
    protected String startTime;
    protected String executeTime;
    protected String scoreTaskId;
    protected ScoreInstance scoreInstance;
    protected List<RuleCategory> standardCate;
    protected List<String> ruleIdList;
    protected String scoreJobStartTime;
    protected String categoryId;
    protected String scoreType;

    public ParentScoreCallable(ScoreEntity scoreEntity, RecordDataEnum recordDataEnum) {
        this.scoreEntity = scoreEntity;
        startTime = scoreEntity.getStartTime();
        executeTime = scoreEntity.getExecuteTime();
        scoreTaskId = scoreEntity.getScoreTaskId();
        scoreInstance = scoreEntity.getScoreInstance();
        ruleIdList = scoreEntity.getRuleIdList();
        scoreJobStartTime = scoreEntity.getScoreJobStartTime();
        categoryId = scoreEntity.getCategoryId();
        scoreType = scoreEntity.getStandardScoreType();
        this.recordDataEnum = recordDataEnum;
        switch (recordDataEnum) {
            case RECORD_QUALITY_STANDARD:
                standardCate = scoreEntity.getStandardCate();
                this.redisDatasetQuality = DATA_SET_QUALITY_STANDARD;
                break;
            case RECORD_QUALITY_CONSISTENCY:
                standardCate = scoreEntity.getConsistencyCate();
                scoreType = scoreEntity.getConsistencyScoreType();
                redisDatasetQuality = DATA_SET_QUALITY_CONSISTENCY;
                break;
            case RECORD_QUALITY_COMPLETE:
                standardCate = scoreEntity.getCompleteCate();
                scoreType = scoreEntity.getCompleteScoreType();
                redisDatasetQuality = DATA_SET_QUALITY_COMPLETE;
                break;
            case RECORD_QUALITY_STABILITY:
                standardCate = scoreEntity.getStabilityCate();
                scoreType = scoreEntity.getStabilityScoreType();
                redisDatasetQuality = DATA_SET_QUALITY_STABILITY;
                break;
            case RECORD_QUALITY_RELEVANCE:
                standardCate = scoreEntity.getRelevanceCate();
                scoreType = scoreEntity.getRelevanceScoreType();
                redisDatasetQuality = DATA_SET_QUALITY_RELEVANCE;
                break;
            case RECORD_QUALITY_TIMELINES:
                standardCate = scoreEntity.getTimelinesCate();
                scoreType = scoreEntity.getTimelinesScoreType();
                redisDatasetQuality = DATA_SET_QUALITY_TIMELINES;
                break;
            default:
                break;
        }
        ruleCategoryQualityService = SpringContextUtil.getBean(RuleCategoryQualityService.class);
        preDataRuleService = SpringContextUtil.getBean(PreDataRuleService.class);
        datasetService = SpringContextUtil.getBean(DatasetService.class);
        preDataScoreService = SpringContextUtil.getBean(PreDataScoreService.class);
        dataVolumeStatisticsService = SpringContextUtil.getBean(DataVolumeStatisticsService.class);
        instanceQualityService = SpringContextUtil.getBean(InstanceQualityService.class);
    }

    @Override
    public Map<String, Object> call() throws Exception {

        List<StandardDataSet> standardDataSetList = scoreEntity.getStandardDataSetList();
        //标准流水号
        standardIdList = standardDataSetList.stream().map(e -> String.valueOf(e.getStandardId())).distinct()
            .collect(Collectors.toList());
        //数据集id
        dataSetIdList = standardDataSetList.stream().map(e -> String.valueOf(e.getDataSetId())).distinct()
            .collect(Collectors.toList());
        //数据集DataSetCode
        for (StandardDataSet dataSet : standardDataSetList) {
            if (StringUtils.isNotEmpty(dataSet.getMetasetNameEn()) && !dataSetCodeList.contains(dataSet.getMetasetNameEn())) {
                dataSetCodeList.add(dataSet.getMetasetNameEn());
            }
        }
        //机构编码
        if (CollectionUtils.isNotEmpty(scoreInstance.getOrgCodeDms())) {
            orgCodeList.addAll(scoreInstance.getOrgCodeDms());
        }
        if (CollectionUtils.isNotEmpty(scoreInstance.getOrgCodes())) {
            for(String orgCode : scoreInstance.getOrgCodes()){
                if(!orgCodeList.contains(orgCode)){
                    orgCodeList.add(orgCode);
                }
            }
        }

        //数据源ID
        if (StringUtils.isNotEmpty(scoreInstance.getDatasourceId())) {
            dataSourceList = Arrays.asList(scoreInstance.getDatasourceId().split(CommonConstant.COMMA));
        }
        /***根据评分方式执行不同逻辑处理***/
        if(ScoreTypeEnum.CUMULATIVE_MEAN.getValue().equals(scoreType)){
            /***累计均值***/
            cumulateMean();
        }else if(ScoreTypeEnum.CLASSIFICATION_MEAN.getValue().equals(scoreType)){
            /***分类加权均值***/
            classificationMean();
        }else{
            /***数据集累计均值***/
            datasetCumulativeMean();
        }

        //规则分类评分插入规则分类质量表中
        saveToRuleCategory();

        RedisRepository redisRepository = scoreEntity.getRedisRepository();
        //保存数据集质量数据到redis
        redisRepository.set(redisDatasetQuality + scoreInstance.getId(), dataSetQualityList);
        //保存采集数据及数据集上传数到redis
        redisRepository.set(recordDataEnum.name() + scoreInstance.getId(), recordList);

        Map<String, Object> map = new HashMap<>();
        map.put(recordDataEnum.name(), standardRuleEntityList);
        log.info(recordDataEnum.getDesc() + "规则评分数完成" + executeTime);
        return map;
    }


    /***
     * 数据集累计均值（sum(每个数据集评分)/数据集个数 * 规则分类权重）
     */
    private void datasetCumulativeMean() throws ParseException {
        /***日数据***/
        getDatasetCumulativeMeanData(Arrays.asList(startTime), startTime, CommonConstant.CYCLE_DAY);
        //判断当前执行时间是否是周日
        if (DateUtils.isSunday(startTime)) {
            /***周数据***/
            String newStartTime = DateUtils.getPastDate(startTime, WEEK);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getDatasetCumulativeMeanData(interval, newStartTime, CommonConstant.CYCLE_WEEK);
        }
        //判断当前执行时间是否是每月最后一天
        if (DateUtils.isMonthLastDay(startTime)) {
            /***月数据***/
            //获取月天数
            String date = DateUtils.getOldMonth(startTime);
            int monthNum = DateUtils.getDaysOfMonth(date);
            String newStartTime = DateUtils.getPastDate(startTime, monthNum - 1);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getDatasetCumulativeMeanData(interval, newStartTime, CommonConstant.CYCLE_MONTH);
        }
    }


    /***
     * 日、周、月数据（数据集累计均值）
     */
    private void getDatasetCumulativeMeanData(List<String> interval, String startTime, String cycleType) {
        //根据数据集获取每个数据集评分
        getEachDataSetScore(interval, startTime, cycleType);
        //构造数据集数据
        getDataSetQualityData(startTime, cycleType);
        //构造数据集采集数据量、实际数据集上传量、为上传天数数据
        ScoreSumListEntity scoreSumListEntity = getOtherData(startTime, interval, cycleType);
        recordList.add(scoreSumListEntity);
        //获取子类规则分类数据
        getChildCategoryData(dataSetIdList, startTime, cycleType);
    }


    /***
     * 根据数据集获取每个数据集评分（数据集累计均值）
     */
    private void getEachDataSetScore(List<String> interval, String startTime, String cycleType) {
        Double finalScore;
        Long successTotal = 0L;
        Long failTotal = 0L;
        BigDecimal totalScore = BigDecimal.ZERO;
        switch (recordDataEnum) {
            case RECORD_QUALITY_STABILITY:
                for (String datasetId : dataSetIdList) {
                    //查询质控结果失败明细数据表，获取所有数据集的失败数据的总分数
                    List<PreDataScore> preDataScoreList = getFailScoreList(ruleIdList, ListUtil.toList(datasetId), startTime);
                    //查询质控结果汇总表，统计出所有数据集的成功总分数（sum(成功数)*100）
                    List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, ListUtil.toList(datasetId), startTime, false, false);
                    dealWithArray(preDataScoreList, preDataRuleList);
                    ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
                    Long successNum = scoreSumEntity.getSuccessAmount();
                    Long failNum = scoreSumEntity.getFailAmount();
                    Long scoreNum = scoreSumEntity.getSumScore();
                    BigDecimal score = BigDecimal.ZERO;
                    if (successNum + failNum > 0) {
                        score = BigDecimal.valueOf(scoreNum).divide(BigDecimal.valueOf(successNum + failNum), 2, BigDecimal.ROUND_HALF_UP);
                    }
                    totalScore = score.add(totalScore);
                    successTotal += successNum;
                    failTotal += failNum;
                }
                finalScore = totalScore.divide(new BigDecimal(dataSetIdList.size()), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                break;
            case RECORD_QUALITY_RELEVANCE:
                // 遍历计算子类规则分类评分
                for (String datasetId : dataSetIdList) {
                    //查询质控结果汇总表,按标准版本、数据表名分组,查询出各个数据集的成功数和失败数
                    List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, Arrays.asList(datasetId), startTime, true, false);

                    if (CollectionUtils.isEmpty(preDataRuleList)) {
                        continue;
                    }
                    /**
                     * 评分部分：
                     * 1、以关联性规则id、数据来源id/机构ids、起止日期、list<标准版本id，数据表名>作为入参，查询质控结果汇总表，以<标准版本、数据表名>作为维度分组，查询出各个数据集的成功数和失败数，作为各个数据集的校验成功数和校验失败数
                     * 2、数据集a的关联率=数据集a的校验成功数*1.0000/(数据集a的校验成功数+数据集a的校验失败数)
                     * 3、业务约束性得分=（各数据集的关联率之和）/数据集数量*100
                     */
                    ScoreSumEntity scoreSumEntity = getAllDataSetSum(preDataRuleList, startTime, dataSetIdList.size());
                    successTotal += scoreSumEntity.getSuccessAmount();
                    failTotal += scoreSumEntity.getFailAmount();
                    totalScore = totalScore.add(BigDecimal.valueOf(scoreSumEntity.getScore()));
                }
                //数据集累计均值
                finalScore = totalScore.divide(new BigDecimal(dataSetIdList.size()), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                break;
            default:
                RuleCategory ruleCategory = new RuleCategory();
                ruleCategory.setScoreType(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue());
                ruleCategory.setWeight(100L);
                ScoreSumEntity scoreSumEntity = getChildScoreByType(ruleIdList, startTime, ruleCategory, interval, 100L);
                finalScore = scoreSumEntity.getScore();
                successTotal = scoreSumEntity.getSuccessAmount();
                failTotal = scoreSumEntity.getFailAmount();
                break;
        }

        StandardRuleEntity entity = getStandardRuleEntity(scoreEntity.getStartTime(), successTotal, failTotal, finalScore, cycleType);
        RuleCategoryQuality quality = getRuleCategoryQuality(
            RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(scoreEntity.getStartTime())
                .ruleCategoryScore(finalScore).cycle(cycleType).build(),successTotal, failTotal, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
    }


    /***
     * 分类加权均值(规范性评分 = sum(子类规则分类评分*子类规则分类权重))
     */
    private void classificationMean() throws ParseException {
        /***日数据***/
        getClassificationData(CommonConstant.CYCLE_DAY, Arrays.asList(startTime), startTime);
        //判断当前执行时间是否是周日
        if (DateUtils.isSunday(startTime)) {
            /***周数据***/
            String newStartTime = DateUtils.getPastDate(startTime, WEEK);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getClassificationData(CommonConstant.CYCLE_WEEK, interval, newStartTime);
        }
        //判断当前执行时间是否是每月最后一天
        if (DateUtils.isMonthLastDay(startTime)) {
            /***月数据***/
            //获取月天数
            String date = DateUtils.getOldMonth(startTime);
            int monthNum = DateUtils.getDaysOfMonth(date);
            String newStartTime = DateUtils.getPastDate(startTime, monthNum - 1);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getClassificationData(CommonConstant.CYCLE_MONTH, interval, newStartTime);
        }
    }


    /***
     * 日、周、月数据（分类加权均值）
     */
    private void getClassificationData(String cycleType, List<String> interval, String startTime) {
        //获取子类规则评分之和，并构造规则质量表数据和规范性评分数据
        getChildScore(interval, cycleType, startTime);
        //构造数据集数据
        getDataSetQualityData(startTime, cycleType);
        //构造数据集采集数据量、实际数据集上传量、为上传天数数据
        ScoreSumListEntity scoreSumListEntity = getOtherData(startTime, interval, cycleType);
        recordList.add(scoreSumListEntity);
        //获取子类规则分类数据
        getChildCategoryData(dataSetIdList, startTime, cycleType);
    }


    /***
     * 获取子类规则评分之和，并构造规则质量表数据和规范性评分数据
     */
    private void getChildScore(List<String> interval, String cycleType, String startTime) {
        //遍历计算子类规则分类评分
        Double finalScore = 0.0;
        Long successTotal = 0L;
        Long failTotal = 0L;
        //获取子类规则总权重
        Long allWeight = CommonUtil.getAllWeight(standardCate);
        for (RuleCategory ruleCategory : standardCate) {
            List<Rule> ruleList = ruleCategory.getRules();
            if (CollectionUtils.isNotEmpty(ruleList)) {
                List<String> ruleIdList = ruleList.stream().map(e -> String.valueOf(e.getId())).collect(Collectors.toList());
                //根据子类规则分类评分方式获取数据
                ScoreSumEntity scoreSumEntity = getChildScoreByType(ruleIdList, startTime, ruleCategory, interval, allWeight);
                //子类规则分类数据
                Double score = 0.0;
                if(allWeight != 0){
                    score = scoreSumEntity.getScore() * ruleCategory.getWeight() / allWeight;
                }
                finalScore += score;
                successTotal += scoreSumEntity.getSuccessAmount() == null ? 0L : scoreSumEntity.getSuccessAmount();
                failTotal += scoreSumEntity.getFailAmount() == null ? 0L : scoreSumEntity.getFailAmount();
            }
        }
        StandardRuleEntity entity = getStandardRuleEntity(startTime, successTotal, failTotal, finalScore, cycleType);
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(startTime)
                .ruleCategoryScore(finalScore).cycle(cycleType).build(), successTotal, failTotal, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
    }


    /***
     * 根据子类规则分类评分方式获取数据
     * @return
     */
    private ScoreSumEntity getChildScoreByType(List<String> ruleIdList, String startTime,
                                           RuleCategory ruleCategory, List<String> interval, Long allWeight) {
        ScoreSumEntity result = new ScoreSumEntity();
        Double totalScore = 0.0;
        Long successTotal = 0L;
        Long failTotal = 0L;
        Double finalScore = 0.0;

        switch (recordDataEnum) {
            case RECORD_QUALITY_STABILITY:
            case RECORD_QUALITY_TIMELINES:
                boolean isStability = recordDataEnum == RecordDataEnum.RECORD_QUALITY_STABILITY;
                if (ruleCategory.getScoreType().equals(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue())) {
                    for (String i : dataSetIdList) {
                        //查询质控结果失败明细数据表，获取所有数据集的失败数据的总分数
                        List<PreDataScore> preDataScoreList = getFailScoreList(ruleIdList, Arrays.asList(i), startTime);
                        //查询质控结果汇总表，统计出所有数据集的成功总分数（sum(成功数)*100）
                        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, Arrays.asList(i),
                            startTime, false, false);
                        dealWithArray(preDataScoreList, preDataRuleList);
                        ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
                        Long successNum = scoreSumEntity.getSuccessAmount();
                        Long failNum = scoreSumEntity.getFailAmount();
                        Double score = 0.0;
                        Long scoreNum = scoreSumEntity.getSumScore();
                        if (successNum + failNum > 0) {
                            score = BigDecimal.valueOf(scoreNum).divide(BigDecimal.valueOf(successNum + failNum), 2, BigDecimal.ROUND_HALF_UP)
                                .doubleValue();
                        }
                        totalScore += score;
                        successTotal += successNum;
                        failTotal += failNum;
                    }
                    finalScore = new BigDecimal(totalScore).divide(new BigDecimal(dataSetIdList.size()), 2,
                        BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(ruleCategory.getWeight() / allWeight)).doubleValue();
                } else {
                    //查询质控结果失败明细数据表，获取所有数据集的失败数据的总分数
                    List<PreDataScore> preDataScoreList = getFailScoreList(ruleIdList, dataSetIdList,startTime);
                    //查询质控结果汇总表，统计出所有数据集的成功总分数（sum(成功数)*100）
                    List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                        false, false);
                    dealWithArray(preDataScoreList, preDataRuleList);
                    ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
                    Long successNum = scoreSumEntity.getSuccessAmount();
                    Long failNum = scoreSumEntity.getFailAmount();
                    Double score = 0.0;
                    Long scoreNum = scoreSumEntity.getSumScore();
                    if (successNum + failNum > 0) {
                        score = BigDecimal.valueOf(scoreNum).divide(BigDecimal.valueOf(successNum + failNum), 2, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal(ruleCategory.getWeight() / allWeight))
                            .doubleValue();
                    }
                    finalScore += score;
                    successTotal += successNum;
                    failTotal += failNum;
                }
                break;
            case RECORD_QUALITY_RELEVANCE:
                if (ruleCategory.getScoreType().equals(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue())) {
                    for (String i : dataSetIdList) {
                        //查询质控结果汇总表,按标准版本、数据表名分组,查询出各个数据集的成功数和失败数
                        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList,
                            Arrays.asList(i), startTime, true, false);
                        if (CollectionUtil.isEmpty(preDataRuleList)) {
                            continue;
                        }
                        ScoreSumEntity scoreSumEntity = getAllDataSetSum(preDataRuleList, startTime, dataSetIdList.size());
                        Long successNum = scoreSumEntity.getSuccessAmount();
                        Long failNum = scoreSumEntity.getFailAmount();
                        totalScore += new BigDecimal(scoreSumEntity.getSumScore())
                            .multiply(new BigDecimal(ruleCategory.getWeight() / allWeight)).doubleValue();
                        successTotal += successNum;
                        failTotal += failNum;
                    }
                    finalScore = new BigDecimal(totalScore).divide(new BigDecimal(dataSetIdList.size()),
                        2, BigDecimal.ROUND_HALF_UP).doubleValue();
                } else {
                    List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                        true, false);
                    if (CollectionUtil.isNotEmpty(preDataRuleList)) {
                        ScoreSumEntity scoreSumEntity = getAllDataSetSum(preDataRuleList, startTime, dataSetIdList.size());
                        Long successNum = scoreSumEntity.getSuccessAmount();
                        Long failNum = scoreSumEntity.getFailAmount();
                        finalScore += new BigDecimal(scoreSumEntity.getSumScore())
                            .multiply(new BigDecimal(ruleCategory.getWeight() / allWeight)).doubleValue();
                        successTotal += successNum;
                        failTotal += failNum;
                    }
                }
                break;
            default:
                if (ruleCategory.getScoreType().equals(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue())) {
                    for (String i : dataSetIdList) {
                        //ES中获取规则分类数据
                        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, Arrays.asList(i),
                            startTime, false, false);
                        ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
                        Long successNum = scoreSumEntity.getSuccessAmount();
                        Long failNum = scoreSumEntity.getFailAmount();
                        Double score = CommonUtil.getCumulateScore(successNum, failNum);
                        totalScore += score;
                        successTotal += successNum;
                        failTotal += failNum;
                    }
                    finalScore = new BigDecimal(totalScore).divide(new BigDecimal(dataSetIdList.size()), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                } else {
                    List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                        false, false);
                    ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
                    Long successNum = scoreSumEntity.getSuccessAmount();
                    Long failNum = scoreSumEntity.getFailAmount();
                    Double score = CommonUtil.getFailTimesRate(successNum, failNum);
                    finalScore += score;
                    successTotal += successNum;
                    failTotal += failNum;
                }
                break;
        }
        result.setSuccessAmount(successTotal);
        result.setFailAmount(failTotal);
        result.setScore(finalScore);
        return result;
    }


    /***
     * 日、周、月数据（累计均值）
     */
    private void cumulateMean() throws ParseException {
        /***日数据***/
        getCumulateData(CommonConstant.CYCLE_DAY, Arrays.asList(startTime), startTime);
        //判断当前执行时间是否是周日
        if (DateUtils.isSunday(startTime)) {
            /***周数据***/
            String newStartTime = DateUtils.getPastDate(startTime, WEEK);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getCumulateData(CommonConstant.CYCLE_WEEK, interval, newStartTime);
        }
        //判断当前执行时间是否是每月最后一天
        if (DateUtils.isMonthLastDay(startTime)) {
            /***月数据***/
            //获取月天数
            String date = DateUtils.getOldMonth(startTime);
            int monthNum = DateUtils.getDaysOfMonth(date);
            String newStartTime = DateUtils.getPastDate(startTime, monthNum - 1);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getCumulateData(CommonConstant.CYCLE_MONTH, interval, newStartTime);
        }
    }


    /***
     * 日、周、月数据(累计均值)
     */
    private void getCumulateData(String cycleType, List<String> interval, String startTime) throws ParseException {
        Double score = 0D;
        ScoreSumEntity scoreSumEntity;
        Long successNum;
        Long failNum;
        switch (recordDataEnum) {
            case RECORD_QUALITY_STABILITY:
            case RECORD_QUALITY_TIMELINES:
                boolean isStability = recordDataEnum == RecordDataEnum.RECORD_QUALITY_STABILITY;
                //查询质控结果失败明细数据表，获取所有数据集的失败数据的总分数
                List<PreDataScore> preDataScoreList = getFailScoreList(ruleIdList, dataSetIdList, startTime);
                //查询质控结果汇总表，统计出所有数据集的成功总分数（sum(成功数)*100）
                List<PreDataRule> preDataRuleList1 = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                    false, false);
                dealWithArray(preDataScoreList, preDataRuleList1);
                scoreSumEntity = getSum(preDataRuleList1, interval);
                successNum = scoreSumEntity.getSuccessAmount();
                failNum = scoreSumEntity.getFailAmount();
                Long scoreNum = scoreSumEntity.getSumScore();
                if (successNum + failNum > 0) {
                    score = BigDecimal.valueOf(scoreNum).divide(BigDecimal.valueOf(successNum + failNum), 2, BigDecimal.ROUND_HALF_UP)
                        .doubleValue();
                }
                break;
            case RECORD_QUALITY_RELEVANCE:
                //查询质控结果汇总表,按标准版本、数据表名分组,查询出各个数据集的成功数和失败数
                List<PreDataRule> preDataRuleList2 = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                    true, false);
                if (CollectionUtil.isEmpty(preDataRuleList2)) {
                    return;
                }
                /**
                 * 评分部分：
                 * 1、以关联性规则id、数据来源id/机构ids、起止日期、list<标准版本id，数据表名>作为入参，查询质控结果汇总表，以<标准版本、数据表名>作为维度分组，查询出各个数据集的成功数和失败数，作为各个数据集的校验成功数和校验失败数
                 * 2、数据集a的关联率=数据集a的校验成功数*1.0000/(数据集a的校验成功数+数据集a的校验失败数)
                 * 3、业务约束性得分=（各数据集的关联率之和）/数据集数量*100
                 */
                scoreSumEntity = getAllDataSetSum(preDataRuleList2, startTime, dataSetIdList.size());
                successNum = scoreSumEntity.getSuccessAmount();
                failNum = scoreSumEntity.getFailAmount();
                score = scoreSumEntity.getScore();
                break;
            default:
                //ES中获取规则分类数据
                List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime,
                    false, false);
                scoreSumEntity = getSum(preDataRuleList, interval);
                successNum = scoreSumEntity.getSuccessAmount() == null ? 0L : scoreSumEntity.getSuccessAmount();
                failNum = scoreSumEntity.getFailAmount() == null ? 0L : scoreSumEntity.getFailAmount();
                score = CommonUtil.getCumulateScore(successNum, failNum);
                break;
        }

        StandardRuleEntity entity = getStandardRuleEntity(startTime, successNum, failNum, score, cycleType);
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(startTime)
            .ruleCategoryScore(score).cycle(cycleType).build(), successNum, failNum, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
        //构造数据集数据
        getDataSetQualityData(startTime,cycleType);
        //构造数据集采集数据量、实际数据集上传量、为上传天数数据
        ScoreSumListEntity scoreSumListEntity = getOtherData(startTime, interval, cycleType);
        recordList.add(scoreSumListEntity);
        //获取子类规则分类数据
        getChildCategoryData(dataSetIdList, startTime, cycleType);
    }

    public List<PreDataRule> getRuleAndDatasetResult(List<String> ruleIdList, List<String> dataSetIdList, String startTime,
                                                     boolean isDataset, boolean isRule){
        List<PreDataRule> list = getPreDataRuleList(ruleIdList, dataSetIdList, startTime);
        List<PreDataRule> resultList = new ArrayList<>();
        if (isRule) {
            Map<String, List<PreDataRule>> map = list.stream().collect(Collectors.groupingBy(a ->
                a.getDatasetId() + "&" + a.getSourceId() + "&" + a.getCycleDay() + "&" + a.getStandardId() + "&" + a.getRuleId()
            ));
            map.forEach((key, tList) -> {
                Long failTotal = tList.stream().mapToLong(PreDataRule::getCheckNum).sum();
                Long successTotal = tList.stream().mapToLong(entity -> entity.getValidateNum() - entity.getCheckNum()).sum();

                PreDataRule preDataRule = new PreDataRule();
                preDataRule.setCheckNum(failTotal);
                preDataRule.setSuccessNum(successTotal);
                preDataRule.setDatasetId(key.split("&")[0]);
                preDataRule.setSourceId(key.split("&")[1]);
                preDataRule.setCycleDay(key.split("&")[2]);
                preDataRule.setStandardId(key.split("&")[3]);
                preDataRule.setRuleId(key.split("&")[4]);
                preDataRule.setDatasetCode(datasetService.getCodeById(preDataRule.getDatasetId()));
                resultList.add(preDataRule);
            });
        } else if (isDataset) {
            boolean isOrgListNotEmpty = CollectionUtils.isNotEmpty(orgCodeList);
            Map<String, List<PreDataRule>> map = list.stream().collect(Collectors.groupingBy(a -> {
                StringBuilder sb = new StringBuilder(a.getDatasetId() + "&" + a.getSourceId() + "&" + a.getCycleDay()
                    + "&" + a.getStandardId());
                if (isOrgListNotEmpty) {
                    sb.append("&").append(a.getOrgCode());
                }
                return sb.toString();
            }));
            map.forEach((key, tList) -> {
                Long failTotal = tList.stream().mapToLong(PreDataRule::getCheckNum).sum();
                Long successTotal = tList.stream().mapToLong(entity -> entity.getValidateNum() - entity.getCheckNum()).sum();

                PreDataRule preDataRule = new PreDataRule();
                preDataRule.setCheckNum(failTotal);
                preDataRule.setSuccessNum(successTotal);
                preDataRule.setDatasetId(key.split("&")[0]);
                preDataRule.setSourceId(key.split("&")[1]);
                preDataRule.setCycleDay(key.split("&")[2]);
                preDataRule.setStandardId(key.split("&")[3]);
                if (isOrgListNotEmpty) {
                    preDataRule.setOrgCode(key.split("&")[4]);
                }
                preDataRule.setDatasetCode(datasetService.getCodeById(preDataRule.getDatasetId()));
                resultList.add(preDataRule);
            });
        } else {
            Map<String, List<PreDataRule>> map = list.stream().collect(Collectors.groupingBy(PreDataRule::getCycleDay));
            map.forEach((key, tList) -> {
                Long failTotal = tList.stream().mapToLong(PreDataRule::getCheckNum).sum();
                Long successTotal = tList.stream().mapToLong(entity -> entity.getValidateNum() - entity.getCheckNum()).sum();

                PreDataRule preDataRule = new PreDataRule();
                preDataRule.setCheckNum(failTotal);
                preDataRule.setSuccessNum(successTotal);
                preDataRule.setCycleDay(key);
                resultList.add(preDataRule);
            });
        }
        return resultList;
    }

    public List<PreDataScore> getFailScoreList(List<String> ruleIdList, List<String> dataSetIdList, String startTime){
        List<PreDataScore> resultList = new ArrayList<>();
        List<PreDataScore> preDataScoreList = preDataScoreService.list(getQueryWrapper(ruleIdList, dataSetIdList, startTime));
        Map<String, List<PreDataScore>> map = preDataScoreList.stream().collect(Collectors.groupingBy(resultStatistic ->
            resultStatistic.getCycleDay() + "&" + resultStatistic.getDatasetId() + "&"
                + resultStatistic.getSourceId() + "&" + resultStatistic.getRuleId() + "&" + resultStatistic.getOrgCode()
        ));
        List<PreDataScore> tempList = new ArrayList<>();
        map.forEach((key, preDataScores) -> {
            tempList.add(preDataScores.get(0));
        });
        if (CollectionUtils.isEmpty(tempList)) {
            return resultList;
        }
        Map<String, List<PreDataScore>> newMap = tempList.stream().collect(Collectors.groupingBy(a ->
            a.getCycleDay() + "&" + a.getSourceId()));
        newMap.forEach((key, tList) -> {
            Long scoreTotal = tList.stream().mapToLong(PreDataScore::getTotalScore).sum();
            PreDataScore preDataScore = new PreDataScore();
            preDataScore.setCycleDay(key.split("&")[0]);
            preDataScore.setTotalScore(scoreTotal);
            resultList.add(preDataScore);
        });
        return resultList;
    }

    public List<PreDataRule> getPreDataRuleList(List<String> ruleIdList, List<String> dataSetIdList, String startTime) {
        return preDataRuleService.list(getQueryWrapper(ruleIdList, dataSetIdList, startTime));
    }

    public QueryWrapper getQueryWrapper(List<String> ruleIdList, List<String> dataSetIdList, String startTime) {
        QueryWrapper queryWrapper = new QueryWrapper<>();
        if(CollectionUtils.isNotEmpty(dataSourceList)){
            queryWrapper.in(CommonConstant.SOURCE_ID_UPPER, dataSourceList);
        }else{
            queryWrapper.in(CommonConstant.ORG_CODE_UPPER, orgCodeList);
        }
        queryWrapper.in(CommonConstant.STANDARD_ID_UPPER, standardIdList);
        queryWrapper.in(CommonConstant.DATASET_ID_UPPER, dataSetIdList);
        queryWrapper.in(CommonConstant.RULE_ID_UPPER, ruleIdList);
        queryWrapper.between(CommonConstant.CYCLE_DAY_UPPER, startTime, executeTime);
        return queryWrapper;
    }

    public ScoreSumEntity getAllDataSetSum(List<PreDataRule> preDataRuleList, String startTime, Integer dataSetSize){
        ScoreSumEntity result = new ScoreSumEntity();
        Set<String> set = new HashSet<>();
        for (PreDataRule preDataRule : preDataRuleList) {
            if (DateUtils.dataToStamp(startTime) <= DateUtils.dataToStamp(preDataRule.getCycleDay())) {
                set.add(preDataRule.getCycleDay());
            }
        }
        Map<String, List<PreDataRule>> map = preDataRuleList.stream().collect(Collectors.groupingBy(a ->
            a.getDatasetId() + CommonConstant.UNDERSCORE + a.getStandardId() + CommonConstant.UNDERSCORE
                + a.getCycleDay()
        ));

        List<ScoreSumEntity> scoreSumEntityArrayList = new ArrayList<>();
        map.forEach((key, tlist) -> {
            Long successNum = tlist.stream().mapToLong(PreDataRule::getSuccessNum).sum();
            Long failNum = tlist.stream().mapToLong(PreDataRule::getCheckNum).sum();
            if (DateUtils.dataToStamp(startTime) <= DateUtils.dataToStamp(key.split(CommonConstant.UNDERSCORE)[ARRAY_INDEX_AT2])) {
                ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
                scoreSumEntity.setDatasetId(key.split(CommonConstant.UNDERSCORE)[0]);
                scoreSumEntity.setStandardId(key.split(CommonConstant.UNDERSCORE)[1]);
                scoreSumEntity.setSuccessAmount(successNum);
                scoreSumEntity.setFailAmount(failNum);
                scoreSumEntityArrayList.add(scoreSumEntity);
            }
        });

        Long totalSuccess = 0L;
        Long totalFail = 0L;
        Double totalRate = 0.00;

        for (ScoreSumEntity scoreSumEntity : scoreSumEntityArrayList) {
            Long success = scoreSumEntity.getSuccessAmount();
            Long fail = scoreSumEntity.getFailAmount();
            Long total = success + fail;
            if (total > 0) {
                totalRate += new BigDecimal(success).divide(new BigDecimal(total), 4, BigDecimal.ROUND_HALF_UP)
                    .doubleValue();
            }
            totalSuccess += success;
            totalFail += fail;
        }
        //得分=（各数据集的关联率之和）/ (数据集数量 * 有数据天数)*100
        Double finalScore = new BigDecimal(totalRate)
            .divide(new BigDecimal(dataSetSize * set.size()), 4, BigDecimal.ROUND_HALF_UP)
            .multiply(new BigDecimal(100)).doubleValue();
        result.setSuccessAmount(totalSuccess);
        result.setFailAmount(totalFail);
        result.setScore(finalScore);
        return result;
    }

    public ScoreSumEntity getSum(List<PreDataRule> preDataRuleList, List<String> interval) {
        ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
        Long sumSuccess = 0L;
        Long sumFail = 0L;
        Long sumTotal = 0L;
        Long sumScore = 0L;
        for (PreDataRule preDataRule : preDataRuleList) {
            Long successNum = preDataRule.getSuccessNum();
            Long failNum = preDataRule.getCheckNum();
            sumScore += preDataRule.getScore() == null ? 0L : preDataRule.getScore();
            sumSuccess += successNum;
            sumFail += failNum;
        }
        sumTotal = sumSuccess + sumFail;
        scoreSumEntity.setSuccessAmount(sumSuccess);
        scoreSumEntity.setFailAmount(sumFail);
        scoreSumEntity.setTotalAmount(sumTotal);
        scoreSumEntity.setSumScore(sumScore);
        scoreSumEntity.setNoUploadDayAmount(getNoUploadDayAmount(interval));
        return scoreSumEntity;
    }


    private int getNoUploadDayAmount(List<String> interval){
        if(interval.size() == 1){
            return getTodayDataAmount();
        }else{
            interval = interval.subList(0, interval.size() - 1);
            QueryWrapper<InstanceQuality> queryWrapper = new QueryWrapper<>();
            LambdaQueryWrapper<InstanceQuality> lambdaQueryWrapper = queryWrapper.lambda();
            lambdaQueryWrapper.in(InstanceQuality::getScoreInstanceId, scoreInstance.getId());
            lambdaQueryWrapper.in(InstanceQuality::getStartDay, interval);
            lambdaQueryWrapper.gt(InstanceQuality::getRecordAmount, 0);
            lambdaQueryWrapper.groupBy(InstanceQuality::getStartDay);
            lambdaQueryWrapper.select(InstanceQuality::getStartDay);
            return interval.size() - instanceQualityService.list(lambdaQueryWrapper).size() + getTodayDataAmount();
        }
    }

    private int getTodayDataAmount(){
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        LambdaQueryWrapper<DataVolumeStatistics> lambdaQueryWrapper = queryWrapper.lambda();
        if(CollectionUtils.isNotEmpty(orgCodeList)){
            lambdaQueryWrapper.in(DataVolumeStatistics::getOrgCode, orgCodeList);
        }else{
            if(CollectionUtils.isNotEmpty(dataSourceList)){
                lambdaQueryWrapper.in(DataVolumeStatistics::getSourceId, dataSourceList);
            }
        }
        lambdaQueryWrapper.in(DataVolumeStatistics::getDatasetId, dataSetIdList);
        lambdaQueryWrapper.in(DataVolumeStatistics::getCycleDay, executeTime);
        lambdaQueryWrapper.gt(DataVolumeStatistics::getDataAmount, 0);
        lambdaQueryWrapper.groupBy(DataVolumeStatistics::getCycleDay);
        lambdaQueryWrapper.select(DataVolumeStatistics::getCycleDay);
        return 1 - dataVolumeStatisticsService.list(lambdaQueryWrapper).size();
    }

    protected void dealWithArray(List<PreDataScore> preDataScoreList, List<PreDataRule> preDataRuleList) {
        if (CollectionUtils.isNotEmpty(preDataRuleList)) {
            preDataRuleList.stream().forEach(preDataRule -> {
                preDataScoreList.stream().forEach(preDataScore -> {
                    if (preDataScore.getCycleDay().equals(preDataRule.getCycleDay())) {
                        preDataRule.setScore(preDataScore.getTotalScore());
                    }
                });
            });
        }
    }


    public void getDataSetQualityData(String newStartTime, String cycleType) {
        //ES中获取数据集数据
        List<PreDataRule> preDataRuleList = getStaticAndVolumeData(newStartTime);
        //处理ES数据集数据
        dealWithDataSet(preDataRuleList, newStartTime, cycleType);
    }

    public List<PreDataRule> getStaticAndVolumeData(String startTime) {
        /***查询质控结果汇总表数据***/
        List<PreDataRule> result = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime, true, false);

        /***查询数据量统计表数据***/
        List<DataVolumeStatistics> dataVolumeStatisticsList = getVolumeData(dataSetIdList, startTime, true);

        List<PreDataRule> list = new ArrayList<>();
        boolean isOrgListNotEmpty = CollectionUtils.isNotEmpty(orgCodeList);
        boolean isDataSourceListNotEmpty = CollectionUtils.isNotEmpty(dataSourceList);
        if (CollectionUtils.isNotEmpty(result)) {
            for (PreDataRule preDataRule : result) {
                for (DataVolumeStatistics dataVolumeStatistics : dataVolumeStatisticsList) {
                    boolean isTrue = preDataRule.getCycleDay().equals(dataVolumeStatistics.getCycleDay())
                        && preDataRule.getDatasetId().equals(dataVolumeStatistics.getDatasetId());
                    setPreDataRuleDataAmount(isOrgListNotEmpty, isDataSourceListNotEmpty, preDataRule, dataVolumeStatistics, isTrue);
                }
            }
        } else {
            if (CollectionUtils.isNotEmpty(dataVolumeStatisticsList)) {
                for (DataVolumeStatistics dataVolumeStatistics : dataVolumeStatisticsList) {
                    if (CollectionUtils.isEmpty(result)) {
                        PreDataRule preDataRule = new PreDataRule();
                        BeanUtils.copyProperties(dataVolumeStatistics, preDataRule);
                        preDataRule.setSuccessNum(0L);
                        preDataRule.setCheckNum(0L);
                        list.add(preDataRule);
                    }
                    for (PreDataRule preDataRule : result) {
                        setPreDataRuleDataAmountAndAdd(list, isOrgListNotEmpty, isDataSourceListNotEmpty, dataVolumeStatistics, preDataRule);
                        preDataRule.setSuccessNum(preDataRule.getValidateNum() - preDataRule.getCheckNum());
                    }
                }
                result = list;
            }
        }
        return result;
    }

    public void setPreDataRuleDataAmountAndAdd(List<PreDataRule> list, boolean isOrgListNotEmpty, boolean isDataSourceListNotEmpty, DataVolumeStatistics dataVolumeStatistics, PreDataRule preDataRule) {
        boolean isTrue = preDataRule.getCycleDay().equals(dataVolumeStatistics.getCycleDay())
            && preDataRule.getDatasetId().equals(dataVolumeStatistics.getDatasetId());
        if (isDataSourceListNotEmpty) {
            if (dataVolumeStatistics.getSourceId().equals(preDataRule.getSourceId()) && isTrue){
                preDataRule.setDataAmount(dataVolumeStatistics.getDataAmount());
                list.add(preDataRule);
            }
        } else if (isOrgListNotEmpty) {
            if (dataVolumeStatistics.getOrgCode().equals(preDataRule.getOrgCode()) && isTrue){
                preDataRule.setDataAmount(dataVolumeStatistics.getDataAmount());
                list.add(preDataRule);
            }
        }else{

        }
    }

    public void setPreDataRuleDataAmount(boolean isOrgListNotEmpty, boolean isDataSourceListNotEmpty, PreDataRule preDataRule, DataVolumeStatistics dataVolumeStatistics, boolean isTrue) {
        if (isDataSourceListNotEmpty) {
            if (preDataRule.getSourceId().equals(dataVolumeStatistics.getSourceId()) && isTrue) {
                preDataRule.setDataAmount(dataVolumeStatistics.getDataAmount());
            }
        } else if (isOrgListNotEmpty) {
            if (preDataRule.getOrgCode().equals(dataVolumeStatistics.getOrgCode()) && isTrue) {
                preDataRule.setDataAmount(dataVolumeStatistics.getDataAmount());
            }
        }else{

        }
    }

    public List<DataVolumeStatistics> getVolumeData(List<String> dataSetIdList, String startTime, boolean isCycle){
        List<DataVolumeStatistics> result = new ArrayList<>();
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if(CollectionUtils.isNotEmpty(dataSourceList)){
            queryWrapper.in(CommonConstant.SOURCE_ID_UPPER, dataSourceList);
        }else{
            queryWrapper.in(CommonConstant.ORG_CODE_UPPER, orgCodeList);
        }
        queryWrapper.in(CommonConstant.STANDARD_ID_UPPER, standardIdList);
        queryWrapper.in(CommonConstant.DATASET_ID_UPPER, dataSetIdList);
        queryWrapper.between(CommonConstant.CYCLE_DAY_UPPER, startTime, executeTime);
        queryWrapper.gt(CommonConstant.DATA_AMOUNT_UPPER, 0);
        List<DataVolumeStatistics> dataVolumeStatisticsList = dataVolumeStatisticsService.list(queryWrapper);
        Map<String, List<DataVolumeStatistics>> tempMap = dataVolumeStatisticsList.stream().collect(Collectors.groupingBy(resultStatistic ->
            resultStatistic.getCycleDay() + "&" + resultStatistic.getDatasetId()
                + "&" + resultStatistic.getSourceId() + "&" + resultStatistic.getOrgCode() + "&" + resultStatistic.getStandardId()
        ));
        List<DataVolumeStatistics> tempList = new ArrayList<>();
        tempMap.forEach((key, list) -> {
            tempList.add(list.get(0));
        });
        if (isCycle) {
            boolean isOrgListNotEmpty = CollectionUtils.isNotEmpty(orgCodeList);
            Map<String, List<DataVolumeStatistics>> map = getDataVolumeStatisticsStringListMap(tempList, isOrgListNotEmpty);
            map.forEach((key, tList) -> {
                Long dataAmount = tList.stream().mapToLong(DataVolumeStatistics::getDataAmount).sum();
                DataVolumeStatistics dataVolumeStatistics = new DataVolumeStatistics();
                dataVolumeStatistics.setDataAmount(dataAmount);
                dataVolumeStatistics.setDatasetId(key.split("&")[0]);
                dataVolumeStatistics.setSourceId(key.split("&")[1]);
                dataVolumeStatistics.setCycleDay(key.split("&")[2]);
                dataVolumeStatistics.setStandardId(key.split("&")[3]);
                if (isOrgListNotEmpty) {
                    dataVolumeStatistics.setOrgCode(key.split("&")[4]);
                }
                dataVolumeStatistics.setDatasetCode(datasetService.getCodeById(dataVolumeStatistics.getDatasetId()));
                result.add(dataVolumeStatistics);
            });
        } else {
            Map<String, List<DataVolumeStatistics>> map = tempList.stream().collect(Collectors.groupingBy(a ->
                a.getDatasetId() + "&" + a.getCycleDay()));
            map.forEach((key, tList) -> {
                Long dataAmount = tList.stream().mapToLong(DataVolumeStatistics::getDataAmount).sum();

                DataVolumeStatistics dataVolumeStatistics = new DataVolumeStatistics();
                dataVolumeStatistics.setDataAmount(dataAmount);
                dataVolumeStatistics.setDatasetId(key.split("&")[0]);
                dataVolumeStatistics.setCycleDay(key.split("&")[1]);
                dataVolumeStatistics.setCountDataAmount(dataAmount > 0 ? 1 : 0);
                result.add(dataVolumeStatistics);
            });
        }
        return result;
    }

    public Map<String, List<DataVolumeStatistics>> getDataVolumeStatisticsStringListMap(List<DataVolumeStatistics> tempList,
                                                                                        boolean isOrgListNotEmpty) {
        return tempList.stream().collect(Collectors.groupingBy(a -> {
                    StringBuilder keyStr = new StringBuilder(a.getDatasetId() + "&" + a.getSourceId()
                        + "&" + a.getCycleDay() + "&" + a.getStandardId());
                    if (isOrgListNotEmpty) {
                        keyStr.append("&").append(a.getOrgCode());
                    }
                    return keyStr.toString();
                }));
    }

    public void dealWithDataSet(List<PreDataRule> preDataRuleList, String startTime, String timeCycle) {
        if (CollectionUtils.isNotEmpty(preDataRuleList)) {
            List<PreDataRule> preDataRuleList1 = getDataSetSum(preDataRuleList);
            preDataRuleList1.stream().forEach(e -> {
                dataSetQualityList.add(getDataSetQuality(startTime, timeCycle, e));
            });
        }
    }


    public List<PreDataRule> getDataSetSum(List<PreDataRule> preDataRuleList) {
        List<PreDataRule> result = new ArrayList<>();
        //根据标准版本流水号、数据来源ID，表名，时间周期分组
        Map<String, List<PreDataRule>> map = preDataRuleList.stream().collect(Collectors.groupingBy(a ->
            a.getDatasetId() + "&" + a.getStandardId() + "&" + a.getSourceId() + "&" + a.getCycleDay()
                + "&" + a.getOrgCode()
        ));
        map.forEach((key, tList) -> {
            Long success = tList.stream().mapToLong(temp ->
                new BigDecimal(temp.getSuccessNum() == null ? "0" : temp.getSuccessNum().toString()).longValue()
            ).sum();
            Long fail = tList.stream().mapToLong(temp ->
                new BigDecimal(temp.getCheckNum() == null ? "0" : temp.getCheckNum().toString()).longValue()
            ).sum();
            Long record = tList.stream().mapToLong(temp ->
                new BigDecimal(temp.getDataAmount() == null ? "0" : temp.getDataAmount().toString()).longValue()
            ).sum();
            PreDataRule preDataRule = new PreDataRule();
            preDataRule.setDatasetId(key.split("&")[0]);
            preDataRule.setStandardId(key.split("&")[1]);
            preDataRule.setSourceId(key.split("&")[2]);
            preDataRule.setCycleDay(key.split("&")[3]);
            preDataRule.setDatasetCode(datasetService.getCodeById(preDataRule.getDatasetId()));
            preDataRule.setSuccessNum(success);
            preDataRule.setCheckNum(fail);
            preDataRule.setDataAmount(record);
            result.add(preDataRule);
        });
        return result;
    }


    public DataSetQuality getDataSetQuality(String startTime, String timeCycle, PreDataRule preDataRule) {
        DataSetQuality quality = new DataSetQuality();
        quality.setScoreInstanceId(String.valueOf(scoreInstance.getId()));
        quality.setCycle(timeCycle);
        quality.setStartDay(startTime.split("T")[0]);
        quality.setEndDay(executeTime.split("T")[0]);
        quality.setStandardId(preDataRule.getStandardId());
        quality.setSourceId(preDataRule.getSourceId());
        quality.setDataSetCode(preDataRule.getDatasetCode());
        quality.setDataSetId(preDataRule.getDatasetId());
        quality.setRecordAmount(preDataRule.getDataAmount() == null ? 0L : preDataRule.getDataAmount());
        quality.setCheckTimes(preDataRule.getSuccessNum() + preDataRule.getCheckNum());
        quality.setCheckFailTimes(preDataRule.getCheckNum());
        return quality;
    }

    public StandardRuleEntity getStandardRuleEntity(String startDay, Long successNum, Long failNum, Double score,
                                                           String timeCycle) {
        StandardRuleEntity entity = new StandardRuleEntity();
        entity.setScoreTaskId(scoreTaskId);
        entity.setScoreInstance(scoreInstance);
        entity.setSuccessNum(successNum);
        entity.setFailNum(failNum);
        entity.setCycle(timeCycle);
        entity.setCheckTotal(successNum + failNum);
        entity.setScore(score);
        entity.setStartTime(startDay);
        entity.setEndTime(executeTime);
        return entity;
    }

    public RuleCategoryQuality getRuleCategoryQuality(RuleCategoryQuality ruleCategoryQuality, Long successNum,
                                                      Long failNum, String categoryId) {
        ruleCategoryQuality.setScoreInstanceId(String.valueOf(scoreInstance.getId()));
        ruleCategoryQuality.setStartDay(ruleCategoryQuality.getStartDay().split("T")[0]);
        ruleCategoryQuality.setEndDay(executeTime.split("T")[0]);
        ruleCategoryQuality.setRuleCategoryId(categoryId);
        ruleCategoryQuality.setCheckTimes(successNum + failNum);
        ruleCategoryQuality.setCheckFailTimes(failNum);
        Double data = CommonUtil.getFailTimesRate(successNum, failNum);
        ruleCategoryQuality.setCheckFailTimesRate(data);
        ruleCategoryQuality.setCycleDay(DateUtils.string2Date(ruleCategoryQuality.getStartDay(), DateUtils.DEFINE_YYYY_MM_DD));
        ruleCategoryQuality.setCreateTime(DateUtils.date2String(new Date(), DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS));
        return ruleCategoryQuality;
    }


    /***
     * 构造数据集采集数据量、实际数据集上传量、为上传天数数据
     * @return
     */
    public ScoreSumListEntity getOtherData(String newStartTime, List<String> interval, String cycleType) {
        //ES获取数据量统计表数据
        List<DataVolumeStatistics> dataVolumeStatisticsList = getVolumeData(dataSetIdList, newStartTime, false);
        //获取采集数据条数和实际数据集上传数量
        List<ScoreSumEntity> scoreSumEntityList = getAcData(dataVolumeStatisticsList, dataSetIdList);
        //获取周、月的未上传数据天数
        Integer noDataNum = getNoUploadDayAmount(interval);
        return getRecordData(scoreSumEntityList, cycleType, noDataNum, dataSetIdList.size());
    }

    public List<ScoreSumEntity> getAcData(List<DataVolumeStatistics> dataVolumeStatisticsList, List<String> dataSetIdList) {
        List<ScoreSumEntity> resultList = new ArrayList<>();
        for (String datasetId : dataSetIdList) {
            //实际上传数据集数量
            Integer actDataSetNum = 0;
            //采集数据条数
            Long recordNum = 0L;
            for (DataVolumeStatistics dataVolumeStatistics : dataVolumeStatisticsList) {
                if (datasetId.equals(dataVolumeStatistics.getDatasetId())) {
                    if (dataVolumeStatistics.getCountDataAmount() > 0) {
                        actDataSetNum = 1;
                    }
                    recordNum += dataVolumeStatistics.getDataAmount();
                }
            }
            ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
            scoreSumEntity.setDatasetId(datasetId);
            scoreSumEntity.setRealityDataSetAmount(actDataSetNum);
            scoreSumEntity.setTotalAmount(recordNum);
            resultList.add(scoreSumEntity);
        }
        return resultList;
    }


    public Integer getWeekAndMonthNoDataNum(List<DataVolumeStatistics> dataVolumeStatisticsList, List<String> interval) {
        Set<String> set = new HashSet<>();
        if (CollectionUtils.isNotEmpty(dataVolumeStatisticsList)) {
            for (DataVolumeStatistics dataVolumeStatistics : dataVolumeStatisticsList) {
                set.add(dataVolumeStatistics.getCycleDay());
            }
        }
        return interval.size() - set.size();
    }

    public ScoreSumListEntity getRecordData(List<ScoreSumEntity> scoreSumEntityList, String cycle, Integer noDataNum,
                                            Integer dataSetNum) {
        ScoreSumListEntity result = new ScoreSumListEntity();
        List<ScoreSumEntity> scoreSumEntityArrayList = new ArrayList<>();
        for (ScoreSumEntity entity : scoreSumEntityList) {
            ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
            Integer actDataSetNum = entity.getRealityDataSetAmount();
            scoreSumEntity.setDatasetId(entity.getDatasetId());
            scoreSumEntity.setTotalAmount(entity.getTotalAmount());
            scoreSumEntity.setHopeDataSetAmount(dataSetNum);
            scoreSumEntity.setRealityDataSetAmount(actDataSetNum);
            scoreSumEntity.setMissDataSetAmount(Math.max(dataSetNum - actDataSetNum, 0));
            scoreSumEntity.setNoUploadDayAmount(noDataNum);
            scoreSumEntityArrayList.add(scoreSumEntity);
        }
        result.setData(scoreSumEntityArrayList);
        result.setCycleDay(cycle);
        return result;
    }

    public void saveToRuleCategory() {
        ruleCategoryQualityService.saveBatch(qualityList, CommonConstant.CK_SAVE_BATCH_NUM);
    }

    /***
     * 获取子类规则分类数据
     */
    public void getChildCategoryData(List<String> dataSetIdList, String newStartTime, String type) {
        if (CollectionUtils.isEmpty(standardCate)) {
            return;
        }
        for (RuleCategory category : standardCate) {
            List<Rule> ruleList = category.getRules();
            if (CollectionUtils.isNotEmpty(ruleList)) {
                //获取时间区间段内所有时间
                List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
                List<String> ruleIds = ruleList.stream().map(e -> e.getId()).collect(Collectors.toList());
                List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIds,  dataSetIdList, newStartTime, false, false);
                switch (type) {
                    case CommonConstant.CYCLE_DAY:
                        castESDayData(preDataRuleList, type, category.getId(), newStartTime);
                        break;
                    case CommonConstant.CYCLE_WEEK:
                    case CommonConstant.CYCLE_MONTH:
                        castESWeekAndMonthData(preDataRuleList, type, interval, category.getId(), newStartTime);
                        break;
                    default:
                        break;
                }
            }
        }
    }


    private void castESDayData(List<PreDataRule> preDataRuleList, String type, String childCategoryId, String newStartTime) {
        if (CollectionUtils.isNotEmpty(preDataRuleList)) {
            for (PreDataRule preDataRule : preDataRuleList) {
                castESDayScore(preDataRule, type, childCategoryId, newStartTime);
            }
        }
    }


    private void castESDayScore(PreDataRule preDataRule, String type, String childCategoryId, String startTime) {
        Long successNum = preDataRule.getSuccessNum();
        Long failNum = preDataRule.getCheckNum();
        Double score = new BigDecimal(successNum).divide(new BigDecimal(successNum + failNum),
            4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(startTime)
            .ruleCategoryScore(score).cycle(type).build(),successNum, failNum, childCategoryId);
        qualityList.add(quality);
    }


    private void castESWeekAndMonthData(List<PreDataRule> preDataRuleList, String type, List<String> interval,
                                        String childCategoryId, String newStartTime) {
        if (CollectionUtils.isNotEmpty(preDataRuleList)) {
            ScoreSumEntity scoreSumEntity = getSum(preDataRuleList, interval);
            Long successNum = scoreSumEntity.getSuccessAmount();
            Long failNum = scoreSumEntity.getFailAmount();
            Double score = new BigDecimal(successNum).divide(new BigDecimal(successNum + failNum),
                4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
            RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(newStartTime)
                .ruleCategoryScore(score).cycle(type).build(),successNum, failNum, childCategoryId);
            qualityList.add(quality);
        }
    }
}
