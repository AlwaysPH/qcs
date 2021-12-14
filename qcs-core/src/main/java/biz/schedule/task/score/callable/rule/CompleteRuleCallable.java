package biz.schedule.task.score.callable.rule;

import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.core.biz.schedule.task.score.callable.ParentScoreCallable;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.clickhouse.PreDataRule;
import com.gwi.qcs.model.domain.mysql.Rule;
import com.gwi.qcs.model.domain.mysql.RuleCategory;
import com.gwi.qcs.model.domain.mysql.RuleCategoryQuality;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapenum.RecordDataEnum;
import com.gwi.qcs.model.mapenum.ScoreTypeEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

public class CompleteRuleCallable extends ParentScoreCallable {

    private Logger log = LoggerFactory.getLogger(CompleteRuleCallable.class);

    private static final int WEEK = 6;

    private static final String DAY_CYCLE = "day";

    private static final String WEEK_CYCLE = "week";

    private static final String MONTH_CYCLE = "month";

    private static final String SUCCESS_STRING = "SUCCESS_AMOUNT";

    private static final String FAIL_STRING = "FAIL_AMOUNT";

    private static final String TOTAL_STRING = "TOTAL";

    private static final String DATE_STRING = "date";

    private static final String ES_DATA_STRING = "data";

    private static final String DATASET_STRING = "dataSet";

    private static final String BINGD_STRING = "bind";

    private static final String DATASET_KEY = "数据集上传率";

    private static final String BINGD_KEY = "业务约束性";

    private static final String CATEGORY_ID_STRING = "categoryId";

    private static final String RULES_STRING = "rules";

    private static final String SCOREINSTANCE_STRING = "scoreInstance";

    private static final String REDIS_DATASET_QUALITY = "dataSet_quality_complete_";

    private static final String RECORD_QUALITY = "record_complete_";

    private static final String COUNT_DATA_AMOUNT = "COUNT_DATA_AMOUNT";

    List<RuleCategory> completeAllRuleList;

    public CompleteRuleCallable(ScoreEntity scoreEntity) {
        super(scoreEntity, RecordDataEnum.RECORD_QUALITY_COMPLETE);
        completeAllRuleList = scoreEntity.getCompleteAllRuleList();
    }

    @Override
    public Map<String, Object> call() throws Exception {
        standardCate = scoreEntity.getCompleteCate();
        List<StandardDataSet> standardDataSetList = scoreEntity.getStandardDataSetList();

        //标准流水号
        standardIdList = standardDataSetList.stream().map(e -> String.valueOf(e.getStandardId())).distinct().collect(Collectors.toList());
        //数据集id
        dataSetIdList = standardDataSetList.stream().map(e -> String.valueOf(e.getDataSetId())).distinct().collect(Collectors.toList());
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
        if(StringUtils.isNotEmpty(scoreInstance.getDatasourceId())){
            dataSourceList = Arrays.asList(scoreInstance.getDatasourceId().split(","));
        }

        //实际上传数据集数量
        int actDataSetNum = 0;

        /***根据评分方式执行不同逻辑处理***/
        if(ScoreTypeEnum.CUMULATIVE_MEAN.getValue().equals(scoreType)){
            /***累计均值***/
            cumulateMean(actDataSetNum, false);
        }
        if(ScoreTypeEnum.CLASSIFICATION_MEAN.getValue().equals(scoreType)){
            /***分类加权均值***/
            cumulateMean(actDataSetNum, true);
        }
        if(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue().equals(scoreType)){
            /***数据集累计均值***/
            dataSetCumulateMean(actDataSetNum);
        }

        //规则分类评分插入规则分类质量表中
        saveToRuleCategory();

        //保存数据集质量数据到redis
        RedisRepository redisRepository = scoreEntity.getRedisRepository();
        redisRepository.set(REDIS_DATASET_QUALITY + scoreInstance.getId(), dataSetQualityList);
        //保存采集数据及数据集上传数到redis
        redisRepository.set(RecordDataEnum.RECORD_QUALITY_COMPLETE.name() + scoreInstance.getId(), recordList);

        Map<String, Object> map = new HashMap<>();
        map.put(recordDataEnum.name(), standardRuleEntityList);
        log.info(recordDataEnum.getDesc() + "规则评分数完成" + executeTime);
        return map;
    }

    private void dataSetCumulateMean(int actDataSetNum) throws ParseException {
        /***日数据***/
        getDataSetCumulateMeanData(actDataSetNum, Arrays.asList(startTime), startTime, DAY_CYCLE, 1);
        //判断当前执行时间是否是周日
        if(DateUtils.isSunday(startTime)){
            /***周数据***/
            String newStartTime = DateUtils.getPastDate(startTime, WEEK);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getDataSetCumulateMeanData(actDataSetNum, interval, newStartTime, WEEK_CYCLE, 7);
        }
        //判断当前执行时间是否是每月最后一天
        if(DateUtils.isMonthLastDay(startTime)) {
            /***月数据***/
            //获取月天数
            String date = DateUtils.getOldMonth(startTime);
            int monthNum = DateUtils.getDaysOfMonth(date);
            String newStartTime = DateUtils.getPastDate(startTime, monthNum - 1);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getDataSetCumulateMeanData(actDataSetNum, interval, newStartTime, MONTH_CYCLE, monthNum);
        }
    }

    /***
     * 日、周、月数据（数据集累计均值）
     */
    private void getDataSetCumulateMeanData(int actDataSetNum, List<String> interval, String startTime,
                                            String cycleType, Integer daysNum) {
        //根据数据集获取每个数据集评分
        getEachDataSetScore(actDataSetNum, startTime, cycleType, daysNum);
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
    private void getEachDataSetScore(int actDataSetNum, String startTime, String cycleType, Integer daysNum) {
        //获取每个数据集完整性评分
        Double totalScore = 0.00;
        Long successNum = 0L;
        Long failNum = 0L;
        for(String i : dataSetIdList){
            //计算数据集上传率
            Double dataSetUploadRate = getDataSetUploadRate(Arrays.asList(i), startTime, actDataSetNum, daysNum);
            //计算约束性得分
            List<ScoreSumEntity> scoreSumEntityList = getBindDayScore(Arrays.asList(i), startTime);
            //计算完整性得分
            ScoreSumEntity scoreSumEntity = getFinalScore(dataSetUploadRate, scoreSumEntityList);
            totalScore += scoreSumEntity.getScore();
            successNum += scoreSumEntity.getSuccessAmount();
            failNum += scoreSumEntity.getFailAmount();
        }
        Double finalScore = new BigDecimal(totalScore).divide(new BigDecimal(dataSetIdList.size()), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        finalScore = Double.valueOf(new DecimalFormat("#.00").format(finalScore));
        StandardRuleEntity entity = getStandardRuleEntity(startTime, successNum, failNum, finalScore, cycleType);
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(startTime)
            .ruleCategoryScore(finalScore).cycle(cycleType).build(), successNum, failNum, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
    }

    /***
     * 计算每个数据集完整性得分（数据集累计均值）
     * @return
     */
    private ScoreSumEntity getFinalScore(Double dataSetUploadRate, List<ScoreSumEntity> scoreSumEntityList) {
        ScoreSumEntity result = new ScoreSumEntity();
        Double totalScore = 0.00;
        Long successNum = 0L;
        Long failNum = 0L;
        Long totalWeight = getTotalWeight();
        for(ScoreSumEntity scoreSumEntity : scoreSumEntityList) {
            totalScore = getScore(dataSetUploadRate, totalScore, totalWeight, scoreSumEntity);
            successNum += scoreSumEntity.getSuccessAmount();
            failNum += scoreSumEntity.getFailAmount();
        }
        result.setSuccessAmount(successNum);
        result.setFailAmount(failNum);
        result.setScore(totalScore);
        return result;
    }

    /***
     * 计算每个数据集约束性得分（数据集累计均值）
     * @return
     */
    private List<ScoreSumEntity> getBindDayScore( List<String> dataSetIdList, String startTime) {
        //查询质控结果汇总数据表，获取各个数据集成功数和失败数
        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList,  dataSetIdList, startTime, false, true);
        //处理相同时间的数据集数据
        List<ScoreSumListEntity> scoreSumListEntityList = dealWithAllDataSet(preDataRuleList);
        //计算业务约束性得分
        List<ScoreSumEntity> resultList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(scoreSumListEntityList)){
            scoreSumListEntityList.stream().forEach(entity -> castBindDayScore(entity, resultList));
        }
        return resultList;
    }

    /***
     * 累计均值
     */
    private void cumulateMean(int actDataSetNum, boolean isClassfication) throws ParseException {
        /***日数据***/
        getCumulateMeanData(actDataSetNum, DAY_CYCLE, Arrays.asList(startTime), startTime,1, isClassfication);
        //判断当前执行时间是否是周日
        if(DateUtils.isSunday(startTime)){
            /***周数据***/
            String newStartTime = DateUtils.getPastDate(startTime, WEEK);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getCumulateMeanData(actDataSetNum, WEEK_CYCLE, interval, newStartTime, 7, isClassfication);
        }
        //判断当前执行时间是否是每月最后一天
        if(DateUtils.isMonthLastDay(startTime)) {
            /***月数据***/
            //获取月天数
            String date = DateUtils.getOldMonth(startTime);
            int monthNum = DateUtils.getDaysOfMonth(date);
            String newStartTime = DateUtils.getPastDate(startTime, monthNum - 1);
            //获取时间区间段内所有时间
            List<String> interval = CommonUtil.getIntervalDate(newStartTime, executeTime);
            getCumulateMeanData(actDataSetNum, MONTH_CYCLE, interval, newStartTime, monthNum, isClassfication);
        }
    }

    /***
     * 日、周、月数据(累计均值)
     */
    private void getCumulateMeanData(int actDataSetNum, String cycleType, List<String> interval, String startTime,
                                     Integer daysNum, boolean isClassfication) {
        if(isClassfication){
            /**分类加权评分方式**/
            getClassificationData(cycleType, startTime, actDataSetNum, daysNum);
        }else {
            /**累计均值**/
            //计算数据集上传率
            Double dataSetUploadRate = getDataSetUploadRate(dataSetIdList, startTime, actDataSetNum, daysNum);
            //计算完整性得分（数据集上传率*权重+业务约束性得分*权重）
            getCompletedScore(cycleType, startTime, dataSetUploadRate);
        }
        //构造数据集数据
        getDataSetQualityData(startTime, cycleType);
        //构造数据集采集数据量、实际数据集上传量、为上传天数数据
        ScoreSumListEntity scoreSumListEntity = getOtherData(startTime, interval, cycleType);
        recordList.add(scoreSumListEntity);
        //获取子类规则分类数据
        getChildCategoryData(dataSetIdList, startTime, cycleType);
    }

    private void getClassificationData(String cycleType, String startTime, int actDataSetNum, Integer daysNum) {
        Double dataSetUploadRate = 0.0;
        Double bussScore = 0.0;
        Long successNum = 0L;
        Long failNum = 0L;
        //获取子类规则总权重
        Long allWeight = CommonUtil.getAllWeight(completeAllRuleList);
        //分别计算数据集上传率和业务约束性总得分
        for(RuleCategory ruleCategory : completeAllRuleList) {
            if(ruleCategory.getName().contains(DATASET_KEY)){
                //数据集上传率
                if(scoreType.equals(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue())){
                    //数据集累计均值
                    Double score = 0.0;
                    for(String i : dataSetIdList){
                        score += getDataSetUploadRate(Arrays.asList(i), startTime,actDataSetNum,daysNum);
                    }
                    if(allWeight != 0){
                        dataSetUploadRate = BigDecimal.valueOf(score).divide(new BigDecimal(dataSetIdList.size()), 2, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal(ruleCategory.getWeight() /allWeight)).doubleValue();
                    }
                }else {
                    if(allWeight != 0){
                        dataSetUploadRate = getDataSetUploadRate(dataSetIdList, startTime, actDataSetNum, daysNum)
                            * ruleCategory.getWeight() / allWeight;
                    }
                }
            }else {
                //业务约束性得分
                ScoreSumEntity scoreSumEntity = getBussScore(ruleCategory, startTime, ruleCategory.getScoreType(),
                    ruleCategory.getWeight(), allWeight);
                bussScore = scoreSumEntity.getScore();
                successNum = scoreSumEntity.getSuccessAmount();
                failNum = scoreSumEntity.getFailAmount();
            }
        }
        Double score = dataSetUploadRate + bussScore;
        score = Double.valueOf(new DecimalFormat("#.00").format(score));
        StandardRuleEntity entity = getStandardRuleEntity(startTime, successNum, failNum, score, cycleType);
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(startTime)
            .ruleCategoryScore(score).cycle(cycleType).build(), successNum, failNum, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
    }

    private ScoreSumEntity getBussScore(RuleCategory ruleCategory, String startTime,String scoreType, Long weight, Long allWeight) {
        Double score = 0.0;
        Long successNum = 0L;
        Long failNum = 0L;
        List<Rule> ruleList = ruleCategory.getRules();
        if (CollectionUtils.isNotEmpty(ruleList)) {
            List<String> ruleIdList = ruleList.stream().map(e -> String.valueOf(e.getId())).collect(Collectors.toList());
            if(scoreType.equals(ScoreTypeEnum.DATASET_CUMULATIVE_MEAN.getValue())){
                //数据集累计均值
                Double totalScore = 0.0;
                for(String i : dataSetIdList){
                    List<ScoreSumEntity> scoreSumEntityList = getEachDataSetBussScore(ruleIdList, Arrays.asList(i), startTime);
                    if(CollectionUtils.isNotEmpty(scoreSumEntityList)){
                        ScoreSumEntity scoreSumEntity = scoreSumEntityList.get(0);
                        totalScore += scoreSumEntity.getScore();
                        successNum += scoreSumEntity.getSuccessAmount();
                        failNum += scoreSumEntity.getFailAmount();
                    }
                }
                score = BigDecimal.valueOf(totalScore).divide(new BigDecimal(dataSetIdList.size()), 4, BigDecimal.ROUND_HALF_UP)
                    .multiply(new BigDecimal(100)).doubleValue();
            }else {
                List<ScoreSumEntity> scoreSumEntityList = getEachDataSetBussScore(ruleIdList, dataSetIdList, startTime);
                if(CollectionUtils.isNotEmpty(scoreSumEntityList)){
                    ScoreSumEntity scoreSumEntity = scoreSumEntityList.get(0);
                    score = scoreSumEntity.getScore();
                    successNum = scoreSumEntity.getSuccessAmount();
                    failNum = scoreSumEntity.getFailAmount();
                }
            }
        }
        ScoreSumEntity result = new ScoreSumEntity();
        result.setSuccessAmount(successNum);
        result.setFailAmount(failNum);
        if(allWeight != 0){
            result.setScore(BigDecimal.valueOf(score).multiply(new BigDecimal(weight/allWeight)).doubleValue());
        }else{
            result.setScore(0.0);
        }
        return result;
    }

    private List<ScoreSumEntity> getEachDataSetBussScore(List<String> ruleIdList, List<String> dataSetIdList, String startTime) {
        //查询质控结果汇总数据表，获取各个数据集成功数和失败数
        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime, false, true);
        //处理相同时间的数据集数据
        List<ScoreSumListEntity> scoreSumListEntityList = dealWithAllDataSet(preDataRuleList);
        //计算业务约束性得分
        List<ScoreSumEntity> resultList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(scoreSumListEntityList)){
            scoreSumListEntityList.stream().forEach(entity -> castBindDayScore(entity, resultList));
        }
        return resultList;
    }

    /***
     * 计算完整性得分（数据集上传率*权重+业务约束性得分*权重）
     */
    private void getCompletedScore(String cycleType, String startTime, Double dataSetUploadRate) {
        //查询质控结果汇总数据表，获取各个数据集成功数和失败数
        List<PreDataRule> preDataRuleList = getRuleAndDatasetResult(ruleIdList, dataSetIdList, startTime, false, true);
        //处理相同时间的数据集数据
        List<ScoreSumListEntity> scoreSumListEntityList = dealWithAllDataSet(preDataRuleList);
        //计算业务约束性得分
        List<ScoreSumEntity> resultList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(scoreSumListEntityList)){
            scoreSumListEntityList.stream().forEach(entity -> castBindDayScore(entity, resultList));
        }
        /**计算完整性得分， 完整性评分=数据集上传率*权重+业务约束性得分*权重**/
        //计算完整性得分
        completeScore(startTime, resultList, dataSetUploadRate, cycleType);
    }

    /***
     * 获取数据集上传率
     * @return
     */
    private Double getDataSetUploadRate(List<String> dataSetIdList, String startTime, int actDataSetNum,
                                        Integer daysNum) {
        /**计算数据集上传率**/
        //ES获取数据量统计表数据,获取实际上传数据集数量
        List<DataVolumeStatistics> dataVolumeStatisticsList = getVolumeData(dataSetIdList, startTime, false);
        Set<String> set = new HashSet<>();
        for(DataVolumeStatistics dataVolumeStatistics : dataVolumeStatisticsList){
            if(dataVolumeStatistics.getCountDataAmount() > 0){
                actDataSetNum += 1;
            }
            set.add(dataVolumeStatistics.getCycleDay());
        }
        return new BigDecimal(actDataSetNum).divide(new BigDecimal(dataSetIdList.size()).multiply(new BigDecimal(daysNum)),
            4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
    }

    /***
     * 计算完整性得分
     */
    private void completeScore(String newStartTime, List<ScoreSumEntity> scoreSumEntityList, Double dataUploadRate, String dayCycle) {
        Double score = 0.00;
        Long successNum = 0L;
        Long failNum = 0L;
        Long totalWeight = getTotalWeight();
        for(ScoreSumEntity scoreSumEntity : scoreSumEntityList){
            score = getScore(dataUploadRate, score, totalWeight, scoreSumEntity);
            successNum += scoreSumEntity.getSuccessAmount();
            failNum += scoreSumEntity.getFailAmount();
        }
        score = Double.valueOf(new DecimalFormat("#.00").format(CollectionUtils.isEmpty(scoreSumEntityList) ? score : score/scoreSumEntityList.size()));
        StandardRuleEntity entity = getStandardRuleEntity(newStartTime, successNum, failNum, score, dayCycle);
        RuleCategoryQuality quality = getRuleCategoryQuality(RuleCategoryQuality.RuleCategoryQualityBuilder.aRuleCategoryQuality().startDay(newStartTime)
            .ruleCategoryScore(score).cycle(dayCycle).build(), successNum, failNum, categoryId);
        standardRuleEntityList.add(entity);
        qualityList.add(quality);
    }

    private Long getTotalWeight() {
        Long totalWeight = 0L;
        for(RuleCategory ruleCategory : completeAllRuleList){
            totalWeight += ruleCategory.getWeight() == null ? 0L : ruleCategory.getWeight();
        }
        if(totalWeight == 0){
            // 如果没有配置权重
            totalWeight = 100L;
        }
        return totalWeight;
    }

    private Double getScore(Double dataUploadRate, Double score, Long totalWeight, ScoreSumEntity scoreSumEntity) {
        String ruleId = scoreSumEntity.getRuleId();
        Double total = scoreSumEntity.getScore();
        Double setScore = 0.00;
        Double bindScore = 0.00;
        for(RuleCategory ruleCategory : completeAllRuleList){
            if(ruleCategory.getName().contains(DATASET_KEY)){
                /***数据集上传率最终得分***/
                Long weight = ruleCategory.getWeight() == null ? 50L : ruleCategory.getWeight();
                setScore += new BigDecimal(dataUploadRate).multiply(new BigDecimal(weight))
                    .divide(new BigDecimal(totalWeight), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
            }else if(ruleCategory.getName().contains(BINGD_KEY)){
                /***业务约束性最终得分***/
                Long weight = ruleCategory.getWeight() == null ? 50L : ruleCategory.getWeight();
                List<Rule> ruleList = ruleCategory.getRules();
                for(Rule rule : ruleList){
                    if(ruleId.equals(rule.getId())){
                        bindScore += new BigDecimal(total).multiply(new BigDecimal(weight))
                            .divide(new BigDecimal(totalWeight), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                        break;
                    }
                }
            }else{

            }
        }
        //完整性得分
        score += setScore + bindScore;
        return score;
    }

    private void castBindDayScore(ScoreSumListEntity scoreSumListEntity, List<ScoreSumEntity> resultList) {
        String dayStr = scoreSumListEntity.getCycleDay();
        List<ScoreSumEntity> scoreSumEntityList = scoreSumListEntity.getData();
        Double totalScore = 0.00;
        Long totalSuccessNum = 0L;
        Long totalFailNum = 0L;
        for (ScoreSumEntity entity : scoreSumEntityList) {
            Long successNum = entity.getSuccessAmount();
            Long failNum = entity.getFailAmount();
            //业务约束性符合率
            Double score = new BigDecimal(successNum).divide(new BigDecimal(successNum + failNum),
                4, BigDecimal.ROUND_HALF_UP).doubleValue();
            totalSuccessNum += successNum;
            totalFailNum += failNum;
            totalScore += score;
        }
        //业务约束性得分
        Double businessScore = new BigDecimal(totalScore).divide(new BigDecimal(scoreSumEntityList.size()),
            4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
        ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
        scoreSumEntity.setSuccessAmount(totalSuccessNum);
        scoreSumEntity.setFailAmount(totalFailNum);
        scoreSumEntity.setScore(businessScore);
        scoreSumEntity.setCycleDay(dayStr);
        ScoreSumEntity item = scoreSumEntityList.get(0);
        scoreSumEntity.setDatasetId(item.getDatasetId());
        scoreSumEntity.setStandardId(item.getStandardId());
        scoreSumEntity.setRuleId(item.getRuleId());
        resultList.add(scoreSumEntity);
    }

    private List<ScoreSumListEntity> dealWithAllDataSet(List<PreDataRule> preDataRuleList) {
        List<ScoreSumListEntity> scoreSumListEntityList = new ArrayList<>();
        if(CollectionUtils.isEmpty(preDataRuleList)){
            return scoreSumListEntityList;
        }
        //获取所有时间周期
        List<String> dateList = new ArrayList<>();
        preDataRuleList.stream().forEach(e -> {
            String cycle = e.getCycleDay();
            if(!dateList.contains(cycle)){
                dateList.add(cycle);
            }
        });

        //合并时间相同的数据集数据
        dateList.stream().forEach(cycleDay -> {
            List<ScoreSumEntity> scoreSumEntityList = new ArrayList<>();
            ScoreSumListEntity scoreSumListEntity = new ScoreSumListEntity();
            preDataRuleList.stream().forEach(preDataRule -> {
                if(cycleDay.equals(preDataRule.getCycleDay())){
                    ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
                    scoreSumEntity.setSuccessAmount(preDataRule.getSuccessNum());
                    scoreSumEntity.setFailAmount(preDataRule.getCheckNum());
                    scoreSumEntity.setDatasetId(preDataRule.getDatasetId());
                    scoreSumEntity.setStandardId(preDataRule.getStandardId());
                    scoreSumEntity.setRuleId(preDataRule.getRuleId());
                    scoreSumEntityList.add(scoreSumEntity);
                }
            });
            scoreSumListEntity.setCycleDay(cycleDay);
            scoreSumListEntity.setData(scoreSumEntityList);
            scoreSumListEntityList.add(scoreSumListEntity);
        });
        return scoreSumListEntityList;
    }
}
