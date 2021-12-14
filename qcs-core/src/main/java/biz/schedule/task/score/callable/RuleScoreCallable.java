package biz.schedule.task.score.callable;

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.schedule.task.JobExecutor;
import com.gwi.qcs.core.biz.schedule.task.score.callable.rule.*;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataOrgService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataSourceService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetQualityService;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.clickhouse.PreDataOrg;
import com.gwi.qcs.model.domain.clickhouse.PreDataSource;
import com.gwi.qcs.model.domain.mysql.DataSetQuality;
import com.gwi.qcs.model.domain.mysql.ScoreInstance;
import com.gwi.qcs.model.domain.mysql.ScoreInstanceRule;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapenum.DqRuleEnum;
import com.gwi.qcs.model.mapenum.RecordDataEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class RuleScoreCallable implements Callable<List<StandardRuleEntity>> {

    public static final String WEEK = "week";
    public static final String MONTH = "month";
    public static final String DATASET_GROUP = "dataset_group";
    public static final String DATASET_ID = "DATASET_ID";
    public static final String CYCLE_DAY = "CYCLE_DAY";
    public static final String SOURCE_ID = "SOURCE_ID";
    public static final String STANDARD_ID = "STANDARD_ID";
    public static final String ORG_CODE = "ORG_CODE";

    private final String jobPoolName = "taskPool-for-scoreRule-[%s]";

    PreDataOrgService preDataOrgService;

    PreDataSourceService preDataSourceService;

    DatasetQualityService datasetQualityService;

    DataVolumeStatisticsService dataVolumeStatisticsService;

    ScoreEntity scoreEntity;

    public RuleScoreCallable(ScoreEntity scoreEntity) {
        this.scoreEntity = scoreEntity;
        preDataOrgService = SpringContextUtil.getBean(PreDataOrgService.class);
        preDataSourceService = SpringContextUtil.getBean(PreDataSourceService.class);
        datasetQualityService = SpringContextUtil.getBean(DatasetQualityService.class);
        dataVolumeStatisticsService = SpringContextUtil.getBean(DataVolumeStatisticsService.class);
    }

    @Override
    public List<StandardRuleEntity> call() throws Exception {
        List<Future<Map<String, Object>>> list = new ArrayList<>();
        ScoreInstance scoreInstance = scoreEntity.getScoreInstance();
        JobExecutor excutor = new JobExecutor(StringUtils.replace(jobPoolName, "%s", String.valueOf(scoreInstance.getId())),
            scoreEntity.getScoreRuleThreadpoolSize());
        List<StandardRuleEntity> result;
        Set<String> ruleSet = new HashSet<>();
        try{
            /**规范性规则评分**/
            String scoreTaskId = scoreEntity.getScoreTaskId();
            List<ScoreInstanceRule> standardRuleList = scoreEntity.getStandardRuleList();
            if(CollectionUtils.isNotEmpty(standardRuleList)){
                String categoryId = standardRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = standardRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new StandardRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_STANDARD.name(), DqRuleEnum.STANDARD_RULE.name());
            }

            /**一致性规则评分**/
            List<ScoreInstanceRule> consistencyRuleList = scoreEntity.getConsistencyRuleList();
            if(CollectionUtils.isNotEmpty(consistencyRuleList)){
                String categoryId = consistencyRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = consistencyRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new ConsistencyRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_CONSISTENCY.name(), DqRuleEnum.CONSISTENCY_RULE.name());
            }

            /**完整性规则评分**/
            List<ScoreInstanceRule> completeRuleList = scoreEntity.getCompleteRuleList();
            if(CollectionUtils.isNotEmpty(completeRuleList)){
                String categoryId = completeRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = completeRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new CompleteRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_COMPLETE.name(), DqRuleEnum.COMPLETE_RULE.name());
            }

            /**稳定性规则评分**/
            List<ScoreInstanceRule> stabilityRuleList = scoreEntity.getStabilityRuleList();
            if(CollectionUtils.isNotEmpty(stabilityRuleList)){
                String categoryId = stabilityRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = stabilityRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new StabilityRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_STABILITY.name(), DqRuleEnum.STABILITY_RULE.name());
            }

            /**关联性规则评分**/
            List<ScoreInstanceRule> relevanceRuleList = scoreEntity.getRelevanceRuleList();
            if(CollectionUtils.isNotEmpty(relevanceRuleList)){
                String categoryId = relevanceRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = relevanceRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new RelevanceRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_RELEVANCE.name(), DqRuleEnum.RELEVANCE_RULE.name());
            }

            /**及时性规则评分**/
            List<ScoreInstanceRule> timelinesRuleList = scoreEntity.getTimelinesRuleList();
            if(CollectionUtils.isNotEmpty(timelinesRuleList)){
                String categoryId = timelinesRuleList.get(0).getParentCategoryId();
                //获取规则id
                List<String> ruleIdList = timelinesRuleList.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
                ruleSet.addAll(ruleIdList);
                scoreEntity.setRuleIdList(ruleIdList);
                scoreEntity.setCategoryId(categoryId);
                ScoreEntity param = new ScoreEntity();
                BeanUtils.copyProperties(scoreEntity, param);
                Future<Map<String, Object>> future = excutor.startRun(new TimelinesRuleCallable(param));
                list.add(future);
            }else {
                //清除redis数据
                cleanRedisData(RecordDataEnum.RECORD_QUALITY_TIMELINES.name(), DqRuleEnum.TIMELINES_RULE.name());
            }
            List<Map<String, Object>> mapList = new ArrayList<>();
            for (Future<Map<String, Object>> future : list) {
                mapList.add(future.get());
            }
            /**汇总数据集质量数据**/
            //从redis中获取六大规则数据集质量数据
            List<DataSetQuality> dataSetQualityList = getDataSetQualityList(ruleSet);
            log.info("六大规则数据集质量数据插入ES数据集质量表（DATASET_QUALITY）开始......"
                + " JobId:" + scoreTaskId + ", instanceId:" + scoreInstance.getId());
            //数据集质量汇总数据插入到ES数据集质量表（DATASET_QUALITY）
            if(CollectionUtils.isNotEmpty(dataSetQualityList)){
                datasetQualityService.saveBatch(dataSetQualityList, CommonConstant.CK_SAVE_BATCH_NUM);
            }
            log.info("六大规则数据集质量数据插入ES数据集质量表（DATASET_QUALITY）结束......");

            /***计算综合得分***/
            log.info("计算评分对象综合得分开始......" + " JobId:" + scoreTaskId + ", instanceId:" + scoreInstance.getId());
            result = overallRatings(mapList);
            //从redis中获取六大规则数据集数量和上传等数据
            List<ScoreSumEntity> recordList = getRecordData();
            for(StandardRuleEntity entity : result){
                String cycle = entity.getCycle();
                for(ScoreSumEntity scoreSumEntity : recordList){
                    if(cycle.equals(scoreSumEntity.getCycleDay())){
                        entity.setRecordAmount(scoreSumEntity.getTotalAmount());
                        entity.setHopeDataSetAmount(scoreSumEntity.getHopeDataSetAmount());
                        entity.setRealityDataSetAmount(scoreSumEntity.getRealityDataSetAmount());
                        entity.setMissDataSetAmount(scoreSumEntity.getMissDataSetAmount());
                        entity.setNoUploadDayAmount(scoreSumEntity.getNoUploadDayAmount());
                    }
                }
            }
            //拼接评分对象质量表问题条数数据
            for(StandardRuleEntity entity : result){
                String cycle = entity.getCycle();
                Long totalProblemNum = 0L;
                for(DataSetQuality dataSetQuality : dataSetQualityList){
                    if(cycle.equals(dataSetQuality.getCycle())){
                        totalProblemNum += dataSetQuality.getCheckFailAmount();
                    }
                }
                entity.setProblemNum(totalProblemNum);
            }
            log.info("计算评分对象综合得分结束......" + " JobId:" + scoreTaskId + ", instanceId:" + scoreInstance.getId() + ", data:共{}条", result.size());
        }catch (Exception e){
            log.error("计算六大规则评分失败！", e);
            throw new Exception(e);
        }finally {
            excutor.shutdown();
        }
        return result;
    }

    private Map<String, Long> getTotalRecordCount(Map<String, List<StandardRuleEntity>> groupMap){
        Map<String, Long> map = Maps.newHashMap();
        for(String item : groupMap.keySet()){
            StandardRuleEntity entity = groupMap.get(item).get(0);
            String key = entity.getCycle();
            if(! map.containsKey(key)){
                map.put(key, getTotalRecordCount(entity.getStartTime(), entity.getEndTime()));
            }
        }
        return map;
    }


    private Long getTotalRecordCount(String startDay, String endDay){
        List<StandardDataSet> standardDataSetList = scoreEntity.getStandardDataSetList();
        //数据集id
        List<String> dataSetIdList = standardDataSetList.stream().map(e -> String.valueOf(e.getDataSetId())).distinct()
            .collect(Collectors.toList());
        ScoreInstance scoreInstance = scoreEntity.getScoreInstance();
        List<String> orgCodeList = new ArrayList<>();
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
        List<String> dataSourceList = new ArrayList<>();
        if (StringUtils.isNotEmpty(scoreInstance.getDatasourceId())) {
            dataSourceList = Arrays.asList(scoreInstance.getDatasourceId().split(CommonConstant.COMMA));
        }
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
        lambdaQueryWrapper.between(DataVolumeStatistics::getCycleDay, startDay, endDay);
        return dataVolumeStatisticsService.list(lambdaQueryWrapper).stream()
            .mapToLong(DataVolumeStatistics::getDataAmount).sum();
    }

    private List<ScoreSumEntity> getRecordData() {
        List<ScoreSumEntity> result = new ArrayList<>();
        RedisRepository redisRepository = scoreEntity.getRedisRepository();
        Long scoreInstanceId = scoreEntity.getScoreInstance().getId();
        String standRecordKey = RecordDataEnum.RECORD_QUALITY_STANDARD.name() + scoreInstanceId;
        List<ScoreSumListEntity> standRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(standRecordKey);
        redisRepository.del(standRecordKey);
        String conRecordKey = RecordDataEnum.RECORD_QUALITY_CONSISTENCY.name() + scoreInstanceId;
        List<ScoreSumListEntity> conRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(conRecordKey);
        redisRepository.del(conRecordKey);
        String comRecordKey = RecordDataEnum.RECORD_QUALITY_COMPLETE.name() + scoreInstanceId;
        List<ScoreSumListEntity> comRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(comRecordKey);
        redisRepository.del(comRecordKey);
        String staRecordKey = RecordDataEnum.RECORD_QUALITY_STABILITY.name() + scoreInstanceId;
        List<ScoreSumListEntity> stabRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(staRecordKey);
        redisRepository.del(staRecordKey);
        String releRecordKey = RecordDataEnum.RECORD_QUALITY_RELEVANCE.name() + scoreInstanceId;
        List<ScoreSumListEntity> releRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(releRecordKey);
        redisRepository.del(releRecordKey);
        String timeRecordKey = RecordDataEnum.RECORD_QUALITY_TIMELINES.name() + scoreInstanceId;
        List<ScoreSumListEntity> timeRecordArray = (List<ScoreSumListEntity>) redisRepository
            .get(timeRecordKey);
        redisRepository.del(timeRecordKey);

        List<ScoreSumListEntity> list = new ArrayList<>();
        summaryRecordData(standRecordArray, list);
        summaryRecordData(conRecordArray, list);
        summaryRecordData(comRecordArray, list);
        summaryRecordData(stabRecordArray, list);
        summaryRecordData(releRecordArray, list);
        summaryRecordData(timeRecordArray, list);

        //根据日、周、月分组
        Map<String, List<ScoreSumListEntity>> map = list.stream().collect(
            Collectors.groupingBy(ScoreSumListEntity::getCycleDay));
        List<String> dataSetIdList = scoreEntity.getStandardDataSetList().stream()
            .map(e -> String.valueOf(e.getDataSetId())).collect(Collectors.toList());
        map.forEach((cycleDay, tList) -> {
            List<ScoreSumEntity> array = new ArrayList<>();
            for (ScoreSumListEntity scoreSumListEntity : tList) {
                List<ScoreSumEntity> scoreSumEntityList = scoreSumListEntity.getData();
                Map<String, List<ScoreSumEntity>> dataSetMap = scoreSumEntityList.stream()
                    .collect(Collectors.groupingBy(ScoreSumEntity::getDatasetId));
                dataSetMap.forEach((dataSetId, setList) -> {
                    getArray(result, dataSetIdList, cycleDay, array, setList);
                });
            }
            if (CollectionUtils.isNotEmpty(array)) {
                Long recordNum = 0L;
                Integer realDataSetNum = 0;
                for (ScoreSumEntity scoreSumEntity : array) {
                    recordNum += scoreSumEntity.getTotalAmount();
                    realDataSetNum += scoreSumEntity.getRealityDataSetAmount();
                }
                ScoreSumEntity entity = array.get(0);
                ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
                scoreSumEntity.setTotalAmount(recordNum);
                scoreSumEntity.setHopeDataSetAmount(dataSetIdList.size());
                scoreSumEntity.setRealityDataSetAmount(realDataSetNum > dataSetIdList.size() ? dataSetIdList.size() : realDataSetNum);
                scoreSumEntity.setNoUploadDayAmount(entity.getNoUploadDayAmount());
                scoreSumEntity.setMissDataSetAmount(Math.max(dataSetIdList.size() - realDataSetNum, 0));
                scoreSumEntity.setCycleDay(entity.getCycleDay());
                result.add(scoreSumEntity);
            }
        });
        return result;
    }

    private void getArray(List<ScoreSumEntity> result, List<String> dataSetIdList, String cycleDay,
                          List<ScoreSumEntity> array, List<ScoreSumEntity> setList) {
        if (setList.size() == dataSetIdList.size()) {
            Long recordNum = setList.stream().mapToLong(entity ->
                null == entity.getTotalAmount() ? 0L : entity.getTotalAmount()).sum();
            Integer realityDataSetAmount = setList.stream().mapToInt(entity ->
                null == entity.getRealityDataSetAmount() ? 0 : entity.getRealityDataSetAmount()).sum();
            ScoreSumEntity entity = setList.get(0);
            ScoreSumEntity scoreSumEntity = new ScoreSumEntity();
            scoreSumEntity.setTotalAmount(recordNum);
            scoreSumEntity.setHopeDataSetAmount(dataSetIdList.size());
            scoreSumEntity.setRealityDataSetAmount(realityDataSetAmount);
            scoreSumEntity.setNoUploadDayAmount(entity.getNoUploadDayAmount());
            scoreSumEntity.setMissDataSetAmount(0);
            scoreSumEntity.setCycleDay(cycleDay);
            result.add(scoreSumEntity);
        } else {
            ScoreSumEntity entity = setList.get(0);
            entity.setCycleDay(cycleDay);
            if (!array.contains(entity)) {
                array.add(entity);
            }
        }
    }

    private void summaryRecordData(List<ScoreSumListEntity> scoreSumListEntityList, List<ScoreSumListEntity> list) {
        if(CollectionUtils.isNotEmpty(scoreSumListEntityList)){
            for (ScoreSumListEntity scoreSumListEntity : scoreSumListEntityList) {
                if (!list.contains(scoreSumListEntity)) {
                    list.add(scoreSumListEntity);
                }
            }
        }
    }


    private List<DataSetQuality> getDataSetQualityList(Set<String> ruleSet) {
        List<DataSetQuality> result = new ArrayList<>();
        Map<String, Long> weekMap = Maps.newHashMap();
        Map<String, Long> mouthMap = Maps.newHashMap();
        RedisRepository redisRepository = scoreEntity.getRedisRepository();
        ScoreInstance scoreInstance = scoreEntity.getScoreInstance();
        Long scoreInstanceId = scoreInstance.getId();
        String standardKey = ParentScoreCallable.DATA_SET_QUALITY_STANDARD + scoreInstanceId;
        List<DataSetQuality> standardList = (List<DataSetQuality>) redisRepository
            .get(standardKey);
        redisRepository.del(standardKey);
        addRecordAmount(standardList, weekMap, mouthMap);
        String consistencyKey = ParentScoreCallable.DATA_SET_QUALITY_CONSISTENCY + scoreInstanceId;
        List<DataSetQuality> consistencyList = (List<DataSetQuality>) redisRepository
            .get(consistencyKey);
        redisRepository.del(consistencyKey);
        addRecordAmount(consistencyList, weekMap, mouthMap);
        String completeKey = ParentScoreCallable.DATA_SET_QUALITY_STANDARD + scoreInstanceId;
        List<DataSetQuality> completeList = (List<DataSetQuality>) redisRepository
            .get(completeKey);
        redisRepository.del(completeKey);
        addRecordAmount(completeList, weekMap, mouthMap);
        String stabilityKey = ParentScoreCallable.DATA_SET_QUALITY_STABILITY + scoreInstanceId;
        List<DataSetQuality> stabilityList = (List<DataSetQuality>) redisRepository
            .get(stabilityKey);
        redisRepository.del(stabilityKey);
        addRecordAmount(stabilityList, weekMap, mouthMap);
        String relevanceKey = ParentScoreCallable.DATA_SET_QUALITY_RELEVANCE + scoreInstanceId;
        List<DataSetQuality> relevanceList = (List<DataSetQuality>) redisRepository
            .get(relevanceKey);
        redisRepository.del(relevanceKey);
        addRecordAmount(relevanceList, weekMap, mouthMap);
        String timelinesKey = ParentScoreCallable.DATA_SET_QUALITY_TIMELINES + scoreInstanceId;
        List<DataSetQuality> timelinesList = (List<DataSetQuality>) redisRepository
            .get(timelinesKey);
        redisRepository.del(timelinesKey);
        addRecordAmount(timelinesList, weekMap, mouthMap);

        //各个规则数据集质量数据根据日、周、月汇总
        List<DataSetQuality> setList = new ArrayList<>();
        summaryQualityData(standardList, setList);
        summaryQualityData(consistencyList, setList);
        summaryQualityData(completeList, setList);
        summaryQualityData(stabilityList, setList);
        summaryQualityData(relevanceList, setList);
        summaryQualityData(timelinesList, setList);

        if (CollectionUtils.isEmpty(setList)) {
            return result;
        }
        //根据标准版本流水号、数据来源ID，表名，日、周、月分组汇总数据
        Map<String, List<DataSetQuality>> map = setList.stream().collect(Collectors.groupingBy(a -> {
            return a.getStandardId() + "&" + a.getSourceId() + "&" + a.getDataSetCode() + "&" + a.getCycle();
        }));
        try {
            map.forEach((key, tlist) -> {
                Long checkTotal = tlist.stream().mapToLong(DataSetQuality::getCheckTimes).sum();
                Long fail = tlist.stream().mapToLong(DataSetQuality::getCheckFailTimes).sum();
                DataSetQuality entity = tlist.get(0);
                Long recordTotal = entity.getRecordAmount();
                if(WEEK.equals(entity.getCycle())){
                    recordTotal = weekMap.get(getKey(entity));
                }else if ( MONTH.equals(entity.getCycle())){
                    recordTotal = mouthMap.get(getKey(entity));
                }else{

                }

                long proNum = 0L;
                if(StringUtils.isNotEmpty(entity.getSourceId())){
                    QueryWrapper<PreDataSource> queryWrapper = new QueryWrapper<>();
                    LambdaQueryWrapper<PreDataSource> lambdaQueryWrapper = queryWrapper.lambda();
                    lambdaQueryWrapper.eq(PreDataSource::getDatasetId, entity.getDataSetId());
                    lambdaQueryWrapper.eq(PreDataSource::getStandardId, entity.getStandardId());
                    lambdaQueryWrapper.eq(PreDataSource::getSourceId, entity.getSourceId());
                    queryWrapper.between(CommonConstant.CYCLE_DAY_UPPER, entity.getStartDay(), entity.getEndDay());
                    proNum = preDataSourceService.list(queryWrapper).stream().mapToLong(PreDataSource::getProblemNum).sum();
                }else if(StringUtils.isNotEmpty(scoreInstance.getOrgCode())){
                    QueryWrapper<PreDataOrg> queryWrapper = new QueryWrapper<>();
                    LambdaQueryWrapper<PreDataOrg> lambdaQueryWrapper = queryWrapper.lambda();
                    lambdaQueryWrapper.eq(PreDataOrg::getDatasetId, entity.getDataSetId());
                    lambdaQueryWrapper.eq(PreDataOrg::getStandardId, entity.getStandardId());
                    queryWrapper.in(CommonConstant.ORG_CODE_UPPER,
                        Arrays.asList(scoreInstance.getOrgCode().split(CommonConstant.COMMA)));
                    queryWrapper.between(CommonConstant.CYCLE_DAY_UPPER, entity.getStartDay(), entity.getEndDay());
                    proNum = preDataOrgService.list(queryWrapper).stream().mapToLong(PreDataOrg::getProblemNum).sum();
                }else{

                }
                log.info("评分对象ID：" + scoreInstance.getId() + "，数据集ID:" + entity.getDataSetId() + "问题条数: " + proNum);
                DataSetQuality quality = new DataSetQuality();
                quality.setScoreInstanceId(entity.getScoreInstanceId());
                quality.setCycle(key.split("&")[3]);
                quality.setStartDay(entity.getStartDay().split("T")[0]);
                quality.setEndDay(entity.getEndDay().split("T")[0]);
                quality.setStandardId(entity.getStandardId());
                quality.setSourceId(entity.getSourceId());
                quality.setDataSetCode(entity.getDataSetCode());
                quality.setDataSetId(entity.getDataSetId());
                quality.setCycleDay(DateUtils.string2Date(quality.getStartDay(), DateUtils.DEFINE_YYYY_MM_DD));
                quality.setRecordAmount(recordTotal);
                quality.setCheckFailAmount(proNum);
                quality.setCheckFailRate(Double.valueOf(String.format("%.2f", divide(proNum, recordTotal) * 100)));
                quality.setCheckTimes(checkTotal);
                quality.setCheckFailTimes(fail);
                quality.setCheckFailTimesRate(Double.valueOf(String.format("%.2f", divide(fail, checkTotal) * 100)));
                quality.setCreateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                result.add(quality);
            });
        } catch (Exception e) {
            log.error("查询质控问题数据失败！", e);
        }
        return result;
    }

    private void addRecordAmount(List<DataSetQuality> standardList, Map<String, Long> weekMap, Map<String, Long> mouthMap) {
        boolean isAddWeek = weekMap.isEmpty();
        boolean isAddMonth = mouthMap.isEmpty();
        if(CollectionUtils.isNotEmpty(standardList)){
            for(DataSetQuality dataSetQuality : standardList){
                String cycle = dataSetQuality.getCycle();
                String key = getKey(dataSetQuality);
                Long recordAmount = dataSetQuality.getRecordAmount() == null ? 0 : dataSetQuality.getRecordAmount();
                if(isAddWeek && WEEK.equals(cycle)){
                    weekMap.put(key, weekMap.getOrDefault(key, 0L) + recordAmount);
                }else if(isAddMonth && MONTH.equals(cycle)){
                    mouthMap.put(key, mouthMap.getOrDefault(key, 0L) + recordAmount);
                }else{

                }
            }
        }
    }

    private String getKey(DataSetQuality dataSetQuality) {
        return dataSetQuality.getStartDay() + "_" + dataSetQuality.getScoreInstanceId()  + "_" + dataSetQuality.getSourceId() + "_" + dataSetQuality.getDataSetId();
    }

    private void summaryQualityData(List<DataSetQuality> list, List<DataSetQuality> setList) {
        if (null == list || list.size() <= 0) {
            return;
        }
        setList.addAll(list);
    }

    private List<StandardRuleEntity> overallRatings(List<Map<String, Object>> list) {
        List<StandardRuleEntity> result = new ArrayList<>();
        if (CollUtil.isEmpty(list)) {
            return result;
        }
        Long standardWeight = scoreEntity.getStandardWeight();
        Long consistencyWeight = scoreEntity.getConsistencyWeight();
        Long completeWeight = scoreEntity.getCompleteWeight();
        Long stabilityWeight = scoreEntity.getStabilityWeight();
        Long relevanceWeight = scoreEntity.getRelevanceWeight();
        Long timelinesWeight = scoreEntity.getTimelinesWeight();
        Long totalWeight = standardWeight + consistencyWeight + completeWeight + stabilityWeight + relevanceWeight + timelinesWeight;
        totalWeight = totalWeight == 0 ? 100L : totalWeight;
        //计算每个规则乘以权重之后，日、周、月的最终得分
        List<StandardRuleEntity> mapList = new ArrayList<>();
        for (Map<String, Object> map : list) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                RecordDataEnum recordDataEnum = RecordDataEnum.valueOf(entry.getKey());
                switch (recordDataEnum) {
                    case RECORD_QUALITY_STANDARD:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, standardWeight, totalWeight);
                        break;
                    case RECORD_QUALITY_CONSISTENCY:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, consistencyWeight, totalWeight);
                        break;
                    case RECORD_QUALITY_COMPLETE:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, completeWeight, totalWeight);
                        break;
                    case RECORD_QUALITY_STABILITY:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, stabilityWeight, totalWeight);
                        break;
                    case RECORD_QUALITY_RELEVANCE:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, relevanceWeight, totalWeight);
                        break;
                    case RECORD_QUALITY_TIMELINES:
                        getResultScore((List<StandardRuleEntity>) entry.getValue(), mapList, timelinesWeight, totalWeight);
                        break;
                    default:
                        break;
                }
            }
        }
        //六大规则最终得分根据日、周、月分组，并计算日、周、月的综合得分
        Map<String, List<StandardRuleEntity>> groupMap = mapList.stream().collect(Collectors.groupingBy(StandardRuleEntity::getCycle));
        Map<String, Long> countMap = getTotalRecordCount(groupMap);
        groupMap.forEach((key, entityList) -> {
            Double score = entityList.stream().mapToDouble(ScoreResultBaseEntity::getScore).sum();
            Long success = entityList.stream().mapToLong(ScoreResultBaseEntity::getSuccessNum).sum();
            Long fail = entityList.stream().mapToLong(ScoreResultBaseEntity::getFailNum).sum();
            StandardRuleEntity entity = entityList.get(0);
            StandardRuleEntity standardRuleEntity = new StandardRuleEntity();
            standardRuleEntity.setCycle(key);
            standardRuleEntity.setSuccessNum(success);
            standardRuleEntity.setFailNum(fail);
            standardRuleEntity.setScore(countMap.get(key) == 0 ? 0 : score);
            standardRuleEntity.setCheckTotal(success + fail);
            standardRuleEntity.setScoreTaskId(entity.getScoreTaskId());
            standardRuleEntity.setScoreInstance(entity.getScoreInstance());
            standardRuleEntity.setStartTime(entity.getStartTime());
            standardRuleEntity.setEndTime(entity.getEndTime());
            result.add(standardRuleEntity);
        });
        return result;
    }

    private void getResultScore(List<StandardRuleEntity> list, List<StandardRuleEntity> mapList, Long weight, Long totalWeight) {
        Map<String, List<StandardRuleEntity>> groupMap = list.stream().collect(Collectors.groupingBy(StandardRuleEntity::getCycle));
        //统计每个规则日、周、月汇总数据
        groupMap.forEach((key, entityList) -> {
            Double score = entityList.stream().mapToDouble(ScoreResultBaseEntity::getScore).sum();
            Long success = entityList.stream().mapToLong(ScoreResultBaseEntity::getSuccessNum).sum();
            Long fail = entityList.stream().mapToLong(ScoreResultBaseEntity::getFailNum).sum();
            StandardRuleEntity entity = entityList.get(0);
            StandardRuleEntity standardRuleEntity = new StandardRuleEntity();
            standardRuleEntity.setCycle(key);
            standardRuleEntity.setSuccessNum(success);
            standardRuleEntity.setFailNum(fail);
            BigDecimal decimal = BigDecimal.valueOf(score * weight / totalWeight);
            standardRuleEntity.setScore(decimal.setScale(2,   BigDecimal.ROUND_HALF_UP).doubleValue());
            standardRuleEntity.setCheckTotal(success + fail);
            standardRuleEntity.setScoreTaskId(entity.getScoreTaskId());
            standardRuleEntity.setScoreInstance(entity.getScoreInstance());
            standardRuleEntity.setStartTime(entity.getStartTime());
            standardRuleEntity.setEndTime(entity.getEndTime());
            mapList.add(standardRuleEntity);
        });
    }

    private void cleanRedisData(String recordName, String dataSetRedisName) {
        RedisRepository redisRepository = scoreEntity.getRedisRepository();
        Long scoreInstanceId = scoreEntity.getScoreInstance().getId();
        redisRepository.set(recordName + scoreInstanceId, null);
        redisRepository.set(dataSetRedisName.toLowerCase() + scoreInstanceId, null);
    }

    private Double divide(Long divisor, Long dividend) {
        if (divisor == null || dividend == null || dividend == 0) {
            return 0.0;
        } else {
            return (divisor * 1.0000 / dividend);
        }
    }
}
