package biz.service;

import cn.afterturn.easypoi.excel.ExcelExportUtil;
import cn.afterturn.easypoi.excel.entity.TemplateExportParams;
import cn.hutool.core.collection.CollectionUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.Page;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.greatwall.component.value.dto.common.CommonResponse;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.FailResultInfoService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataRuleService;
import com.gwi.qcs.core.biz.service.dao.mysql.*;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.clickhouse.FailResultInfo;
import com.gwi.qcs.model.domain.clickhouse.PreDataRule;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.ResultStatisticVO;
import com.gwi.qcs.model.entity.PageSearch;
import com.gwi.qcs.model.entity.RuleStaticDetailEntity;
import com.gwi.qcs.model.entity.RuleStaticPojo;
import com.gwi.qcs.model.mapper.mysql.*;
import com.gwi.qcs.service.api.dto.ExportProblemReq;
import com.gwi.qcs.service.api.dto.QualityProblemDetailReq;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class RuleStaticDetailService {

    private Logger log = LoggerFactory.getLogger(RuleStaticDetailService.class);

    public static final String STANDARD = "0";

    public static final String UNIQUE = "1";

    public static final String CONSISTENCY = "2";

    public static final String DATASETUP = "3";

    public static final String BUSINESS = "4";

    public static final String STABILITY = "5";

    public static final String RELEVANCE = "6";

    public static final String TIMELINES = "7";

    public static final String DATA = "data";

    public static final String TOTAL = "totalCount";

    public static final String PAGE_SIZE = "pageSize";

    public static final String TOTAL_PAGE = "totalPage";

    public static final String CURR_PAGE = "currPage";

    private static final String ORGCODE_DATA = "orgCode_";

    private static final String SOURCEID_DATA = "sourceId_";

    /**
     * dataset表查询结果临时缓存起来，避免在for循环中遍历查数据库表
     */
    private static HashMap<String, Dataset> DATASET_CACHE_MAP = Maps.newHashMap();

    @Autowired
    ScoreDataSetMapper scoreDataSetMapper;

    @Autowired
    DatasetMapper dataSetMapper;

    @Autowired
    DatasetFieldMapper datasetFieldMapper;

    @Autowired
    ScoreInstanceService scoreInstanceService;

    @Autowired
    RuleMapper ruleMapper;

    @Autowired
    RuleCategoryMapper ruleCategoryMapper;

    @Autowired
    RuleService ruleService;

    @Autowired
    RuleCategoryMapper categoryMapper;

    @Autowired
    ParameterMapper parameterMapper;

    @Autowired
    RedisRepository redisRepository;

    @Autowired
    ScoreInstanceMapper scoreInstanceMapper;

    @Autowired
    DmQueryService dmQueryService;

    @Autowired
    ParameterService parameterService;

    @Autowired
    FailResultInfoService failResultInfoService;

    @Autowired
    PreDataRuleService preDataRuleService;

    @Autowired
    DataVolumeStatisticsService dataVolumeStatisticsService;

    @Autowired
    DatasetService datasetService;

    @Autowired
    DatasetFiledService datasetFiledService;

    public Map<String, Object> getRuleStaticDetail(RuleStaticPojo ruleStaticPojo) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        String ruleId = ruleStaticPojo.getRuleId();
        String ruleCategoryId = ruleStaticPojo.getRuleCategoryId();
        List<String> datasetIds = ruleStaticPojo.getDatasetIds();
        String startDate = ruleStaticPojo.getStartDate();
        String endDate = ruleStaticPojo.getEndDate();
        String type = ruleStaticPojo.getType();
        Integer pageIndex = ruleStaticPojo.getPage();
        Integer pageSize = ruleStaticPojo.getSize();
        Page page = new Page(pageIndex, pageSize);
        List<String> orgCode = ruleStaticPojo.getOrgCode();
        String areaCode = ruleStaticPojo.getAreaCode();
        String scoreInstanceId = ruleStaticPojo.getScoreInstanceId();
        String standardId = ruleStaticPojo.getStandardId();
        String sourceId = "";
        if (StringUtils.isNotBlank(scoreInstanceId)) {
            //评分对象获取数据源和机构编码
            QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
            queryWrapper.in("id", scoreInstanceId);
            ScoreInstance instance = scoreInstanceMapper.selectOne(queryWrapper);
            orgCode = orgCode == null ? new ArrayList<>() : orgCode;
            if (CommonConstant.ORG_JBYL.equals(instance.getName())) {
                orgCode.addAll(scoreInstanceService.getChildrenOrgCodeList(Long.parseLong(scoreInstanceId)));
            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(instance.getScoreTag())) {
                orgCode.add(instance.getOrgCode());
                if(ruleStaticPojo.getOrgCode().isEmpty()){
                    ruleStaticPojo.setOrgCode(orgCode);
                }
            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(instance.getScoreTag())) {
                sourceId = instance.getDatasourceId();
                ruleStaticPojo.setSourceId(sourceId);
            } else {

            }
        }else{
            if(StringUtils.isNotBlank(areaCode)){
                List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaCode(areaCode);
                for (RrsOrganization organization : orgList) {
                    orgCode.add(organization.getOrgCode());
                }
            }
        }

        List<String> jobId = new ArrayList<>();
        Parameter parameter = new Parameter();
        parameter.setCode("ES_MAX_PAGESIZE");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        PageSearch pageSearch = null;
        switch (type) {
            //规范性
            case STANDARD:
                result = getStandardData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    false, sourceId, standardId, orgCode,page);
                break;
            //规范性中的唯一性
            case UNIQUE:
                pageSearch = getUniqueData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    orgCode, sourceId, standardId, page);
                if(pageSearch != null){
                    result = pageSearch.getRuleStaticDetailEntityList();
                }
                break;
            //一致性
            case CONSISTENCY:
                result = getStandardData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    false, sourceId, standardId, orgCode, page);
                break;
            //完整性中数据集上传率
            case DATASETUP:
                result = getDataSetUpData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    false, sourceId, standardId, page);
                break;
            //完整性中业务约束性
            case BUSINESS:
                result = getBusinessData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    false, sourceId, standardId, page);
                break;
            //稳定性
            case STABILITY:
                pageSearch = getStabilityData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    orgCode,sourceId, standardId, page);
                if(pageSearch != null){
                    result = pageSearch.getRuleStaticDetailEntityList();
                }
                break;
            //关联性
            case RELEVANCE:
                result = getRelevanceDataByCK(ruleStaticPojo, false , page);
                break;
            //及时性
            case TIMELINES:
                result = getTimelinesData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    false, sourceId, standardId, page);
                break;
            default:
                break;
        }


        //按检出问题数排序
        boolean isTrue = STANDARD.equals(type) || CONSISTENCY.equals(type) || BUSINESS.equals(type) || TIMELINES.equals(type);
        if(result != null && isTrue){
            result = result.stream().sorted(Comparator.comparing(RuleStaticDetailEntity::getProblemNum).reversed()).collect(Collectors.toList());
        }

        Map<String, Object> map = new HashMap<>();
        map.put(TOTAL, page.getTotal());
        map.put(PAGE_SIZE, pageSize);
        map.put(CURR_PAGE, pageIndex);
        map.put(TOTAL_PAGE, page.getPages());
        map.put(DATA, result);
        return map;
    }


    /**
     * 根据数据集id获取数据集实例
     *
     * @param ids
     * @return
     */
    private List<Dataset> getDataSetListByBatchIds(List<String> ids) {
        List<Long> datasetIds = new ArrayList<>();
        if (CollectionUtil.isEmpty(ids)) {
            return Lists.newArrayList();
        }

        for (String datasetId : ids) {
            datasetIds.add(Long.parseLong(datasetId));
        }
        return dataSetMapper.getDataSetByBatchIds(datasetIds);
    }


    /**
     * 添加数据集信息到结果集
     */
    private void setDataSetName(RuleStaticDetailEntity entity, Map<Long, String> dataSetNameMap, String datasetId, String datasetCode) {
        //数据集信息
        entity.setMetaSetCode(datasetCode);

        String dataSetName = StringUtils.EMPTY;
        Long dataSetId = Long.parseLong(datasetId);
        if (dataSetId != null) {
            if (dataSetNameMap.containsKey(dataSetId)) {
                dataSetName = dataSetNameMap.get(dataSetId);
            } else {
                Dataset dataSet = dataSetMapper.getDataBySetId(dataSetId, CommonConstant.SOURCE_TYPE_INSTANCE);
                if (dataSet != null) {
                    dataSetName = dataSet.getMetasetName();
                }
                dataSetNameMap.put(dataSetId, dataSetName);
            }
        }
        entity.setDataSetName(dataSetName);
    }


    /***
     * 规范性、一致性数据详情
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param sourceId
     * @return
     */
    private List<RuleStaticDetailEntity> getStandardData(List<String> datasetIds,
                                                         String startDate, String endDate, List<String> jobId, String ruleId, String ruleCategoryId,
                                                         Boolean isExport, String sourceId, String standardId, List<String> orgCode, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //获取质控结果表数据集字段信息、校验次数、检出问题数（校验失败次数）
        PreDataRule statistic = new PreDataRule();
        statistic.setRuleId(ruleId);
        statistic.setCategoryId(ruleCategoryId);
        statistic.setSourceId(sourceId);
        statistic.setStandardId(standardId);
        List<PreDataRule> resultStatisticList = preDataRuleService.findPreDataRules(statistic, datasetIds, startDate, endDate, orgCode, page);
        if (CollectionUtils.isEmpty(resultStatisticList)) {
            return result;
        }

        if(page != null){
            page.setTotal(preDataRuleService.getPageInfo().getTotal());
            page.setPages(preDataRuleService.getPageInfo().getPages());
        }

        Map<Long, String> fieldNameMap = Maps.newHashMap();
        for (PreDataRule resultStatistic : resultStatisticList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            // 补充数据集信息
            entity.setMetaSetCode(datasetService.getCodeById(resultStatistic.getDatasetId()));
            entity.setDataSetName(datasetService.getNameById(resultStatistic.getDatasetId()));
            entity.setFieldName(resultStatistic.getDatasetItemCode());
            entity.setMetaDataName(datasetFiledService.getNameById(resultStatistic.getDatasetItemId()));
            entity.setFieldType(datasetFiledService.getTypeOrDesById(resultStatistic.getDatasetItemId(), true));
            entity.setFieldDesc(datasetFiledService.getTypeOrDesById(resultStatistic.getDatasetItemId(), false));
            Long successNum = resultStatistic.getValidateNum() - resultStatistic.getCheckNum();
            Long failNum = resultStatistic.getCheckNum();
            entity.setCheckCount(successNum + failNum);
            entity.setProblemNum(failNum);
            if (isExport) {
                entity.setProblemPercent(Double.valueOf(String.format("%.2f", (failNum * 1.0 / (successNum + failNum)) * 100)));
            } else {
                entity.setProblemPercent(Double.valueOf(String.format("%.4f", (failNum * 1.0 / (successNum + failNum)))));
            }
            entity.setJobId(jobId);
            entity.setRuleId(resultStatistic.getRuleId());
            entity.setStandardId(resultStatistic.getStandardId());
            entity.setDataSetId(resultStatistic.getDatasetId());
            entity.setDatasetItemId(resultStatistic.getDatasetItemId());
            entity.setCategoryId(resultStatistic.getCategoryId());
            setFieldInfo(fieldNameMap, resultStatistic, entity);
            result.add(entity);
        }
        return result;
    }

    /***
     * 规范性中的唯一性数据详情
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param jobId
     * @param orgCode
     * @param sourceId
     * @return
     */
    private PageSearch getUniqueData(List<String> datasetIds, String startDate, String endDate,
                                     List<String> jobId, String ruleId, String ruleCategoryId, List<String> orgCode,
                                     String sourceId, String standardId, Page page) {
        PageSearch resultPageSearch = new PageSearch();
        List<RuleStaticDetailEntity> result = new ArrayList<>();

        FailResultInfo failResultInfo = new FailResultInfo();
        failResultInfo.setSourceId(sourceId);
        failResultInfo.setRuleId(ruleId);
        failResultInfo.setStandardId(standardId);
        failResultInfo.setCategoryId(ruleCategoryId);
        List<FailResultInfo> resultSampleList = failResultInfoService.failResultInfos(failResultInfo, datasetIds, startDate, endDate, orgCode, page);
        if (CollectionUtils.isEmpty(resultSampleList)) {
            return resultPageSearch;
        }
        if(page != null){
            page.setTotal(preDataRuleService.getPageInfo().getTotal());
            page.setPages(preDataRuleService.getPageInfo().getPages());
        }

        Map<String, String> primaryMap = Maps.newHashMap();
        for (FailResultInfo item : resultSampleList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            //字段信息
            String dataSetId = item.getDatasetId();
            String primaryKey = StringUtils.EMPTY;
            if (primaryMap.containsKey(dataSetId)) {
                primaryKey = primaryMap.get(dataSetId);
            } else {
                List<DatasetField> fieldList = datasetFieldMapper.getPKDataByDataSetId(Long.valueOf(dataSetId));
                if (CollectionUtils.isNotEmpty(fieldList)) {
                    List<DatasetField> pkList = fieldList.stream().filter(e -> "1".equals(e.getIsKey())).collect(Collectors.toList());
                    List<String> nameList = pkList.stream().map(e -> e.getElementName()).collect(Collectors.toList());
                    primaryKey = String.join(",", nameList);
                }
                primaryMap.put(dataSetId, primaryKey);
            }
            entity.setPrimaryKey(primaryKey);
            entity.setPrimaryKeyValue(item.getPkValue());
            entity.setRepeatNum(item.getVerifyResult());
            entity.setDataUpTime(item.getCycleDay());
            entity.setDataQCSTime(item.getCreationTime());
            entity.setJobId(jobId);
            entity.setStandardId(item.getStandardId());
            entity.setMetaSetCode(item.getDatasetCode());
            entity.setDataSetId(item.getDatasetId());
            entity.setDataSetName(item.getDatasetName());
            entity.setDatasetItemId(item.getDatasetItemId());
            entity.setCategoryId(item.getCategoryId());
            result.add(entity);
        }
        resultPageSearch.setRuleStaticDetailEntityList(result);
        resultPageSearch.setTotal(resultSampleList.size());
        return resultPageSearch;
    }

    public Map<String, Object> getAll(QualityProblemDetailReq qualityProblemDetailReq, ScoreInstance scoreInstance) {
        qualityProblemDetailReq.setDqObj(scoreInstanceMapper.getStandardId(qualityProblemDetailReq.getDqObj()));
        Map<String, Object> map = new HashMap<>();
        int total = 0;
        int pageSize = qualityProblemDetailReq.getSize();
        int pageIndex = qualityProblemDetailReq.getPage();
        map.put(PAGE_SIZE, pageSize);
        map.put(CURR_PAGE, pageIndex);
        int totalPage = 0;
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        List<String> datasetList = scoreInstanceService.getDatasetIdListByScoreId(scoreInstance.getId());
        List<PreDataRule> array = preDataRuleService.get(qualityProblemDetailReq, ruleService.getRuleIdList(qualityProblemDetailReq.getRuleId()), scoreInstance, datasetList);
        if (CollectionUtils.isEmpty(array)) {
            map.put(DATA, result);
            map.put(TOTAL, total);
            map.put(TOTAL_PAGE, totalPage);
            return map;
        }
        QueryWrapper<Dataset> queryWrapper = new QueryWrapper<>();
        if (StringUtils.isNotEmpty(qualityProblemDetailReq.getTableId())) {
            queryWrapper.eq(CommonConstant.DATASET_ID, qualityProblemDetailReq.getTableId());
        }
        List<Dataset> dataSetList = dataSetMapper.selectList(queryWrapper);
        Map<String, String> dataSetNameMap = Maps.newHashMap();
        for (Dataset dataSet : dataSetList) {
            dataSetNameMap.put(dataSet.getMetasetCode(), dataSet.getMetasetName());
        }
        Map<Long, String> fieldNameMap = Maps.newHashMap();
        Map<Long, String> ruleDescMap = Maps.newHashMap();
        Map<String, String> topCategoryMap = Maps.newHashMap();
        for (PreDataRule preDataRule : array) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            String ruleIdStr = preDataRule.getRuleId();
            if (StringUtils.isNotEmpty(ruleIdStr)) {
                String ruleDesc = StringUtils.EMPTY;
                Long ruleId = Long.parseLong(ruleIdStr);
                if (ruleDescMap.containsKey(ruleId)) {
                    ruleDesc = ruleDescMap.get(ruleId);
                } else {
                    Rule rule = ruleMapper.selectById(ruleId);
                    if (rule != null) {
                        ruleDesc = rule.getDes();
                    }
                    ruleDescMap.put(ruleId, ruleDesc);
                }
                entity.setRuleDesc(ruleDesc);
            }
            entity.setRuleId(ruleIdStr);
            String categoryId = preDataRule.getCategoryId();
            String topCategoryName = StringUtils.EMPTY;
            if (StringUtils.isNotEmpty(categoryId)) {
                if (topCategoryMap.containsKey(categoryId)) {
                    topCategoryName = topCategoryMap.get(categoryId);
                } else {
                    topCategoryName = ruleCategoryMapper.getTopName(categoryId);
                    topCategoryMap.put(categoryId, topCategoryName);
                }
            }
            entity.setCategoryId(categoryId);
            entity.setCategoryName(preDataRule.getCategoryName());
            entity.setTopCategoryName(topCategoryName);
            entity.setRuleName(preDataRule.getRuleName());
            String dataSetCode = preDataRule.getDatasetCode();
            String dataSetName = StringUtils.EMPTY;
            if (StringUtils.isEmpty(dataSetCode)) {
                dataSetCode = StringUtils.EMPTY;
            } else {
                dataSetName = dataSetNameMap.getOrDefault(dataSetCode, StringUtils.EMPTY);
            }
            entity.setMetaSetCode(dataSetCode);
            entity.setDataSetName(dataSetName);

            setFieldInfo(fieldNameMap, preDataRule, entity);
            entity.setProblemNum(preDataRule.getCheckNum());
            entity.setDataSetId(preDataRule.getDatasetId());
            result.add(entity);
        }
        List<RuleStaticDetailEntity> returnList;
        if (pageIndex != -1) {
            // 不是导出操作
            returnList = result.stream().skip((long) (pageIndex - 1) * pageSize).limit(pageSize).collect(Collectors.toList());
            total = returnList.size();
            map.put(TOTAL, total);
            totalPage = (int) Math.ceil(1.0 * total / pageSize);
            map.put(TOTAL_PAGE, totalPage);
        } else {
            returnList = result;
        }
        map.put(DATA, returnList);
        return map;
    }

    /**
     * 设置字段编码和字段名称
     *
     * @param fieldNameMap
     * @param failResultInfo
     * @param entity
     */
    private void setFieldInfo(Map<Long, String> fieldNameMap, FailResultInfo failResultInfo, RuleStaticDetailEntity entity) {
        String fieldName = failResultInfo.getDatasetItemCode();
        String metaDataName = StringUtils.EMPTY;
        if (StringUtils.isEmpty(fieldName)) {
            fieldName = StringUtils.EMPTY;
        } else {
            Long fieldId = Long.parseLong(failResultInfo.getDatasetItemId());
            if (fieldNameMap.containsKey(fieldId)) {
                metaDataName = fieldNameMap.get(fieldId);
            } else {
                QueryWrapper<DatasetField> datasetFieldQueryWrapper = new QueryWrapper<>();
                datasetFieldQueryWrapper.eq(CommonConstant.DATASET_ID, failResultInfo.getDatasetId());
                datasetFieldQueryWrapper.eq(CommonConstant.ID, fieldId);
                List<DatasetField> datasetFieldList = datasetFieldMapper.selectList(datasetFieldQueryWrapper);
                if (CollectionUtils.isNotEmpty(datasetFieldList)) {
                    DatasetField field = datasetFieldList.get(0);
                    metaDataName = field.getElementName();
                }
                fieldNameMap.put(fieldId, metaDataName);
            }
        }
        entity.setMetaDataName(metaDataName);
        entity.setFieldName(fieldName);
    }


    /**
     * 设置字段编码和字段名称
     *
     * @param fieldNameMap
     * @param preDataRule
     * @param entity
     */
    private void setFieldInfo(Map<Long, String> fieldNameMap, PreDataRule preDataRule, RuleStaticDetailEntity entity) {
        String fieldName = preDataRule.getDatasetItemCode();
        String metaDataName = StringUtils.EMPTY;
        if (StringUtils.isEmpty(fieldName)) {
            fieldName = StringUtils.EMPTY;
        } else {
            if(StringUtils.isNotBlank(preDataRule.getDatasetItemId())){
                Long fieldId = Long.parseLong(preDataRule.getDatasetItemId());
                if (fieldNameMap.containsKey(fieldId)) {
                    metaDataName = fieldNameMap.get(fieldId);
                } else {
                    QueryWrapper<DatasetField> datasetFieldQueryWrapper = new QueryWrapper<>();
                    datasetFieldQueryWrapper.eq(CommonConstant.DATASET_ID, preDataRule.getDatasetId());
                    datasetFieldQueryWrapper.eq(CommonConstant.ID, fieldId);
                    List<DatasetField> datasetFieldList = datasetFieldMapper.selectList(datasetFieldQueryWrapper);
                    if (CollectionUtils.isNotEmpty(datasetFieldList)) {
                        DatasetField field = datasetFieldList.get(0);
                        metaDataName = field.getElementName();
                    }
                    fieldNameMap.put(fieldId, metaDataName);
                }
            }
        }
        entity.setMetaDataName(metaDataName);
        entity.setFieldName(fieldName);
    }

    /***
     * 完整性中数据集上传率
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param jobId
     * @param orgCode
     * @param sourceId
     * @return
     */
    private List<RuleStaticDetailEntity> getDataSetUpData(List<String> datasetIds, String startDate,
                                                          String endDate, List<String> jobId, String ruleId, String ruleCategoryId, List<String> orgCode, boolean isExport,
                                                          String sourceId, String standardId, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //查询数据量统计表，获取上传数据量
        DataVolumeStatistics dataVolumeStatistics = new DataVolumeStatistics();
        dataVolumeStatistics.setSourceId(sourceId);
        dataVolumeStatistics.setStandardId(standardId);
        List<DataVolumeStatistics> volumeStatistics = dataVolumeStatisticsService.findDataVolumeStatistics(
            dataVolumeStatistics, datasetIds, startDate, endDate, orgCode, page);

        if (CollectionUtils.isEmpty(volumeStatistics)) {
            return result;
        }
        if(page != null){
            page.setTotal(dataVolumeStatisticsService.getPageInfo().getTotal());
            page.setPages(dataVolumeStatisticsService.getPageInfo().getPages());
        }
        List<RuleStaticDetailEntity> list = new ArrayList<>();
        for (DataVolumeStatistics statistics : volumeStatistics) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(statistics.getDatasetCode());
            entity.setDataSetName(statistics.getDatasetName());
            entity.setDataUpNum(statistics.getDataAmount());
            entity.setDataUpTime(statistics.getCycleDay());
            entity.setStandardId(statistics.getStandardId());
            entity.setCategoryId(ruleCategoryId);
            entity.setJobId(jobId);
            list.add(entity);
        }
        if (null != list && list.size() > 0) {
            Map<String, List<RuleStaticDetailEntity>> map = list.stream().collect(Collectors.groupingBy(entity -> {
                return entity.getDataUpTime() + "&" + entity.getMetaSetCode() + "&" + entity.getDataSetName() + "&" + entity.getDataQCSTime() + "&" + entity.getStandardId();
            }));
            map.forEach((key, tlist) -> {
                Long sum = tlist.stream().mapToLong(tmap -> {
                    return tmap.getDataUpNum();
                }).sum();

                RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
                entity.setMetaSetCode(key.split("&")[1]);
                entity.setDataSetName(key.split("&")[2]);
                entity.setDataUpTime(key.split("&")[0].split("T")[0]);
                entity.setDataQCSTime(key.split("&")[3]);
                entity.setStandardId(key.split("&")[4]);
                entity.setDataUpNum(sum);
                entity.setJobId(tlist.get(0).getJobId());
                result.add(entity);
            });
            result.sort(Comparator.comparing(RuleStaticDetailEntity::getDataUpTime).reversed());
        }
        return result;
    }

    /***
     * 完整性中业务约束性
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param jobId
     * @param ruleId
     * @param ruleCategoryId
     * @param orgCode
     * @param sourceId
     * @return
     */
    private List<RuleStaticDetailEntity> getBusinessData(List<String> datasetIds, String startDate,
                                                         String endDate, List<String> jobId, String ruleId, String ruleCategoryId, List<String> orgCode, Boolean isExport,
                                                         String sourceId, String standardId, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //获取质控结果表规则信息、校验次数、检出问题数（校验失败次数）
        PreDataRule statistic = new PreDataRule();
        statistic.setRuleId(ruleId);
        statistic.setCategoryId(ruleCategoryId);
        statistic.setSourceId(sourceId);
        statistic.setStandardId(standardId);
        List<PreDataRule> resultStatisticList = preDataRuleService.findPreDataRules(statistic, datasetIds, startDate, endDate,orgCode, page);
        if (CollectionUtils.isEmpty(resultStatisticList)) {
            return result;
        }
        if(page != null){
            page.setTotal(preDataRuleService.getPageInfo().getTotal());
            page.setPages(preDataRuleService.getPageInfo().getPages());
        }
        // 从数据库一次性获取数据集
        for (PreDataRule resultStatistic : resultStatisticList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setRuleName(resultStatistic.getRuleName());
            Long successNum = resultStatistic.getValidateNum() - resultStatistic.getCheckNum();
            Long failNum = resultStatistic.getCheckNum();
            entity.setCheckCount(successNum + failNum);
            entity.setProblemNum(failNum);
            if (isExport) {
                entity.setProblemPercent(Double.valueOf(String.format("%.2f", (failNum * 1.0 / (successNum + failNum)) * 100)));
            } else {
                entity.setProblemPercent(Double.valueOf(String.format("%.4f", (failNum * 1.0 / (successNum + failNum)))));
            }
            entity.setJobId(jobId);
            entity.setStandardId(resultStatistic.getStandardId());
            entity.setMetaSetCode(resultStatistic.getDatasetCode());
            entity.setDataSetId(resultStatistic.getDatasetId());
            entity.setDataSetName(resultStatistic.getDatasetName());
            entity.setDatasetItemId(resultStatistic.getDatasetItemId());
            entity.setCategoryId(resultStatistic.getCategoryId());
            result.add(entity);
        }
        return result;
    }

    /***
     * 稳定性
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param jobId
     * @param ruleId
     * @param ruleCategoryId
     * @param orgCode
     * @param sourceId
     * @return
     */
    private PageSearch getStabilityData(List<String> datasetIds, String startDate, String endDate,
                                        List<String> jobId, String ruleId, String ruleCategoryId, List<String> orgCode,
                                        String sourceId, String standardId, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //查询失败明细表，获取连续无数据上传数和结果数
        PageSearch resultPageSearch = new PageSearch();
        FailResultInfo failResultInfo = new FailResultInfo();
        failResultInfo.setSourceId(sourceId);
        failResultInfo.setRuleId(ruleId);
        failResultInfo.setStandardId(standardId);
        failResultInfo.setCategoryId(ruleCategoryId);
        List<FailResultInfo> resultSampleList = failResultInfoService.failResultInfos(failResultInfo, datasetIds, startDate, endDate, orgCode, page);
        if (CollectionUtils.isEmpty(resultSampleList)) {
            return resultPageSearch;
        }
        if(page != null){
            page.setTotal(failResultInfoService.getPageInfo().getTotal());
            page.setPages(failResultInfoService.getPageInfo().getPages());
        }

        for (FailResultInfo sample : resultSampleList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setNoUpdataNum(sample.getVerifyResult());
            entity.setCheckResult(sample.getScore() == null ? null : Long.parseLong(sample.getScore()));
            entity.setDataUpTime(sample.getCycleDay());
            //根据orgCode和dataSourceId获取机构名称和数据源名称
            entity.setOrgCodeName(getOrgCodeName(sample.getOrgCode(), sample.getSourceId()));
            entity.setDataQCSTime(sample.getCreationTime());
            entity.setJobId(jobId);
            entity.setStandardId(sample.getStandardId());
            entity.setMetaSetCode(sample.getDatasetCode());
            entity.setDataSetId(sample.getDatasetId());
            entity.setDataSetName(sample.getDatasetName());
            entity.setDatasetItemId(sample.getDatasetItemId());
            entity.setCategoryId(sample.getCategoryId());
            result.add(entity);
        }
        resultPageSearch.setRuleStaticDetailEntityList(result);
        return resultPageSearch;
    }

    /***
     * 根据orgCode获取机构名称
     * @param orgCode
     * @param sourceId
     * @return
     */
    private String getOrgCodeName(String orgCode, String sourceId) {
        String result = "";
        //优先从缓存拿取
        if (redisRepository.exists(ORGCODE_DATA + orgCode)) {
            if (null != redisRepository.get(ORGCODE_DATA + orgCode)) {
                result = (String) redisRepository.get(ORGCODE_DATA + orgCode);
                return result;
            }
        }
        if (redisRepository.exists(SOURCEID_DATA + orgCode)) {
            if (null != redisRepository.get(SOURCEID_DATA + orgCode)) {
                result = (String) redisRepository.get(SOURCEID_DATA + orgCode);
                return result;
            }
        }

        //根据orgCode，查询DM中机构名称, 若通过orgCode查询为空，则去拿sourceId查询
        RrsOrganization organization = dmQueryService.getOrgByCode(orgCode);
        if (null != organization && StringUtils.isNotEmpty(organization.getOrgName())) {
            result = organization.getOrgName();
            redisRepository.set(ORGCODE_DATA + orgCode, result);
        } else {
            RrsOrganization rrsOrganization = dmQueryService.getOrgByCode(sourceId);
            if (null != rrsOrganization && StringUtils.isNotEmpty(rrsOrganization.getOrgName())) {
                result = rrsOrganization.getOrgName();
                redisRepository.set(SOURCEID_DATA + sourceId, result);
            }
        }
        return result;
    }


    /**
     * 关联系 by CK
     * @param ruleStaticPojo
     * @param page
     * @return
     */
    private List<RuleStaticDetailEntity> getRelevanceDataByCK(RuleStaticPojo ruleStaticPojo,Boolean isExport, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        PreDataRule statistic = new PreDataRule();
        statistic.setRuleId(ruleStaticPojo.getRuleId());
        statistic.setCategoryId(ruleStaticPojo.getRuleCategoryId());
        statistic.setSourceId(ruleStaticPojo.getSourceId());
        statistic.setStandardId(ruleStaticPojo.getStandardId());
        List<PreDataRule> resultStatic = preDataRuleService.findPreDataRules(
            statistic, ruleStaticPojo.getDatasetIds(), ruleStaticPojo.getStartDate(), ruleStaticPojo.getEndDate(),ruleStaticPojo.getOrgCode(), page);

        if(page != null){
            page.setTotal(preDataRuleService.getPageInfo().getTotal());
            page.setPages(preDataRuleService.getPageInfo().getPages());
        }
        /**查询失败明细表，获取关联数据集信息**/
        List<ResultStatisticVO> rsList = new ArrayList<>();
        resultStatic.forEach(rs ->{
            ResultStatisticVO statisticVO = new ResultStatisticVO();
            BeanUtils.copyProperties(rs, statisticVO);
            statisticVO.setSuccessAmount(rs.getValidateNum() - rs.getCheckNum());
            statisticVO.setFailAmount(rs.getCheckNum());
            statisticVO.setDatasetName(rs.getDatasetName());
            FailResultInfo failResultInfo = new FailResultInfo();
            failResultInfo.setOrgCode(rs.getOrgCode());
            failResultInfo.setRuleId(rs.getRuleId());
            failResultInfo.setCategoryId(rs.getCategoryId());
            failResultInfo.setDatasetId(rs.getDatasetId());
            failResultInfo.setLimitOne(true);
            List<FailResultInfo> resultSamples = failResultInfoService.failResultInfos(
                failResultInfo, null, ruleStaticPojo.getStartDate(), ruleStaticPojo.getEndDate(), null,null);
            if(resultSamples != null && !resultSamples.isEmpty()){
                String originalValue = resultSamples.get(0).getOriginalValue();
                if(StringUtils.isNotBlank(originalValue)){
                    String[] ext2 = originalValue.split(",");
                    statisticVO.setOriginalValue(ext2[ext2.length - 1]);
                }
            }
            rsList.add(statisticVO);
        });

        if (CollectionUtils.isEmpty(rsList)) {
            return result;
        }

        for (ResultStatisticVO resultStatisticVO : rsList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            //获取关联数据集
            String relateCode = resultStatisticVO.getOriginalValue();
            if (StringUtils.isNotEmpty(relateCode)) {
                if (relateCode.contains(".")) {
                    String[] split = relateCode.split("\\.");
                    relateCode = split[1];
                }
                entity.setRelateMetaSetCode(relateCode);
                entity.setRelateDataSetName(datasetService.getNameByCode(relateCode));
            }
            Long successNum = resultStatisticVO.getSuccessAmount();
            Long failNum = resultStatisticVO.getFailAmount();
            entity.setCheckCount(successNum + failNum);
            entity.setProblemNum(failNum);
            if (isExport) {
                entity.setProblemPercent(Double.valueOf(String.format("%.2f", (failNum * 1.0 / (successNum + failNum)) * 100)));
            } else {
                entity.setProblemPercent(Double.valueOf(String.format("%.4f", (failNum * 1.0 / (successNum + failNum)))));
            }
            entity.setJobId(Arrays.asList());
            entity.setStandardId(resultStatisticVO.getStandardId());
            entity.setDataSetId(resultStatisticVO.getDatasetId());
            entity.setDataSetName(resultStatisticVO.getDatasetName());
            entity.setMetaSetCode(resultStatisticVO.getDatasetCode());
            entity.setDatasetItemId(resultStatisticVO.getDatasetItemId());
            result.add(entity);
        }
        return result;
    }

    /***
     * 及时性
     * @param datasetIds
     * @param startDate
     * @param endDate
     * @param jobId
     * @param ruleId
     * @param ruleCategoryId
     * @param orgCode
     * @param sourceId
     * @return
     */
    private List<RuleStaticDetailEntity> getTimelinesData(List<String> datasetIds, String startDate,
                                                          String endDate, List<String> jobId, String ruleId, String ruleCategoryId, List<String> orgCode, Boolean isExport,
                                                          String sourceId, String standardId, Page page) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //获取质控结果表校验次数、检出问题数（校验失败次数）
        PreDataRule statistic = new PreDataRule();
        statistic.setRuleId(ruleId);
        statistic.setCategoryId(ruleCategoryId);
        statistic.setSourceId(sourceId);
        statistic.setStandardId(standardId);
        List<PreDataRule> resultStatisticList = preDataRuleService.findPreDataRules(statistic, datasetIds, startDate, endDate, orgCode, page);
        if (CollectionUtils.isEmpty(resultStatisticList)) {
            return result;
        }

        if(page != null){
            page.setTotal(preDataRuleService.getPageInfo().getTotal());
            page.setPages(preDataRuleService.getPageInfo().getPages());
        }

        for (PreDataRule resultStatistic : resultStatisticList) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            Long successNum = resultStatistic.getValidateNum() - resultStatistic.getCheckNum();
            Long failNum =  resultStatistic.getCheckNum();
            entity.setCheckCount(successNum + failNum);
            entity.setProblemNum(failNum);
            if (isExport) {
                entity.setProblemPercent(Double.valueOf(String.format("%.2f", (failNum * 1.0 / (successNum + failNum)) * 100)));
            } else {
                entity.setProblemPercent(Double.valueOf(String.format("%.4f", (failNum * 1.0 / (successNum + failNum)))));
            }
            entity.setJobId(jobId);
            entity.setMetaSetCode(resultStatistic.getDatasetCode());
            entity.setStandardId(resultStatistic.getStandardId());
            entity.setDataSetId(resultStatistic.getDatasetId());
            entity.setDataSetName(resultStatistic.getDatasetName());
            entity.setDatasetItemId(resultStatistic.getDatasetItemId());
            entity.setCategoryId(resultStatistic.getCategoryId());
            result.add(entity);
        }
        return result;
    }

    /***
     * 获取问题数据详情
     * @param ruleStaticPojo
     * @return
     */
    public Map<String, Object> getProblemDetail(RuleStaticPojo ruleStaticPojo) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        Integer pageIndex = ruleStaticPojo.getPage();
        Integer pageSize = ruleStaticPojo.getSize();
        String dataSetId = ruleStaticPojo.getDatasetId();
        String startDate = ruleStaticPojo.getStartDate();
        String endDate = ruleStaticPojo.getEndDate();
        List<String> orgCode = ruleStaticPojo.getOrgCode();
        String ruleId = ruleStaticPojo.getRuleId();
        String ruleCatagoryId = ruleStaticPojo.getRuleCategoryId();
        String standardId = ruleStaticPojo.getStandardId();
        String scoreInstanceId = ruleStaticPojo.getScoreInstanceId();
        String sourceId = "";
        //评分对象获取数据源和机构编码
        if (StringUtils.isNotEmpty(scoreInstanceId)) {
            QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
            queryWrapper.in("id", scoreInstanceId);
            ScoreInstance instance = scoreInstanceMapper.selectOne(queryWrapper);
            if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getOrgCode())) {
                orgCode = orgCode == null ? new ArrayList<>() : orgCode;
                orgCode.add(instance.getOrgCode());
                if(ruleStaticPojo.getOrgCode().isEmpty()){
                    ruleStaticPojo.setOrgCode(orgCode);
                }
            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getDatasourceId())) {
                sourceId = instance.getDatasourceId();
                ruleStaticPojo.setSourceId(sourceId);
            } else {

            }
        }

        long totalPage = 0;
        long total = 0;

        FailResultInfo failResultInfo = new FailResultInfo();
        failResultInfo.setSourceId(sourceId);
        failResultInfo.setRuleId(ruleId);
        failResultInfo.setStandardId(standardId);
        failResultInfo.setCategoryId(ruleCatagoryId);
        failResultInfo.setDatasetId(dataSetId);
        failResultInfo.setDatasetItemId(ruleStaticPojo.getDatasetItemId());
        List<FailResultInfo> resultSamples = failResultInfoService.failResultInfos(
            failResultInfo, null, startDate, endDate, orgCode, new Page(pageIndex, pageSize));

        if (CollectionUtils.isNotEmpty(resultSamples)) {
            String type = ruleStaticPojo.getType();
            switch (type) {
                //规范性
                case STANDARD:
                    result = getStandardProblemData(resultSamples, ruleStaticPojo, false, null);
                    break;
                //一致性
                case CONSISTENCY:
                    result = getConsistencyProblemData(resultSamples, ruleStaticPojo, false, null);
                    break;
                //业务约束性
                case BUSINESS:
                    result = getBusinessProblemData(resultSamples, ruleStaticPojo, false, null);
                    break;
                //关联性
                case RELEVANCE:
                    result = getRelevanceProblemData(resultSamples, ruleStaticPojo, false, null);
                    break;
                //及时性
                case TIMELINES:
                    result = getTimelinesProblemData(resultSamples, ruleStaticPojo, false, null);
                    break;
                default:
                    result = null;
            }

            if(result != null && !result.isEmpty()){
                total = failResultInfoService.getPageInfo().getTotal();
                totalPage = failResultInfoService.getPageInfo().getPages();
            }
        }

        Map<String, Object> map = new HashMap<>();
        map.put(TOTAL, total);
        map.put(PAGE_SIZE, pageSize);
        map.put(CURR_PAGE, pageIndex);
        map.put(TOTAL_PAGE, totalPage);
        map.put(DATA, result);
        return map;
    }


    /***
     * 规范性下钻问题数详情数据
     * @param exportNum
     * @return
     */
    private List<RuleStaticDetailEntity> getStandardProblemData(List<FailResultInfo> resultSamples, RuleStaticPojo ruleStaticPojo, Boolean isExport, Integer exportNum) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        Map<Long, String> fieldNameMap = Maps.newHashMap();
        for (FailResultInfo failResultInfo : resultSamples) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(StringUtils.isEmpty(ruleStaticPojo.getMetaSetCode()) ? failResultInfo.getDatasetCode() : ruleStaticPojo.getMetaSetCode());
            entity.setDataSetName(StringUtils.isEmpty(ruleStaticPojo.getDataSetName())
                ? getDataSetNameForCache(failResultInfo.getDatasetCode()) : ruleStaticPojo.getDataSetName());
            entity.setFieldName(failResultInfo.getFieldName());
            entity.setMetaDataName(failResultInfo.getMetaDataName());
            entity.setRuleName(failResultInfo.getRuleName());
            entity.setPrimaryKeyValue(failResultInfo.getPkValue());
            entity.setFieldValue(this.getFiledValue(failResultInfo));
            entity.setDataUpTime(failResultInfo.getCycleDay());
            entity.setRuleId(failResultInfo.getRuleId());
            setFieldInfo(fieldNameMap, failResultInfo, entity);
            entity.setDataQCSTime(failResultInfo.getCreationTime());
            result.add(entity);
        }
        return result;
    }

    private String getFiledValue(FailResultInfo failResultInfo) {
        String ext2 = failResultInfo.getOriginalValue();
        String result = StringUtils.EMPTY;
        if (StringUtils.isNotEmpty(ext2)) {
            String[] strArr = ext2.split(",");
            if (strArr.length > 0) {
                result = strArr[0];
            }
        }
        return result;
    }

    /***
     * 一致性规则问题数据
     * @param exportNum
     * @return
     */
    private List<RuleStaticDetailEntity> getConsistencyProblemData(List<FailResultInfo> resultSamples, RuleStaticPojo ruleStaticPojo, Boolean isExport, Integer exportNum) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //查询ES失败明细表，获取数据集具体规则失败数据
        for (FailResultInfo failResultInfo : resultSamples) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(StringUtils.isEmpty(ruleStaticPojo.getMetaSetCode()) ? failResultInfo.getDatasetCode() : ruleStaticPojo.getMetaSetCode());
            entity.setDataSetName(StringUtils.isEmpty(ruleStaticPojo.getDataSetName())
                ? getDataSetNameForCache(failResultInfo.getDatasetCode()) : ruleStaticPojo.getDataSetName());
            entity.setFieldName(ruleStaticPojo.getFieldName());
            entity.setMetaDataName(ruleStaticPojo.getMetaDataName());
            entity.setRuleName(failResultInfo.getRuleName());
            entity.setPrimaryKeyValue(failResultInfo.getPkValue());
            entity.setFieldValue(this.getFiledValue(failResultInfo));
            entity.setCompareValue(0L);
            entity.setDataUpTime(failResultInfo.getCycleDay());
            entity.setDataQCSTime(failResultInfo.getCreationTime());
            result.add(entity);
        }
        return result;
    }

    /***
     * 业务约束性规则问题数据
     * @param exportNum
     * @return
     */
    private List<RuleStaticDetailEntity> getBusinessProblemData(List<FailResultInfo> resultSamples, RuleStaticPojo pojo, Boolean isExport, Integer exportNum) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //查询ES失败明细表，获取数据集具体规则失败数据
        for (FailResultInfo failResultInfo : resultSamples) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(StringUtils.isEmpty(pojo.getMetaSetCode()) ? failResultInfo.getDatasetCode() : pojo.getMetaSetCode());
            entity.setDataSetName(StringUtils.isEmpty(pojo.getDataSetName())
                ? getDataSetNameForCache(failResultInfo.getDatasetCode()) : pojo.getDataSetName());
            entity.setFieldName(pojo.getFieldName());
            entity.setMetaDataName(pojo.getMetaDataName());
            entity.setRuleName(failResultInfo.getRuleName());
            entity.setPrimaryKeyValue(failResultInfo.getPkValue());
            entity.setDataUpTime(failResultInfo.getCycleDay());
            entity.setDataQCSTime(failResultInfo.getCreationTime());
            result.add(entity);
        }
        return result;
    }

    /***
     * 关联性问题数据
     * @param exportNum
     * @return
     */
    private List<RuleStaticDetailEntity> getRelevanceProblemData(List<FailResultInfo> resultSamples, RuleStaticPojo pojo, Boolean isExport, Integer exportNum) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        for (FailResultInfo failResultInfo : resultSamples) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(StringUtils.isEmpty(pojo.getMetaSetCode()) ? failResultInfo.getDatasetCode() : pojo.getMetaSetCode());
            entity.setDataSetName(StringUtils.isEmpty(pojo.getDataSetName())
                ? getDataSetNameForCache(failResultInfo.getDatasetCode()) : pojo.getDataSetName());
            //获取关联数据集
            if (StringUtils.isNotEmpty(failResultInfo.getOriginalValue())) {
                String[] ext2 = failResultInfo.getOriginalValue().split(",");
                String relateCode = ext2[ext2.length - 1];
                if (StringUtils.isNotEmpty(relateCode)) {
                    entity.setRelateMetaSetCode(relateCode.split("\\.").length > 1 ? relateCode.split("\\.")[1] : relateCode);
                    entity.setRelateDataSetName(datasetService.getNameByCode(entity.getRelateMetaSetCode()));
                }
            }
            entity.setPrimaryKeyValue(failResultInfo.getPkValue());
            entity.setDataUpTime(failResultInfo.getCycleDay());
            entity.setDataQCSTime(failResultInfo.getCreationTime());
            result.add(entity);
        }
        return result;
    }

    /***
     * 及时性问题数据
     * @param exportNum
     * @return
     */
    private List<RuleStaticDetailEntity> getTimelinesProblemData(List<FailResultInfo> resultSamples, RuleStaticPojo pojo, Boolean isExport, Integer exportNum) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //查询ES失败明细表，获取主键值和数据延迟上传天数
        Map<Long, String> dataSetNameMap = Maps.newHashMap();
        for (FailResultInfo failResultInfo : resultSamples) {
            RuleStaticDetailEntity entity = new RuleStaticDetailEntity();
            entity.setMetaSetCode(StringUtils.isEmpty(pojo.getMetaSetCode()) ? failResultInfo.getDatasetCode() : pojo.getMetaSetCode());
            setDataSetName(entity, dataSetNameMap, failResultInfo.getDatasetId(), failResultInfo.getDatasetCode());
            entity.setPrimaryKeyValue(failResultInfo.getPkValue());
            entity.setNoUpdataNum(failResultInfo.getVerifyResult());
            entity.setDataDelayUpNum(StringUtils.isBlank(failResultInfo.getVerifyResult()) || "null".equals(failResultInfo.getVerifyResult()) ? null : Long.parseLong(failResultInfo.getVerifyResult()));
            // 将之前的firstValue存入exit2字段，改成originalValue存入
            entity.setBusinessTime(StringUtils.isEmpty(failResultInfo.getOriginalValue()) ? StringUtils.EMPTY : failResultInfo.getOriginalValue().split(",")[0]);
            entity.setDataUpTime(failResultInfo.getCycleDay());
            entity.setDataQCSTime(failResultInfo.getCreationTime());
            String ext2 = failResultInfo.getOriginalValue();
            //及时性场景下这里为业务时间
            if (StringUtils.isNoneBlank(ext2) && ext2.contains(",")) {
                entity.setBusinessTime(ext2.substring(0, ext2.lastIndexOf(",")));
            }
            result.add(entity);
        }
        return result;
    }

    public CommonResponse<Map<String, Object>> exportRuleForDubbo(ExportProblemReq exportProblemReq) {
        RuleStaticPojo ruleStaticPojo = new RuleStaticPojo();
        BeanUtils.copyProperties(exportProblemReq, ruleStaticPojo);
        if (StringUtils.isNotEmpty(exportProblemReq.getDatasetId())) {
            ruleStaticPojo.setDatasetIds(Arrays.asList(exportProblemReq.getDatasetId()));
        }
        ruleStaticPojo.setStandardId(scoreInstanceMapper.getStandardId(Long.parseLong(exportProblemReq.getScoreInstanceId())).toString());
        Map<String, Object> map;
        switch (ruleStaticPojo.getType()) {
            case STABILITY:
            case DATASETUP:
            case UNIQUE:
                map = this.exportRuleDetail(ruleStaticPojo, null);
                break;
            default:
                map = this.exportProblemDetail(ruleStaticPojo, null);
                break;
        }
        map.put("metaDataName", exportProblemReq.getMetaDataName());
        List<RuleStaticDetailEntity> list = (List<RuleStaticDetailEntity>) map.get("list");
        Integer maxExportNum = parameterService.getMaxExportNum();
        if (list.size() >= maxExportNum) {
            return CommonResponse.failed("导出数据不得超过" + maxExportNum + "条, 请重新选择");
        }
        return CommonResponse.succeed(map);
    }

    public Map<String, Object> exportRuleDetail(RuleStaticPojo ruleStaticPojo, HttpServletResponse response) {
        String type = ruleStaticPojo.getType();
        List<String> datasetIds = ruleStaticPojo.getDatasetIds();
        List<String> orgCode = ruleStaticPojo.getOrgCode();
        String startDate = ruleStaticPojo.getStartDate();
        String endDate = ruleStaticPojo.getEndDate();
        List<String> jobId = ruleStaticPojo.getJobId();
        String ruleId = ruleStaticPojo.getRuleId();
        String ruleCategoryId = ruleStaticPojo.getRuleCategoryId();
        String standardId = ruleStaticPojo.getStandardId();
        String scoreInstanceId = ruleStaticPojo.getScoreInstanceId();
        String areaCode = ruleStaticPojo.getAreaCode();
        String sourceId = "";
        if (StringUtils.isNotEmpty(scoreInstanceId)) {
            //评分对象获取数据源和机构编码
            QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
            queryWrapper.in("id", scoreInstanceId);
            ScoreInstance instance = scoreInstanceMapper.selectOne(queryWrapper);
            if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getOrgCode())) {
                orgCode = orgCode == null ? new ArrayList<>() : orgCode;
                orgCode.add(instance.getOrgCode());
                if(ruleStaticPojo.getOrgCode().isEmpty()){
                    ruleStaticPojo.setOrgCode(orgCode);
                }
            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getDatasourceId())) {
                sourceId = instance.getDatasourceId();
                ruleStaticPojo.setSourceId(sourceId);
            } else {

            }
        }else{
            if(StringUtils.isNotBlank(areaCode)){
                List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaCode(areaCode);
                for (RrsOrganization organization : orgList) {
                    orgCode.add(organization.getOrgCode());
                }
            }
        }
        List<RuleStaticDetailEntity> result = new ArrayList<>();

        Parameter parameter = new Parameter();
        parameter.setCode("ES_MAX_PAGESIZE");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);

        PageSearch pageSearch = null;
        switch (type) {
            //规范性
            case STANDARD:
                result = getStandardData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    true,  sourceId, standardId, orgCode,null);
                break;
            //规范性中的唯一性
            case UNIQUE:
                pageSearch = getUniqueData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    orgCode, sourceId, standardId, null);
                break;
            //一致性
            case CONSISTENCY:
                result = getStandardData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    true,  sourceId, standardId, orgCode,null);
                break;
            //完整性中数据集上传率
            case DATASETUP:
                result = getDataSetUpData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    true,sourceId, standardId, null);
                break;
            //完整性中业务约束性
            case BUSINESS:
                result = getBusinessData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    true,sourceId, standardId, null);
                break;
            //稳定性
            case STABILITY:
                pageSearch = getStabilityData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId,
                    orgCode, sourceId, standardId, null);
                break;
            //关联性
            case RELEVANCE:
                result = getRelevanceDataByCK(ruleStaticPojo, true , null);
                break;
            //及时性
            case TIMELINES:
                result = getTimelinesData(datasetIds, startDate, endDate, jobId, ruleId, ruleCategoryId, orgCode,
                    true,sourceId, standardId, null);
                break;
            default:
                result = null;
        }
        if (pageSearch != null) {
            result = pageSearch.getRuleStaticDetailEntityList();
        }

        //POI导出EXCEL
        String filePath = getRuleDetailFilePath(type);
        if (StringUtils.isEmpty(filePath)) {
            log.info("未找到导出EXCEL模板！");
        }
        Map<String, Object> map = new HashMap<>();
        map.put("list", result);
        map.put("ruleName", getRuleName(ruleId, ruleCategoryId));
        map.put("headsName", ruleStaticPojo.getHeadsName());

        if (response != null) {
            setExportResponse(ruleStaticPojo, response, filePath, map);
        }
        return map;
    }

    private void setExportResponse(RuleStaticPojo ruleStaticPojo, HttpServletResponse response, String filePath, Map<String, Object> map) {
        TemplateExportParams params = new TemplateExportParams(filePath);
        params.setScanAllsheet(true);
        params.setColForEach(true);
        // 导出excel
        try (Workbook book = ExcelExportUtil.exportExcel(params, map)) {
            response.setContentType("mutipart/form-data");
            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-disposition", "attachment;filename=ruleDetail_" + ruleStaticPojo.getStartDate() + "~" + ruleStaticPojo.getEndDate() + ".xlsx");
            book.write(response.getOutputStream());
        } catch (Exception e) {
            log.error("下载失败;{}", e);
            throw new BizException("下载失败," + e.getMessage());
        }
    }

    private String getRuleDetailFilePath(String type) {
        String result = "";
        switch (type) {
            //规范性
            case STANDARD:
                result = "excel/ruleDetail/standard.xlsx";
                break;
            //规范性中的唯一性
            case UNIQUE:
                result = "excel/ruleDetail/unique.xlsx";
                break;
            //一致性
            case CONSISTENCY:
                result = "excel/ruleDetail/standard.xlsx";
                break;
            //完整性中数据集上传率
            case DATASETUP:
                result = "excel/ruleDetail/dataSetUp.xlsx";
                break;
            //完整性中业务约束性
            case BUSINESS:
                result = "excel/ruleDetail/business.xlsx";
                break;
            //稳定性
            case STABILITY:
                result = "excel/ruleDetail/stability.xlsx";
                break;
            //关联性
            case RELEVANCE:
                result = "excel/ruleDetail/relevance.xlsx";
                break;
            //及时性
            case TIMELINES:
                result = "excel/ruleDetail/timelines.xlsx";
                break;
            default:
                result = null;
        }
        return result;
    }

    private String getRuleName(String ruleId, String ruleCategoryId) {
        String result = "";
        if (StringUtils.isNotEmpty(ruleId)) {
            Rule queryRule = new Rule();
            queryRule.setId(ruleId);
            Rule rule = ruleMapper.selectById(queryRule);
            result = rule.getName();
        }
        if (StringUtils.isNotEmpty(ruleCategoryId)) {
            RuleCategory query = new RuleCategory();
            query.setId(ruleCategoryId);
            RuleCategory category = categoryMapper.selectById(query);
            result = category.getName();
        }
        return result;
    }

    public Map<String, Object> exportProblemDetail(RuleStaticPojo ruleStaticPojo, HttpServletResponse response) {
        String dataSetId = ruleStaticPojo.getDatasetId();
        String startDate = ruleStaticPojo.getStartDate();
        String endDate = ruleStaticPojo.getEndDate();
        String type = ruleStaticPojo.getType();
        List<String> orgCode = ruleStaticPojo.getOrgCode();
        String ruleId = ruleStaticPojo.getRuleId();
        String ruleCatagoryId = ruleStaticPojo.getRuleCategoryId();
        String standardId = ruleStaticPojo.getStandardId();
        String scoreInstanceId = ruleStaticPojo.getScoreInstanceId();
        String sourceId = "";
        if (StringUtils.isNotEmpty(scoreInstanceId)) {
            //评分对象获取数据源和机构编码
            QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
            queryWrapper.in("id", scoreInstanceId);
            ScoreInstance instance = scoreInstanceMapper.selectOne(queryWrapper);
            if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getOrgCode())) {
                orgCode = orgCode == null ? new ArrayList<>() : orgCode;
                orgCode.add(instance.getOrgCode());
                if(ruleStaticPojo.getOrgCode().isEmpty()){
                    ruleStaticPojo.setOrgCode(orgCode);
                }
            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(instance.getScoreTag())
                && StringUtils.isNotEmpty(instance.getDatasourceId())) {
                sourceId = instance.getDatasourceId();
                ruleStaticPojo.setSourceId(sourceId);
            } else {

            }
        }
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        //获取导出最大条数
        Parameter parameter = new Parameter();
        parameter.setCode("MAX_EXPORT_NUM");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Integer exportNum = Integer.valueOf(parameterMapper.selectOne(queryWrapper).getValue());

        FailResultInfo failResultInfo = new FailResultInfo();
        failResultInfo.setSourceId(sourceId);
        failResultInfo.setRuleId(ruleId);
        failResultInfo.setStandardId(standardId);
        failResultInfo.setCategoryId(ruleCatagoryId);
        failResultInfo.setDatasetId(dataSetId);
        failResultInfo.setDatasetItemId(ruleStaticPojo.getDatasetItemId());
        List<FailResultInfo> resultSamples = failResultInfoService.failResultInfos(
            failResultInfo, null, startDate, endDate, orgCode, null);

        if (CollectionUtils.isNotEmpty(resultSamples)) {
            switch (type) {
                //规范性
                case STANDARD:
                    result = getStandardProblemData(resultSamples, ruleStaticPojo, true, exportNum);
                    break;
                //一致性
                case CONSISTENCY:
                    result = getConsistencyProblemData(resultSamples, ruleStaticPojo, true, exportNum);
                    break;
                //业务约束性
                case BUSINESS:
                    result = getBusinessProblemData(resultSamples, ruleStaticPojo, true, exportNum);
                    break;
                //关联性
                case RELEVANCE:
                    result = getRelevanceProblemData(resultSamples, ruleStaticPojo, true, exportNum);
                    break;
                //及时性
                case TIMELINES:
                    result = getTimelinesProblemData(resultSamples, ruleStaticPojo, true, exportNum);
                    break;
                default:
                    result = null;
            }
        }

        //POI导出EXCEL
        String filePath = getProblemDetailFilePath(type);
        if (StringUtils.isEmpty(filePath)) {
            log.info("未找到导出EXCEL模板！");
        }
        Map<String, Object> map = new HashMap<>();
        map.put("list", result);
        map.put("headsName", ruleStaticPojo.getHeadsName());
        map.put("ruleName", getRuleName(ruleStaticPojo.getRuleId(), ruleStaticPojo.getRuleCategoryId()));
        map.put("metaDataName", ruleStaticPojo.getMetaDataName());

        if (response != null) {
            TemplateExportParams params = new TemplateExportParams(filePath);
            params.setScanAllsheet(true);
            params.setColForEach(true);
            // 导出excel
            try (Workbook book = ExcelExportUtil.exportExcel(params, map)) {
                response.setContentType("mutipart/form-data");
                response.setCharacterEncoding("utf-8");
                response.setHeader("Content-disposition", "attachment;filename=problemDetail_" + ruleStaticPojo.getStartDate() + "~" + ruleStaticPojo.getEndDate() + ".xlsx");
                book.write(response.getOutputStream());
            } catch (Exception e) {
                log.error("下载失败;{}", e);
                throw new BizException("下载失败," + e.getMessage());
            }
        }
        return map;
    }

    private String getProblemDetailFilePath(String type) {
        String result = "";
        switch (type) {
            //规范性
            case STANDARD:
                result = "excel/problemDetail/standardProblem.xlsx";
                break;
            //一致性
            case CONSISTENCY:
                result = "excel/problemDetail/consistencyProblem.xlsx";
                break;
            //完整性中业务约束性
            case BUSINESS:
                result = "excel/problemDetail/businessProblem.xlsx";
                break;
            //关联性
            case RELEVANCE:
                result = "excel/problemDetail/relevanceProblem.xlsx";
                break;
            //及时性
            case TIMELINES:
                result = "excel/problemDetail/timelinesProblem.xlsx";
                break;
            default:
                result = null;
        }
        return result;
    }

    private Map<String, Object> filterByPage(List<RuleStaticDetailEntity> list, Integer pageIndex, Integer pageSize, String type, Integer staTotal) {
        List<RuleStaticDetailEntity> result = new ArrayList<>();
        Integer total = 0;
        Integer totalPage = 1;
        if (null != list && list.size() > 0) {
            if (type.equals(STABILITY) || type.equals(UNIQUE)) {
                result = list;
                total = staTotal;
                totalPage = (int) Math.ceil(1.0 * staTotal / pageSize);
            } else {
                result = list.stream().skip((long) (pageIndex - 1) * pageSize).limit(pageSize).collect(Collectors.toList());
                total = list.size();
                totalPage = (int) Math.ceil(1.0 * total / pageSize);
            }
        }
        Map<String, Object> map = new HashMap<>();
        map.put(TOTAL, total);
        map.put(PAGE_SIZE, pageSize);
        map.put(CURR_PAGE, pageIndex);
        map.put(TOTAL_PAGE, totalPage);
        map.put(DATA, result);
        return map;
    }

    /**
     * 从缓存中取dataset name
     *
     * @param metasetCode 数据表code
     * @return DataSetName
     */
    private String getDataSetNameForCache(String metasetCode) {
        if (StringUtils.isEmpty(metasetCode)) {
            return StringUtils.EMPTY;
        }

        //处理ods_ggws.EPI_CHILDINFO 这种需要截取后面真实的表code
        if (metasetCode.contains(CommonConstant.POINT)) {
            //+1 从.号后面开始算
            metasetCode = metasetCode.substring(metasetCode.lastIndexOf(CommonConstant.POINT) + 1);
        }

        if (!DATASET_CACHE_MAP.containsKey(metasetCode)) {
            Dataset dataSet = dataSetMapper.getDataSetByMetasetCode(metasetCode);
            if (dataSet == null) {
                return StringUtils.EMPTY;
            }
            DATASET_CACHE_MAP.put(metasetCode, dataSet);

            return dataSet.getMetasetName();
        }

        return DATASET_CACHE_MAP.get(metasetCode).getMetasetName();
    }

}
