package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.google.common.collect.Lists;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.constant.DmAreaConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.core.biz.service.DmQueryService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataRuleService;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.clickhouse.PreDataRule;
import com.gwi.qcs.model.domain.clickhouse.PreDataRuleField;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.RuleQueryDto;
import com.gwi.qcs.model.entity.RuleQuerierResult;
import com.gwi.qcs.model.entity.RuleQueryResult;
import com.gwi.qcs.model.mapper.mysql.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class RuleService extends SuperServiceImpl<RuleMapper, Rule> {

    private RuleQueryDto ruleQueryDto;

    private String exceptionName;

    @Resource
    private RuleMapper ruleMapper;

    @Resource
    private RuleDetailMapper ruleDetailMapper;

    @Resource
    private ScoreInstanceMapper scoreInstanceMapper;

    @Resource
    private DataSourceDatesetMapper dataSourceDateSetMapper;

    @Resource
    private DatasetMapper dataSetMapper;

    @Autowired
    private RuleCategoryService ruleCategoryService;

    @Autowired
    private ScoreInstanceService scoreInstanceService;

    @Autowired
    private DmQueryService dmQueryService;

    @Resource
    private DataSourceService dataSourceService;

    @Autowired
    private ScoreInstanceRuleMapper scoreInstanceRuleMapper;

    @Autowired
    private InstanceRuleMapper instanceRuleMapper;

    @Autowired
    private RuleDetailValMapper ruleDetailValMapper;

    @Autowired
    private PreDataRuleService preDataRuleService;

    @Autowired
    private DataVolumeStatisticsService dataVolumeStatisticsService;

    public String getNameById(String id, Map<String, String> map){
        if(! map.isEmpty()){
            return map.getOrDefault(id, CommonConstant.DASH);
        }
        map = this.list().stream().collect(Collectors.toMap(Rule::getId, Rule::getName));
        return getNameById(id, map);
    }

    /**
     * 4.6获取规则分类明细信息接口
     *
     * @param rule
     * @return
     */
    public List<Rule> getRule(Rule rule) {
        Wrapper<Rule> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) queryWrapper).setEntity(rule);
        ((QueryWrapper<Rule>) queryWrapper).orderByDesc("CREATE_AT");
        return ruleMapper.selectList(queryWrapper);
    }

    /**
     * 获取规则分类明细详情信息接口
     *
     * @param rule
     * @return
     */
    public List<Rule> getRuleDetail(Rule rule) {
        return ruleMapper.getRuleDetail(rule);
    }

    /**
     * 4.8新增规则分类明细信息接口
     *
     * @param rule
     * @return
     */
    @Transactional
    public String addRule(Rule rule) {
        int i = 0;
        Wrapper<Rule> wrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) wrapper).eq("NAME", rule.getName());
        long count = ruleMapper.selectCount(wrapper);
        if (count > 0) {
            throw new BizException("该规则已存在！");
        }
        i += ruleMapper.insert(rule);
        if (rule.getList() != null){
            for (RuleDetail rd : rule.getList()) {
                rd.setRuleId(rule.getId());
                i += ruleDetailMapper.insert(rd);
            }
        }
        return rule.getId();
    }

    /**
     * 修改规则分类明细信息接口
     *
     * @param rule
     * @return
     */
    @Transactional
    public int updateRule(Rule rule) {
        int i = 0;
        checkUpdate(rule);
        i += ruleMapper.updateById(rule);
        if (rule.getList() != null) {
            RuleDetail rdd = new RuleDetail();
            rdd.setRuleId(rule.getId());
            Wrapper<RuleDetail> detailWrapper = new UpdateWrapper<>();
            ((UpdateWrapper<RuleDetail>) detailWrapper).setEntity(rdd);
            i += ruleDetailMapper.delete(detailWrapper);
            for (RuleDetail rd : rule.getList()) {
                i += ruleDetailMapper.insert(rd);
            }
        }
        return i;
    }

    private void checkUpdate(Rule rule) {
        Wrapper<Rule> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) queryWrapper).notIn("ID", rule.getId());
        List<Rule> rules = ruleMapper.selectList(queryWrapper);
        rules.forEach(ruletemp -> {
            if (ruletemp.getName().equals(rule.getName())) {
                throw new BizException("规则名称已存在！");
            }
        });
    }

    /**
     * 删除规则分类明细信息接口
     *
     * @param id
     * @return
     */
    @Transactional
    public int deleteRule(String id) {
        int i = 0;
        Rule rule = new Rule();
        rule.setId(id);
        RuleDetail rd = new RuleDetail();
        rd.setRuleId(id);
        Wrapper<RuleDetail> detailWrapper = new UpdateWrapper<>();
        ((UpdateWrapper<RuleDetail>) detailWrapper).setEntity(rd);
        checkInstanceRule(id);
        checkScoreRule(id);
        i += ruleDetailMapper.delete(detailWrapper);
        i += ruleMapper.deleteById(id);
        return i;
    }

    private void checkScoreRule(String id) {
        Wrapper<ScoreInstanceRule> wrapper1 = new QueryWrapper<>();
        ((QueryWrapper<ScoreInstanceRule>) wrapper1).eq("biz_type", 1);
        ((QueryWrapper<ScoreInstanceRule>) wrapper1).eq("biz_id", id);
        long cout = scoreInstanceRuleMapper.selectCount(wrapper1);
        if (cout > 0) {
            throw new BizException("该规则已被使用，不可删除！");
        }
    }

    private void checkInstanceRule(String id) {
        Wrapper<InstanceRule> wrapper = new QueryWrapper<>();
        ((QueryWrapper<InstanceRule>) wrapper).eq("RULE_ID", id);
        long count = instanceRuleMapper.selectCount(wrapper);
        if (count > 0) {
            throw new BizException("该规则已被使用，不可进行此操作！");
        }
    }

    /**
     * 修改规则分类明细信息状态接口
     *
     * @param rule
     * @return
     */
    @Transactional
    public int switchRule(Rule rule) {
        if (CommonConstant.DISABLE_1.equals(rule.getStatus())){
            checkInstanceRule(rule.getId());
        }
        return ruleMapper.updateById(rule);
    }

    public int checkRuleDetai(String id) {
        Wrapper<RuleDetailVal> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleDetailVal>) queryWrapper).eq("RULE_DETAIL_ID", id);
        return ruleDetailValMapper.selectCount(queryWrapper);
    }

    public List<String> getRuleIdList(String categoryId){
        QueryWrapper<Rule> ruleQueryWrapper = new QueryWrapper<>();
        ruleQueryWrapper.eq(CommonConstant.CATEGORY_ID, categoryId);
        ruleQueryWrapper.select(CommonConstant.ID);
        return ruleMapper.selectObjs(ruleQueryWrapper)
            .stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.toList());
    }

    /**
     * 规则质控查询
     * @param ruleQueryDto
     * @return
     */
    public List<RuleQueryResult> getAllRuleListByCK(RuleQueryDto ruleQueryDto) {
        this.exceptionName = parameterService.getByCode("EXCEPTION_NAME").getValue();
        List<String> datasetIdList = new ArrayList<>();
        List<PreDataRule> preDataRules = Lists.newArrayList();
        this.ruleQueryDto = ruleQueryDto;

        switch (ruleQueryDto.getType()) {
            case CommonConstant.QUERY_RULE_OF_ALL: {
                PreDataRule preDataRule = new PreDataRule();
                if(StringUtils.isNotBlank(ruleQueryDto.getStandardId())){
                    preDataRule.setStandardId(ruleQueryDto.getStandardId());
                }
                preDataRules = preDataRuleService.findPreDataRules(preDataRule, ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
                break;
            }
            case CommonConstant.QUERY_RULE_OF_SCORE: {
                checkScore(ruleQueryDto);

                ScoreInstance scoreInstance = scoreInstanceMapper.selectById(ruleQueryDto.getScoreInstanceId());
                if (scoreInstance == null) {
                    throw new BizException("评分对象不存在");
                }

                // 根据类型，得出需要查询的对象及子对象
                List<ScoreInstance> vosToQuery = new ArrayList<>();
                if (CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(scoreInstance.getIsCategory())) {
                    vosToQuery.add(scoreInstance);
                } else {
                    List<ScoreInstance> childScoreInstances = scoreInstanceService.getAllChild(scoreInstance.getId());

                    for (ScoreInstance childScoreInstance : childScoreInstances) {
                        if (CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(childScoreInstance.getIsCategory())) {
                            vosToQuery.add(childScoreInstance);
                        }
                    }
                }

                for (ScoreInstance vo : vosToQuery) {
                    List<String> ruleIds = new ArrayList<>();

                    // 查询规则ids
                    List<Rule> rules = ruleMapper.selectListForScoreInstance(vo.getId());
                    if (CollectionUtils.isNotEmpty(rules)) {
                        for (Rule rule : rules) {
                            ruleIds.add(rule.getId());
                        }
                    }

                    // 查询数据集流水号
                    List<Dataset> datasets = dataSetMapper.selectListByScoreInstanceId(vo.getId(),
                        CommonConstant.SOURCE_TYPE_INSTANCE);
                    if (CollectionUtils.isNotEmpty(datasets)) {
                        for (Dataset dataset : datasets) {
                            datasetIdList.add(dataset.getDataSetId().toString());
                        }
                    }

                    // 查询机构codes或数据来源ids
                    if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(vo.getScoreTag()) && StringUtils.isNotBlank(vo.getOrgCode())) {
                        ruleQueryDto.setOrgCode(vo.getOrgCode());
                        List<PreDataRule> orgRules = preDataRuleService.getListByOrgCodeAndRule(datasetIdList, ruleQueryDto, ruleIds);
                        if (CollectionUtils.isNotEmpty(orgRules)) {
                            preDataRules.addAll(orgRules);
                        }
                    } else {
                        ruleQueryDto.setSourceId(vo.getDatasourceId());
                        List<PreDataRule> sourceRules = preDataRuleService.getListBySourceIdAndRule(datasetIdList, ruleQueryDto, ruleIds);
                        if (CollectionUtils.isNotEmpty(sourceRules)) {
                            preDataRules.addAll(sourceRules);
                        }
                    }
                }
                break;
            }
            case CommonConstant.QUERY_RULE_OF_DATASOURCE: {
                checkDatasource(ruleQueryDto);
                QueryWrapper queryWrapperDataSource = new QueryWrapper<DataSource>();
                queryWrapperDataSource.eq("CODE", ruleQueryDto.getSourceId());
                DataSource dataSource = dataSourceService.getOne(queryWrapperDataSource);
                if (dataSource == null) {
                    throw new BizException("数据源不存在");
                }
                QueryWrapper queryWrapper = new QueryWrapper<DataSourceDataset>();
                queryWrapper.eq("DATASOURCE_ID", dataSource.getId());
                List<DataSourceDataset> sdatasets = dataSourceDateSetMapper.selectList(queryWrapper);
                sdatasets.forEach(scoreInstanceDataSet -> {
                    datasetIdList.add(scoreInstanceDataSet.getDataSetId().toString());
                });
                if (dataSourceService.checkIsYiLiaoBySourceId(ruleQueryDto.getSourceId())) {
                    // 数据来源查询依照目前的场景需求，只需要根据标准id查询即可。因为基本医疗在hive中有很多不同的sourceId
                    ruleQueryDto.setSourceId(null);
                }
                preDataRules = preDataRuleService.getListBySourceId(datasetIdList, ruleQueryDto);
                break;
            }
            case CommonConstant.QUERY_RULE_OF_AREA: {
                checkArea(ruleQueryDto);
                List<String> orgs = Lists.newArrayList();
                List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaCode(ruleQueryDto.getAreaCode());
                if (CollectionUtils.isNotEmpty(orgList)) {
                    orgs.addAll(orgList.stream().map(RrsOrganization::getOrgCode).collect(Collectors.toList()));
                }
                preDataRules = preDataRuleService.getListByOrgs(orgs, ruleQueryDto);
                break;
            }
            case CommonConstant.QUERY_RULE_OF_ORG: {
                checkOrg(ruleQueryDto);
                List<String> orgs = new ArrayList<>();
                orgs.add(ruleQueryDto.getOrgCode());
                preDataRules = preDataRuleService.getListByOrgs(orgs, ruleQueryDto);
                break;
            }
            case CommonConstant.QUERY_RULE_OF_DATASET: {
                checkDataset(ruleQueryDto);
                QueryWrapper queryWrapper = new QueryWrapper<Dataset>();
                ((QueryWrapper) queryWrapper).eq("STANDARD_ID", ruleQueryDto.getStandardId());
                ((QueryWrapper) queryWrapper).eq("METASET_CODE", ruleQueryDto.getDatasetCode());
                List<Dataset> sdatasets = dataSetMapper.selectList(queryWrapper);
                sdatasets.forEach(scoreInstanceDataSet -> {
                    datasetIdList.add(scoreInstanceDataSet.getDataSetId().toString());
                });
                preDataRules = preDataRuleService.getListByDatasetIds(datasetIdList, ruleQueryDto);
                break;
            }
            default:
                break;
        }
        List<RuleCategory> ruleOldList = ruleCategoryService.getRuleCategory(new RuleCategory());
        return mergeCKList(preDataRules, ruleOldList);
    }

    private List<RuleQueryResult> mergeRuleFieldList(List<PreDataRuleField> rules, List<RuleCategory> ruleOldList) {

        Map<String, List<PreDataRuleField>> ruleFieldMap = rules.stream().collect(Collectors.groupingBy(rule -> {
            return rule.getCategoryId();
        }));
        Map<String, List<PreDataRuleField>> rulesMap2 = rules.stream().collect(Collectors.groupingBy(rule -> {
            return rule.getRuleId();
        }));
        ruleFieldMap.putAll(rulesMap2);
        List<RuleQueryResult> noDataList = castRuleFieldList(ruleFieldMap, ruleOldList, null, 0);
        sumAllSon(noDataList, null);
        return noDataList;
    }

    private List<RuleQueryResult> castRuleFieldList(Map<String, List<PreDataRuleField>> ruleFieldMap, List<RuleCategory> ruleOldList, String topName, int level) {
        List<RuleQueryResult> rList = new ArrayList<>();
        for (RuleCategory ruleCategory : ruleOldList) {
            RuleQueryResult ruleQueryResult = new RuleQueryResult();
            ruleQueryResult.setCategoryId(ruleCategory.getId());
            ruleQueryResult.setCategoryName(ruleCategory.getName());
            ruleQueryResult.setId(UUID.randomUUID().toString().replaceAll("-", ""));
            ruleQueryResult.setCheckCount(0L);
            ruleQueryResult.setCheckError(0L);
            ruleQueryResult.setErrorPercent(0d);
            ruleQueryResult.setDatasetIds(getDatasetIdByRuleField(ruleFieldMap.get(ruleCategory.getId())));
            ruleQueryResult.setLevel(level + ",0");
            if (topName == null || level == 0) {
                topName = ruleCategory.getName();
            }
            ruleQueryResult.setTopName(topName);
            if (ruleCategory.getChildren() != null && !ruleCategory.getChildren().isEmpty()) {
                ruleQueryResult.setIsEnd(0);
                ruleQueryResult.setChildren(castRuleFieldList(ruleFieldMap, ruleCategory.getChildren(), topName, level + 1));
            } else {
                ruleQueryResult.setIsEnd(1);
                List<RuleQueryResult> lastList = new ArrayList<>();
                for (Rule rule : ruleCategory.getRules()) {
                    RuleQueryResult result = new RuleQueryResult();
                    result.setRuleId(rule.getId());
                    result.setIsEnd(1);
                    result.setCategoryId(ruleCategory.getId());
                    result.setCategoryName(ruleCategory.getName());
                    result.setRuleName(rule.getName());
                    result.setLevel((level + 1) + ",99");
                    result.setDatasetIds(getDatasetIdByRuleField(ruleFieldMap.get(rule.getId())));
                    result.setTopName(topName);
                    result.setId(UUID.randomUUID().toString().replaceAll("-", ""));
                    List<PreDataRuleField> fieldList = ruleFieldMap.get(rule.getId());
                    if (CollectionUtils.isNotEmpty(fieldList)) {
                        Long validateNum = fieldList.stream().mapToLong(PreDataRuleField::getValidateNum).sum();
                        Long checkNum = fieldList.stream().mapToLong(PreDataRuleField::getCheckNum).sum();
                        result.setCheckCount(validateNum);
                        result.setCheckError(checkNum);
                        result.setErrorPercent(getPercent(result));
                    }
                    lastList.add(result);
                }
                ruleQueryResult.setChildren(lastList);
            }
            rList.add(ruleQueryResult);
        }
        return rList;
    }

    private List<Long> getScoreInstancsIds(List<ScoreInstance> scoreInstances, String scoreInstanceId) {
        List<Long> ids = new ArrayList<>();
        ids.add(Long.parseLong(scoreInstanceId));
        for (ScoreInstance scoreInstance : scoreInstances) {
            if (scoreInstance.getParentId() != null && scoreInstance.getParentId().toString().equals(scoreInstanceId)) {
                ids.addAll(getScoreInstancsIds(scoreInstances, scoreInstance.getId().toString()));
            }
        }
        return ids;
    }

    private List<String> getOrgByArea(String areaCode) {
        List<String> orgCodeList = new ArrayList<>();
        RrsArea area = dmQueryService.findAreaByAreaCode(areaCode);
        if (DmAreaConstant.CITY_LEVEL.equals(area.getLevelCode())) {
            List<RrsArea> areaList = dmQueryService.findAreaListByPid(area.getId());
            for (RrsArea childArea : areaList) {
                List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaId(childArea.getId());
                orgCodeList.addAll(orgList.stream().map(RrsOrganization::getOrgCode).collect(Collectors.toList()));
            }
        } else if (DmAreaConstant.AREA_LEVEL.equals(area.getLevelCode())) {
            List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaId(area.getId());
            orgCodeList.addAll(orgList.stream().map(RrsOrganization::getOrgCode).collect(Collectors.toList()));
        }
        return orgCodeList;
    }


    /**
     * 合并list by CK
     * @param rules
     * @param ruleOldList
     * @return
     */
    private List<RuleQueryResult> mergeCKList(List<PreDataRule> rules, List<RuleCategory> ruleOldList) {

        Map<String, List<PreDataRule>> rulesMap = rules.stream().collect(Collectors.groupingBy(rule -> {
            return rule.getCategoryId();
        }));
        Map<String, List<PreDataRule>> rulesMap2 = rules.stream().collect(Collectors.groupingBy(rule -> {
            return String.valueOf(rule.getRuleId());
        }));
        rulesMap.putAll(rulesMap2);
        List<RuleQueryResult> noDataList = castRuleListByCK(rulesMap, ruleOldList, null, 0);
        sumAllSon(noDataList, null);
        return noDataList;
    }

    private void sumAllSon(List<RuleQueryResult> noDataList, RuleQueryResult father) {
        for (int i = 0; i < noDataList.size(); i++) {
            if (noDataList.get(i).getChildren() != null && !noDataList.get(i).getChildren().isEmpty()) {
                sumAllSon(noDataList.get(i).getChildren(), noDataList.get(i));
            }
            if (father != null) {
                father.setCheckCount(noDataList.get(i).getCheckCount() + father.getCheckCount());
                father.setCheckError(noDataList.get(i).getCheckError() + father.getCheckError());
                father.setErrorPercent(getPercent(father));
            }
        }
    }

    @Autowired
    private ParameterService parameterService;


    private List<RuleQueryResult> castRuleListByCK(Map<String, List<PreDataRule>> ruleMap, List<RuleCategory> ruleOldList, String topName, int level) {
        List<RuleQueryResult> rList = new ArrayList<>();
        for (RuleCategory ruleCategory : ruleOldList) {
            RuleQueryResult ruleQueryResult = new RuleQueryResult();
            ruleQueryResult.setCategoryId(ruleCategory.getId());
            ruleQueryResult.setCategoryName(ruleCategory.getName());
            ruleQueryResult.setId(UUID.randomUUID().toString().replaceAll("-", ""));
            ruleQueryResult.setCheckCount(0L);
            ruleQueryResult.setCheckError(0L);
            ruleQueryResult.setErrorPercent(0d);
            ruleQueryResult.setDatasetIds(getDatasetIdByCK(ruleMap.get(ruleCategory.getId())));
            ruleQueryResult.setLevel(level + ",0");
            if (topName == null || level == 0) {
                topName = ruleCategory.getName();
            }
            ruleQueryResult.setTopName(topName);
            if (ruleCategory.getChildren() != null && !ruleCategory.getChildren().isEmpty()) {
                ruleQueryResult.setIsEnd(0);
                ruleQueryResult.setChildren(castRuleListByCK(ruleMap, ruleCategory.getChildren(), topName, level + 1));
            } else {
                ruleQueryResult.setIsEnd(1);
                List<RuleQueryResult> lastList = new ArrayList<>();
                for (Rule rule : ruleCategory.getRules()) {
                    RuleQueryResult result = new RuleQueryResult();
                    result.setRuleId(rule.getId());
                    result.setIsEnd(1);
                    result.setCategoryId(ruleCategory.getId());
                    result.setCategoryName(ruleCategory.getName());
                    result.setRuleName(rule.getName());
                    result.setLevel((level + 1) + ",99");
                    result.setDatasetIds(getDatasetIdByCK(ruleMap.get(rule.getId())));
                    result.setTopName(topName);
                    result.setId(UUID.randomUUID().toString().replaceAll("-", ""));
                    if (ruleQueryResult.getCategoryName().contains(this.exceptionName)) {
                        result.setCheckCount(queryCKCheckCount(this.ruleQueryDto));
                        result.setCheckError(queryCKErrorCount(this.ruleQueryDto));
                        result.setErrorPercent(getPercent(result));
                    } else {
                        result.setCheckCount(getCheckCountByCK(ruleMap.get(rule.getId())));
                        result.setCheckError(getCheckErrorByCK(ruleMap.get(rule.getId())));
                        result.setErrorPercent(getPercent(result));
                    }
                    lastList.add(result);
                }
                ruleQueryResult.setChildren(lastList);
            }
            rList.add(ruleQueryResult);
        }
        return rList;
    }

    private List<String> getDatasetId(List<RuleQuerierResult> list) {
        if (list == null) {
            return new ArrayList<>();
        } else {
            return list.stream().map(ruleQuerierResult -> {
                return ruleQuerierResult.getDatasetId();
            }).distinct().collect(Collectors.toList());
        }
    }


    private List<String> getDatasetIdByCK(List<PreDataRule> list) {
        if (list == null) {
            return new ArrayList<>();
        } else {
            return list.stream().map(preDataRule -> {
                return preDataRule.getDatasetId();
            }).distinct().collect(Collectors.toList());
        }
    }

    private List<String> getDatasetIdByRuleField(List<PreDataRuleField> list) {
        if (list == null) {
            return new ArrayList<>();
        } else {
            return list.stream().map(preDataRule -> {
                return preDataRule.getDatasetId();
            }).distinct().collect(Collectors.toList());
        }
    }

    private Double getPercent(RuleQueryResult c) {
        if (c.getCheckCount() == 0) {
            return 0d;
        } else {
            return new BigDecimal(c.getCheckError()).divide(new BigDecimal(c.getCheckCount()), BigDecimal.ROUND_HALF_UP, 6).doubleValue();
        }
    }

    private Long getCheckError(List<RuleQuerierResult> list) {
        if (list == null) {
            return 0L;
        }
        return list.stream().mapToLong(RuleQuerierResult::getFailAmount).sum();
    }

    private Long getCheckCount(List<RuleQuerierResult> list) {
        if (list == null) {
            return 0L;
        }
        return list.stream().mapToLong(ruleQuerierResult -> {
            return ruleQuerierResult.getFailAmount() + ruleQuerierResult.getSuccessAmount();
        }).sum();
    }


    private Long getCheckErrorByCK(List<PreDataRule> list) {
        if (list == null) {
            return 0L;
        }
        return list.stream().mapToLong(PreDataRule::getCheckNum).sum();
    }

    private Long getCheckCountByCK(List<PreDataRule> list) {
        if (list == null) {
            return 0L;
        }
        return list.stream().mapToLong(PreDataRule::getValidateNum).sum();
    }


    /**
     * 查询所有数据集的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkDataset(RuleQueryDto ruleQueryDto) {
        if (StringUtils.isBlank(ruleQueryDto.getStandardId())) {
            throw new BizException("标准id不能为空！");
        }
        if (StringUtils.isBlank(ruleQueryDto.getDatasetCode())) {
            throw new BizException("数据集编码不能为空！");
        }
    }

    /**
     * 查询所有机构的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkOrg(RuleQueryDto ruleQueryDto) {
        if (StringUtils.isBlank(ruleQueryDto.getOrgCode())) {
            throw new BizException("机构编码不能为空！");
        }
    }

    /**
     * 查询所有区域的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkArea(RuleQueryDto ruleQueryDto) {
        if (StringUtils.isBlank(ruleQueryDto.getAreaCode())) {
            throw new BizException("区域编码不能为空！");
        }
    }

    /**
     * 查询数据源的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkDatasource(RuleQueryDto ruleQueryDto) {
        if (StringUtils.isBlank(ruleQueryDto.getSourceId())) {
            throw new BizException("数据来源id不能为空！");
        }
    }

    /**
     * 查询评分的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkScore(RuleQueryDto ruleQueryDto) {
        if (StringUtils.isBlank(ruleQueryDto.getScoreInstanceId())) {
            throw new BizException("评分对象id不能为空！");
        }
    }

    /**
     * 查询所有规则的参数检查
     *
     * @param ruleQueryDto
     */
    private void checkRuleAll(RuleQueryDto ruleQueryDto) {

    }


    public Long queryCKCheckCount(RuleQueryDto ruleQueryDto) {
        DataVolumeStatistics dataVolumeStatistics = new DataVolumeStatistics();
        dataVolumeStatistics.setOrgCode(ruleQueryDto.getOrgCode());
        dataVolumeStatistics.setDatasetCode(ruleQueryDto.getDatasetCode());
        dataVolumeStatistics.setStandardId(ruleQueryDto.getStandardId());
        dataVolumeStatistics.setSourceId(ruleQueryDto.getSourceId());
        List<DataVolumeStatistics> volumeStatistics = dataVolumeStatisticsService.findDataVolumeStatistics(
            dataVolumeStatistics, ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return volumeStatistics.stream().mapToLong(DataVolumeStatistics::getDataAmount).sum();
    }


    public Long queryCKErrorCount(RuleQueryDto ruleQueryDto) {
        PreDataRule preDataRule = new PreDataRule();
        preDataRule.setOrgCode(ruleQueryDto.getOrgCode());
        preDataRule.setDatasetId(ruleQueryDto.getDatasetCode());
        preDataRule.setStandardId(ruleQueryDto.getStandardId());
        List<PreDataRule> statistic = preDataRuleService.findPreDataRules(
            preDataRule, ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return statistic.stream().mapToLong(PreDataRule::getCheckNum).sum();
    }
}
