package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.page.PageMethod;
import com.google.common.collect.Maps;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.core.biz.service.dao.mysql.*;
import com.gwi.qcs.model.domain.clickhouse.PreDataRule;
import com.gwi.qcs.model.domain.mysql.Instance;
import com.gwi.qcs.model.domain.mysql.ScoreInstance;
import com.gwi.qcs.model.dto.RuleQueryDto;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataRuleMapper;
import com.gwi.qcs.service.api.dto.QualityProblemDetailReq;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * (PreDataRule)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:37
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataRuleService extends AbstractSuperServiceImpl<PreDataRuleMapper, PreDataRule> {

    private static Logger logger = LoggerFactory.getLogger(PreDataRuleService.class);

    @Autowired
    ParameterService parameterService;

    @Autowired
    DatasetFiledService datasetFiledService;

    @Autowired
    DatasetService datasetService;

    @Autowired
    RuleService ruleService;

    @Autowired
    RuleCategoryService ruleCategoryService;

    @Autowired
    InstanceService instanceService;

    @Resource
    PreDataRuleMapper preDataRuleMapper;

    @Getter
    @Setter
    private Page<PreDataRule> pageInfo;

    /**
     * 删除维度数据
     *
     * @param condition 条件
     */
    @Override
    public void delByPreDataCondition(PreDataCondition condition) {
        getBaseMapper().delByCycleDayAndDatasetIdAndStandardId(condition.getCycleDay(), condition.getDatasetIds(), condition.getStandardId());

        Map<String, Object> map = MapUtil
            .builder("CYCLE_DAY", (Object) condition.getCycleDay())
            .put("STANDARD_ID", condition.getStandardId())
            .build();
        querySleepByConditionMap(map);
    }


    /**
     * 根据日期范围查询
     * @return
     */
    public List<PreDataRule> findPreDataRules(PreDataRule preDataRule, String beginDate,String endDate){
        if(StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)){
            return null;
        }
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>(preDataRule);
        queryWrapper.between("CYCLE_DAY" , beginDate , endDate);
        return this.list(queryWrapper);
    }



    /**
     * 根据日期范围查询
     * @return
     */
    public List<PreDataRule> findPreDataRules(PreDataRule preDataRule, List<String> datasetIds, String beginDate, String endDate,List<String> orgCodes, Page page){
        logger.info("当前页：" + page.getPageNum() + " -- 每页数：" + page.getPageSize());
        if(StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)){
            return null;
        }

        String instanceName = "";
        if(StringUtils.isNotBlank(preDataRule.getStandardId())){
            Instance instance = instanceService.getByStandardId(Long.valueOf(preDataRule.getStandardId()));
            instanceName = instance.getName();
        }

        List<PreDataRule> list = null;
        //区分基本医疗和其他
        if(CommonConstant.ORG_JBYL.equals(instanceName)){
            if(page != null){
                page.setPageNum((page.getPageNum() - 1) * page.getPageSize());
            }
            list = preDataRuleMapper.findPreDataRulesByJBYL(preDataRule ,datasetIds ,orgCodes , beginDate , endDate, page);
            int totol = preDataRuleMapper.getPdrByJBYLTotol(preDataRule, datasetIds, orgCodes, beginDate, endDate);
            if(page != null){
                pageInfo = new Page<>();
                pageInfo.setTotal(totol);
                pageInfo.setPages(totol / page.getPageSize());
            }
        }else{
            if (page != null) {
                pageInfo = PageHelper.startPage(page.getPageNum(), page.getPageSize());
            }
            QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>(preDataRule);
            queryWrapper.select("sum(VALIDATE_NUM) as validateNum","sum(CHECK_NUM) as checkNum" ,
                "case when validateNum = 0 then 0.0  else round(checkNum*100/validateNum, 2) end as rate",
                CommonConstant.STANDARD_ID_UPPER , CommonConstant.STAN_TYPE_UPPER , CommonConstant.CATEGORY_ID_UPPER,
                CommonConstant.RULE_ID_UPPER, CommonConstant.DATASET_ID_UPPER, CommonConstant.DATASET_ITEM_ID_UPPER);
            queryWrapper.between("CYCLE_DAY" , beginDate , endDate);
            if(datasetIds != null && !datasetIds.isEmpty()){
                queryWrapper.in("DATASET_ID", datasetIds);
            }
            if(orgCodes != null && !orgCodes.isEmpty()){
                queryWrapper.in("ORG_CODE", orgCodes);
            }
            queryWrapper.orderByDesc("rate");
            queryWrapper.groupBy(CommonConstant.STANDARD_ID_UPPER , CommonConstant.STAN_TYPE_UPPER , CommonConstant.CATEGORY_ID_UPPER,
                CommonConstant.RULE_ID_UPPER, CommonConstant.DATASET_ID_UPPER, CommonConstant.DATASET_ITEM_ID_UPPER);
            list = this.list(queryWrapper);
        }
        return expandParamHandle(list);
    }


    private List<PreDataRule> expandParamHandle(List<PreDataRule> list){
        Map<String, String> ruleNameMap = Maps.newHashMap();
        Map<String, String> ruleCategoryMap = Maps.newHashMap();
        for(PreDataRule pdr : list){
            pdr.setDatasetItemCode(datasetFiledService.getCodeById(pdr.getDatasetItemId()));
            pdr.setDatasetCode(datasetService.getCodeById(pdr.getDatasetId()));
            pdr.setDatasetName(datasetService.getNameById(pdr.getDatasetId()));
            pdr.setRuleName(ruleService.getNameById(pdr.getRuleId(), ruleNameMap));
            pdr.setCategoryName(ruleCategoryService.getNameById(pdr.getCategoryId(), ruleCategoryMap));
        }
        return list;
    }

    /**
     * 根据数据集Ids获取规则质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     */
    public List<PreDataRule> getListByDatasetIds(List<String> datasetIds, RuleQueryDto ruleQueryDto) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        if(CollectionUtils.isNotEmpty(datasetIds)){
            queryWrapper.in("DATASET_ID", datasetIds);
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    /**
     * 根据机构编码和规则获取规则质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     * @param ruleIds 规则Ids
     */
    public List<PreDataRule> getListByOrgCodeAndRule(List<String> datasetIds, RuleQueryDto ruleQueryDto, List<String> ruleIds) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("ORG_CODE", ruleQueryDto.getOrgCode());
        if (StringUtils.isNotBlank(ruleQueryDto.getDatasetId())) {
            queryWrapper.eq("DATASET_ID", ruleQueryDto.getDatasetId());
        } else {
            if(CollectionUtils.isNotEmpty(datasetIds)){
                queryWrapper.in("DATASET_ID", datasetIds);
            }
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        if (CollectionUtils.isNotEmpty(ruleIds)) {
            queryWrapper.in("RULE_ID", ruleIds);
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    /**
     * 根据数据来源Id和规则获取规则质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     * @param ruleIds 规则Ids
     */
    public List<PreDataRule> getListBySourceIdAndRule(List<String> datasetIds, RuleQueryDto ruleQueryDto, List<String> ruleIds) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        if (StringUtils.isNotBlank(ruleQueryDto.getSourceId())) {
            queryWrapper.eq("SOURCE_ID", ruleQueryDto.getSourceId());
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getDatasetId())) {
            queryWrapper.eq("DATASET_ID", ruleQueryDto.getDatasetId());
        } else {
            if(CollectionUtils.isNotEmpty(datasetIds)){
                queryWrapper.in("DATASET_ID", datasetIds);
            }
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        if (CollectionUtils.isNotEmpty(ruleIds)) {
            queryWrapper.in("RULE_ID", ruleIds);
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    /**
     * 根据数据来源Id获取规则质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     */
    public List<PreDataRule> getListBySourceId(List<String> datasetIds, RuleQueryDto ruleQueryDto) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        if (StringUtils.isNotBlank(ruleQueryDto.getSourceId())) {
            queryWrapper.eq("SOURCE_ID", ruleQueryDto.getSourceId());
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getDatasetId())) {
            queryWrapper.eq("DATASET_ID", ruleQueryDto.getDatasetId());
        } else {
            if(CollectionUtils.isNotEmpty(datasetIds)){
                queryWrapper.in("DATASET_ID", datasetIds);
            }
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    /**
     * 根据所选机构获取规则质控列表
     *
     * @param orgs 所选机构
     * @param ruleQueryDto 规则查询对象
     */
    public List<PreDataRule> getListByOrgs(List<String> orgs, RuleQueryDto ruleQueryDto) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("ORG_CODE", orgs);
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        if (StringUtils.isNotBlank(ruleQueryDto.getDatasetId())) {
            queryWrapper.eq("DATASET_ID", ruleQueryDto.getDatasetId());
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    public List<PreDataRule> get(QualityProblemDetailReq qualityProblemDetailReq, List<String> ruleIdList, ScoreInstance scoreInstance,
                                List<String> datasetList) {
        QueryWrapper<PreDataRule> queryWrapper = new QueryWrapper<>();
        queryWrapper.select("sum(CHECK_NUM) as checkNum", CommonConstant.DATASET_ID_UPPER, CommonConstant.DATASET_ITEM_ID_UPPER,
            CommonConstant.CATEGORY_ID_UPPER, CommonConstant.RULE_ID_UPPER);
        queryWrapper.orderByDesc("checkNum");
        LambdaQueryWrapper<PreDataRule> lambdaQueryWrapper = queryWrapper.lambda();
        lambdaQueryWrapper.groupBy(PreDataRule::getCheckNum, PreDataRule::getDatasetId, PreDataRule::getDatasetItemId,
            PreDataRule::getCategoryId, PreDataRule::getRuleId);
        lambdaQueryWrapper.gt(PreDataRule::getCheckNum, 0);
        if(CollectionUtils.isNotEmpty(ruleIdList)){
            lambdaQueryWrapper.in(PreDataRule::getRuleId, ruleIdList);
        }
        if(qualityProblemDetailReq.getDqObj() != null){
            lambdaQueryWrapper.eq(PreDataRule::getStandardId, qualityProblemDetailReq.getDqObj());
        }
        if(StringUtils.isNotEmpty(scoreInstance.getOrgCode())){
            lambdaQueryWrapper.eq(PreDataRule::getOrgCode, scoreInstance.getOrgCode());
        }
        if (CollectionUtils.isNotEmpty(datasetList)) {
            lambdaQueryWrapper.in(PreDataRule::getDatasetId, datasetList);
        }
        if (StringUtils.isNotEmpty(scoreInstance.getDatasourceId())) {
            lambdaQueryWrapper.eq(PreDataRule::getSourceId, scoreInstance.getDatasourceId());
        }
        if (StringUtils.isNotEmpty(qualityProblemDetailReq.getTableId())) {
            lambdaQueryWrapper.eq(PreDataRule::getDatasetId, qualityProblemDetailReq.getTableId());
        }
        if (StringUtils.isNotEmpty(qualityProblemDetailReq.getFieldCode())) {
            lambdaQueryWrapper.eq(PreDataRule::getDatasetItemId, qualityProblemDetailReq.getFieldCode());
        }
        if (StringUtils.isNotEmpty(qualityProblemDetailReq.getStartTime()) && StringUtils.isNotEmpty(qualityProblemDetailReq.getEndTime())) {
            lambdaQueryWrapper.between(PreDataRule::getCycleDay, qualityProblemDetailReq.getStartTime(), qualityProblemDetailReq.getEndTime());
        }
        int pageIndex = qualityProblemDetailReq.getPage();
        Page<PreDataRule> result;
        if(pageIndex != -1){
            result = PageMethod.startPage(pageIndex,qualityProblemDetailReq.getSize());
        }else{
            result = PageMethod.startPage(1, parameterService.getMaxExportNum());
        }
        Map<String, String> ruleNameMap = Maps.newHashMap();
        Map<String, String> ruleCategoryMap = Maps.newHashMap();
        for(PreDataRule preDataRule : result.getResult()){
            preDataRule.setDatasetItemCode(datasetFiledService.getCodeById(preDataRule.getDatasetItemId()));
            preDataRule.setDatasetCode(datasetService.getCodeById(preDataRule.getDatasetId()));
            preDataRule.setRuleName(ruleService.getNameById(preDataRule.getRuleId(), ruleNameMap));
            preDataRule.setCategoryName(ruleCategoryService.getNameById(preDataRule.getCategoryId(), ruleCategoryMap));
        }
        return result.getResult();
    }
}