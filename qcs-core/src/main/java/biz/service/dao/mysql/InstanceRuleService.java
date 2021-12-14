package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.mapenum.DqRuleEnum;
import com.gwi.qcs.model.mapper.mysql.InstanceRuleMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 规则实例表
 *
 * @author ljl
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class InstanceRuleService extends SuperServiceImpl<InstanceRuleMapper, InstanceRule> {
    public static final int STABILITY_NUM = 3;
    public static final int UNIQUE_NUM = 1;
    public static final int STR_LENGTH_NUM = 3;
    public static final int NUMBER_TYPE_NUM = 1;
    public static final int NORMAL_PARAM_RANGE_NUM = 2;
    public static final int NOT_NULL_NUM = 1;
    public static final int RELEVANCE_RULE_1_NUM = 4;
    public static final int RELEVANCE_RULE_2_NUM = 6;
    public static final int RELEVANCE_RULE_3_NUM = 8;
    public static final String JAVA_LANG_STRING = "java.lang.String";
    public static final String JAVA_LANG_INTEGER = "java.lang.Integer";
    public static final int MAX_RELEVANCE_NUM = 3;
    public static final int NUM_2 = 2;

    @Autowired
    ParameterService parameterService;

    @Autowired
    InstanceDatasetService instanceDatasetService;

    @Autowired
    RuleDetailValService ruleDetailValService;

    @Autowired
    RuleDetailService ruleDetailService;

    @Autowired
    RuleService ruleService;

    @Autowired
    DatasetService datasetService;

    @Autowired
    DatasetFiledService datasetFiledService;

    @Autowired
    DsmRefService dsmRefService;

    @Autowired
    DatasetInstanceRuleService datasetInstanceRuleService;

    @Transactional
    public void generateRule(Long instanceId){
        Rule rule = new Rule();
        rule.setAutoGenerate(true);
        Map<String, String> ruleMap = ruleService.list(new QueryWrapper<>(rule)).stream()
            .collect(Collectors.toMap(Rule::getRuleCode, Rule::getId));
        if(ruleMap.isEmpty()){
            return;
        }

        // 删除自动生成的规则
        InstanceRule instanceRuleQuery = new InstanceRule();
        instanceRuleQuery.setAutoGenerate(true);
        QueryWrapper<InstanceRule> queryWrapper = new QueryWrapper<>(instanceRuleQuery);
        List<Dataset> datasetList = datasetService.getBaseMapper().getByInstanceId(instanceId);
        if(CollectionUtils.isNotEmpty(datasetList)){
            queryWrapper.in(CommonConstant.DATASET_ID_UPPER, datasetList.stream()
                .map(Dataset::getDataSetId).collect(Collectors.toList()));
        }
        queryWrapper.in(CommonConstant.RULE_ID_UPPER, ruleMap.values());
        List<String> instanceRuleIdList = this.list(queryWrapper).stream()
            .map(InstanceRule::getId).collect(Collectors.toList());
        for(String id : instanceRuleIdList){
            RuleDetailVal ruleDetailVal = new RuleDetailVal();
            ruleDetailVal.setInstanceRuleId(id);
            ruleDetailValService.remove(new QueryWrapper<>(ruleDetailVal));

            DatasetInstanceRule datasetInstanceRule = new DatasetInstanceRule();
            datasetInstanceRule.setInstanceRuleId(id);
            datasetInstanceRuleService.remove(new QueryWrapper<>(datasetInstanceRule));
        }
        this.removeByIds(instanceRuleIdList);

        // 再次自动生成规则
        Map<String, String> fieldCodeMap = Maps.newHashMap();
        InstanceDataSet instanceDataSetQuery = new InstanceDataSet();
        instanceDataSetQuery.setInstanceId(instanceId);
        List<InstanceDataSet> instanceDataSetList = instanceDatasetService.list(new QueryWrapper<>(instanceDataSetQuery));
        for(InstanceDataSet instanceDataSet : instanceDataSetList){
            Dataset datasetQuery = new Dataset();
            datasetQuery.setDataSetId(instanceDataSet.getDataSetId());
            datasetQuery.setSourceType(CommonConstant.SOURCE_TYPE_INSTANCE);
            Dataset dataset = datasetService.getOne(new QueryWrapper<>(datasetQuery));
            if(dataset == null){
                continue;
            }
            InstanceRule instanceRuleParam = new InstanceRule();
            instanceRuleParam.setInstanceDataSet(instanceDataSet);
            instanceRuleParam.setDataset(dataset);
            instanceRuleParam.setDatasetFieldDomain(new DatasetField());
            if(ruleMap.containsKey(DqRuleEnum.STABILITY_RULE.getName())) {
                generateRuleDetailVal(ruleMap, DqRuleEnum.STABILITY_RULE, instanceRuleParam);
            }
            if(ruleMap.containsKey(DqRuleEnum.UNIQUE_RULE.getName())) {
                generateRuleDetailVal(ruleMap, DqRuleEnum.UNIQUE_RULE, instanceRuleParam);
            }
            if(ruleMap.containsKey(DqRuleEnum.RELEVANCE_RULE_1.getName())
                || ruleMap.containsKey(DqRuleEnum.RELEVANCE_RULE_2.getName())
                || ruleMap.containsKey(DqRuleEnum.RELEVANCE_RULE_3.getName())) {
                DsmRef dsmRefQuery = new DsmRef();
                dsmRefQuery.setStandardId(dataset.getStandardId());
                dsmRefQuery.setSubMetasetCode(dataset.getMetasetCode());
                Map<String, List<DsmRef>> dsmRefMap = dsmRefService.list(new QueryWrapper<>(dsmRefQuery)).stream()
                    .collect(Collectors.groupingBy(DsmRef::getMainMetasetCode));
                for(Map.Entry<String, List<DsmRef>> entry : dsmRefMap.entrySet()){
                    fieldCodeMap = generateRelevanceRule(ruleMap, fieldCodeMap, dataset, instanceRuleParam, entry.getValue());
                }
            }
            if(ruleMap.containsKey(DqRuleEnum.STR_LENGTH_RULE.getName())
                || ruleMap.containsKey(DqRuleEnum.NUMBER_TYPE_RULE.getName())
                || ruleMap.containsKey(DqRuleEnum.NORMAL_PARAM_RANGE_RULE.getName())
                || ruleMap.containsKey(DqRuleEnum.NOT_NULL_RULE.getName())) {
                DatasetField datasetFieldQuery = new DatasetField();
                datasetFieldQuery.setDatasetId(dataset.getDataSetId());
                datasetFieldQuery.setSourceType(CommonConstant.SOURCE_TYPE_INSTANCE);
                List<DatasetField> datasetFieldList = datasetFiledService.list(new QueryWrapper<>(datasetFieldQuery));
                for(DatasetField datasetField : datasetFieldList){
                    String metadataType = datasetField.getMetadataType();
                    String dataDicCode = datasetField.getDataDicCode();
                    instanceRuleParam.setDatasetFieldDomain(datasetField);
                    if(ruleMap.containsKey(DqRuleEnum.STR_LENGTH_RULE.getName())
                        && JAVA_LANG_STRING.equals(metadataType)
                        && StringUtils.isEmpty(dataDicCode)){
                        generateRuleDetailVal(ruleMap, DqRuleEnum.STR_LENGTH_RULE, instanceRuleParam);
                    }
                    if(ruleMap.containsKey(DqRuleEnum.NUMBER_TYPE_RULE.getName())
                        && JAVA_LANG_INTEGER.equals(metadataType)) {
                        generateRuleDetailVal(ruleMap, DqRuleEnum.NUMBER_TYPE_RULE, instanceRuleParam);
                    }
                    if(ruleMap.containsKey(DqRuleEnum.NORMAL_PARAM_RANGE_RULE.getName())
                        && StringUtils.isNotEmpty(dataDicCode)) {
                        generateRuleDetailVal(ruleMap, DqRuleEnum.NORMAL_PARAM_RANGE_RULE, instanceRuleParam);
                    }
                    if(ruleMap.containsKey(DqRuleEnum.NOT_NULL_RULE.getName())
                        && CommonConstant.NOT_NULL.equals(datasetField.getIsNull())) {
                        generateRuleDetailVal(ruleMap, DqRuleEnum.NOT_NULL_RULE, instanceRuleParam);
                    }
                }
            }
        }
    }

    private Map<String, String> generateRelevanceRule(Map<String, String> ruleMap, Map<String, String> fieldCodeMap,
                                                      Dataset dataset, InstanceRule instanceRuleParam,
                                                      List<DsmRef> dsmRefList) {
        int size = dsmRefList.size();
        if(size > 0 && size <= MAX_RELEVANCE_NUM){
            if(fieldCodeMap.isEmpty()){
                fieldCodeMap = datasetFiledService.getMetaSetAndMetDataForFieldNameMap(dataset.getStandardId(),
                    CommonConstant.SOURCE_TYPE_INSTANCE);
            }
            if(instanceRuleParam.getFieldCodeMap() == null){
                instanceRuleParam.setFieldCodeMap(fieldCodeMap);
            }
            DqRuleEnum dqRuleEnum;
            if(size == 1){
                dqRuleEnum = DqRuleEnum.RELEVANCE_RULE_1;
            }else if(size == NUM_2){
                dqRuleEnum = DqRuleEnum.RELEVANCE_RULE_2;
            }else{
                dqRuleEnum = DqRuleEnum.RELEVANCE_RULE_3;
            }
            instanceRuleParam.setDsmRefList(dsmRefList);
            generateRuleDetailVal(ruleMap, dqRuleEnum, instanceRuleParam);
        }
        return fieldCodeMap;
    }

    private void generateRuleDetailVal(Map<String, String> ruleMap, DqRuleEnum dqRuleEnum, InstanceRule instanceRuleParam) {
        String ruleId = ruleMap.get(dqRuleEnum.getName());
        RuleDetail ruleDetailQuery = new RuleDetail();
        ruleDetailQuery.setRuleId(ruleId);
        List<RuleDetail> ruleDetailList = ruleDetailService.list(new QueryWrapper<>(ruleDetailQuery));
        String hintName = dqRuleEnum.getName();
        int num;
        switch (dqRuleEnum){
            case STABILITY_RULE:
                num = STABILITY_NUM;
                break;
            case UNIQUE_RULE:
                num = UNIQUE_NUM;
                break;
            case STR_LENGTH_RULE:
                num = STR_LENGTH_NUM;
                break;
            case NUMBER_TYPE_RULE:
                num = NUMBER_TYPE_NUM;
                break;
            case NORMAL_PARAM_RANGE_RULE:
                num = NORMAL_PARAM_RANGE_NUM;
                break;
            case NOT_NULL_RULE:
                num = NOT_NULL_NUM;
                break;
            case RELEVANCE_RULE_1:
                num = RELEVANCE_RULE_1_NUM;
                break;
            case RELEVANCE_RULE_2:
                num = RELEVANCE_RULE_2_NUM;
                break;
            case RELEVANCE_RULE_3:
                num = RELEVANCE_RULE_3_NUM;
                break;
            default:
                return;
        }
        if(ruleDetailList.size() != num){
            log.error(hintName + "规则被修改不能自动生成");
            return;
        }

        DatasetField datasetField = instanceRuleParam.getDatasetFieldDomain();
        InstanceDataSet instanceDataSet = instanceRuleParam.getInstanceDataSet();
        Dataset dataset = instanceRuleParam.getDataset();

        InstanceRule instanceRule = new InstanceRule();
        instanceRule.setRuleId(ruleId);
        instanceRule.setDatasetId(instanceDataSet.getDataSetId().toString());
        instanceRule.setDatasetCode(dataset.getMetasetCode());
        String metadataCode = StringUtils.EMPTY;
        String metadataName = StringUtils.EMPTY;
        if(dqRuleEnum == DqRuleEnum.STR_LENGTH_RULE
            || dqRuleEnum == DqRuleEnum.NUMBER_TYPE_RULE
            || dqRuleEnum == DqRuleEnum.NORMAL_PARAM_RANGE_RULE
            || dqRuleEnum == DqRuleEnum.NOT_NULL_RULE){
            metadataCode = datasetField.getFieldName();
            metadataName = datasetField.getMetadataName();
        }
        instanceRule.setMetadataCode(metadataCode);
        int count = this.count(new QueryWrapper<>(instanceRule));
        if(count > 0){
            return;
        }
        instanceRule.setDatasetField(datasetField.getId() == null ? null : datasetField.getId().toString());
        instanceRule.setDatasetName(dataset.getMetasetName());
        instanceRule.setMetadataName(metadataName);
        instanceRule.setAutoGenerate(true);
        instanceRule.setStatus(CommonConstant.ENABLE_0);
        this.save(instanceRule);

        String instanceRuleId = instanceRule.getId();
        for(int i = 0; i < num; i++){
            RuleDetail ruleDetail = ruleDetailList.get(i);
            RuleDetailVal ruleDetailVal = new RuleDetailVal();
            ruleDetailVal.setInstanceRuleId(instanceRuleId);
            ruleDetailVal.setRuleDetailId(ruleDetail.getId());
            ruleDetailVal.setType(ruleDetail.getType());
            ruleDetailVal.setArgName("arg" + (i + 1));
            String val = getVal(dqRuleEnum, i , instanceRuleParam, ruleDetailVal);
            if(StringUtils.isEmpty(val)){
                log.error("自动生成规则获取值为空：{}", ruleDetail);
                break;
            }
            ruleDetailVal.setVal(val);
            ruleDetailValService.save(ruleDetailVal);
        }

        datasetInstanceRuleService.save(new DatasetInstanceRule(instanceDataSet.getId().toString(), instanceRuleId));
    }

    private String getVal(DqRuleEnum dqRuleEnum, int i, InstanceRule instanceRuleParam, RuleDetailVal ruleDetailVal){
        Dataset dataset = instanceRuleParam.getDataset();
        DatasetField datasetField = instanceRuleParam.getDatasetFieldDomain();
        String val = StringUtils.EMPTY;
        DsmRef dsmRef;
        switch (dqRuleEnum){
            case STABILITY_RULE:
                // 稳定性天数默认为3天内100分，7天内50分
                String autoStabilityRuleVal = parameterService.getParameterByCode("auto_stability_rule_val");
                switch (i){
                    case 0:
                        val = dataset.getMetasetCode();
                        break;
                    case 1:
                        val = String.valueOf(Integer.parseInt(autoStabilityRuleVal.split(CommonConstant.COMMA)[0]) + 1);
                        break;
                    default:
                        val = String.valueOf(Integer.parseInt(autoStabilityRuleVal.split(CommonConstant.COMMA)[1]) + 1);
                        break;
                }
                break;
            case UNIQUE_RULE:
                val = dataset.getMetasetCode();
                break;
            case STR_LENGTH_RULE:
                switch (i){
                    case 0:
                        val = getFieldVal(dataset, datasetField, ruleDetailVal);
                        break;
                    case 1:
                        val = "0";
                        break;
                    default:
                        val = datasetField.getMetadataFormat().split("\\.\\.")[1];
                        break;
                }
                break;
            case NORMAL_PARAM_RANGE_RULE:
                if(i == 0){
                    val = getFieldVal(dataset, datasetField, ruleDetailVal);
                }else {
                    val = CommonConstant.SEMICOLON + datasetField.getDataDicCode() + CommonConstant.SEMICOLON;
                }
                break;
            case NUMBER_TYPE_RULE:
            case NOT_NULL_RULE:
                val = getFieldVal(dataset, datasetField, ruleDetailVal);
                break;
            case RELEVANCE_RULE_1:
            case RELEVANCE_RULE_2:
            case RELEVANCE_RULE_3:
                dsmRef = instanceRuleParam.getDsmRefList().get(0);
                switch (i){
                    case 0:
                        val = dsmRef.getSubMetasetCode();
                        break;
                    case 1:
                        val = dsmRef.getMainMetasetCode();
                        break;
                    default:
                        val = getFieldVal(instanceRuleParam, i);
                        break;
                }
                break;
            default:
                break;
        }
        return val;
    }

    private String getFieldVal(InstanceRule instanceRuleParam, int i) {
        DsmRef dsmRef = instanceRuleParam.getDsmRefList().get(i/2 - 1);
        String metaSetCode = dsmRef.getMainMetasetCode();
        String metadataCode = dsmRef.getMainMetadataCode();
        if(i % NUM_2 == 0){
            metaSetCode = dsmRef.getSubMetasetCode();
            metadataCode = dsmRef.getSubMetadataCode();
        }
        return metaSetCode + "[" + instanceRuleParam.getFieldCodeMap().get(metaSetCode + metadataCode) + "]";
    }

    private String getFieldVal(Dataset dataset, DatasetField datasetField, RuleDetailVal ruleDetailVal) {
        ruleDetailVal.setNull(!CommonConstant.NOT_NULL.equals(datasetField.getIsNull()));
        return dataset.getMetasetCode() + "[" + datasetField.getFieldName() + "]";
    }

    public List<InstanceRule> selectByDatasetCodeAndDatasetField(String datasetCode, Long datasetFieldId) {
        return this.getBaseMapper().selectByDatasetCodeAndDatasetField(datasetCode, String.valueOf(datasetFieldId));
    }

}
