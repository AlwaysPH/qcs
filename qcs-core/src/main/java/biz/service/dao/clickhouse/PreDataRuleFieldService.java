package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.PreDataRuleField;
import com.gwi.qcs.model.dto.RuleQueryDto;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataRuleFieldMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (PreDataRuleField)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:37
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataRuleFieldService extends AbstractSuperServiceImpl<PreDataRuleFieldMapper, PreDataRuleField> {

    /**机构质控查询预处理表*/
    private static final Integer REFTYPE_1 = 1;
    /**数据来源质控查询预处理表*/
    private static final Integer REFTYPE_2 = 2;

    /**
     * 删除维度数据
     *
     * @param condition 条件
     */
    @Override
    public void delByPreDataCondition(PreDataCondition condition) {
        getBaseMapper().delByCycleDayAndStandardId(condition.getCycleDay(), condition.getStandardId());

        Map<String, Object> map = MapUtil
            .builder("CYCLE_DAY", (Object) condition.getCycleDay())
            .put("STANDARD_ID", condition.getStandardId())
            .build();
        querySleepByConditionMap(map);
    }

    /**
     * 根据机构编码和规则获取规则字段质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     * @param ruleIds 规则Ids
     */
    public List<PreDataRuleField> getListByOrgCodeAndRule(List<String> datasetIds, RuleQueryDto ruleQueryDto, List<String> ruleIds) {
        QueryWrapper<PreDataRuleField> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("REF_TYPE", REFTYPE_1);
        queryWrapper.eq("REF_VAL", ruleQueryDto.getOrgCode());
        queryWrapper.in("DATASET_ID", datasetIds);
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
     * 根据数据来源Id和规则获取规则字段质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     * @param ruleIds 规则Ids
     */
    public List<PreDataRuleField> getListBySourceIdAndRule(List<String> datasetIds, RuleQueryDto ruleQueryDto, List<String> ruleIds) {
        QueryWrapper<PreDataRuleField> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("REF_TYPE", REFTYPE_2);
        if (StringUtils.isNotBlank(ruleQueryDto.getSourceId())) {
            queryWrapper.eq("REF_VAL", ruleQueryDto.getSourceId());
        }
        queryWrapper.in("DATASET_ID", datasetIds);
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
     * 根据数据来源Id获取规则字段质控列表
     *
     * @param datasetIds 数据集Ids
     * @param ruleQueryDto 规则查询对象
     */
    public List<PreDataRuleField> getListBySourceId(List<String> datasetIds, RuleQueryDto ruleQueryDto) {
        QueryWrapper<PreDataRuleField> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("REF_TYPE", REFTYPE_2);
        if (StringUtils.isNotBlank(ruleQueryDto.getSourceId())) {
            queryWrapper.eq("REF_VAL", ruleQueryDto.getSourceId());
        }
        queryWrapper.in("DATASET_ID", datasetIds);
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }

    /**
     * 根据所选机构获取规则字段质控列表
     *
     * @param orgs 所选机构
     * @param ruleQueryDto 规则查询对象
     */
    public List<PreDataRuleField> getListByOrgs(List<String> orgs, RuleQueryDto ruleQueryDto) {
        QueryWrapper<PreDataRuleField> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("REF_TYPE", REFTYPE_1);
        queryWrapper.in("REF_VAL", orgs);
        if (StringUtils.isNotBlank(ruleQueryDto.getStandardId())) {
            queryWrapper.eq("STANDARD_ID", ruleQueryDto.getStandardId());
        }
        queryWrapper.between("CYCLE_DAY", ruleQueryDto.getStartDate(), ruleQueryDto.getEndDate());
        return this.list(queryWrapper);
    }
}