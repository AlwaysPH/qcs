package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.PreDataOrg;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataOrgMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (PreDataOrg)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:37
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataOrgService extends AbstractSuperServiceImpl<PreDataOrgMapper, PreDataOrg> {

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
     * 根据医疗机构、标准版本Id和日期获取机构质控列表
     *
     * @param orgCodes  医疗机构
     * @param standardId 标准版本Id
     * @param startDay  开始日期
     * @param endDay  结束日期
     */
    public List<PreDataOrg> getListByOrgCodes(List<String> orgCodes, String standardId, String startDay, String endDay) {
        QueryWrapper<PreDataOrg> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("ORG_CODE", orgCodes);
        if (StringUtils.isNotBlank(standardId)) {
            queryWrapper.eq("STANDARD_ID", standardId);
        }
        queryWrapper.between("CYCLE_DAY", startDay, endDay);
        return this.list(queryWrapper);
    }

    /**
     * 根据医疗机构、数据集Ids和日期获取机构质控列表
     *
     * @param orgCodes  医疗机构
     * @param datasetIds 数据集Ids
     * @param startDay  开始日期
     * @param endDay  结束日期
     */
    public List<PreDataOrg> getListByDatasetIds(List<String> orgCodes, List<String> datasetIds, String startDay, String endDay) {
        QueryWrapper<PreDataOrg> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("ORG_CODE", orgCodes);
        if(CollectionUtils.isNotEmpty(datasetIds)){
            queryWrapper.in("DATASET_ID", datasetIds);
        }
        queryWrapper.between("CYCLE_DAY", startDay, endDay);
        return this.list(queryWrapper);
    }
}