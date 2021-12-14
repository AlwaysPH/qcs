package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.PreDataSource;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataSourceMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (PreDataSource)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:38
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataSourceService extends AbstractSuperServiceImpl<PreDataSourceMapper, PreDataSource> {

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
     * 根据数据来源Id、数据集Ids、日期获取数据来源质控列表
     *
     * @param sourceId 数据来源Id
     * @param dataSetIds 数据集Ids
     * @param startDay  开始日期
     * @param endDay  结束日期
     */
    public List<PreDataSource> getListBySourceId(String sourceId, List<String> dataSetIds, String startDay, String endDay) {
        QueryWrapper<PreDataSource> queryWrapper = new QueryWrapper<>();
        if(StringUtils.isNotEmpty(sourceId)){
            queryWrapper.eq("SOURCE_ID", sourceId);
        }
        if(CollectionUtils.isNotEmpty(dataSetIds)){
            queryWrapper.in("DATASET_ID", dataSetIds);
        }
        queryWrapper.between("CYCLE_DAY", startDay, endDay);
        return this.list(queryWrapper);
    }

    /**
     * 根据标准版本Id、数据集Ids、日期获取数据来源质控列表
     *
     * @param standardId 标准版本Id
     * @param dataSetIds 数据集Ids
     * @param startDay  开始日期
     * @param endDay  结束日期
     */
    public List<PreDataSource> getListByStandardId(String standardId, List<String> dataSetIds, String startDay, String endDay) {
        QueryWrapper<PreDataSource> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("STANDARD_ID", standardId);
        if(CollectionUtils.isNotEmpty(dataSetIds)){
            queryWrapper.in("DATASET_ID", dataSetIds);
        }
        queryWrapper.between("CYCLE_DAY", startDay, endDay);
        return this.list(queryWrapper);
    }
}