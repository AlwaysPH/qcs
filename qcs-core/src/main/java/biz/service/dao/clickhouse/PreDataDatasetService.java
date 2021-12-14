package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.PreDataDataset;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataDatasetMapper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (PreDataDateset)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:37
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataDatasetService extends AbstractSuperServiceImpl<PreDataDatasetMapper, PreDataDataset> {

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
     * 根据数据集Ids和日期获取数据集质控列表
     *
     * @param datasetIds 数据集Ids
     * @param beginDate  开始日期
     * @param endDate  结束日期
     */
    public List<PreDataDataset> getListByDatasetIds(List<String> datasetIds, String beginDate, String endDate) {
        QueryWrapper<PreDataDataset> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("DATASET_ID", datasetIds);
        queryWrapper.between("CYCLE_DAY", beginDate, endDate);
        return this.list(queryWrapper);
    }
}