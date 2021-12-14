package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.PreDataInstance;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.PreDataInstanceMapper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (PreDataInstance)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:37
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class PreDataInstanceService extends AbstractSuperServiceImpl<PreDataInstanceMapper, PreDataInstance> {

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
            .put("DATASET_ID", condition.getDatasetIds())
            .put("STANDARD_ID", condition.getStandardId())
            .build();
        querySleepByConditionMap(map);
    }

    /**
     * 根据评分对象Ids和日期获取评分对象质控列表
     *
     * @param instanceIds 评分对象Ids
     * @param startDay  开始日期
     * @param endDay  结束日期
     */
    public List<PreDataInstance> getListByInstanceIds(List<String> instanceIds, String startDay, String endDay) {
        QueryWrapper<PreDataInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("SCORE_INSTANCE_ID", instanceIds);
        queryWrapper.between("CYCLE_DAY", startDay, endDay);
        return this.list(queryWrapper);
    }
}