package biz.service.dao.clickhouse;

import cn.hutool.core.thread.ThreadUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.greatwall.component.ccyl.common.service.ISuperService;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.model.entity.PreDataCondition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author ljl
 * @description
 * @date 2021/2/22 10:03
 **/
public abstract class AbstractSuperServiceImpl<M extends BaseMapper<T>, T> extends SuperServiceImpl<M, T> implements ISuperService<T> {

    private static final int SLEEP_TIME = 2;

    /**
     * 删除维度表数据
     *
     * @param condition 条件
     */
    protected abstract void delByPreDataCondition(PreDataCondition condition);

    /**
     * 根据日期和数据id查询数据量，不为空则休眠
     *
     * @param cycleDay  日期
     * @param datasetId 数据集
     */
    protected void querySleepByCycleDayAndDatasetIdAndStandardId(String cycleDay, String datasetId, String standardId) {
        int count;
        do {
            ThreadUtil.sleep(SLEEP_TIME, TimeUnit.SECONDS);

            QueryWrapper<T> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("CYCLE_DAY", cycleDay);
            queryWrapper.eq("CYCLE_DAY", cycleDay);
            queryWrapper.eq("STANDARD_ID", standardId);
            count = getBaseMapper().selectCount(queryWrapper);
        } while (count != 0);
    }

    /**
     * 根据日期和数据id查询数据量，不为空则休眠
     *
     * @param condition 条件map
     */
    protected void querySleepByConditionMap(Map<String, Object> condition) {
        int count;
        do {
            ThreadUtil.sleep(SLEEP_TIME, TimeUnit.SECONDS);

            QueryWrapper<T> queryWrapper = new QueryWrapper<>();
            for (String key : condition.keySet()) {
                Object value = condition.get(key);
                if (value instanceof String) {
                    queryWrapper.eq(key, condition.get(key));
                } else if (value instanceof List) {
                    queryWrapper.in(key, condition.get(key));
                } else {

                }
            }
            count = getBaseMapper().selectCount(queryWrapper);
        } while (count != 0);
    }
}
