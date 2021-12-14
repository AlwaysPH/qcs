package biz.schedule.task.dataquality.service;

import com.gwi.qcs.model.entity.PreDataCondition;

/**
 * 维度表数据处理
 *
 * @author ljl
 * @date 2021/3/9 13:54
 **/
public interface PreDataProcess {

    /**
     * 机构维度表
     *
     * @param condition 条件
     */
    void preDataOrg(PreDataCondition condition);

    /**
     * 数据来源维度表
     *
     * @param condition 条件
     */
    void preDataSource(PreDataCondition condition);

    /**
     * 数据集维度表
     *
     * @param condition 条件
     */
    void preDataDataset(PreDataCondition condition);

    /**
     * PRE_DATA_RULE规则维度表
     *
     * @param condition 条件
     */
    void preDataRule(PreDataCondition condition);

    /**
     * PRE_DATA_SCORE评分统计表
     */
    void preDataScore(PreDataCondition condition);
}
