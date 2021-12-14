package biz.schedule.task.dataquality.service.impl;

import com.gwi.qcs.core.biz.schedule.task.dataquality.service.PreDataProcess;
import com.gwi.qcs.core.biz.service.dao.clickhouse.*;
import com.gwi.qcs.core.biz.service.dao.mysql.ScoreInstanceService;
import com.gwi.qcs.model.domain.clickhouse.*;
import com.gwi.qcs.model.entity.PreDataCondition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 维度表处理实现
 *
 * @author ljl
 * @date 2021/3/9 13:57
 **/
@Slf4j
@Service
public class PreDataProcessImpl implements PreDataProcess {

    @Autowired
    PreDataOrgService preDataOrgService;

    @Autowired
    PreDataRuleFieldService preDataRuleFieldService;

    @Autowired
    PreDataSourceService preDataSourceService;

    @Autowired
    PreDataInstanceService preDataInstanceService;

    @Autowired
    PreDataDatasetService preDataDatasetService;

    @Autowired
    DataVolumeStatisticsService volumeStatisticsService;

    /**
     * 评分对象服务
     */
    @Autowired
    ScoreInstanceService scoreInstanceService;

    @Override
    public void preDataOrg(PreDataCondition orgCondition) {

    }

    @Override
    public void preDataSource(PreDataCondition sourceCondition) {

    }

    @Override
    public void preDataDataset(PreDataCondition datasetCondition) {

    }

    @Override
    public void preDataRule(PreDataCondition condition) {

    }

    @Override
    public void preDataScore(PreDataCondition condition) {

    }
}
