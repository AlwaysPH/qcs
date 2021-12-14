package biz.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.gwi.qcs.common.utils.RetryUtils;
import com.gwi.qcs.core.biz.properties.SparkLivyProperties;
import com.gwi.qcs.core.biz.service.SparkLivyService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskDatasetService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskRuleService;
import com.gwi.qcs.livy.LivyProcess;
import com.gwi.qcs.livy.bean.SparkEngineBean;
import com.gwi.qcs.livy.bean.YarnState;
import com.gwi.qcs.model.dto.DeleteDataRequest;
import com.gwi.qcs.model.entity.SparkArgs;
import com.gwi.qcs.model.entity.SparkDelArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.gwi.qcs.common.constant.DqTaskConstant.SparkLivy.SUCCESS;

/**
 * Spark livy调用实现
 *
 * @author ljl
 * @date 2021/3/3 11:55
 **/
@Slf4j
@Service
public class SparkLivyServiceImpl implements SparkLivyService {

    @Autowired
    public SparkLivyProperties properties;

    @Autowired
    public TaskRuleService taskRuleService;

    @Autowired
    public TaskDatasetService taskDatasetService;

    @Value("${qcs.spark.active}")
    public String profile;

    @Override
    public boolean startJob(String taskId, String cycleDay, String jobType, String standardId) {
        SparkEngineBean engineParam = new SparkEngineBean();
        BeanUtil.copyProperties(properties, engineParam);
        engineParam.setLivyURL(properties.getLivyUrl());

        //把TaskID和任务类型传给Spark livy模块

        engineParam.setArgs(new String[]{JSON.toJSONString(SparkArgs.builder().taskId(Long.parseLong(taskId))
            .corePoolSize(Integer.parseInt(properties.getExecutorCount()))
            .cycleDay(cycleDay).taskType(jobType).standardId(standardId).configFileType(profile)
            .datasetIdList(taskDatasetService.getDatasetIdList(taskId))
            .ruleIdList(taskRuleService.getRuleIdList(taskId)).build())});

        YarnState result = RetryUtils.retryEveryOnException(properties.getRetry(), () -> {
            log.info("spark livy startSparkApplication, param :{}", JSON.toJSONString(engineParam));
            return LivyProcess.startSparkApplication(engineParam);
        }, state -> SUCCESS.equals(state.getState()));

        return SUCCESS.equals(result.getState());
    }

    @Override
    public boolean startDelJob(DeleteDataRequest deleteData) {
        SparkEngineBean engineParam = new SparkEngineBean();
        engineParam.setLivyURL(properties.getLivyUrl());
        engineParam.setFile(properties.getDelFile());
        engineParam.setJars(properties.getJars());
        engineParam.setClassName(properties.getDelClassName());

        engineParam.setArgs(new String[]{JSON.toJSONString(SparkDelArgs.builder().schema(deleteData.getSchema())
             .dataset(deleteData.getDataset())
             .cycleDay(deleteData.getCycleDay())
             .dbTypes(deleteData.getDbTypes())
             .extraMap(deleteData.getExtraMap())
             .configFileType(profile).build())});

        YarnState result = RetryUtils.retryEveryOnException(properties.getRetry(), () -> {
            log.info("spark livy SparkDelApplication, param :{}", JSON.toJSONString(engineParam));
            return LivyProcess.sparkDelApplication(engineParam);
        }, state -> SUCCESS.equals(state.getState()));

        return SUCCESS.equals(result.getState());
    }

    @Override
    public boolean killJob(String sparkAppId) {
        if (StrUtil.isEmpty(sparkAppId)) {
            log.error("没有spark application id");
            return false;
        }

        return LivyProcess.killYarnApplication(properties.getMasterUrl(), sparkAppId);
    }

    @Override
    public YarnState status(String sparkAppId) {
        if (StrUtil.isEmpty(sparkAppId)) {
            return YarnState.undefined(StringUtils.EMPTY);
        }
        return LivyProcess.getStateByApplicationId(properties.getMasterUrl(), sparkAppId);
    }

}
