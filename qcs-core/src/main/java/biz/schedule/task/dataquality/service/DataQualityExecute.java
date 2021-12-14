package biz.schedule.task.dataquality.service;

import com.gwi.qcs.model.entity.JobDefinition;
import org.quartz.JobExecutionException;

/**
 * 质控任务执行器接口
 *
 * @author: ljl
 * @date: 2021/2/2 14:49
 **/
public interface DataQualityExecute {

    /**
     * 执行质控任务
     *
     * @param jobDefinition job信息
     * @throws JobExecutionException
     */
    void executeJob(JobDefinition jobDefinition) throws JobExecutionException;

}
