package biz.schedule.task;

import com.alibaba.fastjson.JSON;
import com.gwi.qcs.core.biz.schedule.AbstractJobAbstract;
import com.gwi.qcs.core.biz.schedule.task.dataquality.service.DataQualityExecute;
import com.gwi.qcs.core.biz.schedule.task.dataquality.service.impl.ContinueExecuteImpl;
import com.gwi.qcs.core.biz.schedule.task.dataquality.service.impl.TempExecuteImpl;
import com.gwi.qcs.model.entity.JobDefinition;
import com.gwi.qcs.model.mapper.mysql.SqlExecHistoryMapper;
import com.gwi.qcs.model.mapper.mysql.TaskProgressMapper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 质控任务
 *
 * @author x
 */
@Component
@Slf4j
public class DataQualityJobAbstract extends AbstractJobAbstract implements InterruptableJob {

    /**
     * 质控任务分组名称
     */
    public static final String QCS_JOB_GROUP = "qcsTask";

    /**
     * job 是否中断
     */
    private boolean interrupted = false;

    @Autowired
    TaskProgressMapper taskProgressMapper;

    @Autowired
    SqlExecHistoryMapper sqlExecMapper;

    @Override
    public void executeTaskJob(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        JobDefinition jobDefinition = JSON.parseObject(JSON.toJSONString(dataMap), JobDefinition.class);
        log.info("开始质控任务：Job信息:{}", jobDefinition);

        if (interrupted) {
            log.error("任务中断，状态interrupted，Job信息:{}", jobDefinition);
            return;
        }

        getExecute(jobDefinition.isUnbounded())
                .executeJob(jobDefinition);
    }

    public DataQualityExecute getExecute(boolean isContinueJob) {
        if (isContinueJob) {
            return new ContinueExecuteImpl();
        }
        return new TempExecuteImpl();
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }
}
