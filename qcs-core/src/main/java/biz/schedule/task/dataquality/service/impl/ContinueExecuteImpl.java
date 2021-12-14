package biz.schedule.task.dataquality.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateBetween;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.gwi.qcs.core.biz.schedule.task.dataquality.service.AbstractDataQualityExecute;
import com.gwi.qcs.model.domain.mysql.SqlExecHistory;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import com.gwi.qcs.model.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 持续任务实现
 *
 * @author ljl
 * @date 2021/2/2 14:51
 **/
@Slf4j
@Service
public class ContinueExecuteImpl extends AbstractDataQualityExecute {

    @Override
    public boolean execute() throws JobExecutionException {
        //任务执行到哪天
        TaskProgress execDateProgress = taskProgressService().getBaseMapper().selectLastCycleDayByTaskId(jobDefinition.getJobId());

        DateTime startJobDate;
        if (execDateProgress != null) {
            startJobDate = DateUtil.offsetDay(execDateProgress.getCycleDay(), 1);
        } else {
            startJobDate = startDate;
        }

        DateTime endJobDate = null;
        String cycleDay = jobDefinition.getExtras().get(JobDefinition.CONTINUE_CYCLEDAY);
        if (StringUtils.isNotBlank(cycleDay)) {
            endJobDate = DateUtil.parseDate(cycleDay);
        } else {
            endJobDate = DateUtil.yesterday();
        }

        //计算任务间距日期
        long betweenDay = DateBetween.create(startJobDate, endJobDate).between(DateUnit.DAY);

        for (int i = 0; i <= betweenDay; i++) {
            execDates.add(DateUtil.offsetDay(startDate, i));
        }

        for (DateTime execDate : execDates) {
            TaskProgress progress = new TaskProgress();
            progress.setCycleDay(execDate);
            progress.setStandardId(standardId);
            List<TaskProgress> taskProgress = taskProgressService().selectListByObject(progress);

            //有当天执行记录
            if (CollUtil.isNotEmpty(taskProgress)) {
                //当前任务是否为持续任务。持续任务结束流程，临时任务覆盖
                if (jobDefinition.isUnbounded()) {
                    log.error("持续任务，当天{}已存在执行记录，跳过执行下一天，Job信息:{}", execDate.toDateStr(), jobDefinition);
                    continue;
                }
            }

            //得到当前日期SQL
            List<SqlExecHistory> currentSqlList = buildSqlOfExecData(jobDefinition, execDate);

            //SQL记录表
            sqlExecHistoryService().saveBatch(currentSqlList);

            //任务执行记录
            taskProgressService().save(buildDqTaskProgress(execDate));

            //启动Spark任务
            boolean ok = sparkBridge(execDate);
            if (!ok) {
                return false;
            }
        }
        return true;
    }

}
