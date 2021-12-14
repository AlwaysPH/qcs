package biz.schedule.task.dataquality.service.impl;

import cn.hutool.core.date.DateBetween;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.gwi.qcs.core.biz.schedule.task.dataquality.service.AbstractDataQualityExecute;
import com.gwi.qcs.model.domain.mysql.SqlExecHistory;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 临时任务实现
 *
 * @author ljl
 * @date 2021/2/2 14:51
 **/
@Slf4j
@Service
public class TempExecuteImpl extends AbstractDataQualityExecute {

    @Override
    public boolean execute() throws JobExecutionException {
        //默认添加开始日期
        execDates.add(startDate);
        //计算任务间距日期
        long betweenDay = DateBetween.create(startDate, endDate).between(DateUnit.DAY);

        for (int i = 1; i <= betweenDay; i++) {
            execDates.add(DateUtil.offsetDay(startDate, i));
        }

        for (DateTime execDate : execDates) {
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
