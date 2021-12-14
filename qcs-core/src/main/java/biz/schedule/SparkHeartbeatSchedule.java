package biz.schedule;

import cn.hutool.core.date.DateBetween;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.cron.CronUtil;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.RetryUtils;
import com.gwi.qcs.core.biz.properties.SparkLivyProperties;
import com.gwi.qcs.core.biz.service.SparkLivyService;
import com.gwi.qcs.core.biz.service.TaskService;
import com.gwi.qcs.core.biz.service.dao.mysql.SqlExecHistoryService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.livy.bean.YarnState;
import com.gwi.qcs.model.domain.mysql.Task;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Spark心跳检测定时
 *
 * @author ljl
 * @date 2021/3/3 9:12
 **/
@Slf4j
@Component
public class SparkHeartbeatSchedule implements CommandLineRunner {

    @Autowired
    SparkLivyService sparkLivyService;

    @Autowired
    TaskService taskService;

    @Autowired
    SqlExecHistoryService execHistoryService;

    @Autowired
    TaskProgressService taskProgressService;

    @Autowired
    SparkLivyProperties properties;

    @Override
    public void run(String... args) throws Exception {
        CronUtil.schedule(properties.getTaskRepairCron(), (cn.hutool.cron.task.Task) this::repairTask);
        CronUtil.schedule(properties.getHeartbeatCron(), (cn.hutool.cron.task.Task) this::heartbeat);
        CronUtil.setMatchSecond(true);
        CronUtil.start();
    }

    public void repairTask() {
        log.info("repairTask");
        List<Task> tasks = taskService.getBaseMapper().selectAllByRunningAndDq();
        for (Task task : tasks) {
            String startTime = task.getDataStarttime();
            String endTime = task.getDataEndtime();

            TaskProgress taskProgress = new TaskProgress();
            taskProgress.setTaskId(Long.valueOf(task.getId()));
            List<TaskProgress> taskProgresses = taskProgressService.selectListByObject(taskProgress);
            //成功天数
            long succeedDays = taskProgresses.stream().filter(progress -> EnumConstant.TaskStatus.EXEC_SUCCEED.getValue() == progress.getStatus()).count();

            //计算失败数据天数
            long failDays = taskProgresses.stream().filter(progress -> EnumConstant.TaskStatus.EXEC_FAILED.getValue() == progress.getStatus()).count();

            //该任务有执行失败的记录，任务直接修改为失败
            if (failDays > 0) {
                UpdateWrapper<Task> updateWrapper = new UpdateWrapper<>();
                updateWrapper.eq("ID", task.getId());
                updateWrapper.set("STATUS", EnumConstant.TaskStatus.EXEC_FAILED.getValue());
                taskService.update(updateWrapper);
                continue;
            }

            if (StrUtil.isEmpty(endTime)) {
                continue;
            }

            //计算任务间距日期； +1时加上当天
            long betweenDay = DateBetween.create(DateUtil.parse(startTime), DateUtil.parse(endTime)).between(DateUnit.DAY) + CommonConstant.CURRENT_DAY_FLAG;

            //任务执行成功
            if (succeedDays == betweenDay) {
                UpdateWrapper<Task> updateWrapper = new UpdateWrapper<>();
                updateWrapper.eq("ID", task.getId());
                updateWrapper.set("STATUS", EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
                updateWrapper.set("TASK_ENDTIME", DateUtil.now());
                taskService.update(updateWrapper);
                log.info("任务执行成功: id:{}, 任务日期范围为：{}-{}", task.getId(), startTime, endTime);
            }
        }
    }

    public void heartbeat() {
        log.info("heartbeat");
        List<TaskProgress> taskProgresses = taskProgressService.getBaseMapper().selectRunningProgress();

        for (TaskProgress taskProgress : taskProgresses) {
            //没有SparkId，跳过
            if (StrUtil.isEmpty(taskProgress.getSparkTaskId())) {
                continue;
            }

            YarnState yarnState = RetryUtils.retryEveryOnException(properties.getRetry(), () -> {
                return sparkLivyService.status(taskProgress.getSparkTaskId());
            }, state -> {
                //获取集群状态失败
                if (state == null || StrUtil.isEmpty(state.getState())) {
                    //休眠两分钟
                    ThreadUtil.sleep(TimeUnit.MICROSECONDS.convert(DqTaskConstant.SparkLivy.SLEEP_INTERVAL, TimeUnit.MINUTES));
                    return false;
                }
                return true;
            });

            if (yarnState == null) {
                continue;
            }

            Task task = new Task();
            task.setId(String.valueOf(taskProgress.getTaskId()));
            task.setBizType(CommonConstant.TASK_QUALITY);
            Task currentTask = taskService.queryTaskById(task);

            switch (yarnState.getState()) {
                case DqTaskConstant.SparkLivy.KILLED:
                case DqTaskConstant.SparkLivy.FAILED:
                    //spark 任务失败，直接更新为任务失败
                    currentTask.setStatus(String.valueOf(EnumConstant.TaskStatus.EXEC_FAILED.getValue()));
                    taskService.updateById(currentTask);

                    taskProgress.setStatus(EnumConstant.TaskStatus.EXEC_FAILED.getValue());
                    taskProgressService.updateById(taskProgress);
                    break;
                default:
                    break;
            }
        }
    }


}
