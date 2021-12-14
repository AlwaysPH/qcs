package biz.schedule;

import cn.hutool.core.collection.CollUtil;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.service.SparkLivyService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;

/**
 * 调度监听器
 *
 * @author ljl
 * @date 2021/3/1 9:20
 **/
@Slf4j
public class SchedulerListener {

    public void jobPaused(JobKey jobKey) {
        log.info("任务暂停,jobPaused jobId : {}", jobKey);
        //修改任务进度
        updateTaskProgressStatus(jobKey.getName(), EnumConstant.TaskStatus.EXEC_PAUSE.getValue());
    }

    public void jobResumed(JobKey jobKey) {
        log.info("任务恢复,jobResumed jobId : {}", jobKey);
        updateTaskProgressStatus(jobKey.getName(), EnumConstant.TaskStatus.EXECUTING.getValue());
    }

    public void jobAdded(JobDetail jobDetail) {
    }

    /**
     * 任务取消时执行
     *
     * @param jobKey key
     */
    public void jobDeleted(JobKey jobKey) {
        log.info("任务删除,jobDeleted jobId : {}", jobKey);
        TaskProgressService taskService = SpringContextUtil.getBean(TaskProgressService.class);
        TaskProgress taskProgress = new TaskProgress();
        taskProgress.setTaskId(Long.valueOf(jobKey.getName()));
        if (CollUtil.isEmpty(taskService.selectListByObject(taskProgress))) {
            return;
        }

        updateTaskProgressStatus(jobKey.getName(), EnumConstant.TaskStatus.EXEC_CANCEL.getValue());

        TaskProgress progress = SpringContextUtil.getBean(TaskProgressService.class).getBaseMapper().selectLastCycleDayByTaskId(Long.parseLong(jobKey.getName()));

        if (progress == null) {
            log.warn("无法停止spark 任务，任务id:{}", jobKey.getName());
            return;
        }

        if (progress.getJobType() == DqTaskConstant.Task.DQ) {
            //停止任务
            SpringContextUtil.getBean(SparkLivyService.class).killJob(progress.getSparkTaskId());
        }
    }

    public void jobUnscheduled(TriggerKey triggerKey) {
        log.info("jobUnscheduled " + triggerKey);
    }

    /**
     * 修改最新一条任务进度表-任务状态
     *
     * @param taskId 任务ID
     * @param status 状态
     */
    private void updateTaskProgressStatus(String taskId, Integer status) {
        //修改任务状态
        TaskProgressService taskProgressService = SpringContextUtil.getBean(TaskProgressService.class);
        TaskProgress taskProgress = taskProgressService.getBaseMapper().selectLastCycleDayByTaskId(Long.parseLong(taskId));
        if (taskProgress == null) {
            return;
        }

        if (EnumConstant.TaskStatus.EXECUTING.getValue() == taskProgress.getStatus()
                || EnumConstant.TaskStatus.NOT_EXEC.getValue() == taskProgress.getStatus()) {
            log.info("更新进度表状态：taskId:{}, 更新前状态：{}, 更新后：{}", taskId, taskProgress.getStatus(), status);
            taskProgress.setStatus(status);
            taskProgressService.updateById(taskProgress);
        }
    }
}
