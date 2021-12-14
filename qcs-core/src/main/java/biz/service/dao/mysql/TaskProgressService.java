package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import com.gwi.qcs.model.mapper.mysql.TaskProgressMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author ljl
 * @create 2021-02-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class TaskProgressService extends SuperServiceImpl<TaskProgressMapper, TaskProgress> {


    public Date getTaskStartTime(Long id){
        TaskProgress taskProgress = this.getById(id);
        Date date = DateUtils.addDays(DateUtils.getDayStartTime(), -1);
        if(taskProgress != null && taskProgress.getCycleDay() != null){
            date = DateUtils.addDays(taskProgress.getCycleDay(), 1);
        }
        return date;
    }

    public boolean checkDataQualityIsFinish(Set<String> standardIdSet, Date cycleDay){
        boolean isFinish = true;
        for(String standardId : standardIdSet){
            TaskProgress query = new TaskProgress();
            query.setCycleDay(cycleDay);
            query.setJobType(DqTaskConstant.Task.DQ);
            query.setStandardId(standardId);
            QueryWrapper<TaskProgress> queryWrapper = new QueryWrapper<>(query);
            queryWrapper.orderByDesc(CommonConstant.CREATE_AT_UPPER);
            TaskProgress taskProgress = this.getOne(queryWrapper, false);
            if(taskProgress == null || EnumConstant.TaskStatus.EXEC_SUCCEED.getValue() != taskProgress.getStatus()){
                isFinish = false;
                log.info("未获取到质控任务数据，不执行此任务！");
                break;
            }
        }
        return isFinish;
    }

    /**
     * 根据对象查询一条记录
     *
     * @param progress
     * @return
     */
    public TaskProgress selectOneByObject(TaskProgress progress) {
        QueryWrapper<TaskProgress> wrapper = new QueryWrapper<>();
        wrapper.setEntity(progress);
        return this.getOne(wrapper);
    }

    /**
     * 获取最新的一条
     * @param taskId
     * @return
     */
    public TaskProgress getRecentlyByTaskId(String taskId){
        QueryWrapper<TaskProgress> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc(CommonConstant.CREATE_AT_UPPER);
        queryWrapper.lambda().eq(TaskProgress::getTaskId, taskId);
        return this.getOne(queryWrapper, false);
    }

    /**
     * 根据对象查询多条记录
     *
     * @param progress
     * @return
     */
    public List<TaskProgress> selectListByObject(TaskProgress progress) {
        QueryWrapper<TaskProgress> wrapper = new QueryWrapper<>();
        wrapper.setEntity(progress);
        return this.list(wrapper);
    }

    /**
     * 查询成功的任务
     *
     * @param cycleDay   日期
     * @param standardId 标准版本
     * @return
     */
    public List<TaskProgress> selectSucTaskByCycleDayAndStandardId(String cycleDay, String standardId) {
        QueryWrapper<TaskProgress> wrapper = new QueryWrapper<>();
        wrapper.eq("STATUS", EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
        wrapper.eq("CYCLE_DAY", cycleDay);
        wrapper.eq("STANDARD_ID", standardId);
        return this.list(wrapper);
    }
}
