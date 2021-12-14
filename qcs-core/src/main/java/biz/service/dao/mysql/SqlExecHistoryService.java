package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.google.common.collect.Lists;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.SqlExecHistory;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import com.gwi.qcs.model.mapper.mysql.SqlExecHistoryMapper;
import com.gwi.qcs.model.mapper.mysql.TaskProgressMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author ljl
 * @create 2021-02-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class SqlExecHistoryService extends SuperServiceImpl<SqlExecHistoryMapper, SqlExecHistory> {

    @Autowired
    TaskProgressMapper taskProgressMapper;

    /**
     * 获取最新一天的所有SQL执行记录
     *
     * @param taskId
     * @return
     */
    public List<SqlExecHistory> selectLastCycleDayAllByTaskId(@Param("taskId") String taskId) {
        TaskProgress taskProgress = taskProgressMapper.selectLastCycleDayByTaskId(Long.parseLong(taskId));
        if (taskProgress == null) {
            return Lists.newArrayList();
        }

        if (Integer.parseInt(CommonConstant.TASK_CONTINUE_TYPE) == taskProgress.getJobType()) {
            return getBaseMapper().selectAllByTaskIdAndCycleDay(taskId, taskProgress.getCycleDay());
        } else {
            return getBaseMapper().selectAllByTaskId(taskId);
        }
    }

}
