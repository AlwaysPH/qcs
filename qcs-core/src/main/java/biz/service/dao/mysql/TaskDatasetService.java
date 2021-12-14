package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.TaskDataset;
import com.gwi.qcs.model.mapper.mysql.TaskDatasetMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zsd
 * @create 2020-08-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class TaskDatasetService extends SuperServiceImpl<TaskDatasetMapper, TaskDataset> {
    public List<String> getDatasetIdList(String taskId){
        LambdaQueryWrapper<TaskDataset> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskDataset::getTaskId, taskId);
        queryWrapper.select(TaskDataset::getDatasetId);
        return this.listObjs(queryWrapper, Object::toString);
    }
}
