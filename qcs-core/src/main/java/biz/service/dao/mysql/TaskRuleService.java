package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.TaskRule;
import com.gwi.qcs.model.mapper.mysql.TaskRuleMapper;
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
public class TaskRuleService extends SuperServiceImpl<TaskRuleMapper, TaskRule> {
    public List<String> getRuleIdList(String taskId){
        LambdaQueryWrapper<TaskRule> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskRule::getTaskId, taskId);
        queryWrapper.select(TaskRule::getRuleId);
        return this.listObjs(queryWrapper, Object::toString);
    }
}
