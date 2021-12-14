package biz.schedule.task.dataquality.function;

import cn.hutool.core.date.DateTime;
import com.gwi.qcs.model.entity.DatasetDescriptor;
import com.gwi.qcs.model.entity.EvaluationDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;

/**
 * 基础函数抽象类
 *
 * @author x
 */
public abstract class BaseFunction {

    /**
     * 构造质控SQL
     *
     * @param rule          规则
     * @param dataset       数据集
     * @param jobDefinition job信息
     * @param execDate      执行日期
     * @return sql
     * @throws Exception
     */
    public abstract String buildSql(EvaluationDescriptor rule, DatasetDescriptor dataset, JobDefinition jobDefinition, DateTime execDate) throws Exception;

}
