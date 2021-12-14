package biz.schedule.task.dataquality.function;

import cn.hutool.core.date.DateTime;
import com.gwi.qcs.core.biz.schedule.task.dataquality.function.sql.MergeRuleSqlFunction;
import com.gwi.qcs.core.biz.schedule.task.dataquality.function.sql.RuleSqlFunction;
import com.gwi.qcs.model.entity.DatasetDescriptor;
import com.gwi.qcs.model.entity.EvaluationDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;

/**
 * Function管理类
 *
 * @author x
 */
@Slf4j
public class FunctionFactory {
    private final EvaluationDescriptor rule;
    private final DatasetDescriptor dataset;
    private final JobDefinition jobDefinition;
    private DateTime execDate;

    public FunctionFactory(EvaluationDescriptor rule, DatasetDescriptor dataset, JobDefinition jobDefinition, DateTime execDate) {
        this.rule = rule;
        this.dataset = dataset;
        this.jobDefinition = jobDefinition;
        this.execDate = execDate;
    }

    public String getFunctionSql() throws Exception {
        String sql;

        if (rule.isMergeSql()) {
            sql = new MergeRuleSqlFunction().buildSql(this.rule, this.dataset, this.jobDefinition, execDate);
        } else {
            sql = new RuleSqlFunction().buildSql(this.rule, this.dataset, this.jobDefinition, execDate);
        }
        log.debug("生成质控SQL，规则：{}，数据集：{}：sql:{}", rule.getRuleCode(), dataset.getCode(), sql);
        return sql;
    }

}
