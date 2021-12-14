package biz.schedule.task.dataquality.function.sql;

import cn.hutool.core.date.DateTime;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.entity.DataSourceDescriptor;
import com.gwi.qcs.model.entity.DatasetDescriptor;
import com.gwi.qcs.model.entity.EvaluationDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

import static com.gwi.qcs.model.entity.DatasetDescriptor.*;

/**
 * 规则SQL组装实现类
 *
 * @author yanhan
 * @create 2020-06-11 20:21
 **/
@Slf4j
public class RuleSqlFunction extends AbstractRuleSqlFunction {

    @Override
    public String buildSql(EvaluationDescriptor rule, DatasetDescriptor dataset, JobDefinition jobDefinition, DateTime execDate) throws Exception {
        //从JobDefinition中获取schema,jobId,standardId,standardName
        String datasourceSchema = jobDefinition.getInput().getSettings().get(DataSourceDescriptor.SCHEMA);

        //构造PKValue
        String pkValue = buildPKValue(dataset);

        //校验规则 eg: arg1 IS NOT NULL
        String originalValue = buildOriginalValueKey(rule);

        //质控二期修改结果字段2020-07-02
        String evalResult = buildEvalScoreKey(rule);

        String resultSql = StringUtils.EMPTY;
        //表达式模式
        if (rule.getType().equals(EvaluationDescriptor.EvaluationType.EXPR)) {
            String firstValue = buildFirstKey(rule);

            resultSql = String.format(new StringBuilder().append(" SELECT ")
                            .append(" %s AS SOURCE_ID, ")
                            .append(" %s AS ORG_CODE, ")
                            .append(" %s AS PK_VALUE, ")
                            .append(" NVL(%s,'')||'' AS FIRST_VALUE, ")
                            .append(" %s AS ORIGINAL_VALUE, ")
                            .append(" %s AS EVAL_RESULT ")
                            .append("FROM  %s").toString(),
                    dataset.getMetadata().get(DATASET_METADATA_SOURCE_FILED),
                    dataset.getMetadata().get(DATASET_METADATA_ORG_FILED),
                    pkValue,
                    firstValue,
                    originalValue,
                    evalResult,
                    datasourceSchema + "." + dataset.getCode());
        } else if (rule.getType().equals(EvaluationDescriptor.EvaluationType.SQL)) {
            //SQL模式
            resultSql = originalValue.replace("#SOURCE_ID#", dataset.getMetadata().get(DATASET_METADATA_SOURCE_FILED))
                    .replace("#ORG_CODE#", dataset.getMetadata().get(DATASET_METADATA_ORG_FILED))
                    .replace("#SCHEMA#", datasourceSchema)
                    .replace("#PK_VALUE#", pkValue)
                    .replace("#PRIMARY_KEYS#", String.join(",", dataset.getPrimaryKeys()))
                    //SQL模式下需要特别设置“机构编码范围”和“数据来源范围”，根据SQL模板中预置的“CUSTOM_ORG_OR_SOURCE”关键字替代
                    .replaceAll("#CUSTOM_ORG_OR_SOURCE#", dataRangeWhere(dataset, jobDefinition));
        } else {

        }

        String whereCondition = StringUtils.EMPTY;
        resultSql = resultSql.replaceAll("#CYCLE_DAY_FIELD#", dataset.getMetadata().get(DATASET_METADATA_DATE_FILED))
                .replaceAll("#CYCLE_DAY#", "'" + execDate.toDateStr() + "'");

        if (rule.getType().equals(EvaluationDescriptor.EvaluationType.EXPR)) {
            whereCondition = dateWhere(resultSql, execDate) + dataRangeWhere(dataset, jobDefinition);
        }

        return resultSql + whereCondition;
    }

    @Override
    protected String buildFirstKey(EvaluationDescriptor rule) {
        //质控2.0 三期新增字段原始值
        List<String> firstValues = Lists.newArrayList();
        for (List<EvaluationDescriptor.Arg> args : rule.getArgs()) {
            for (EvaluationDescriptor.Arg arg : args) {
                if (CommonConstant.VAL_TYPE_FIELD.equals(arg.getType())) {
                    firstValues.add(arg.getValue());
                }
            }
        }

        String firstValue = "''";
        if (!CollectionUtils.isEmpty(firstValues)) {
            firstValue = Joiner.on("||','||").join(firstValues);
        }
        return firstValue;
    }

    @Override
    protected String buildEvalScoreKey(EvaluationDescriptor rule) {
        String originalValue = buildOriginalValueKey(rule);

        //质控二期修改结果字段2020-07-02
        String evalResult = "";
        if (!rule.isRecommission()) {
            StringBuffer sb = new StringBuffer();
            sb.append("(CASE WHEN ");
            sb.append(originalValue);
            sb.append(" THEN 100 ELSE 0 END)");
            evalResult = sb.toString();
        } else {
            evalResult = rule.getRecommissionTemplate().replaceAll("#RESULT#", originalValue);
        }
        return evalResult;
    }

    @Override
    protected String buildOriginalValueKey(EvaluationDescriptor rule) {
        //替换Arg参数
        String template = rule.getTemplate();
        //将校验规则中的arg参数替换为对应的值
        for (List<EvaluationDescriptor.Arg> args : rule.getArgs()) {
            for (EvaluationDescriptor.Arg arg : args) {
                template = template.replace(arg.getName(), arg.getValue());

                if (CommonUtil.hasNullFiledForRule(rule.getRuleCode()) && arg.isNull()) {
                    template = String.format("(%s OR %s)", arg.getValue() + " IS NULL", template);
                }
            }
        }
        return template;
    }
}