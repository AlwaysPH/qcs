package biz.schedule.task.dataquality.function.sql;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.util.StrUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.entity.DataSourceDescriptor;
import com.gwi.qcs.model.entity.DatasetDescriptor;
import com.gwi.qcs.model.entity.EvaluationDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;
import com.gwi.qcs.model.mapenum.DqRuleEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gwi.qcs.model.entity.DatasetDescriptor.DATASET_METADATA_ORG_FILED;
import static com.gwi.qcs.model.entity.DatasetDescriptor.DATASET_METADATA_SOURCE_FILED;

/**
 * 把同类规则进行合并，暂时合并的为普通值域和非空
 *
 * @author ljl
 * @date 2021-01-29
 */
@Slf4j
public class MergeRuleSqlFunction extends AbstractRuleSqlFunction {

    /**
     * 分隔符
     */
    public static final String SEPARATE = " ||'~'|| ";

    @Override
    protected String buildOriginalValueKey(EvaluationDescriptor rule) {
        StringBuilder keyBuilder = new StringBuilder();

        String template;
        //将校验规则中的arg参数替换为对应的值
        for (List<EvaluationDescriptor.Arg> args : rule.getArgs()) {
            template = rule.getTemplate();
            for (EvaluationDescriptor.Arg arg : args) {
                template = template.replace(arg.getName(), arg.getValue());
            }
            keyBuilder.append(String.format("(%s)", template));
            keyBuilder.append(SEPARATE);
        }

        return StrUtil.removeSuffix(keyBuilder, SEPARATE);
    }

    @Override
    protected String buildEvalScoreKey(EvaluationDescriptor rule) {
        StringBuilder templateBuilder = new StringBuilder();

        String template = StringUtils.EMPTY;
        //将校验规则中的arg参数替换为对应的值
        for (List<EvaluationDescriptor.Arg> args : rule.getArgs()) {
            template = rule.getTemplate();
            templateBuilder.append("(CASE WHEN ");
            for (EvaluationDescriptor.Arg arg : args) {
                template = template.replace(arg.getName(), arg.getValue());
                if (CommonUtil.hasNullFiledForRule(rule.getRuleCode()) && CommonConstant.VAL_TYPE_FIELD.equals(arg.getType()) && arg.isNull()) {
                    template = String.format("(%s OR %s)", arg.getValue() + " IS NULL", template);
                }
            }
            templateBuilder.append(template);
            templateBuilder.append(" THEN 100 ELSE 0 END)");
            templateBuilder.append(SEPARATE);
        }
        return StrUtil.removeSuffix(templateBuilder, SEPARATE);
    }

    @Override
    protected String buildFirstKey(EvaluationDescriptor rule) {
        //三期新增字段原始值
        StringBuilder firstBuilder = new StringBuilder();
        boolean isNotNullRule = DqRuleEnum.NOT_NULL_RULE.getName().equals(rule.getRuleCode());
        for (List<EvaluationDescriptor.Arg> args : rule.getArgs()) {
            StringBuilder keyBuilder = new StringBuilder();
            List<String> firstValues = Lists.newArrayList();
            for (EvaluationDescriptor.Arg arg : args) {
                if (CommonConstant.VAL_TYPE_FIELD.equals(arg.getType())) {
                    firstValues.add(arg.getValue());
                }
            }
            if (isNotNullRule) {
                keyBuilder.append(" random_prefix( ");
            }
            keyBuilder.append(" NVL( ");
            String firstValue = "''";
            if (!CollectionUtils.isEmpty(firstValues)) {
                firstValue = Joiner.on("||','||").join(firstValues);
            }
            keyBuilder.append(firstValue);
            keyBuilder.append(", '')");
            if (isNotNullRule) {
                keyBuilder.append(" ) ");
            }
            keyBuilder.append(SEPARATE);
            firstBuilder.append(keyBuilder);
        }
        return StrUtil.removeSuffix(firstBuilder, SEPARATE);
    }

    /**
     * 在合并执行场景下，需要增加field字段信息
     * 类似 { 'UPDATE_FLAG' || '~' || 'IS_DOWNLOAD' }
     *
     * @param rule 规则
     * @return key
     */
    protected String buildFieldNameKey(EvaluationDescriptor rule) {
        StringBuilder builder = new StringBuilder();

        //减掉一层结构
        List<EvaluationDescriptor.Arg> args = rule.getArgs().stream().flatMap(new Function<List<EvaluationDescriptor.Arg>, Stream<EvaluationDescriptor.Arg>>() {
            @Override
            public Stream<EvaluationDescriptor.Arg> apply(List<EvaluationDescriptor.Arg> args) {
                //找出arg1，arg1为key
                return args.stream().filter((Predicate<EvaluationDescriptor.Arg>) arg -> "arg1".equals(arg.getName()));
            }
        }).collect(Collectors.toList());

        StringBuilder keyBuilder = new StringBuilder();
        List<String> fieldValues = Lists.newArrayList();
        for (EvaluationDescriptor.Arg arg : args) {
            if (CommonConstant.VAL_TYPE_FIELD.equals(arg.getType())) {
                fieldValues.add("'" + arg.getValue() + "'");
            }
        }
        String fieldValue = StringUtils.EMPTY;
        if (!CollectionUtils.isEmpty(fieldValues)) {
            //通过~号分割
            fieldValue = Joiner.on("|| '~' ||").join(fieldValues);
        }
        keyBuilder.append(fieldValue);
        keyBuilder.append(SEPARATE);
        builder.append(keyBuilder);

        return StrUtil.removeSuffix(builder, SEPARATE);
    }

    /**
     * 在写维度表时，需要字段ID，在合并SQL场景下，增加FIELD_ID字段，通过spark UDF实现  RangeValidUDF
     *
     * @param dataset 数据表
     * @return key
     */
    protected String buildFieldIdKey(String dataset) {
        return String.format(" field_id(FIELD_NAME, '%s')", dataset);
    }

    @Override
    public String buildSql(EvaluationDescriptor rule, DatasetDescriptor dataset, JobDefinition jobDefinition, DateTime execDate) throws Exception {
        String datasourceSchema = jobDefinition.getInput().getSettings().get(DataSourceDescriptor.SCHEMA);

        //构造PKValue
        String pkValue = buildPKValue(dataset);

        //校验规则 eg: arg1 IS NOT NULL
        String evalScoreKey = buildEvalScoreKey(rule);

        String firstValue = buildFirstKey(rule);

        //字段信息
        String fieldValue = buildFieldNameKey(rule);

        String resultSql;
        //非表达式模式
        if (!rule.getType().equals(EvaluationDescriptor.EvaluationType.EXPR)) {
            throw new IllegalStateException("暂时只支持表达式模式");
        }

        StringBuilder insideSql = new StringBuilder().append(" SELECT ")
                .append(" %s AS SOURCE_ID, ")
                .append(" %s AS ORG_CODE, ")
                .append(" %s AS PK_VALUE, ")
                .append(" %s AS FIRST_VALUE0, ")
                .append(" %s AS EVAL_RESULT0, ")
                .append(" %s AS FIELD_NAME0 ")
                .append("FROM %s");

        resultSql = String.format(insideSql.toString(),
                dataset.getMetadata().get(DATASET_METADATA_SOURCE_FILED),
                dataset.getMetadata().get(DATASET_METADATA_ORG_FILED),
                pkValue,
                firstValue,
                evalScoreKey,
                fieldValue,
                datasourceSchema + "." + dataset.getCode());

        //查询列
        String baseColumn = "SOURCE_ID, ORG_CODE, PK_VALUE, FIRST_VALUE, \"\" AS ORIGINAL_VALUE, EVAL_RESULT, FIELD_NAME ";
        //带Filed字段信息的列
        String filedColumn = baseColumn + String.format(" , %s AS FIELD_ID", buildFieldIdKey(dataset.getCode()));

        if (DqRuleEnum.NOT_NULL_RULE.getName().equals(rule.getRuleCode())) {
            baseColumn = "SOURCE_ID, ORG_CODE, PK_VALUE, remove_random_prefix(FIRST_VALUE) AS FIRST_VALUE, \"\" AS ORIGINAL_VALUE, EVAL_RESULT, FIELD_NAME ";
        }

        String outsideSql = "SELECT %s " +
                " FROM ( SELECT %s FROM" +
                "         ( %s ) AS tb" +
                "               LATERAL VIEW posexplode(split(FIRST_VALUE0, '~')) num AS FIRST_VALUE_index, FIRST_VALUE " +
                "               LATERAL VIEW posexplode(split(EVAL_RESULT0, '~')) num AS EVAL_RESULT_index, EVAL_RESULT " +
                "               LATERAL VIEW posexplode(split(FIELD_NAME0, '~')) num AS FIELD_NAME_index, FIELD_NAME" +
                "      WHERE FIRST_VALUE_index = EVAL_RESULT_index " +
                "        AND FIRST_VALUE_index = FIELD_NAME_index " +
                "        AND EVAL_RESULT_index = FIELD_NAME_index) t";
        resultSql = String.format(outsideSql, filedColumn, baseColumn, resultSql + dateWhere(resultSql, execDate) + dataRangeWhere(dataset, jobDefinition));

        return resultSql;
    }

}
