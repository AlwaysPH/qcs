package biz.schedule.task.dataquality.function.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.gwi.qcs.core.biz.schedule.task.dataquality.function.BaseFunction;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.entity.DatasetDescriptor;
import com.gwi.qcs.model.entity.EvaluationDescriptor;
import com.gwi.qcs.model.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.gwi.qcs.model.entity.DatasetDescriptor.DATASET_METADATA_ORG_FILED;
import static com.gwi.qcs.model.entity.DatasetDescriptor.DATASET_METADATA_SOURCE_FILED;

/**
 * 规则SQL抽象类
 *
 * @author ljl
 * @date 2020-9-17
 */
@Slf4j
public abstract class AbstractRuleSqlFunction extends BaseFunction {

    /**
     * 组装pkvalue
     *
     * @param dataset
     * @return
     */
    protected String buildPKValue(DatasetDescriptor dataset) throws Exception {
        //拼接主键值
        Map<String, String> primaryKeyMap = dataset.getPrimaryKeyType();
        return CommonUtil.getPkValue(primaryKeyMap, true);
    }

    /**
     * 构造原始值
     *
     * @param rule
     * @return
     */
    protected abstract String buildFirstKey(EvaluationDescriptor rule);

    /**
     * 构造分值key
     *
     * @param rule
     * @return
     */
    protected abstract String buildEvalScoreKey(EvaluationDescriptor rule);

    /**
     * @param rule
     * @return
     */
    protected abstract String buildOriginalValueKey(EvaluationDescriptor rule);

    /**
     * 获取where 时间分区条件
     *
     * @param resultSql
     * @param execDate
     * @return
     */
    protected String dateWhere(String resultSql, DateTime execDate) {
        StringBuilder dateWhere = new StringBuilder();
        dateWhere.append(resultSql.toUpperCase().contains("WHERE") ? "" : " WHERE ");
        // 按月分区
        String startMonth = DateUtil.format(execDate, "yyyyMM");
        dateWhere.append(" mon ='").append(startMonth).append("' AND");
        // 按天分区
        String startDay = DateUtil.format(execDate, DatePattern.PURE_DATE_PATTERN);
        dateWhere.append(" day ='").append(startDay).append("' ");
        return dateWhere.toString();
    }

    /**
     * 任务配置的机构编码和数据来源条件
     *
     * @param dataset
     * @param jobDefinition
     * @return
     */
    protected String dataRangeWhere(DatasetDescriptor dataset, JobDefinition jobDefinition) {
        List<String> orgCodeRange = jobDefinition.getOrgCodeRange();
        List<String> sourceRange = jobDefinition.getSourceRange();

        String sourceKey = dataset.getMetadata().get(DATASET_METADATA_SOURCE_FILED);
        String orgCodeKey = dataset.getMetadata().get(DATASET_METADATA_ORG_FILED);

        StringBuilder sql = new StringBuilder();

        if (CollUtil.isNotEmpty(sourceRange)) {
            sql.append(String.format(" AND %s in (%s)", sourceKey, CollUtil.join(sourceRange, ",")));
        }

        if (CollUtil.isNotEmpty(orgCodeRange)) {
            sql.append(String.format(" AND %s in (%s)", orgCodeKey, CollUtil.join(orgCodeRange, ",")));
        }

        return sql.toString();
    }
}
