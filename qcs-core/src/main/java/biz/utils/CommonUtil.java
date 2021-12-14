package biz.utils;

import com.google.common.base.Joiner;
import com.gwi.qcs.model.domain.mysql.RuleCategory;
import com.gwi.qcs.model.mapenum.DqRuleEnum;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Builder
public class CommonUtil {

    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);

    private static final String SUCCESS_STRING = "SUCCESS_AMOUNT";

    private static final String FAIL_STRING = "FAIL_AMOUNT";

    private static final String SCORE_STRING = "score";

    private static final String TOTAL_STRING = "TOTAL";

    private static final String DAY_CYCLE = "day";

    private static final String WEEK_CYCLE = "week";

    private static final String MONTH_CYCLE = "month";

    private static final String COUNT_DATA_AMOUNT = "COUNT_DATA_AMOUNT";

    /**
     * 整数类型
     */
    private static final String INTEGER = "java.lang.Integer";
    public static final int SECOND = 2;

    public static List<String> getIntervalDate(String startDate, String endDate) {
        List<String> result = new ArrayList<>();
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        long startTime = 0;
        long endTime = 0;
        try {
            startTime = date.parse(startDate).getTime();
            endTime = date.parse(endDate).getTime();
        } catch (ParseException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        long day = 1000L * 60 * 60 * 24;
        for (long i = startTime; i <= endTime; i += day) {
            result.add(date.format(new Date(i)));
        }
        return result;
    }



    /***
     * 获取子类规则总权重
     * @param ruleCategoryList
     * @return
     */
    public static Long getAllWeight(List<RuleCategory> ruleCategoryList) {
        return ruleCategoryList.stream().mapToLong(RuleCategory::getWeight).sum();
    }

    public static Double getCumulateScore(Long successNum, Long failNum) {
        Double result = 0.00;
        Long total = successNum + failNum;
        if (total.longValue() != 0) {
            result = new BigDecimal(successNum).divide(new BigDecimal(total), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
        }
        return result;
    }

    public static Double getClassificationScore(Long successNum, Long failNum, Long weight, Long allWeight) {
        Double result = 0.00;
        Long total = successNum + failNum;
        if (total.longValue() != 0) {
            Double score = new BigDecimal(successNum).divide(new BigDecimal(total), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
            result = new BigDecimal(score).multiply(new BigDecimal(weight)).divide(new BigDecimal(allWeight), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        return result;
    }

    public static Double getFailTimesRate(Long successNum, Long failNum) {
        Double result = 0.00;
        Long total = successNum + failNum;
        if (total.longValue() != 0) {
            result = new BigDecimal(failNum).divide(new BigDecimal(total), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
        }
        return result;
    }

    public static double getFailRate(Long totalNum, Long failNum) {
        double result = 0.00;
        if (totalNum != 0) {
            result = new BigDecimal(failNum).divide(new BigDecimal(totalNum), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).doubleValue();
        }
        return result;
    }

    public static Double getTimesCumulateScore(Long totalSuccess, Long successNum, Long failNum) {
        Double result = 0.00;
        Long total = successNum + failNum;
        if (total.longValue() != 0) {
            result = new BigDecimal(totalSuccess).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        return result;
    }

    public static Double getStableScore(Long successNum, Long failNum, Long scoreNum, Long weight, Long allWeight) {
        Double result = 0.00;
        Long total = successNum + failNum;
        Long totalSuccess = successNum * 100 + scoreNum;
        if (total.longValue() != 0) {
            result = new BigDecimal(totalSuccess).multiply(new BigDecimal(weight / allWeight)).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        return result;
    }


    public static String getPkValue(Map<String, String> primaryKeyMap, boolean throwException) throws Exception {
        List<String> null2EmptyPrimaryKeys = new ArrayList<>();
        //如果直接用||拼接主键，只要出现有任意一个主键是空的情况下，就会导致整个拼接的值为null，所以需要用NVL函数进行包装处理
        if (!CollectionUtils.isEmpty(primaryKeyMap)) {
            for (Map.Entry<String, String> entry : primaryKeyMap.entrySet()) {
                String primaryKey = entry.getKey();
                if (StringUtils.isBlank(primaryKey)) {
                    log.error("主键值不能为空");
                    if (throwException) {
                        throw new Exception("主键值不能为空");
                    }
                }
                if (INTEGER.equals(entry.getValue())) {
                    // 定制公共卫生的主键，主键类型为数值型
                    null2EmptyPrimaryKeys.add("IF(( " + primaryKey + " is null), '',  cast( " + primaryKey + " AS bigint))");
                } else {
                    null2EmptyPrimaryKeys.add("NVL(" + primaryKey + ",'')");
                }
            }
        } else {
            log.error("主键值不能为空");
            if (throwException) {
                throw new Exception("主键值不能为空");
            }
        }
        //主键拼接用','分割,时间字段无法区分
        return Joiner.on("||','||").join(null2EmptyPrimaryKeys);
    }

    public static String getDateCondition(Date time) {
        String startTimeStr = DateFormatUtils.format(time, "yyyyMMdd");
        String startMonth = startTimeStr.substring(0, 6);
        return " day = '" + startTimeStr + "' and  mon = '" + startMonth + "' ";
    }

    /**
     * 判断当前规则是否合并执行sql
     *
     * @param ruleCode 规则
     * @return result
     */
    public static boolean isMergeSql(String ruleCode) {
        return DqRuleEnum.NORMAL_PARAM_RANGE_RULE.getName().equals(ruleCode)
                || DqRuleEnum.CODE_PARAM_RANGE_RULE.getName().equals(ruleCode)
                || DqRuleEnum.NOT_NULL_RULE.getName().equals(ruleCode);
    }

    /**
     * 判断当前规则是否符合“可空”配置（标准版本中会同步字段是否可空）
     * <p>
     * 非空、稳定性等规则没有可空配置
     *
     * @param ruleCode 规则
     * @return result
     */
    public static boolean hasNullFiledForRule(String ruleCode) {
        return !(DqRuleEnum.NOT_NULL_RULE.getName().equals(ruleCode)
                || DqRuleEnum.STABILITY_RULE.getName().equals(ruleCode)
                || DqRuleEnum.TIMELINES_RULE.getName().equals(ruleCode));
    }
}
