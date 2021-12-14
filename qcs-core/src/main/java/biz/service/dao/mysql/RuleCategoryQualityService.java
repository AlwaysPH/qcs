package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.RuleCategoryQuality;
import com.gwi.qcs.model.mapper.mysql.RuleCategoryQualityMapper;
import com.gwi.qcs.service.api.dto.OrgQualityReportReq;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/**
 * (RuleCategoryQuality)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:38
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class RuleCategoryQualityService extends SuperServiceImpl<RuleCategoryQualityMapper, RuleCategoryQuality> {
    public static final int WEEK = 2;


    public void del(String scoreInstanceId, String endDay) {
        this.remove(new LambdaQueryWrapper<RuleCategoryQuality>().eq(RuleCategoryQuality::getEndDay, endDay)
            .eq(RuleCategoryQuality::getScoreInstanceId, scoreInstanceId));
    }

    /**
     * 根据日期范围查询
     * @return
     */
    public List<RuleCategoryQuality> findResultStatistic(RuleCategoryQuality ruleCategoryQuality, String beginDate, String endDate){
        if(StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)){
            return null;
        }
        QueryWrapper<RuleCategoryQuality> queryWrapper = new QueryWrapper<>(ruleCategoryQuality);
        queryWrapper.between("START_DAY" , beginDate , endDate);
        queryWrapper.orderByDesc("CREATE_TIME");
        return this.list(queryWrapper);
    }


    /**
     * 数据集前5
     *
     * @param orgQualityReportReq
     * @param queryType
     * @return
     */
    public List<RuleCategoryQuality> getRuleCategoryQualityTop5(OrgQualityReportReq orgQualityReportReq, Integer queryType) {
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        LambdaQueryWrapper<RuleCategoryQuality> queryWrapper = new LambdaQueryWrapper<RuleCategoryQuality>()
            .eq(RuleCategoryQuality::getCycle, cycle)
            .eq(RuleCategoryQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(RuleCategoryQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(RuleCategoryQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId());
        List<RuleCategoryQuality> sortList =  this.list(queryWrapper);
        sortList.sort((d1, d2) -> {
            if (d1.getCheckFailTimesRate().doubleValue() < d2.getCheckFailTimesRate().doubleValue()) {
                return 1;
            } else if (d1.getCheckFailTimesRate().doubleValue() == d2.getCheckFailTimesRate().doubleValue()) {
                if (d1.getCheckFailTimes().longValue() < d2.getCheckFailTimes().longValue()) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        });
        return sortList;
    }

    /**
     * 查询问题最多规则分类
     *
     * @param orgQualityReportReq
     * @param queryType
     * @return
     */
    public RuleCategoryQuality getMaxProblemRule(OrgQualityReportReq orgQualityReportReq, Integer queryType) {
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        LambdaQueryWrapper<RuleCategoryQuality> queryWrapper = new LambdaQueryWrapper<RuleCategoryQuality>()
            .eq(RuleCategoryQuality::getCycle, cycle)
            .eq(RuleCategoryQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(RuleCategoryQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(RuleCategoryQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId())
            .select(RuleCategoryQuality::getRuleCategoryId, RuleCategoryQuality::getCheckFailTimes);
        List<RuleCategoryQuality> list =  this.list(queryWrapper);
        if (list.isEmpty()) {
            return null;
        } else {
            return list.stream().max(new Comparator<RuleCategoryQuality>() {
                @Override
                public int compare(RuleCategoryQuality o1, RuleCategoryQuality o2) {
                    if (o1.getCheckFailTimes().longValue() < o2.getCheckFailTimes().longValue()) {
                        return 1;
                    } else if (o1.getCheckFailTimes().longValue() == o2.getCheckFailTimes().longValue()) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            }).get();
        }
    }



    public List<RuleCategoryQuality> get(String cycle, String startTime, String endTime, List<String> instanceIds, List<String> categoryIds){
        LambdaQueryWrapper<RuleCategoryQuality> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.ge(RuleCategoryQuality::getStartDay, startTime)
            .le(RuleCategoryQuality::getEndDay, endTime)
            .eq(RuleCategoryQuality::getCycle, cycle);
        if (CollectionUtils.isNotEmpty(instanceIds)) {
            queryWrapper.in(RuleCategoryQuality::getScoreInstanceId, instanceIds);
        }
        if (CollectionUtils.isNotEmpty(categoryIds)) {
            queryWrapper.in(RuleCategoryQuality::getRuleCategoryId, categoryIds);
        }
        return this.list(queryWrapper);
    }
}