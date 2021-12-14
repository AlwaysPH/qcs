package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.InstanceQuality;
import com.gwi.qcs.model.entity.QualityInfo;
import com.gwi.qcs.model.mapper.mysql.InstanceQualityMapper;
import com.gwi.qcs.service.api.dto.OrgQualityReportReq;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * (InstanceQuality)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:36
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class InstanceQualityService extends SuperServiceImpl<InstanceQualityMapper, InstanceQuality> {

    public static final int WEEK = 2;

    public void del(String scoreInstanceId, String endDay) {
        this.remove(new LambdaQueryWrapper<InstanceQuality>().eq(InstanceQuality::getEndDay, endDay)
            .eq(InstanceQuality::getScoreInstanceId, scoreInstanceId));
    }

    /**
     * 根据时间获取评分对象表数据
     *
     * @param beginDate  开始日期
     * @param endDate  结束日期
     */
    public List<InstanceQuality> findInstanceQualitys(InstanceQuality instanceQuality, String beginDate, String endDate) {
        QueryWrapper<InstanceQuality> queryWrapper = new QueryWrapper<>(instanceQuality);
        queryWrapper.between("START_DAY", beginDate, endDate);
        return this.list(queryWrapper);
    }


    /**
     * 根据时间获取评分对象表数据
     * @param scoreInstanceIds 评分对象ID
     * @param beginDate  开始日期
     * @param endDate  结束日期
     */
    public List<InstanceQuality> findInstanceQualitys(List<String> scoreInstanceIds, String beginDate, String endDate) {
        QueryWrapper<InstanceQuality> queryWrapper = new QueryWrapper<>();
        queryWrapper.between("START_DAY", beginDate, endDate);
        queryWrapper.in("SCORE_INSTANCE_ID", scoreInstanceIds);
        return this.list(queryWrapper);
    }


    /**
     * 查询上一个评分周期的评分
     *
     * @param orgQualityReportReq
     * @param queryType
     * @return
     * @throws ParseException
     */
    public List<InstanceQuality> getInstanceQualityByTopDate(OrgQualityReportReq orgQualityReportReq, Integer queryType) throws ParseException {
        OrgQualityReportReq orgQualityReportReqs = new OrgQualityReportReq();
        orgQualityReportReqs.setEndTime(orgQualityReportReq.getEndTime());
        orgQualityReportReqs.setStartTime(orgQualityReportReq.getStartTime());
        orgQualityReportReqs.setInstanceIds(orgQualityReportReq.getInstanceIds());
        orgQualityReportReqs.setInstanceId(orgQualityReportReq.getInstanceId());
        orgQualityReportReqs.setOrgCode(orgQualityReportReq.getOrgCode());
        orgQualityReportReqs.setQueryType(orgQualityReportReq.getQueryType());
        String cycle = null;
        if (queryType == 1) {
            orgQualityReportReqs.setStartTime(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(orgQualityReportReq.getStartTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -1),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
            orgQualityReportReqs.setEndTime(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(orgQualityReportReq.getEndTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -1),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
        } else if (queryType == WEEK) {
            orgQualityReportReqs.setStartTime(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(orgQualityReportReq.getStartTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -7),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
            orgQualityReportReqs.setEndTime(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(orgQualityReportReq.getEndTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -7),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
        } else {
            orgQualityReportReqs.setStartTime(DateFormatUtils.format(DateUtils.addMonths(DateUtils.parseDate(orgQualityReportReq.getStartTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -1),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
            orgQualityReportReqs.setEndTime(DateFormatUtils.format(DateUtils.addMonths(DateUtils.parseDate(orgQualityReportReq.getEndTime(), com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD), -1),
                com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD));
        }
        return getInstanceQualityByDate(orgQualityReportReqs, queryType);
    }

    public List<InstanceQuality> getInstanceQualityByDate(OrgQualityReportReq orgQualityReportReq, Integer queryType){
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        LambdaQueryWrapper<InstanceQuality> queryWrapper = new LambdaQueryWrapper<InstanceQuality>()
            .eq(InstanceQuality::getCycle, cycle)
            .eq(InstanceQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(InstanceQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(InstanceQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId());
        return this.list(queryWrapper);
    }



    /**
     * 查询机构排名
     *
     * @param instanceIds
     * @param id
     * @param orgQualityReportReq
     * @param queryType
     * @return
     */
    public Integer getTheTopBang(List<Long> instanceIds, Long id, OrgQualityReportReq orgQualityReportReq, Integer queryType) {
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        QueryWrapper<InstanceQuality> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
            .eq(InstanceQuality::getCycle, cycle)
            .eq(InstanceQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(InstanceQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(InstanceQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId())
            .orderByDesc(InstanceQuality::getScore)
            .select(InstanceQuality::getScoreInstanceId, InstanceQuality::getScore);
        List<InstanceQuality> list = this.list(queryWrapper);
        int topNum = 0;
        if (list.isEmpty()) {
            return topNum;
        } else {
            Double score = 0.0;
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getScoreInstanceId().equals(id.toString())) {
                    score = list.get(i).getScore();
                    break;
                }
            }
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getScore().equals(score)) {
                    topNum = i + 1;
                    break;
                }
            }
        }
        return topNum;
    }

    public List<InstanceQuality> get(String cycle, String startTime, String endTime, List<String> instanceIds){
        LambdaQueryWrapper<InstanceQuality> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.ge(InstanceQuality::getStartDay, startTime)
            .le(InstanceQuality::getEndDay, endTime)
            .eq(InstanceQuality::getCycle, cycle);
        if (CollectionUtils.isNotEmpty(instanceIds)) {
            queryWrapper.in(InstanceQuality::getScoreInstanceId, instanceIds);
        }
        return this.list(queryWrapper);
    }

    public List<QualityInfo> getBrotherOrChildQualityInfos(QualityInfo qualityInfo, List<String> scoreInstanceIds){
        List<QualityInfo> qualityInfos = new ArrayList<>();
        List<InstanceQuality> list = this.get(qualityInfo.getCycle(), qualityInfo.getStartDay(), qualityInfo.getEndDay(), scoreInstanceIds);
        for (InstanceQuality instanceQuality : list) {
            QualityInfo info = new QualityInfo();
            BeanUtils.copyProperties(instanceQuality, info);
            qualityInfos.add(info);
        }
        return qualityInfos;
    }
}