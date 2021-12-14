package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.DataSetQuality;
import com.gwi.qcs.model.entity.QualityInfo;
import com.gwi.qcs.model.mapper.mysql.DatasetQualityMapper;
import com.gwi.qcs.service.api.dto.OrgQualityReportReq;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * (DatasetQuality)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:34
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class DatasetQualityService extends SuperServiceImpl<DatasetQualityMapper, DataSetQuality> {
    public static final int WEEK = 2;

    public void del(String scoreInstanceId, String endDay) {
        this.remove(new LambdaQueryWrapper<DataSetQuality>().eq(DataSetQuality::getEndDay, endDay)
            .eq(DataSetQuality::getScoreInstanceId, scoreInstanceId));
    }

    /**
     * 根据日期范围查询
     * @return
     */
    public List<DataSetQuality> findDatasetQualitys(DataSetQuality datasetQuality, String beginDate, String endDate){
        if(StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)){
            return null;
        }
        QueryWrapper<DataSetQuality> queryWrapper = new QueryWrapper<>(datasetQuality);
        queryWrapper.between("START_DAY" , beginDate , endDate);
        return this.list(queryWrapper);
    }




    /**
     * 数据集前5
     *
     * @param orgQualityReportReq
     * @param queryType
     * @return
     */
    public List<DataSetQuality> getDatasetQualityTop5(OrgQualityReportReq orgQualityReportReq, Integer queryType) {
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        QueryWrapper<DataSetQuality> queryWrapper = new QueryWrapper<>();
        queryWrapper.select("sum(CHECK_TIMES) as checkTimes",
            "sum(CHECK_FAIL_TIMES) as checkFailTimes",
            CommonConstant.DATASET_ID_UPPER, CommonConstant.DATASET_CODE_UPPER);
        queryWrapper.lambda()
            .eq(DataSetQuality::getCycle, cycle)
            .eq(DataSetQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(DataSetQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(DataSetQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId())
            .groupBy(DataSetQuality::getCheckTimes, DataSetQuality::getCheckFailTimes,
                DataSetQuality::getDataSetId, DataSetQuality::getDataSetCode);
        List<DataSetQuality> sortList =  this.list(queryWrapper);
        sortList.sort((d1, d2) -> {
            int num4 = 4;
            if ((new BigDecimal(d1.getCheckFailTimes()).divide(new BigDecimal(d1.getCheckTimes()), num4, BigDecimal.ROUND_HALF_UP)).doubleValue()
                < (new BigDecimal(d2.getCheckFailTimes()).divide(new BigDecimal(d2.getCheckTimes()), num4, BigDecimal.ROUND_HALF_UP)).doubleValue()) {
                return 1;
            } else if (new BigDecimal(d1.getCheckFailTimes()).divide(new BigDecimal(d1.getCheckTimes()))
                .compareTo(new BigDecimal(d2.getCheckFailTimes()).divide(new BigDecimal(d2.getCheckTimes()))) == 0) {
                if (d1.getCheckFailTimes().longValue() < d2.getCheckFailTimes().longValue()) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        });
        return sortList.stream().limit(5).collect(Collectors.toList());
    }



    /**
     * 查询问题最多的数据集
     *
     * @param orgQualityReportReq
     * @param queryType
     * @return
     */
    public DataSetQuality getMaxProblemDataset(OrgQualityReportReq orgQualityReportReq, Integer queryType) {
        String cycle = null;
        if (queryType == 1) {
            cycle = "day";
        } else if (queryType == WEEK) {
            cycle = "week";
        } else {
            cycle = "month";
        }
        QueryWrapper<DataSetQuality> queryWrapper = new QueryWrapper<>();
        queryWrapper.select("sum(CHECK_FAIL_RECORD_AMOUNT) as checkFailRecordAmount",
            CommonConstant.DATASET_ID_UPPER, CommonConstant.DATASET_CODE_UPPER);
        queryWrapper.lambda()
            .eq(DataSetQuality::getCycle, cycle)
            .eq(DataSetQuality::getStartDay, orgQualityReportReq.getStartTime())
            .eq(DataSetQuality::getEndDay, orgQualityReportReq.getEndTime())
            .eq(DataSetQuality::getScoreInstanceId, orgQualityReportReq.getInstanceId())
            .groupBy(DataSetQuality::getCheckFailAmount,
                DataSetQuality::getDataSetId, DataSetQuality::getDataSetCode);
        List<DataSetQuality> list =  this.list(queryWrapper);
        if (list.isEmpty()) {
            return null;
        } else {
            return list.stream().max((o1, o2) -> {
                if (o1.getCheckFailAmount() < o2.getCheckFailAmount()) {
                    return 1;
                } else if (o1.getCheckFailAmount().equals(o2.getCheckFailAmount())) {
                    return 0;
                } else {
                    return -1;
                }
            }).get();
        }
    }

    public List<DataSetQuality> get(QualityInfo qualityInfoForQuery, boolean isOnlyQueryStartDay){
        LambdaQueryWrapper<DataSetQuality> queryWrapper = new LambdaQueryWrapper<>();
        if(isOnlyQueryStartDay){
            queryWrapper.between(DataSetQuality::getStartDay, qualityInfoForQuery.getStartDay(), qualityInfoForQuery.getEndDay());
        }else{
            queryWrapper.eq(DataSetQuality::getStartDay, qualityInfoForQuery.getStartDay())
                .eq(DataSetQuality::getEndDay, qualityInfoForQuery.getEndDay());
        }
        queryWrapper.eq(DataSetQuality::getCycle, qualityInfoForQuery.getCycle());
        queryWrapper.eq(DataSetQuality::getScoreInstanceId, qualityInfoForQuery.getScoreInstanceId());
        return this.list(queryWrapper);
    }
}