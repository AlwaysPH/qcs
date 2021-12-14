package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.entity.DatasetCycleDate;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.entity.Volatility;
import com.gwi.qcs.model.mapper.clickhouse.DataVolumeStatisticsMapper;
import com.gwi.qcs.service.api.dto.OrgQualityReportReq;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * (DataVolumeStatistics)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:34
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class DataVolumeStatisticsService extends AbstractSuperServiceImpl<DataVolumeStatisticsMapper, DataVolumeStatistics> {

    @Getter @Setter
    private Page<DataVolumeStatistics> pageInfo;

    /**
     * 删除维度数据
     *
     * @param condition 条件
     */
    @Override
    public void delByPreDataCondition(PreDataCondition condition) {
        getBaseMapper().delByCycleDayAndDatasetIdAndStandardId(condition.getCycleDay(), condition.getDatasetIds(), condition.getStandardId());

        Map<String, Object> map = MapUtil
            .builder("CYCLE_DAY", (Object) condition.getCycleDay())
            .put("DATASET_ID", condition.getDatasetIds())
            .put("STANDARD_ID", condition.getStandardId())
            .build();
        querySleepByConditionMap(map);
    }

    /**
     * 根据数据源获取上传量列表
     *
     * @param volatility
     * @param isYiLiao 是否为基本医疗
     */
    public List<DatasetCycleDate> getCurrentData(Volatility volatility, boolean isYiLiao) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if (isYiLiao) {
            queryWrapper.eq("STANDARD_ID", volatility.getStandardId());
        } else {
            queryWrapper.eq("SOURCE_ID", volatility.getDatasourceId());
        }
        queryWrapper.between("CYCLE_DAY", volatility.getStartDate(), volatility.getEndDate());
        List<DataVolumeStatistics> statList = this.list(queryWrapper);
        if (CollectionUtils.isEmpty(statList)) {
            return Lists.newArrayList();
        }
        Map<String, List<DataVolumeStatistics>> cycleMap = statList.stream().collect(Collectors.groupingBy(stat -> {
            return String.format("%s,%s", stat.getCycleDay(), stat.getDatasetCode());
        }));
        List<DatasetCycleDate> cycleList = groupList(cycleMap);
        return cycleList;
    }

    /**
     * 获取历史上传量列表
     *
     * @param volatility
     * @param isYiLiao 是否为基本医疗
     * @return
     */
    public List<DatasetCycleDate> getHistoryData(Volatility volatility, boolean isYiLiao) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if (isYiLiao) {
            queryWrapper.eq("STANDARD_ID", volatility.getStandardId());
        } else {
            queryWrapper.eq("SOURCE_ID", volatility.getDatasourceId());
        }
        queryWrapper.between("CYCLE_DAY", volatility.getHistoryDaysStar(), volatility.getHistoryDaysEnd());
        List<DataVolumeStatistics> statList = this.list(queryWrapper);
        if (CollectionUtils.isEmpty(statList)) {
            return Lists.newArrayList();
        }
        Map<String, List<DataVolumeStatistics>> cycleMap = statList.stream().collect(Collectors.groupingBy(stat -> {
            return String.format("%s,%s", stat.getCycleDay(), stat.getDatasetCode());
        }));
        List<DatasetCycleDate> cycleList = groupList(cycleMap);
        return cycleList;
    }

    /**
     * 获取当天上传量列表
     *
     * @param volatility
     * @param isYiLiao 是否为基本医疗
     * @return
     */
    public List<DatasetCycleDate> getDataByOneDate(Volatility volatility, boolean isYiLiao) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if (isYiLiao) {
            queryWrapper.eq("STANDARD_ID", volatility.getStandardId());
        } else {
            queryWrapper.eq("SOURCE_ID", volatility.getDatasourceId());
        }
        queryWrapper.eq("CYCLE_DAY", volatility.getCycleDate());
        List<DataVolumeStatistics> statList = this.list(queryWrapper);
        if (CollectionUtils.isEmpty(statList)) {
            return Lists.newArrayList();
        }
        Map<String, List<DataVolumeStatistics>> cycleMap = statList.stream().collect(Collectors.groupingBy(stat -> {
            return String.format("%s,%s", stat.getCycleDay(), stat.getDatasetCode());
        }));
        List<DatasetCycleDate> cycleList = groupList(cycleMap);
        return cycleList;
    }

    /**
     * 根据数据集获取上传量列表
     *
     * @param volatility
     * @param isYiLiao 是否为基本医疗
     * @return
     */
    public List<DatasetCycleDate> getDataByDataset(Volatility volatility, boolean isYiLiao) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if (isYiLiao) {
            queryWrapper.eq("STANDARD_ID", volatility.getStandardId());
        } else {
            queryWrapper.eq("SOURCE_ID", volatility.getDatasourceId());
        }
        queryWrapper.eq("DATASET_CODE", volatility.getDatasetCode());
        queryWrapper.between("CYCLE_DAY", volatility.getStartDate(), volatility.getEndDate());
        List<DataVolumeStatistics> statList = this.list(queryWrapper);
        if (CollectionUtils.isEmpty(statList)) {
            return Lists.newArrayList();
        }
        Map<String, List<DataVolumeStatistics>> cycleMap = statList.stream().collect(Collectors.groupingBy(stat -> {
            return String.format("%s,%s", stat.getCycleDay(), stat.getDatasetCode());
        }));
        List<DatasetCycleDate> cycleList = groupList(cycleMap);
        return cycleList;
    }

    /**
     * 根据数据集获取历史上传量列表
     *
     * @param volatility
     * @param isYiLiao 是否为基本医疗
     * @return
     */
    public List<DatasetCycleDate> getHistoryDataByDatasetCode(Volatility volatility, boolean isYiLiao) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        if (isYiLiao) {
            queryWrapper.eq("STANDARD_ID", volatility.getStandardId());
        } else {
            queryWrapper.eq("SOURCE_ID", volatility.getDatasourceId());
        }
        queryWrapper.eq("DATASET_CODE", volatility.getDatasetCode());
        queryWrapper.between("CYCLE_DAY", volatility.getHistoryDaysStar(), volatility.getHistoryDaysEnd());
        List<DataVolumeStatistics> statList = this.list(queryWrapper);
        if (CollectionUtils.isEmpty(statList)) {
            return Lists.newArrayList();
        }
        Map<String, List<DataVolumeStatistics>> cycleMap = statList.stream().collect(Collectors.groupingBy(stat -> {
            return String.format("%s,%s", stat.getCycleDay(), stat.getDatasetCode());
        }));
        List<DatasetCycleDate> cycleList = groupList(cycleMap);
        return cycleList;
    }

    private List<DatasetCycleDate> groupList(Map<String, List<DataVolumeStatistics>> cycleMap) {
        List<DatasetCycleDate> cycleList = new ArrayList<>();
        cycleMap.forEach((key, list) -> {
            DatasetCycleDate datasetCycleDate = new DatasetCycleDate();
            datasetCycleDate.setCycleDay(key.split(",")[0]);
            datasetCycleDate.setDatasetCode(key.split(",")[1]);
            datasetCycleDate.setStandardId(list.get(0).getStandardId());
            Long amount = list.stream().mapToLong(DataVolumeStatistics::getDataAmount).sum();
            datasetCycleDate.setDataAmount(amount);
            cycleList.add(datasetCycleDate);
        });
        return cycleList;
    }

    /**
     * 根据日期范围查询
     * @return
     */
    public List<DataVolumeStatistics> findDataVolumeStatistics(DataVolumeStatistics dataVolumeStatistics, String beginDate,String endDate){
        if(dataVolumeStatistics == null){
            dataVolumeStatistics = new DataVolumeStatistics();
        }
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>(dataVolumeStatistics);
        queryWrapper.between("CYCLE_DAY" , beginDate , endDate);
        return this.list(queryWrapper);
    }


    /**
     * 根据日期范围查询
     * @return
     */
    public List<DataVolumeStatistics> findDataVolumeStatistics(DataVolumeStatistics dataVolumeStatistics, String beginDate,String endDate, Page page){
        if(page != null){
            pageInfo = PageHelper.startPage(page.getPageNum(),page.getPageSize());
        }
        QueryWrapper<DataVolumeStatistics> queryWrapper;
        if(dataVolumeStatistics == null){
            queryWrapper = new QueryWrapper<>();
        }else{
            queryWrapper = new QueryWrapper<>(dataVolumeStatistics);
        }
        queryWrapper.between("CYCLE_DAY" , beginDate , endDate);
        return this.list(queryWrapper);
    }


    /**
     * 根据datasetIds及日期范围查询
     * @return
     */
    public List<DataVolumeStatistics> findDataVolumeStatistics(
        DataVolumeStatistics dataVolumeStatistics, List<String> datasetIds, String beginDate, String endDate,List<String> orgCodes, Page page){
        if(StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)){
            return null;
        }
        if(page != null){
            pageInfo = PageHelper.startPage(page.getPageNum(),page.getPageSize());
        }

        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>(dataVolumeStatistics);
        queryWrapper.between("CYCLE_DAY" , beginDate , endDate);
        if(datasetIds != null){
            queryWrapper.in("DATASET_ID" , datasetIds);
        }
        if(orgCodes != null && !orgCodes.isEmpty()){
            queryWrapper.in("ORG_CODE" , orgCodes);
        }
        queryWrapper.orderByDesc("CYCLE_DAY");
        return this.list(queryWrapper);
    }

    /**
     * 根据日期和数据机构查询
     *
     * @param condition 条件
     * @return 数据量统计表
     */
    public DataVolumeStatistics selectByCondition(PreDataCondition condition) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("CYCLE_DAY", condition.getCycleDay());

        switch (condition.getType()) {
            case ORG:
                queryWrapper.in("ORG_CODE", condition.getOrgCodes());
                break;
            case SOURCE:
                queryWrapper.in("SOURCE_ID", condition.getSourceIds());
                break;
            case SCORE:
                break;

            default:
                break;
        }
        return this.getOne(queryWrapper);
    }

    /**
     * 根据数据集查询
     *
     * @param cycleDay 日期
     * @param dataset  数据集
     * @return
     */
    public DataVolumeStatistics selectByCycleDayAndDataset(String cycleDay, String dataset) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("CYCLE_DAY", cycleDay);
        queryWrapper.eq("DATASET_ID", dataset);
        return this.getOne(queryWrapper);
    }

    /**
     * 查询机构的数据上传量
     *
     * @param orgQualityReportReq
     * @return
     */
    public List<DataVolumeStatistics> getOrgUpload(OrgQualityReportReq orgQualityReportReq) {
        QueryWrapper<DataVolumeStatistics> queryWrapper = new QueryWrapper<>();
        queryWrapper.select("sum(DATA_AMOUNT) as dataAmount", CommonConstant.CYCLE_DAY_UPPER);
        queryWrapper.lambda()
            .eq(DataVolumeStatistics::getOrgCode, orgQualityReportReq.getOrgCode())
            .between(DataVolumeStatistics::getCycleDay, orgQualityReportReq.getStartTime(), orgQualityReportReq.getEndTime())
            .groupBy(DataVolumeStatistics::getDataAmount, DataVolumeStatistics::getCycleDay);
        return this.list(queryWrapper);
    }

}