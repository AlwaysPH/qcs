package biz.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Maps;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.mysql.DataSourceService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetService;
import com.gwi.qcs.model.domain.mysql.Parameter;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapper.mysql.ParameterMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class VolatilityService {

    private Logger log = LoggerFactory.getLogger(VolatilityService.class);

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private DataVolumeStatisticsService dataVolumeStatisticsService;

    @Autowired
    private ParameterMapper parameterMapper;

    @Autowired
    private DataSourceService dataSourceService;

    /**
     * 系统配置偏离率
     */
    private final String DEVIATION_RATE = "";

    /**
     * 系统配置历史均值计算日期
     */
    private final String HISTORY_DAY = "HISTORY_DAY";

    private final String HISTORY_DAY_START = "HISTORY_DAY_START";

    /**
     * 根据数据源获取上传量列表接口(表格)
     *
     * @param volatility
     * @return
     */
    public List<VolatilityDataset> getDataByDate(Volatility volatility) {
        List<DatasetCycleDate> currentDatas = null;
        try {
            currentDatas = dataVolumeStatisticsService.getCurrentData(volatility,
                dataSourceService.checkIsYiLiaoBySourceId(volatility.getDatasourceId()));
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            currentDatas = new ArrayList<>();
        }
        Map<String, List<DatasetCycleDate>> currentDatasetCodeMap = currentDatas.stream().collect(Collectors.groupingBy(datasetCycleDate -> {
            return datasetCycleDate.getDatasetCode();
        }));
        List<VolatilityDataset> list = new ArrayList<>();
        Map<Long, Map<String, String>> map = new HashMap<>();
        currentDatasetCodeMap.forEach((datasetCode, datas) -> {
            VolatilityDataset volatilityDataset = new VolatilityDataset();
            String datasetFullName = null;
            volatilityDataset.setDataAmount(datas.stream().mapToLong(DatasetCycleDate::getDataAmount).sum());
            List<DataForDate> nDatas = new ArrayList<>();
            for (DatasetCycleDate data : datas) {
                DataForDate dfd = new DataForDate();
                try {
                    if (datasetFullName == null){
                        datasetFullName = datasetService.getNameByCodeMap(Long.valueOf(data.getStandardId()), data.getDatasetCode(), map);
                    }
                    volatility.setDatasetCode(datasetCode);
                    volatility.setDatasetId(data.getDatasetId());
                    dfd.setCycleDay(data.getCycleDay());
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(DateUtils.parseDate(data.getCycleDay(), CommonConstant.DATA_FORMART));
                    dfd.setIsWeek(calendar.get(Calendar.DAY_OF_WEEK));
                } catch (ParseException e) {
                    log.error(ExceptionUtils.getStackTrace(e));
                }
                dfd.setDataAmount(data.getDataAmount());
                dfd.setDatasetCode(datasetCode);
                dfd.setDatasetId(data.getDatasetId());
                nDatas.add(dfd);
            }
            volatilityDataset.setDatasetName(String.format("%s[%s]", datasetFullName, datasetCode));
            List<DataForDate> showDatas = null;
            try {
                showDatas = castDatas(nDatas, volatility, Volatility.DateType.NONE);
            } catch (ParseException e) {
                log.error(ExceptionUtils.getStackTrace(e));
            }
            volatilityDataset.setDataForDates(showDatas);
            list.add(volatilityDataset);
        });
        return list;
    }

    private List<DataForDate> castDatas(List<DataForDate> nDatas, Volatility volatility, Volatility.DateType dateType) throws ParseException {
        List<DataForDate> tList = new ArrayList<>();
        for (Date execDay = DateUtils.parseDate(volatility.getStartDate(), CommonConstant.DATA_FORMART); execDay.getTime() <= DateUtils.parseDate(volatility.getEndDate(), CommonConstant.DATA_FORMART).getTime(); execDay = DateUtils.addDays(execDay, 1)) {
            Date finalExecDay = execDay;
            if (!nDatas.stream().anyMatch(dataForDate -> {
                try {
                    return DateUtils.parseDate(dataForDate.getCycleDay(), CommonConstant.DATA_FORMART).getTime() == finalExecDay.getTime();
                } catch (ParseException e) {
                    log.error(ExceptionUtils.getStackTrace(e));
                    return false;
                }
            })) {
                DataForDate dfd = new DataForDate();
                try {
                    dfd.setCycleDay(DateFormatUtils.format(execDay, CommonConstant.DATA_FORMART));
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(execDay);
                    dfd.setIsWeek(calendar.get(Calendar.DAY_OF_WEEK));
                } catch (ParseException e) {
                    log.error(ExceptionUtils.getStackTrace(e));
                }
                dfd.setDataAmount(0L);
                dfd.setDatasetCode(volatility.getDatasetCode());
                dfd.setDatasetId(volatility.getDatasetId());
                tList.add(dfd);
            }
        }
        tList.addAll(nDatas);
        tList.sort(Comparator.comparingLong(value -> {
            try {
                return DateUtils.parseDate(value.getCycleDay(), CommonConstant.DATA_FORMART).getTime();
            } catch (ParseException e) {
                log.error(ExceptionUtils.getStackTrace(e));
                return 0L;
            }
        }));
        return tList;
    }

    private Long caculateHis(List<DatasetCycleDate> newList, Volatility volatility) {
        return newList.stream().mapToLong(DatasetCycleDate::getDataAmount).sum() / volatility.getHistoryDays();
    }

    /**
     * 根据数据源获取上传量列表接口(折线图)
     *
     * @param toParmater
     * @return
     * @throws ParseException
     */
    public List<DataForDate> getDataLineByDate(Volatility toParmater) throws ParseException {
        long sYearDate = DateUtils.addMonths(DateUtils.parseDate(toParmater.getStartDate(), CommonConstant.DATA_FORMART), 3).getTime();
        long sMonthDate = DateUtils.addMonths(DateUtils.parseDate(toParmater.getStartDate(), CommonConstant.DATA_FORMART), 1).getTime();
        long eEndDate = DateUtils.parseDate(toParmater.getEndDate(), CommonConstant.DATA_FORMART).getTime();
        if (sYearDate < eEndDate) {
            toParmater.setDateType(Volatility.DateType.YEAR);
        } else if (sMonthDate < eEndDate && eEndDate <= sYearDate) {
            toParmater.setDateType(Volatility.DateType.MONTH);
        } else if (eEndDate <= sMonthDate) {
            toParmater.setDateType(Volatility.DateType.WEEK);
        }
        List<DatasetCycleDate> caculteList = null;
        try {
            caculteList = dataVolumeStatisticsService.getCurrentData(toParmater,
                dataSourceService.checkIsYiLiaoBySourceId(toParmater.getDatasourceId()));
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            caculteList = new ArrayList<>();
        }
        List<DataForDate> list = new ArrayList<>();
        caculteList.forEach(data -> {
            DataForDate dfd = new DataForDate();
            try {
                dfd.setCycleDay(data.getCycleDay());
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(DateUtils.parseDate(data.getCycleDay(), CommonConstant.DATA_FORMART));
                dfd.setIsWeek(calendar.get(Calendar.DAY_OF_WEEK));
                if (toParmater.getDatasetCode() == null){
                    toParmater.setDatasetCode(data.getDatasetCode());
                }
                if (toParmater.getDatasetId() == null){
                    toParmater.setDatasetId(data.getDatasetId());
                }
            } catch (ParseException e) {
                log.error(ExceptionUtils.getStackTrace(e));
            }
            dfd.setDataAmount(data.getDataAmount());
            dfd.setDatasetCode(data.getDatasetCode());
            dfd.setDatasetId(data.getDatasetId());
            list.add(dfd);
        });
        List<DataForDate> showDatas = null;
        try {
            showDatas = castDatas(list, toParmater, toParmater.getDateType());
        } catch (ParseException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        // 去重处理
        Map<String, DataForDate> map = Maps.newHashMap();
        if(CollectionUtils.isNotEmpty(showDatas)){
            for(DataForDate dataForDate : showDatas){
                DataForDate mapDate = map.getOrDefault(dataForDate.getCycleDay(), new DataForDate());
                mapDate.setCycleDay(dataForDate.getCycleDay());
                Long dataAmount = mapDate.getDataAmount() == null ? 0L : mapDate.getDataAmount();
                mapDate.setDataAmount(dataAmount + dataForDate.getDataAmount());
                mapDate.setDatasetCode(dataForDate.getDatasetCode());
                mapDate.setDatasetId(dataForDate.getDatasetId());
                mapDate.setIsWeek(dataForDate.getIsWeek());
                map.put(dataForDate.getCycleDay(), mapDate);
            }
        }
        showDatas = new ArrayList<>();
        for(Map.Entry<String, DataForDate> entry : map.entrySet()){
            showDatas.add(entry.getValue());
        }
        showDatas.sort(Comparator.comparingLong(value -> {
            try {
                return DateUtils.parseDate(value.getCycleDay(), CommonConstant.DATA_FORMART).getTime();
            } catch (ParseException e) {
                return 0L;
            }
        }));
        return showDatas;
    }

    /**
     * 击数据标签，显示当日数据异常情况接口
     *
     * @param volatility
     * @return
     */
    public VolatilityDatasetDetail getLineDetail(Volatility volatility) throws ParseException {
        Parameter parameter = new Parameter();
        parameter.setCode(HISTORY_DAY);
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Parameter historyDay = parameterMapper.selectOne(queryWrapper);
        if (historyDay == null) {
            throw new BizException("HISTORY_DAY历史均值计算天数未维护!");
        }
        Parameter parameter2 = new Parameter();
        parameter2.setCode(HISTORY_DAY_START);
        QueryWrapper<Parameter> queryWrapper2 = new QueryWrapper<>();
        queryWrapper2.setEntity(parameter2);
        Parameter historyDayStart = parameterMapper.selectOne(queryWrapper2);
        if (historyDayStart == null) {
            throw new BizException("HISTORY_DAY_START历史均值计算开始日期未维护!");
        }
        volatility.setHistoryDays(Integer.valueOf(historyDay.getValue()));
        volatility.setHistoryDaysStar(historyDayStart.getValue());
        volatility.setHistoryDaysEnd(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(historyDayStart.getValue(), CommonConstant.DATA_FORMART), Integer.valueOf(historyDay.getValue()).intValue()), CommonConstant.DATA_FORMART));
        List<DatasetCycleDate> history = null;
        boolean isYiLiao = dataSourceService.checkIsYiLiaoBySourceId(volatility.getDatasourceId());
        try {
            history = dataVolumeStatisticsService.getHistoryData(volatility, isYiLiao);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            history = new ArrayList<>();
        }
        //历史总值
        Long historyAmount = history.stream().mapToLong(DatasetCycleDate::getDataAmount).sum();
        //历史总均值
        Long avgHistroy = new BigDecimal(historyAmount).divide(new BigDecimal(historyDay.getValue()), 0, BigDecimal.ROUND_HALF_UP).longValue();
        List<DatasetCycleDate> currentDatas = null;
        try {
            currentDatas = dataVolumeStatisticsService.getDataByOneDate(volatility, isYiLiao);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            currentDatas = new ArrayList<>();
        }
        //当前总值
        Long currentAmount = currentDatas.stream().mapToLong(DatasetCycleDate::getDataAmount).sum();
        //数据集的历史总值
        Map<String, List<DatasetCycleDate>> datasetHistoryAmount = history.stream().collect(Collectors.groupingBy(data -> {
            return data.getDatasetCode();
        }));
        //数据集的历史均值
        Map<String, Long> dataset = new HashMap<>();
        datasetHistoryAmount.forEach((key, list) -> {
            dataset.put(key, new BigDecimal(list.stream().mapToLong(DatasetCycleDate::getDataAmount).sum()).divide(new BigDecimal(historyDay.getValue()), 0, BigDecimal.ROUND_HALF_UP).longValue());
        });
        //总数据的偏离率
        Double deviationRate = avgHistroy != 0 ? new BigDecimal(Math.abs(currentAmount - avgHistroy)).divide(new BigDecimal(avgHistroy), 0, BigDecimal.ROUND_HALF_UP).doubleValue() : 0;
        VolatilityDatasetDetail vdd = new VolatilityDatasetDetail();
        vdd.setCycleDay(volatility.getCycleDate());
        vdd.setAvgHistroy(avgHistroy);
        vdd.setDataAmount(currentAmount);
        vdd.setWeek(vdd.getCycleDay());
        vdd.setDeviationRate(deviationRate);
        List<VolatilityDatasetTotal> vdts = new ArrayList<>();
        Map<Long, Map<String, String>> map = new HashMap<>();
        currentDatas.forEach(datasetCycleDate -> {
            VolatilityDatasetTotal vdt = new VolatilityDatasetTotal();
            //数据集的历史均值
            vdt.setAvgHistroy(dataset.get(datasetCycleDate.getDatasetCode()));
            vdt.setCycleDay(volatility.getCycleDate());
            //数据集总量
            vdt.setDataAmount(datasetCycleDate.getDataAmount());
            String datasetName = datasetService.getNameByCodeMap(Long.valueOf(datasetCycleDate.getStandardId()), datasetCycleDate.getDatasetCode(), map);
            vdt.setDatesetName(String.format("%s[%s]", datasetName, datasetCycleDate.getDatasetCode()));
            vdt.setIsWeek(volatility.getCycleDate());
            vdt.setWeek(volatility.getCycleDate());
            vdt.setDatasetId(datasetCycleDate.getDatasetId());
            vdt.setDatasetCode(datasetCycleDate.getDatasetCode());
            //数据集的偏离率
            if (dataset.get(datasetCycleDate.getDatasetCode()) != null){
                vdt.setDeviationRate(new BigDecimal(Math.abs(datasetCycleDate.getDataAmount() - dataset.get(datasetCycleDate.getDatasetCode()))).divide(new BigDecimal(dataset.get(datasetCycleDate.getDatasetCode())), 10, BigDecimal.ROUND_HALF_UP).doubleValue());
            }
            else{
                vdt.setDeviationRate(0d);
            }
            vdts.add(vdt);
        });
        //按偏离率排序
        vdts.sort(Comparator.comparingDouble(value -> {
            return new BigDecimal(value.getDeviationRate()).doubleValue();
        }));
        vdd.setDatesetList(vdts);
        return vdd;
    }

    /**
     * 获取数据集波动性折线图
     *
     * @param volatility
     * @return
     */
    public List<VolatilityDatasetTotal> getDatasetLine(Volatility volatility) throws ParseException {
        Parameter parameter = new Parameter();
        parameter.setCode(HISTORY_DAY);
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Parameter historyDay = parameterMapper.selectOne(queryWrapper);
        if (historyDay == null) {
            throw new BizException("HISTORY_DAY历史均值计算天数未维护!");
        }
        Parameter parameter2 = new Parameter();
        parameter2.setCode(HISTORY_DAY_START);
        QueryWrapper<Parameter> queryWrapper2 = new QueryWrapper<>();
        queryWrapper2.setEntity(parameter2);
        Parameter historyDayStart = parameterMapper.selectOne(queryWrapper2);
        if (historyDayStart == null) {
            throw new BizException("HISTORY_DAY历史均值计算天数未维护!");
        }
        boolean isYiLiao = dataSourceService.checkIsYiLiaoBySourceId(volatility.getDatasourceId());
        volatility.setHistoryDays(Integer.valueOf(historyDay.getValue()));
        volatility.setHistoryDaysStar(historyDayStart.getValue());
        volatility.setHistoryDaysEnd(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(historyDayStart.getValue(), CommonConstant.DATA_FORMART),
            Integer.valueOf(historyDay.getValue()).intValue()), CommonConstant.DATA_FORMART));
        List<DatasetCycleDate> historyDatas = null;
        try {
            historyDatas = dataVolumeStatisticsService.getHistoryDataByDatasetCode(volatility, isYiLiao);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            historyDatas = new ArrayList<>();
        }
        //数据集历史总量
        Long historyAmount = historyDatas.stream().mapToLong(DatasetCycleDate::getDataAmount).sum();
        //数据集历史均值
        Long avgHistroy = new BigDecimal(historyAmount).divide(new BigDecimal(historyDay.getValue()), 0, BigDecimal.ROUND_HALF_UP).longValue();
        List<DatasetCycleDate> datasetCycleDatas = null;
        try {
            datasetCycleDatas = dataVolumeStatisticsService.getDataByDataset(volatility, isYiLiao);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            datasetCycleDatas = new ArrayList<>();
        }
        List<VolatilityDatasetTotal> vdts = new ArrayList<>();
        final String[] datasetName = {null};
        Map<Long, Map<String, String>> map = new HashMap<>();
        datasetCycleDatas.forEach(datasetCycleDate -> {
            VolatilityDatasetTotal vdt = new VolatilityDatasetTotal();
            vdt.setWeek(datasetCycleDate.getCycleDay());
            String datasetFullName = datasetService.getNameByCodeMap(Long.valueOf(datasetCycleDate.getStandardId()), datasetCycleDate.getDatasetCode(), map);
            vdt.setDatesetName(String.format("%s[%s]", datasetFullName, datasetCycleDate.getDatasetCode()));
            if (datasetName[0] == null){
                datasetName[0] = datasetFullName;
            }
            vdt.setDataAmount(datasetCycleDate.getDataAmount());
            vdt.setCycleDay(datasetCycleDate.getCycleDay());
            vdt.setAvgHistroy(avgHistroy);
            vdt.setIsWeek(datasetCycleDate.getCycleDay());
            vdt.setDeviationRate(avgHistroy != 0 ? new BigDecimal(Math.abs(datasetCycleDate.getDataAmount() - avgHistroy)).divide(new BigDecimal(avgHistroy), 10, BigDecimal.ROUND_HALF_UP).doubleValue() : 0L);
            vdts.add(vdt);
        });
        List<VolatilityDatasetTotal> showDatas = null;
        showDatas = castDatasetDatas(vdts, volatility, volatility.getDatasetCode(), datasetName[0], volatility.getDateType(), avgHistroy);
        return showDatas;
    }

    private List<VolatilityDatasetTotal> castDatasetDatas(List<VolatilityDatasetTotal> nDatas, Volatility volatility, String datasetCode, String datasetName, Volatility.DateType dateType, Long avgHistroy) throws ParseException {
        List<VolatilityDatasetTotal> tList = new ArrayList<>();
        loop:
        for (Date execDay = DateUtils.parseDate(volatility.getStartDate(), CommonConstant.DATA_FORMART); execDay.getTime() <= DateUtils.parseDate(volatility.getEndDate(), CommonConstant.DATA_FORMART).getTime(); execDay = DateUtils.addDays(execDay, 1)) {
            Date finalExecDay = execDay;
            if (!nDatas.stream().anyMatch(dataForDate -> {
                try {
                    return DateUtils.parseDate(dataForDate.getCycleDay(), CommonConstant.DATA_FORMART).getTime() == finalExecDay.getTime();
                } catch (ParseException e) {
                    log.error(ExceptionUtils.getStackTrace(e));
                    return false;
                }
            })) {
                VolatilityDatasetTotal vdt = new VolatilityDatasetTotal();
                vdt.setDatesetName(String.format("%s[%s]", datasetName, datasetCode));
                vdt.setWeek(DateFormatUtils.format(execDay, CommonConstant.DATA_FORMART));
                vdt.setDataAmount(0L);
                vdt.setCycleDay(DateFormatUtils.format(execDay, CommonConstant.DATA_FORMART));
                vdt.setAvgHistroy(avgHistroy);
                vdt.setIsWeek(DateFormatUtils.format(execDay, CommonConstant.DATA_FORMART));
                vdt.setDeviationRate(0D);
                tList.add(vdt);
            }
        }
        tList.addAll(nDatas);
        tList.sort(Comparator.comparingLong(value -> {
            try {
                return DateUtils.parseDate(value.getCycleDay(), CommonConstant.DATA_FORMART).getTime();
            } catch (ParseException e) {
                log.error(ExceptionUtils.getStackTrace(e));
                return 0L;
            }
        }));
        return tList;
    }

}
