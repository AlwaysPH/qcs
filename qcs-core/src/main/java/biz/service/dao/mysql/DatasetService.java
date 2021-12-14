package biz.service.dao.mysql;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.core.biz.service.DmQueryService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataDatasetService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataOrgService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.PreDataSourceService;
import com.gwi.qcs.model.domain.clickhouse.PreDataDataset;
import com.gwi.qcs.model.domain.clickhouse.PreDataOrg;
import com.gwi.qcs.model.domain.clickhouse.PreDataSource;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.DataSetQcsQueryRequest;
import com.gwi.qcs.model.dto.DataSetQcsQueryResponse;
import com.gwi.qcs.model.mapper.mysql.DatasetMapper;
import com.gwi.qcs.model.mapper.mysql.RuleMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author yanhan
 * @create 2020-08-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class DatasetService extends SuperServiceImpl<DatasetMapper, Dataset> {

    /**
     * ES中的数据集ID字段
     */
    private static final String ES_FIELD_DATASET_ID = "DATASET_ID";
    /**
     * ES中的数据集ID字段
     */
    private static final String ES_FIELD_STANDARD_ID = "STANDARD_ID";
    /**
     * ES中的周期字段
     */
    private static final String ES_FIELD_CYCLE_DAY = "CYCLE_DAY";
    /**
     * ES中的机构编码字段
     */
    private static final String ES_FIELD_ORG_CODE = "ORG_CODE";
    /**
     * ES中的数据源ID字段
     */
    private static final String ES_FIELD_SOURCE_ID = "SOURCE_ID";

    /**
     * es聚合分组的自定义组名
     */
    private static final String AGG_GROUP_NAME = "dataset_group";
    /**
     * es result_statistic表字段：检验成功次数
     */
    private static final String ES_FIELD_SUCCESS_AMOUNT = "SUCCESS_AMOUNT";
    /**
     * es result_statistic表字段：检出问题数
     */
    private static final String ES_FIELD_FAIL_AMOUNT = "FAIL_AMOUNT";
    /**
     * es data_volume_statistics表字段：数据总量
     */
    private static final String ES_FIELD_DATA_AMOUNT = "DATA_AMOUNT";
    /**
     * es evaluation_result表字段：主键
     */
    private static final String ES_FIELD_RULE_ID = "RULE_ID";
    /**
     * 聚合函数统计校验成功次数别名
     */
    private static final String AGG_SUM_SUCCESS_TIMES = "successTimes";
    /**
     * 聚合函数统计校验失败次数别名
     */
    private static final String AGG_SUM_FAIL_TIMES = "failTimes";
    /**
     * 聚合函数统计总数据量别名
     */
    private static final String AGG_SUM_TOTAL_AMOUNT = "totalAmount";

    /**
     * 查询数据集质控数据类型：评分对象
     */
    private static final int TYPE_SCORE_INSTANCE = 1;
    /**
     * 查询数据集质控数据类型：数据来源
     */
    private static final int TYPE_DATASOURCE = 2;
    /**
     * 查询数据集质控数据类型：区域
     */
    private static final int TYPE_AREA = 3;
    /**
     * 查询数据集质控数据类型：医疗机构
     */
    private static final int TYPE_ORG = 4;

    /**
     * 聚合查询默认条数
     */
    public static final int QUERY_SIZE = 10000;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private ScoreInstanceService scoreInstanceService;

    @Autowired
    private DatasetMapper dataSetMapper;

    @Autowired
    private RuleMapper ruleMapper;

    @Autowired
    private DmQueryService dmQueryService;

    @Autowired
    private DatasetFiledService datasetFiledService;

    @Autowired
    DataSourceService dataSourceService;

    @Autowired
    DsmRefService dsmRefService;

    @Autowired
    ScmDetailService scmDetailService;

    @Autowired
    private PreDataDatasetService preDataDatasetService;

    @Autowired
    private PreDataSourceService preDataSourceService;

    @Autowired
    private PreDataOrgService preDataOrgService;

    /**
     * 标准版本id和名称映射关系map
     */
    Map<Long, String> standardVersionMap = Maps.newHashMap();

    /**
     * 标准版本列表
     */
    List<StandardVersion> standardVersionList = Lists.newArrayList();

    /**
     * 数据集id-数据总量map
     */
    Map<String, Long> totalAmountMap = Maps.newHashMap();
    /**
     * 数据集id-问题数据量map
     */
    Map<String, Long> failAmountMap = Maps.newHashMap();
    /**
     * 数据集id-检验总次数map
     */
    Map<String, Long> totalTimesMap = Maps.newHashMap();
    /**
     * 数据集id-检出问题数map
     */
    Map<String, Long> failTimesMap = Maps.newHashMap();

    public List<Dataset> getByStandardId(Long standardId, String sourceType){
        Dataset dataset = new Dataset();
        dataset.setStandardId(standardId);
        dataset.setSourceType(sourceType);
        QueryWrapper<Dataset> queryWrapper = new QueryWrapper<>(dataset);
        return this.list(queryWrapper);
    }


    public String getNameByCodeMap(Long standardId, String datasetCode, Map<Long, Map<String, String>> map){
        if(map.containsKey(standardId)){
            return map.get(standardId).getOrDefault(datasetCode, CommonConstant.DASH);
        }
        if(!map.isEmpty()){
            return CommonConstant.DASH;
        }
        QueryWrapper<Dataset> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc("SOURCE_TYPE");
        List<Dataset> datasetList = this.list(queryWrapper);
        for(Dataset item : datasetList){
            Map<String, String> valueMap = map.getOrDefault(standardId, new HashMap<>(16));
            valueMap.put(item.getMetasetCode(), item.getMetasetName());
            map.put(standardId, valueMap);
        }
        return getNameByCodeMap(standardId, datasetCode, map);
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD)
    public String getNameByCode(String metasetCode){
        Dataset query = new Dataset();
        query.setMetasetCode(metasetCode);
        Dataset dataset = this.getOne(new QueryWrapper<>(query), false);
        if(dataset == null){
            return CommonConstant.DASH;
        }else{
            return dataset.getMetasetName();
        }
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD)
    public String getCodeById(String datasetId){
        return getString(datasetId,false);
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD)
    public String getNameById(String datasetId){
        return getString(datasetId,true);
    }

    private String getString(String datasetId, boolean isName) {
        Dataset query = new Dataset();
        query.setDataSetId(Long.parseLong(datasetId));
        Dataset dataset = this.getOne(new QueryWrapper<>(query), false);
        if(dataset == null){
            return CommonConstant.DASH;
        }else{
            return isName ? dataset.getMetasetName() : dataset.getMetasetCode();
        }
    }

    public void remove(Long standardId, String sourceType){
        Dataset dataset = new Dataset();
        dataset.setStandardId(standardId);
        dataset.setSourceType(sourceType);
        QueryWrapper<Dataset> queryWrapper = new QueryWrapper<>(dataset);
        this.remove(queryWrapper);

        if(CommonConstant.SOURCE_TYPE_INSTANCE.equals(sourceType)){
            DsmRef dsmRef = new DsmRef();
            dsmRef.setStandardId(standardId);
            QueryWrapper<DsmRef> dsmRefQueryWrapper = new QueryWrapper<>(dsmRef);
            dsmRefService.remove(dsmRefQueryWrapper);

            ScmDetail scmDetail = new ScmDetail();
            scmDetail.setStandardId(standardId);
            QueryWrapper<ScmDetail> scmDetailQueryWrapper = new QueryWrapper<>(scmDetail);
            scmDetailService.remove(scmDetailQueryWrapper);
        }
    }

    public Map<String, String> getNameMap(){
        return this.list().stream().collect(Collectors.toMap(Dataset::getMetasetCode, Dataset::getMetasetName));
    }

    /**
     * @param queryRequest
     * @return
     * @throws BizException
     */
    public List<DataSetQcsQueryResponse> getDataSetQcsByStandard(DataSetQcsQueryRequest queryRequest) throws BizException {
        this.standardVersionList = instanceService.queryStandardVersionList();
        if (CollectionUtils.isNotEmpty(standardVersionList)) {
            for (StandardVersion standard : standardVersionList) {
                standardVersionMap.put(standard.getId(), standard.getStandardName());
            }
        }

        List<DataSetQcsQueryResponse> details = Lists.newArrayList();
        //初始化查询条件组装map
        try {
            String standardId = queryRequest.getStandardId();
            List<Dataset> datasetList = getAllDatasetsByStandard();
            if (CollectionUtils.isEmpty(datasetList)) {
                return details;
            }
            //如果传入了standardId，则只统计该标准下的数据集质控数据，否则就遍历查询所有标准下的数据集质控数据
            if (StringUtils.isNotBlank(standardId)) {
                datasetList = datasetList.stream().filter(t -> t.getStandardId().equals(Long.parseLong(standardId))).collect(Collectors.toList());
            }
            List<String> datasetIds = datasetList.stream().map(t -> t.getDataSetId().toString()).collect(Collectors.toList());
            List<PreDataDataset> dataList = preDataDatasetService.getListByDatasetIds(datasetIds, queryRequest.getStartDate(), queryRequest.getEndDate());
            Map<String, List<PreDataDataset>> groupMap = dataList.stream().collect(Collectors.groupingBy(preDataDataset -> {
                return preDataDataset.getDatasetId();
            }));
            datasetList.forEach(dataset -> {
                DataSetQcsQueryResponse response = new DataSetQcsQueryResponse();
                response.setStandardName(standardVersionMap.get(dataset.getStandardId()));
                response.setStandardId(dataset.getStandardId().toString());
                response.setDataSetId(dataset.getDataSetId().toString());
                response.setDataSetCode(dataset.getMetasetCode());
                response.setDataSetName(dataset.getMetasetName());
                List<PreDataDataset> groupList = groupMap.get(dataset.getDataSetId().toString());
                if (CollectionUtils.isNotEmpty(groupList)) {
                    Long collectNum = groupList.stream().mapToLong(PreDataDataset::getCollectNum).sum();
                    Long problemNum = groupList.stream().mapToLong(PreDataDataset::getProblemNum).sum();
                    Long validateNum = groupList.stream().mapToLong(PreDataDataset::getValidateNum).sum();
                    Long checkNum = groupList.stream().mapToLong(PreDataDataset::getCheckNum).sum();
                    response.setRecordAmount(collectNum);
                    response.setCheckFailedRecordAmount(problemNum);
                    response.setCheckFailRecordRatio(divide(problemNum, collectNum));
                    response.setCheckTimes(validateNum);
                    response.setCheckFailTimes(checkNum);
                    response.setCheckFailedTimesRatio(divide(checkNum, validateNum));
                }
                details.add(response);
            });
            log.info("标准版本包含的数据集列表：{}", JSONUtil.toJsonStr(datasetList));
        } catch (Exception e) {
            log.error("查询数据集质控数据失败", e);
            throw new BizException(e.getMessage());
        }
        return details;
    }

    /**
     * @param queryRequest
     * @return
     * @throws BizException
     */
    public List<DataSetQcsQueryResponse> getDataSetQcsByCondition(DataSetQcsQueryRequest queryRequest) throws BizException {
        this.standardVersionList = instanceService.queryStandardVersionList();
        for (StandardVersion standard : standardVersionList) {
            standardVersionMap.put(standard.getId(), standard.getStandardName() + standard.getVersionCode());
        }
        List<DataSetQcsQueryResponse> details = Lists.newArrayList();
        try {
            //初始化数据集列表
            List<Dataset> datasetList = null;
            //初始化数据集id列表
            List<String> datasetIdList = null;
            //初始化机构编码列表
            List<String> orgCodeList = null;
            List<PreDataOrg> dataOrgList = null;

            //创建dataset id-code id-name映射关系
            List<Dataset> allDatasetList = getAllDatasetsByStandard();
            Map<String, String> idCodeMap = allDatasetList.stream().collect(Collectors.toMap((t -> t.getDataSetId().toString()), Dataset::getMetasetCode, (oldKey, newKey) -> newKey));
            Map<String, String> idNameMap = allDatasetList.stream().collect(Collectors.toMap((t -> t.getDataSetId().toString()), Dataset::getMetasetName, (oldKey, newKey) -> newKey));

            //获取查询类型
            int queryType = queryRequest.getQueryType();
            switch (queryType) {
                case TYPE_SCORE_INSTANCE:
                    List<DataSetQcsQueryResponse> instanceDetails = Lists.newArrayList();
                    Long scoreInstanceId = Long.valueOf(queryRequest.getCode());
                    List<ScoreInstance> allBottomChildren = scoreInstanceService.getAllBottomChildren(scoreInstanceId, CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE);
                    if (CollectionUtils.isNotEmpty(allBottomChildren)) {
                        for (ScoreInstance scoreInstance : allBottomChildren) {
                            //评分对象对应的数据集
                            datasetList = dataSetMapper.selectListByScoreInstanceId(scoreInstance.getId(), CommonConstant.SOURCE_TYPE_INSTANCE);
                            if (CollectionUtils.isNotEmpty(datasetList)) {
                                datasetIdList = datasetList.stream().map(t -> t.getDataSetId().toString()).collect(Collectors.toList());
                            }
                            if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getOrgCode())) {
                                //汇总评分对象对应的机构编码
                                String[] orgCodes = scoreInstance.getOrgCode().split(",");
                                orgCodeList = Arrays.asList(orgCodes);
                                dataOrgList = preDataOrgService.getListByDatasetIds(orgCodeList, datasetIdList, queryRequest.getStartDate(), queryRequest.getEndDate());
                                if (CollectionUtils.isNotEmpty(dataOrgList)) {
                                    List<DataSetQcsQueryResponse> orgCodeDetails = buildOrg(dataOrgList, standardVersionMap, idCodeMap, idNameMap);
                                    instanceDetails.addAll(orgCodeDetails);
                                }
                            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getDatasourceId())) {
                                //汇总评分对象对应的数据来源
                                List<PreDataSource> sourceList = preDataSourceService.getListBySourceId(scoreInstance.getDatasourceId(), datasetIdList, queryRequest.getStartDate(), queryRequest.getEndDate());
                                if (CollectionUtils.isNotEmpty(sourceList)) {
                                    List<DataSetQcsQueryResponse> sourceDetails = buildSource(sourceList, standardVersionMap, idCodeMap, idNameMap);
                                    instanceDetails.addAll(sourceDetails);
                                }
                            } else {
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(instanceDetails)) {
                        details = buildInstance(instanceDetails, standardVersionMap, idCodeMap, idNameMap);
                    }
                    break;
                case TYPE_DATASOURCE:
                    //此处必须先初始化为一个空list，因为后续作为条件去查询es时，list为null和为空是两个不同的逻辑
                    datasetIdList = Lists.newArrayList();
                    QueryWrapper<DataSource> queryWrapper = new QueryWrapper<>();
                    queryWrapper.eq("CODE", queryRequest.getCode());
                    DataSource datasource = dataSourceService.getOne(queryWrapper);
                    if (null != datasource) {
                        datasetList = dataSetMapper.selectListByDatasourceId(datasource.getId());
                        if (CollectionUtils.isNotEmpty(datasetList)) {
                            datasetIdList = datasetList.stream().map(t -> t.getDataSetId().toString()).collect(Collectors.toList());
                        }
                    }
                    List<PreDataSource> sourceList = preDataSourceService.getListBySourceId(
                        dataSourceService.checkIsYiLiaoBySourceId(queryRequest.getCode()) ? null : queryRequest.getCode(),
                        datasetIdList, queryRequest.getStartDate(), queryRequest.getEndDate());
                    if (CollectionUtils.isNotEmpty(sourceList)) {
                        details = buildSource(sourceList, standardVersionMap, idCodeMap, idNameMap);
                    }
                    break;
                case TYPE_AREA:
                    orgCodeList = Lists.newArrayList();
                    String areaId = queryRequest.getCode();
                    RrsArea area = dmQueryService.findAreaByAreaCode(areaId);
                    if (null == area) {
                        throw new BizException("未找到相关区域信息");
                    }
                    List<RrsOrganization> orgList = dmQueryService.findOrgsByAreaId(area.getId());
                    if (CollectionUtils.isNotEmpty(orgList)) {
                        orgCodeList.addAll(orgList.stream().map(RrsOrganization::getOrgCode).collect(Collectors.toList()));
                    }
                    dataOrgList = preDataOrgService.getListByOrgCodes(orgCodeList, null, queryRequest.getStartDate(), queryRequest.getEndDate());
                    if (CollectionUtils.isNotEmpty(dataOrgList)) {
                        details = buildOrg(dataOrgList, standardVersionMap, idCodeMap, idNameMap);
                    }
                    break;
                case TYPE_ORG:
                    orgCodeList = Lists.newArrayList();
                    orgCodeList.add(queryRequest.getCode());
                    dataOrgList = preDataOrgService.getListByOrgCodes(orgCodeList, null, queryRequest.getStartDate(), queryRequest.getEndDate());
                    if (CollectionUtils.isNotEmpty(dataOrgList)) {
                        details = buildOrg(dataOrgList, standardVersionMap, idCodeMap, idNameMap);
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("查询数据集质控数据失败", e);
            throw new BizException(e.getMessage());
        }
        details.sort(Comparator.comparing(DataSetQcsQueryResponse::getCheckFailRecordRatio, Comparator.nullsLast(Double::compareTo))
            .thenComparing(DataSetQcsQueryResponse::getCheckFailedTimesRatio, Comparator.nullsLast(Double::compareTo).reversed()));
        return details;
    }

    private List<DataSetQcsQueryResponse> buildInstance(List<DataSetQcsQueryResponse> instanceDetails, Map<Long, String> standardVersionMap, Map<String, String> idCodeMap, Map<String, String> idNameMap) {
        //初始化返回结果List
        List<DataSetQcsQueryResponse> resultList = Lists.newArrayList();
        Map<String, List<DataSetQcsQueryResponse>> datasetMap = instanceDetails.stream().collect(Collectors.groupingBy(dataSetQcs -> {
            return dataSetQcs.getDataSetId();
        }));
        datasetMap.forEach((datasetId, datas) -> {
            DataSetQcsQueryResponse response = new DataSetQcsQueryResponse();
            response.setStandardName(standardVersionMap.get(Long.valueOf(datas.get(0).getStandardId())));
            response.setStandardId(datas.get(0).getStandardId());
            response.setDataSetId(datasetId);
            response.setDataSetCode(idCodeMap.get(datasetId));
            response.setDataSetName(idNameMap.get(datasetId));
            Long collectNum = datas.stream().mapToLong(DataSetQcsQueryResponse::getRecordAmount).sum();
            response.setRecordAmount(collectNum);
            Long problemNum = datas.stream().mapToLong(DataSetQcsQueryResponse::getCheckFailedRecordAmount).sum();
            response.setCheckFailedRecordAmount(problemNum);
            response.setCheckFailRecordRatio(divide(problemNum, collectNum));
            Long validateNum = datas.stream().mapToLong(DataSetQcsQueryResponse::getCheckTimes).sum();
            response.setCheckTimes(validateNum);
            Long checkNum = datas.stream().mapToLong(DataSetQcsQueryResponse::getCheckFailTimes).sum();
            response.setCheckFailTimes(checkNum);
            response.setCheckFailedTimesRatio(divide(checkNum, validateNum));
            resultList.add(response);
        });
        return resultList;
    }

    private List<DataSetQcsQueryResponse> buildSource(List<PreDataSource> sourceList, Map<Long, String> standardVersionMap, Map<String, String> idCodeMap, Map<String, String> idNameMap) {
        //初始化返回结果List
        List<DataSetQcsQueryResponse> resultList = Lists.newArrayList();
        Map<String, List<PreDataSource>> datasetMap = sourceList.stream().collect(Collectors.groupingBy(preDataSource -> {
            return preDataSource.getDatasetId();
        }));
        datasetMap.forEach((datasetId, datas) -> {
            DataSetQcsQueryResponse response = new DataSetQcsQueryResponse();
            response.setStandardName(standardVersionMap.get(Long.valueOf(datas.get(0).getStandardId())));
            response.setStandardId(datas.get(0).getStandardId());
            response.setDataSetId(datasetId);
            response.setDataSetCode(idCodeMap.get(datasetId));
            response.setDataSetName(idNameMap.get(datasetId));
            Long collectNum = datas.stream().mapToLong(PreDataSource::getCollectNum).sum();
            response.setRecordAmount(collectNum);
            Long problemNum = datas.stream().mapToLong(PreDataSource::getProblemNum).sum();
            response.setCheckFailedRecordAmount(problemNum);
            response.setCheckFailRecordRatio(divide(problemNum, collectNum));
            Long validateNum = datas.stream().mapToLong(PreDataSource::getValidateNum).sum();
            response.setCheckTimes(validateNum);
            Long checkNum = datas.stream().mapToLong(PreDataSource::getCheckNum).sum();
            response.setCheckFailTimes(checkNum);
            response.setCheckFailedTimesRatio(divide(checkNum, validateNum));
            resultList.add(response);
        });
        return resultList;
    }

    private List<DataSetQcsQueryResponse> buildOrg(List<PreDataOrg> orgList, Map<Long, String> standardVersionMap, Map<String, String> idCodeMap, Map<String, String> idNameMap) {
        //初始化返回结果List
        List<DataSetQcsQueryResponse> resultList = Lists.newArrayList();
        Map<String, List<PreDataOrg>> datasetMap = orgList.stream().collect(Collectors.groupingBy(preDataOrg -> {
            return preDataOrg.getDatasetId();
        }));
        datasetMap.forEach((datasetId, datas) -> {
            DataSetQcsQueryResponse response = new DataSetQcsQueryResponse();
            response.setStandardName(standardVersionMap.get(Long.valueOf(datas.get(0).getStandardId())));
            response.setStandardId(datas.get(0).getStandardId());
            response.setDataSetId(datasetId);
            response.setDataSetCode(idCodeMap.get(datasetId));
            response.setDataSetName(idNameMap.get(datasetId));
            Long collectNum = datas.stream().mapToLong(PreDataOrg::getCollectNum).sum();
            response.setRecordAmount(collectNum);
            Long problemNum = datas.stream().mapToLong(PreDataOrg::getProblemNum).sum();
            response.setCheckFailedRecordAmount(problemNum);
            response.setCheckFailRecordRatio(divide(problemNum, collectNum));
            Long validateNum = datas.stream().mapToLong(PreDataOrg::getValidateNum).sum();
            response.setCheckTimes(validateNum);
            Long checkNum = datas.stream().mapToLong(PreDataOrg::getCheckNum).sum();
            response.setCheckFailTimes(checkNum);
            response.setCheckFailedTimesRatio(divide(checkNum, validateNum));
            resultList.add(response);
        });
        return resultList;
    }

    public void dataSetQcsExportByStandard(DataSetQcsQueryRequest queryRequest, HttpServletResponse response) {
        ExcelWriter bigWriter = null;
        SXSSFWorkbook workbook = null;
        try {
            bigWriter = ExcelUtil.getBigWriter();
            workbook = (SXSSFWorkbook) bigWriter.getWorkbook();
            List<DataSetQcsQueryResponse> details = this.getDataSetQcsByStandard(queryRequest);
            SXSSFSheet sheet = (SXSSFSheet) bigWriter.getSheet();
            // 单元格
            SXSSFCell cell;
            // 表格中的行
            SXSSFRow row;

            //创建标题行
            String[] tableHeader = {"标准版本", "数据集表名", "数据集名称", "采集数据量", "问题数据量", "问题数据占比", "校验总次数", "检出问题数", "问题数占比"};

            //需要转换成百分比格式的单元格样式
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setDataFormat((short) 10);

            for (int i = 0; i < details.size(); i++) {
                DataSetQcsQueryResponse detail = details.get(i);
                row = sheet.createRow(i + 1);
                //标准版本
                cell = row.createCell(0);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getStandardName()));

                //数据集表名
                cell = row.createCell(1);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetCode()));
                //数据集名称
                cell = row.createCell(2);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetName()));
                //采集数据量
                cell = row.createCell(3);
                setCellValue(cell, detail.getRecordAmount());
                //问题数据量
                cell = row.createCell(4);
                setCellValue(cell, detail.getCheckFailedRecordAmount());
                //问题数据占比
                cell = row.createCell(5);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailRecordRatio());
                //校验总次数
                cell = row.createCell(6);
                setCellValue(cell, detail.getCheckTimes());
                //检出问题数
                cell = row.createCell(7);
                setCellValue(cell, detail.getCheckFailTimes());
                //问题数占比
                cell = row.createCell(8);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailedTimesRatio());
            }

            //设置列宽
            for (int i = 0; i < tableHeader.length; i++) {
                sheet.setColumnWidth(i, 20 * 256);
            }
            bigWriter.writeHeadRow(Arrays.asList(tableHeader));
            bigWriter.setSheet(sheet);

            response.setContentType("multipart/form-data;charset=utf-8");
            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-disposition", "attachment;filename=" + new String(("质控数据详情_" + queryRequest.getStartDate() + "_" + queryRequest.getEndDate() + ".xlsx").getBytes(), StandardCharsets.ISO_8859_1));
            bigWriter.flush(response.getOutputStream(), true);

            log.info("根据标准导出数据集质控数据成功");
        } catch (BizException | IOException e) {
            log.error("数据集质控数据导出失败", e);
        } finally {
            IoUtil.close(workbook);
            IoUtil.close(bigWriter);
        }
    }

    public void dataSetQcsExport(DataSetQcsQueryRequest queryRequest, HttpServletResponse response) {
        ExcelWriter bigWriter = null;
        SXSSFWorkbook workbook = null;
        try {
            bigWriter = ExcelUtil.getBigWriter();
            workbook = (SXSSFWorkbook) bigWriter.getWorkbook();
            List<DataSetQcsQueryResponse> details = this.getDataSetQcsByCondition(queryRequest);
            SXSSFSheet sheet = (SXSSFSheet) bigWriter.getSheet();
            // 单元格
            SXSSFCell cell;
            // 表格中的行
            SXSSFRow row;

            //创建标题行
            String[] tableHeader = {"数据集表名", "数据集名称", "采集数据量", "问题数据量", "问题数据占比", "校验总次数", "检出问题数", "问题数占比"};

            //需要转换成百分比格式的单元格样式
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setDataFormat((short) 10);

            for (int i = 0; i < details.size(); i++) {
                DataSetQcsQueryResponse detail = details.get(i);
                row = sheet.createRow(i + 1);

                //数据集表名
                cell = row.createCell(0);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetCode()));
                //数据集名称
                cell = row.createCell(1);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetName()));
                //采集数据量
                cell = row.createCell(2);
                setCellValue(cell, detail.getRecordAmount());
                //问题数据量
                cell = row.createCell(3);
                setCellValue(cell, detail.getCheckFailedRecordAmount());
                //问题数据占比
                cell = row.createCell(4);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailRecordRatio());
                //校验总次数
                cell = row.createCell(5);
                setCellValue(cell, detail.getCheckTimes());
                //检出问题数
                cell = row.createCell(6);
                setCellValue(cell, detail.getCheckFailTimes());
                //问题数占比
                cell = row.createCell(7);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailedTimesRatio());
            }

            //设置列宽
            for (int i = 0; i < tableHeader.length; i++) {
                sheet.setColumnWidth(i, 20 * 256);
            }
            bigWriter.writeHeadRow(Arrays.asList(tableHeader));
            bigWriter.setSheet(sheet);

            response.setContentType("multipart/form-data;charset=utf-8");
            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-disposition", "attachment;filename=" + new String(("质控数据详情_" + queryRequest.getStartDate() + "_" + queryRequest.getEndDate() + ".xlsx").getBytes(), StandardCharsets.ISO_8859_1));
            bigWriter.flush(response.getOutputStream(), true);

        } catch (BizException | IOException e) {
            log.error("数据集质控数据导出失败", e);
        } finally {
            IoUtil.close(workbook);
            IoUtil.close(bigWriter);
        }
    }

    private Double round(Double value) {
        //es中返回的类似1/0的分数值为Infinity，即无穷大或无穷小，需要在这里做null处理
        if (value.isInfinite()) {
            return null;
        } else {
            return NumberUtil.round(value, 4).doubleValue();
        }
    }

    /**
     * 获取所有标准下的所有数据集
     *
     * @return
     */
    private List<Dataset> getAllDatasetsByStandard() {
        List<Dataset> resultMapList = Lists.newArrayList();
        for (StandardVersion standardVersion : standardVersionList) {
            List<Dataset> dataSets = instanceService.queryDataSetList(standardVersion.getStandardCode(), standardVersion.getVersionCode());
            resultMapList.addAll(dataSets);
        }

        //筛选出类型为数据集的数据
        resultMapList = resultMapList.stream().filter(t -> "0".equals(t.getFlag())).collect(Collectors.toList());
        return resultMapList;
    }

    private Double divide(Long divisor, Long dividend) {
        if (divisor == null || dividend == null || dividend == 0) {
            return null;
        } else {
            return NumberUtil.div(divisor.floatValue(), dividend.floatValue(), 4);
        }
    }

    private void setCellValue(SXSSFCell cell, Long value) {

        if (null == value) {
            cell.setCellValue("");
        } else {
            cell.setCellValue(value);
        }
    }

    private void setCellValue(SXSSFCell cell, Double value) {

        if (null == value) {
            cell.setCellValue("");
        } else {
            cell.setCellValue(value);
        }
    }

    private void clearCache() {
        this.totalAmountMap.clear();
        this.totalTimesMap.clear();
        this.failTimesMap.clear();
        this.failAmountMap.clear();
    }


    @Data
    class QueryParams {
        /**
         * 机构codes
         */
        List<String> orgCodeList;

        /**
         * 数据来源ids
         */
        List<String> sourceCodeList;

        /**
         * 规则ids
         */
        List<String> ruleIdList;
    }
}
