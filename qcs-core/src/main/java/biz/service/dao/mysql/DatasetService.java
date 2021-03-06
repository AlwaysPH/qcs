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
     * ES???????????????ID??????
     */
    private static final String ES_FIELD_DATASET_ID = "DATASET_ID";
    /**
     * ES???????????????ID??????
     */
    private static final String ES_FIELD_STANDARD_ID = "STANDARD_ID";
    /**
     * ES??????????????????
     */
    private static final String ES_FIELD_CYCLE_DAY = "CYCLE_DAY";
    /**
     * ES????????????????????????
     */
    private static final String ES_FIELD_ORG_CODE = "ORG_CODE";
    /**
     * ES???????????????ID??????
     */
    private static final String ES_FIELD_SOURCE_ID = "SOURCE_ID";

    /**
     * es??????????????????????????????
     */
    private static final String AGG_GROUP_NAME = "dataset_group";
    /**
     * es result_statistic??????????????????????????????
     */
    private static final String ES_FIELD_SUCCESS_AMOUNT = "SUCCESS_AMOUNT";
    /**
     * es result_statistic???????????????????????????
     */
    private static final String ES_FIELD_FAIL_AMOUNT = "FAIL_AMOUNT";
    /**
     * es data_volume_statistics????????????????????????
     */
    private static final String ES_FIELD_DATA_AMOUNT = "DATA_AMOUNT";
    /**
     * es evaluation_result??????????????????
     */
    private static final String ES_FIELD_RULE_ID = "RULE_ID";
    /**
     * ??????????????????????????????????????????
     */
    private static final String AGG_SUM_SUCCESS_TIMES = "successTimes";
    /**
     * ??????????????????????????????????????????
     */
    private static final String AGG_SUM_FAIL_TIMES = "failTimes";
    /**
     * ????????????????????????????????????
     */
    private static final String AGG_SUM_TOTAL_AMOUNT = "totalAmount";

    /**
     * ????????????????????????????????????????????????
     */
    private static final int TYPE_SCORE_INSTANCE = 1;
    /**
     * ????????????????????????????????????????????????
     */
    private static final int TYPE_DATASOURCE = 2;
    /**
     * ??????????????????????????????????????????
     */
    private static final int TYPE_AREA = 3;
    /**
     * ????????????????????????????????????????????????
     */
    private static final int TYPE_ORG = 4;

    /**
     * ????????????????????????
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
     * ????????????id?????????????????????map
     */
    Map<Long, String> standardVersionMap = Maps.newHashMap();

    /**
     * ??????????????????
     */
    List<StandardVersion> standardVersionList = Lists.newArrayList();

    /**
     * ?????????id-????????????map
     */
    Map<String, Long> totalAmountMap = Maps.newHashMap();
    /**
     * ?????????id-???????????????map
     */
    Map<String, Long> failAmountMap = Maps.newHashMap();
    /**
     * ?????????id-???????????????map
     */
    Map<String, Long> totalTimesMap = Maps.newHashMap();
    /**
     * ?????????id-???????????????map
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
        //???????????????????????????map
        try {
            String standardId = queryRequest.getStandardId();
            List<Dataset> datasetList = getAllDatasetsByStandard();
            if (CollectionUtils.isEmpty(datasetList)) {
                return details;
            }
            //???????????????standardId??????????????????????????????????????????????????????????????????????????????????????????????????????????????????
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
            log.info("???????????????????????????????????????{}", JSONUtil.toJsonStr(datasetList));
        } catch (Exception e) {
            log.error("?????????????????????????????????", e);
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
            //????????????????????????
            List<Dataset> datasetList = null;
            //??????????????????id??????
            List<String> datasetIdList = null;
            //???????????????????????????
            List<String> orgCodeList = null;
            List<PreDataOrg> dataOrgList = null;

            //??????dataset id-code id-name????????????
            List<Dataset> allDatasetList = getAllDatasetsByStandard();
            Map<String, String> idCodeMap = allDatasetList.stream().collect(Collectors.toMap((t -> t.getDataSetId().toString()), Dataset::getMetasetCode, (oldKey, newKey) -> newKey));
            Map<String, String> idNameMap = allDatasetList.stream().collect(Collectors.toMap((t -> t.getDataSetId().toString()), Dataset::getMetasetName, (oldKey, newKey) -> newKey));

            //??????????????????
            int queryType = queryRequest.getQueryType();
            switch (queryType) {
                case TYPE_SCORE_INSTANCE:
                    List<DataSetQcsQueryResponse> instanceDetails = Lists.newArrayList();
                    Long scoreInstanceId = Long.valueOf(queryRequest.getCode());
                    List<ScoreInstance> allBottomChildren = scoreInstanceService.getAllBottomChildren(scoreInstanceId, CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE);
                    if (CollectionUtils.isNotEmpty(allBottomChildren)) {
                        for (ScoreInstance scoreInstance : allBottomChildren) {
                            //??????????????????????????????
                            datasetList = dataSetMapper.selectListByScoreInstanceId(scoreInstance.getId(), CommonConstant.SOURCE_TYPE_INSTANCE);
                            if (CollectionUtils.isNotEmpty(datasetList)) {
                                datasetIdList = datasetList.stream().map(t -> t.getDataSetId().toString()).collect(Collectors.toList());
                            }
                            if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getOrgCode())) {
                                //???????????????????????????????????????
                                String[] orgCodes = scoreInstance.getOrgCode().split(",");
                                orgCodeList = Arrays.asList(orgCodes);
                                dataOrgList = preDataOrgService.getListByDatasetIds(orgCodeList, datasetIdList, queryRequest.getStartDate(), queryRequest.getEndDate());
                                if (CollectionUtils.isNotEmpty(dataOrgList)) {
                                    List<DataSetQcsQueryResponse> orgCodeDetails = buildOrg(dataOrgList, standardVersionMap, idCodeMap, idNameMap);
                                    instanceDetails.addAll(orgCodeDetails);
                                }
                            } else if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_SOURCE.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getDatasourceId())) {
                                //???????????????????????????????????????
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
                    //????????????????????????????????????list????????????????????????????????????es??????list???null?????????????????????????????????
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
                        throw new BizException("???????????????????????????");
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
            log.error("?????????????????????????????????", e);
            throw new BizException(e.getMessage());
        }
        details.sort(Comparator.comparing(DataSetQcsQueryResponse::getCheckFailRecordRatio, Comparator.nullsLast(Double::compareTo))
            .thenComparing(DataSetQcsQueryResponse::getCheckFailedTimesRatio, Comparator.nullsLast(Double::compareTo).reversed()));
        return details;
    }

    private List<DataSetQcsQueryResponse> buildInstance(List<DataSetQcsQueryResponse> instanceDetails, Map<Long, String> standardVersionMap, Map<String, String> idCodeMap, Map<String, String> idNameMap) {
        //?????????????????????List
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
        //?????????????????????List
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
        //?????????????????????List
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
            // ?????????
            SXSSFCell cell;
            // ???????????????
            SXSSFRow row;

            //???????????????
            String[] tableHeader = {"????????????", "???????????????", "???????????????", "???????????????", "???????????????", "??????????????????", "???????????????", "???????????????", "???????????????"};

            //????????????????????????????????????????????????
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setDataFormat((short) 10);

            for (int i = 0; i < details.size(); i++) {
                DataSetQcsQueryResponse detail = details.get(i);
                row = sheet.createRow(i + 1);
                //????????????
                cell = row.createCell(0);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getStandardName()));

                //???????????????
                cell = row.createCell(1);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetCode()));
                //???????????????
                cell = row.createCell(2);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetName()));
                //???????????????
                cell = row.createCell(3);
                setCellValue(cell, detail.getRecordAmount());
                //???????????????
                cell = row.createCell(4);
                setCellValue(cell, detail.getCheckFailedRecordAmount());
                //??????????????????
                cell = row.createCell(5);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailRecordRatio());
                //???????????????
                cell = row.createCell(6);
                setCellValue(cell, detail.getCheckTimes());
                //???????????????
                cell = row.createCell(7);
                setCellValue(cell, detail.getCheckFailTimes());
                //???????????????
                cell = row.createCell(8);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailedTimesRatio());
            }

            //????????????
            for (int i = 0; i < tableHeader.length; i++) {
                sheet.setColumnWidth(i, 20 * 256);
            }
            bigWriter.writeHeadRow(Arrays.asList(tableHeader));
            bigWriter.setSheet(sheet);

            response.setContentType("multipart/form-data;charset=utf-8");
            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-disposition", "attachment;filename=" + new String(("??????????????????_" + queryRequest.getStartDate() + "_" + queryRequest.getEndDate() + ".xlsx").getBytes(), StandardCharsets.ISO_8859_1));
            bigWriter.flush(response.getOutputStream(), true);

            log.info("?????????????????????????????????????????????");
        } catch (BizException | IOException e) {
            log.error("?????????????????????????????????", e);
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
            // ?????????
            SXSSFCell cell;
            // ???????????????
            SXSSFRow row;

            //???????????????
            String[] tableHeader = {"???????????????", "???????????????", "???????????????", "???????????????", "??????????????????", "???????????????", "???????????????", "???????????????"};

            //????????????????????????????????????????????????
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setDataFormat((short) 10);

            for (int i = 0; i < details.size(); i++) {
                DataSetQcsQueryResponse detail = details.get(i);
                row = sheet.createRow(i + 1);

                //???????????????
                cell = row.createCell(0);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetCode()));
                //???????????????
                cell = row.createCell(1);
                cell.setCellValue(StrUtil.nullToEmpty(detail.getDataSetName()));
                //???????????????
                cell = row.createCell(2);
                setCellValue(cell, detail.getRecordAmount());
                //???????????????
                cell = row.createCell(3);
                setCellValue(cell, detail.getCheckFailedRecordAmount());
                //??????????????????
                cell = row.createCell(4);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailRecordRatio());
                //???????????????
                cell = row.createCell(5);
                setCellValue(cell, detail.getCheckTimes());
                //???????????????
                cell = row.createCell(6);
                setCellValue(cell, detail.getCheckFailTimes());
                //???????????????
                cell = row.createCell(7);
                cell.setCellStyle(cellStyle);
                setCellValue(cell, detail.getCheckFailedTimesRatio());
            }

            //????????????
            for (int i = 0; i < tableHeader.length; i++) {
                sheet.setColumnWidth(i, 20 * 256);
            }
            bigWriter.writeHeadRow(Arrays.asList(tableHeader));
            bigWriter.setSheet(sheet);

            response.setContentType("multipart/form-data;charset=utf-8");
            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-disposition", "attachment;filename=" + new String(("??????????????????_" + queryRequest.getStartDate() + "_" + queryRequest.getEndDate() + ".xlsx").getBytes(), StandardCharsets.ISO_8859_1));
            bigWriter.flush(response.getOutputStream(), true);

        } catch (BizException | IOException e) {
            log.error("?????????????????????????????????", e);
        } finally {
            IoUtil.close(workbook);
            IoUtil.close(bigWriter);
        }
    }

    private Double round(Double value) {
        //es??????????????????1/0???????????????Infinity????????????????????????????????????????????????null??????
        if (value.isInfinite()) {
            return null;
        } else {
            return NumberUtil.round(value, 4).doubleValue();
        }
    }

    /**
     * ???????????????????????????????????????
     *
     * @return
     */
    private List<Dataset> getAllDatasetsByStandard() {
        List<Dataset> resultMapList = Lists.newArrayList();
        for (StandardVersion standardVersion : standardVersionList) {
            List<Dataset> dataSets = instanceService.queryDataSetList(standardVersion.getStandardCode(), standardVersion.getVersionCode());
            resultMapList.addAll(dataSets);
        }

        //????????????????????????????????????
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
         * ??????codes
         */
        List<String> orgCodeList;

        /**
         * ????????????ids
         */
        List<String> sourceCodeList;

        /**
         * ??????ids
         */
        List<String> ruleIdList;
    }
}
