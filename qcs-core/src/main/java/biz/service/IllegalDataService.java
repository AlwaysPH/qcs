package biz.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.page.PageMethod;
import com.greatwall.component.ccyl.common.model.PageData;
import com.greatwall.component.ccyl.common.utils.DateUtil;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.core.biz.service.dao.clickhouse.OrgErrorService;
import com.gwi.qcs.core.biz.service.dao.clickhouse.UploadTimeErrorService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetService;
import com.gwi.qcs.core.biz.service.dao.mysql.ParameterService;
import com.gwi.qcs.model.domain.clickhouse.OrgError;
import com.gwi.qcs.model.domain.clickhouse.UploadTimeError;
import com.gwi.qcs.model.domain.mysql.Dataset;
import com.gwi.qcs.model.domain.mysql.Parameter;
import com.gwi.qcs.model.dto.IllegalDataRequest;
import com.gwi.qcs.model.entity.IllegalDateDataStatistic;
import com.gwi.qcs.model.entity.IllegalDateDetail;
import com.gwi.qcs.model.entity.IllegalOrganizationStatistic;
import com.gwi.qcs.model.mapper.mysql.DatasetMapper;
import com.gwi.qcs.model.mapper.mysql.ParameterMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class IllegalDataService {

    @Autowired
    ParameterMapper parameterMapper;

    @Autowired
    DatasetMapper dataSetMapper;

    @Autowired
    OrgErrorService orgErrorService;

    @Autowired
    UploadTimeErrorService uploadTimeErrorService;

    @Autowired
    ParameterService parameterService;

    @Autowired
    DatasetService datasetService;

    public List<IllegalDateDetail> getIllegalDateDataDetail(IllegalDataRequest request, Boolean isExport) {
        Page<UploadTimeError> list = this.getUploadTimeErrors(request, isExport);;
        List<UploadTimeError> uploadTimeErrorList = list.getResult();
        Map<String, String> nameMap = parameterService.getSourceIdMap(true);
        List<IllegalDateDetail> result = new ArrayList<>();
        for (UploadTimeError uploadTimeError : uploadTimeErrorList) {
            IllegalDateDetail detail = new IllegalDateDetail();
            detail.setSourceId(uploadTimeError.getSourceId());
            detail.setSourceName(nameMap.getOrDefault(uploadTimeError.getSourceId(), CommonConstant.DASH));
            detail.setDatasetCode(uploadTimeError.getDatasetCode());
            detail.setDatasetName(datasetService.getNameByCode(uploadTimeError.getDatasetCode()));
            detail.setErrorType(uploadTimeError.getErrorType());
            detail.setPrimaryKey(uploadTimeError.getPkValue());
            detail.setCollectionDay(uploadTimeError.getExtit() == null ?
                null : DateUtil.date2String(uploadTimeError.getExtit(), DateUtil.PATTERN_STANDARD));
            result.add(detail);
        }
        return result;
    }

    public PageData getIllegalDateDataStatistics(IllegalDataRequest request, boolean isExport) {
        Page<UploadTimeError> result = getUploadTimeErrors(request, isExport);
        List<UploadTimeError> uploadTimeErrorList = result.getResult();
        List<IllegalDateDataStatistic> adataList = new ArrayList<>();
        Map<String, String> nameMap = parameterService.getSourceIdMap(true);
        for (UploadTimeError uploadTimeError : uploadTimeErrorList) {
            IllegalDateDataStatistic illegalDateDataStatistic = new IllegalDateDataStatistic();
            illegalDateDataStatistic.setSourceId(uploadTimeError.getSourceId());
            illegalDateDataStatistic.setSourceName(nameMap.getOrDefault(uploadTimeError.getSourceId(), CommonConstant.DASH));
            illegalDateDataStatistic.setDatasetCode(uploadTimeError.getDatasetCode());
            illegalDateDataStatistic.setDatasetName(datasetService.getNameByCode(uploadTimeError.getDatasetCode()));
            illegalDateDataStatistic.setErrorType(uploadTimeError.getErrorType());
            illegalDateDataStatistic.setRecordCounts(uploadTimeError.getRecordCounts());
            illegalDateDataStatistic.setCollectionDay(uploadTimeError.getExtit() == null ?
                null : DateUtil.date2String(uploadTimeError.getExtit(), DateUtil.PATTERN_STANDARD));
            adataList.add(illegalDateDataStatistic);
        }
        return new PageData((int)result.getTotal(), result.getPageSize(), result.getPages(), result.getPageNum(), adataList);
    }

    public Page<UploadTimeError> getUploadTimeErrors(IllegalDataRequest request, boolean isExport) {
        Parameter parameter = new Parameter();
        parameter.setCode("ES_MAX_PAGESIZE");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Integer esLimit = Integer.valueOf(parameterMapper.selectOne(queryWrapper).getValue());
        //获取数据集CODE
        String datasetCode = request.getDatasetCode();
        if (StringUtils.isNotEmpty(datasetCode)) {
            try{
                Dataset dataSet = dataSetMapper.getDataBySetId(Long.valueOf(datasetCode), CommonConstant.SOURCE_TYPE_DATA_SOURCE);
                datasetCode = dataSet.getMetasetCode();
            }catch (Exception e){
                log.debug("查询报错：", e);
            }
        }
        QueryWrapper<UploadTimeError> query = new QueryWrapper<>();
        query.select("count(PK_VALUE) as recordCounts", CommonConstant.SOURCE_ID_UPPER, CommonConstant.DATASET_CODE_UPPER,
            "ERROR_TYPE", CommonConstant.EXTIT_UPPER, "PK_VALUE");
        LambdaQueryWrapper<UploadTimeError> lambdaQueryWrapper = query.lambda();
        CharSequence sourceId = request.getSourceId();
        if(StringUtils.isNotEmpty(sourceId)){
            lambdaQueryWrapper.eq(UploadTimeError::getSourceId, sourceId);
        }
        if(StringUtils.isNotEmpty(datasetCode)){
            lambdaQueryWrapper.eq(UploadTimeError::getDatasetCode, datasetCode);
        }
        if(request.getErrorType() != null){
            lambdaQueryWrapper.eq(UploadTimeError::getErrorType, request.getErrorType());
        }
        lambdaQueryWrapper.between(UploadTimeError::getExtit,
            DateUtils.string2Date(request.getStartDate() + DateUtils.DAY_START_TIME, DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS),
            DateUtils.string2Date(request.getEndDate() + DateUtils.DAY_END_TIME, DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS));
        lambdaQueryWrapper.groupBy(UploadTimeError::getPkValue, UploadTimeError::getSourceId, UploadTimeError::getDatasetCode,
            UploadTimeError::getErrorType, UploadTimeError::getExtit);
        Page<UploadTimeError> result;
        if(isExport){
            result = PageMethod.startPage(1, esLimit);
        }else{
            result = PageMethod.startPage(request.getPage(),request.getSize());
        }
        uploadTimeErrorService.list(query);
        return result;
    }

    public PageData getIllegalOrganizationStatistics(IllegalDataRequest request, boolean isExport) {
        Parameter parameter = new Parameter();
        parameter.setCode("ES_MAX_PAGESIZE");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Integer esLimit = Integer.valueOf(parameterMapper.selectOne(queryWrapper).getValue());
        //获取数据集CODE
        String datasetCode = request.getDatasetCode();
        if (StringUtils.isNotEmpty(datasetCode)) {
            try{
                Dataset dataSet = dataSetMapper.getDataBySetId(Long.valueOf(datasetCode), CommonConstant.SOURCE_TYPE_DATA_SOURCE);
                datasetCode = dataSet.getMetasetCode();
            }catch (Exception e){
                log.debug("查询报错：", e);
            }
        }
        QueryWrapper<OrgError> query = new QueryWrapper<>();
        query.select("count(PK_VALUE) as recordCounts", CommonConstant.SOURCE_ID_UPPER, CommonConstant.ORG_CODE_UPPER,
            CommonConstant.DATASET_CODE_UPPER, "UPLOAD_TIME");
        LambdaQueryWrapper<OrgError> lambdaQueryWrapper = query.lambda();
        CharSequence sourceId = request.getSourceId();
        if(StringUtils.isNotEmpty(sourceId)){
            lambdaQueryWrapper.eq(OrgError::getSourceId, sourceId);
        }
        if(StringUtils.isNotEmpty(datasetCode)){
            lambdaQueryWrapper.eq(OrgError::getDatasetCode, datasetCode);
        }
        lambdaQueryWrapper.between(OrgError::getUploadTime,
            DateUtils.string2Date(request.getStartDate() + DateUtils.DAY_START_TIME, DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS),
            DateUtils.string2Date(request.getEndDate() + DateUtils.DAY_END_TIME, DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS));
        lambdaQueryWrapper.groupBy(OrgError::getPkValue, OrgError::getSourceId, OrgError::getOrgCode,
                OrgError::getDatasetCode, OrgError::getUploadTime);
        Page<OrgError> result;
        if(isExport){
            result = PageMethod.startPage(1, esLimit);
        }else{
            result = PageMethod.startPage(request.getPage(),request.getSize());
        }
        List<OrgError> orgErrorList = orgErrorService.list(query);
        List<IllegalOrganizationStatistic> adataList = new ArrayList<>();
        Map<String, String> nameMap = parameterService.getSourceIdMap(true);
        for (OrgError orgError : orgErrorList) {
            IllegalOrganizationStatistic illegalOrganizationStatistic = new IllegalOrganizationStatistic();
            illegalOrganizationStatistic.setDatasetCode(orgError.getDatasetCode());
            illegalOrganizationStatistic.setOrgCode(orgError.getOrgCode());
            illegalOrganizationStatistic.setSourceId(orgError.getSourceId());
            illegalOrganizationStatistic.setRecordCounts(orgError.getRecordCounts());
            illegalOrganizationStatistic.setUploadTime(orgError.getUploadTime() == null ?
                null : DateUtil.date2String(orgError.getUploadTime(), DateUtil.PATTERN_STANDARD));
            illegalOrganizationStatistic.setDatasetName(datasetService.getNameByCode(orgError.getDatasetCode()));
            illegalOrganizationStatistic.setSourceName(nameMap.getOrDefault(orgError.getSourceId(), CommonConstant.DASH));
            adataList.add(illegalOrganizationStatistic);
        }
        return new PageData((int)result.getTotal(), result.getPageSize(), result.getPages(), result.getPageNum(), adataList);
    }

}
