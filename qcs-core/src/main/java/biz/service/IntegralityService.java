package biz.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.common.utils.JDBCTypesUtils;
import com.gwi.qcs.core.biz.service.dao.clickhouse.*;
import com.gwi.qcs.core.biz.service.dao.mysql.DataSourceService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasourceStructService;
import com.gwi.qcs.core.biz.service.dao.mysql.ParameterService;
import com.gwi.qcs.model.domain.clickhouse.*;
import com.gwi.qcs.model.domain.mysql.DataSource;
import com.gwi.qcs.model.domain.mysql.Dataset;
import com.gwi.qcs.model.domain.mysql.DatasourceStruct;
import com.gwi.qcs.model.domain.mysql.Parameter;
import com.gwi.qcs.model.dto.DatasourceValidResultVO;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapper.mysql.DatasetFieldMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * 数据完整性Service
 *
 * @author yanhan
 * @create 2020-07-14 14:52
 **/
@Slf4j
@Service
public class IntegralityService {

    @Autowired
    DataSourceService dataSourceService;

    @Autowired
    DatasourceValidResultService datasourceValidResultService;

    @Autowired
    TableIntegralityDetailService tableIntegralityDetailService;

    @Autowired
    FieldIntegralityDetailService fieldIntegralityDetailService;

    @Autowired
    FieldNormativityDetailService fieldNormativityDetailService;

    @Autowired
    PkIntegralityDetailService pkIntegralityDetailService;

    @Autowired
    private ParameterService parameterService;

    @Autowired
    DatasourceStructService datasourceStructService;

    @Autowired
    private DatasetFieldMapper datasetFieldMapper;

    /**
     * 保存数据源中包含字段的map的key
     */
    private static final String CONTAINED_FIELD_KEY = "containedField";

    /**
     * 保存字段完整性详情的map的key
     */
    private static final String FIELD_INTEGRALITY_DETAIL_KEY = "fieldIntegralityDetail";

    /**
     * ResultSet返回的列名的key
     */
    private static final String COLUMN_NAME = "COLUMN_NAME";

    /**
     * 数据源结构校验结果在es中的索引名
     */
    private static final String ESINDEX_DATASOURCE_VALID_RESULT = "datasource_valid_result";
    /**
     * 表数据完整性权重参数的key
     */
    private static final String TABLE_INTEGRALITY_WEIGHT = "table_integrality_weight";
    /**
     * 字段完整性权重参数的key
     */
    private static final String FIELD_INTEGRALITY_WEIGHT = "field_integrality_weight";
    /**
     * 主键完整性权重参数的key
     */
    private static final String PK_INTEGRALITY_WEIGHT = "pk_integrality_weight";
    /**
     * 字段规范性权重参数的key
     */
    private static final String FIELD_NORMATIVITY_WEIGHT = "field_normativity_weight";
    /**
     * Oracle驱动
     */
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    /**
     * Mysql驱动
     */
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * SqlServer驱动
     */
    private static final String SQLSERVER_DRIVER = "com.microsoft.jdbc.sqlserver.SQLServerDriver";

    /**
     * 在数据源中存在的标识
     */
    private static final int EXIST = 0;

    /**
     * 在数据源中缺失的标识
     */
    private static final int NOT_EXIST = 1;

    /**
     * 字段主键标识 0是 1否
     */
    private static final String IS_KEY = "0";

    /**
     * 执行校验、向ES插入数据
     *
     * @param dataSourceId
     * @param standardId
     * @return
     * @throws BizException
     */
    public DatasourceValidResultVO doValidate(Long dataSourceId, Long standardId) throws BizException {
        try {
            log.info("开始进行数据校验,数据源id:{}，标准id：{}", dataSourceId, standardId);
            long start = System.currentTimeMillis();
            QueryWrapper<DataSource> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("ID", dataSourceId);
            queryWrapper.eq("SM_STANDARD_ID", standardId);
            DataSource dataSource = dataSourceService.getOne(queryWrapper);

            //获取数据源对应的数据集信息
            List<DatasetExtra> datasetList = dataSourceService.queryDataSetListByDatasource(dataSourceId, standardId);
            if (CollUtil.isEmpty(datasetList)) {
                throw new BizException("未找到该数据源对应的数据集信息");
            }
            List<Long> dataSetIdList = datasetList.stream().map(Dataset::getDataSetId).collect(Collectors.toList());
            //数据集标准中的字段信息列表
            List<FieldExtra> fieldList = datasetFieldMapper.getFieldExtraList(dataSetIdList);

            QueryWrapper<DatasourceStruct> structQueryWrapper = new QueryWrapper<>();
            structQueryWrapper.eq("SOURCE_CODE", dataSource.getDataSourceCode());
            List<DatasourceStruct> structList = datasourceStructService.list(structQueryWrapper);
            if (CollUtil.isEmpty(structList)) {
                throw new BizException("未找到该数据源对应的表结构信息");
            }

            //计算数据源中的表数量完整性信息
            List<TableIntegralityDetail> tableIntegralityDetails = getTableIntegralityDetails(dataSource.getDataSourceName(),
                structList, datasetList, dataSourceId, standardId);

            //数据源中包含的表完整性信息
            List<TableIntegralityDetail> containedDatasetList = tableIntegralityDetails.stream().filter(t -> t.getStatus() == 0).collect(Collectors.toList());

            //表数量完整性
            IntegralityOverview tableIntegrality = new IntegralityOverview();
            tableIntegrality.setTotalNum(tableIntegralityDetails.size());
            tableIntegrality.setActualNum(containedDatasetList.size());
            tableIntegrality.setMissingNum(tableIntegrality.getTotalNum() - tableIntegrality.getActualNum());
            tableIntegrality.setMissingRatio(divide(tableIntegrality.getMissingNum(), tableIntegrality.getTotalNum()));

            //计算数据源中字段的信息
            CompletableFuture fieldMapFuture = CompletableFuture.supplyAsync(() -> getFieldMap(dataSource, structList, fieldList, containedDatasetList));
            //主键完整性详情列表
            CompletableFuture pkIntegralityListFuture = CompletableFuture.supplyAsync(() ->
                getPkIntegralityDetailList(dataSource, structList, datasetList, containedDatasetList));

            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(fieldMapFuture, pkIntegralityListFuture);
            combinedFuture.get();
            Map<String, Object> fieldMap = (Map<String, Object>) fieldMapFuture.get();
            List<PkIntegralityDetail> pkIntegralityDetailList = (List<PkIntegralityDetail>) pkIntegralityListFuture.get();

            //字段规范性详情列表
            List<FieldNormativityDetail> fieldNormativityDetailList = (List<FieldNormativityDetail>) fieldMap.get(CONTAINED_FIELD_KEY);
            //字段完整性详情列表
            List<FieldIntegralityDetail> fieldIntegralityDetailList = (List<FieldIntegralityDetail>) fieldMap.get(FIELD_INTEGRALITY_DETAIL_KEY);

            //数据表字段完整性
            IntegralityOverview fieldIntegrality = new IntegralityOverview();
            fieldIntegrality.setTotalNum(fieldIntegralityDetailList.size());
            fieldIntegrality.setActualNum(fieldNormativityDetailList.size());
            fieldIntegrality.setMissingNum(fieldIntegrality.getTotalNum() - fieldIntegrality.getActualNum());
            fieldIntegrality.setMissingRatio(divide(fieldIntegrality.getMissingNum(), fieldIntegrality.getTotalNum()));

            int actualPkNum = (int) pkIntegralityDetailList.stream().filter(t -> t.getStatus() == 0).count();

            //数据表主键完整性
            IntegralityOverview pkIntegrality = new IntegralityOverview();
            pkIntegrality.setTotalNum(pkIntegralityDetailList.size());
            pkIntegrality.setActualNum(actualPkNum);
            pkIntegrality.setMissingNum(pkIntegrality.getTotalNum() - pkIntegrality.getActualNum());
            pkIntegrality.setMissingRatio(divide(pkIntegrality.getMissingNum(), pkIntegrality.getTotalNum()));

            //数据表字段规范性
            NormativityOverview normativity = new NormativityOverview();
            normativity.setTotal(fieldNormativityDetailList.size());

            NormativityOverview.Detail isNullNormativityDetail = new NormativityOverview.Detail();
            List<FieldNormativityDetail> illegalIsNullFieldList = fieldNormativityDetailList.stream().filter(t -> !t.getIsNullOfDatasource().equals(t.getIsNullOfStandard())).collect(Collectors.toList());
            isNullNormativityDetail.setValidateType("字段必填/可选设置");
            isNullNormativityDetail.setIllegalNum(illegalIsNullFieldList.size());
            isNullNormativityDetail.setIllegalRatio(divide(isNullNormativityDetail.getIllegalNum(), normativity.getTotal()));

            NormativityOverview.Detail typeNormativity = new NormativityOverview.Detail();
            //字段类型对比时要进行一次数据库数据类型向JAVA对应类型的转换
            List<FieldNormativityDetail> illegalTypeFieldList = fieldNormativityDetailList.stream().filter(t -> !JDBCTypesUtils.equalsJavaType(t.getTypeOfDatasource(), t.getLengthOfDatasource(), t.getTypeOfStandard())).collect(Collectors.toList());
            typeNormativity.setValidateType("字段类型");
            typeNormativity.setIllegalNum(illegalTypeFieldList.size());
            typeNormativity.setIllegalRatio(divide(typeNormativity.getIllegalNum(), normativity.getTotal()));

            NormativityOverview.Detail lengthNormativity = new NormativityOverview.Detail();
            List<FieldNormativityDetail> illegalLengthFieldList = fieldNormativityDetailList.stream().filter(t -> !t.getLengthOfDatasource().equals(t.getLengthOfStandard())).collect(Collectors.toList());
            lengthNormativity.setValidateType("字段长度");
            lengthNormativity.setIllegalNum(illegalLengthFieldList.size());
            lengthNormativity.setIllegalRatio(divide(lengthNormativity.getIllegalNum(), normativity.getTotal()));

            List<NormativityOverview.Detail> list = Lists.newArrayList();
            list.add(isNullNormativityDetail);
            list.add(typeNormativity);
            list.add(lengthNormativity);
            normativity.setDetails(list);

            //从参数配置表中获取各项评分规则的权重
            Parameter tableIntegralityWeight = parameterService.getByCode(TABLE_INTEGRALITY_WEIGHT);
            Parameter fieldIntegralityWeight = parameterService.getByCode(FIELD_INTEGRALITY_WEIGHT);
            Parameter pkIntegralityWeight = parameterService.getByCode(PK_INTEGRALITY_WEIGHT);
            Parameter fieldNormativityWeight = parameterService.getByCode(FIELD_NORMATIVITY_WEIGHT);
            //表数量完整性评分权重
            int tableIntegralityWeightValue = Integer.parseInt(tableIntegralityWeight.getValue());
            //表字段完整性评分权重
            int fieldIntegralityWeightValue = Integer.parseInt(fieldIntegralityWeight.getValue());
            //表主键完整性评分权重
            int pkIntegralityWeightValue = Integer.parseInt(pkIntegralityWeight.getValue());
            //字段规范性权重
            int fieldNormativityWeightValue = Integer.parseInt(fieldNormativityWeight.getValue());
            //总权重
            int totalWeight = tableIntegralityWeightValue + fieldIntegralityWeightValue + pkIntegralityWeightValue + fieldNormativityWeightValue;
            //计算总分
            double tableIntegralityScore = null2One(divide(tableIntegrality.getActualNum(), tableIntegrality.getTotalNum())) * 100;
            double fieldIntegralityScore = null2One(divide(fieldIntegrality.getActualNum(), fieldIntegrality.getTotalNum())) * 100;
            double pkIntegralityScore = null2One(divide(pkIntegrality.getActualNum(), pkIntegrality.getTotalNum())) * 100;
            int count = (int) fieldNormativityDetailList.stream().filter(t -> t.getIsNullOfStandard().equals(t.getIsNullOfDatasource()) && t.getLengthOfStandard().equals(t.getLengthOfDatasource()) && JDBCTypesUtils.equalsJavaType(t.getTypeOfDatasource(), t.getLengthOfDatasource(), t.getTypeOfStandard())).count();
            double fieldNormativityScore = null2One(divide(count, fieldNormativityDetailList.size())) * 100;

            double finalScore = NumberUtil.div(tableIntegralityScore * tableIntegralityWeightValue + fieldIntegralityScore * fieldIntegralityWeightValue + pkIntegralityScore * pkIntegralityWeightValue + fieldNormativityScore * fieldNormativityWeightValue, totalWeight, 2);

            dataSource.setScore(finalScore);
            DatasourceValidResultVO resultVO = new DatasourceValidResultVO(dataSource, tableIntegrality, fieldIntegrality, pkIntegrality, normativity);

            /*-----------组装保存到es的数据源结构校验结果数据 start------------*/
            DatasourceValidResult result = new DatasourceValidResult();

            //数据源信息
            result.setSourceId(dataSource.getId());
            result.setStandardId(dataSource.getStandardId());
            result.setScore(dataSource.getScore());

            //表完整性数据
            result.setStdTableAmount(tableIntegrality.getTotalNum());
            result.setRealTableAmount(tableIntegrality.getActualNum());
            result.setMissTableAmount(tableIntegrality.getMissingNum());
            result.setMissTableRate(tableIntegrality.getMissingRatio());
            //字段完整性数据
            result.setStdFieldAmount(fieldIntegrality.getTotalNum());
            result.setRealFieldAmount(fieldIntegrality.getActualNum());
            result.setMissFieldRate(fieldIntegrality.getMissingRatio());
            result.setMissFieldAmount(fieldIntegrality.getMissingNum());
            //主键完整性数据
            result.setStdKeyAmount(pkIntegrality.getTotalNum());
            result.setMissKeyAmount(pkIntegrality.getMissingNum());
            result.setRealKeyAmount(pkIntegrality.getActualNum());
            result.setMissKeyRate(pkIntegrality.getMissingRatio());
            //字段规范性数据

            result.setReqFieldMismatchAmount(isNullNormativityDetail.getIllegalNum());
            result.setReqFieldMismatchRate(isNullNormativityDetail.getIllegalRatio());
            result.setFieldTypeMismatchAmount(typeNormativity.getIllegalNum());
            result.setFieldTypeMismatchRate(typeNormativity.getIllegalRatio());
            result.setFieldLenMismatchAmount(lengthNormativity.getIllegalNum());
            result.setFieldLenMismatchRate(lengthNormativity.getIllegalRatio());

            //创建时间 格式 yyyy-MM-dd hh:mm:ss
            result.setCreateTime(DateUtil.formatDateTime(new Date()));
            /*-----------组装保存到es的数据源结构校验结果数据 end------------*/

            //向ES中插入数据源结构校验结果数据
            CompletableFuture datasourceValidResultFuture = CompletableFuture.runAsync(() ->{
                    datasourceValidResultService.getBaseMapper().del(dataSourceId, standardId);
                    datasourceValidResultService.save(result);
                }
                );
            //向ES中插入表数量完整性详情数据
            CompletableFuture tableIntegralityFuture = CompletableFuture.runAsync(() ->
                {
                    tableIntegralityDetailService.getBaseMapper().del(dataSourceId, standardId);
                    tableIntegralityDetailService.saveBatch(tableIntegralityDetails);});
            //向ES中插入字段完整性详情数据
            CompletableFuture fieldIntegralityFuture = CompletableFuture.runAsync(() ->
                {
                    fieldIntegralityDetailService.getBaseMapper().del(dataSourceId, standardId);
                    fieldIntegralityDetailService.saveBatch(fieldIntegralityDetailList);});
            //向ES中插入字段规范性详情数据
            CompletableFuture fieldNormativityFuture = CompletableFuture.runAsync(() ->
                {
                    fieldNormativityDetailService.getBaseMapper().del(dataSourceId, standardId);
                    fieldNormativityDetailService.saveBatch(fieldNormativityDetailList);});
            //向ES中插入主键完整性详情数据
            CompletableFuture pkIntegralityFuture = CompletableFuture.runAsync(() ->
                {
                    pkIntegralityDetailService.getBaseMapper().del(dataSourceId, standardId);
                    pkIntegralityDetailService.saveBatch(pkIntegralityDetailList);});

            CompletableFuture<Void> combinedEsFuture
                = CompletableFuture.allOf(datasourceValidResultFuture, tableIntegralityFuture, fieldIntegralityFuture, fieldNormativityFuture, pkIntegralityFuture);
            combinedEsFuture.join();

            log.info("数据校验完成，耗时：" + (System.currentTimeMillis() - start) + "ms");

            return resultVO;
        } catch (InterruptedException | ExecutionException e) {
            log.error("并发处理数据失败：", e);
            throw new BizException(e.getMessage());
        } catch (NullPointerException e) {
            log.error("权重参数未配置");
            throw new BizException(e.getMessage());
        }
    }

    /**
     * 从es中查询数据源结构校验结果
     *
     * @param dataSourceId
     * @param standardId
     * @return
     */
    public DatasourceValidResultVO getDatasourceValidResultFromEs(Long dataSourceId, Long standardId) {
        /*------------初始化返回给前端的数据结构（没有数据也要按此结构返回） start------------*/
        //表数量完整性
        IntegralityOverview tableIntegrality = new IntegralityOverview();
        //数据表字段完整性
        IntegralityOverview fieldIntegrality = new IntegralityOverview();
        //数据表主键完整性
        IntegralityOverview pkIntegrality = new IntegralityOverview();
        //数据表字段规范性
        NormativityOverview normativity = new NormativityOverview();

        NormativityOverview.Detail lengthNormativity = new NormativityOverview.Detail();
        lengthNormativity.setValidateType("字段长度");

        NormativityOverview.Detail typeNormativity = new NormativityOverview.Detail();
        typeNormativity.setValidateType("字段类型");

        NormativityOverview.Detail isNullNormativityDetail = new NormativityOverview.Detail();
        isNullNormativityDetail.setValidateType("字段必填/可选设置");
        DataSource dataSource = new DataSource();
        /*------------初始化返回给前端的数据结构 end------------*/
        try {
            QueryWrapper<DataSource> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("ID", dataSourceId);
            queryWrapper.eq("SM_STANDARD_ID", standardId);
            dataSource = dataSourceService.getOne(queryWrapper);
            if (null == dataSource) {
                throw new BizException("未找到数据源");
            }
            LambdaQueryWrapper<DatasourceValidResult> lambdaQueryWrapper = new LambdaQueryWrapper<>();
            lambdaQueryWrapper.eq(DatasourceValidResult::getSourceId, dataSourceId);
            lambdaQueryWrapper.eq(DatasourceValidResult::getStandardId, standardId);
            lambdaQueryWrapper.orderByDesc(DatasourceValidResult::getCreateTime);
            DatasourceValidResult result = datasourceValidResultService.getOne(lambdaQueryWrapper, false);
            //如果es有返回，则向刚才组装好的结构中填充数据
            if (null != result) {
                dataSource.setScore(result.getScore());
                //构建表完整性数据
                tableIntegrality.setTotalNum(result.getStdTableAmount());
                tableIntegrality.setMissingNum(result.getMissTableAmount());
                tableIntegrality.setActualNum(result.getRealTableAmount());
                tableIntegrality.setMissingRatio(result.getMissTableRate());
                //构建字段完整性数据
                fieldIntegrality.setTotalNum(result.getStdFieldAmount());
                fieldIntegrality.setActualNum(result.getRealFieldAmount());
                fieldIntegrality.setMissingNum(result.getMissFieldAmount());
                fieldIntegrality.setMissingRatio(result.getMissFieldRate());
                //构建主键完整性数据
                pkIntegrality.setTotalNum(result.getStdKeyAmount());
                pkIntegrality.setActualNum(result.getRealKeyAmount());
                pkIntegrality.setMissingNum(result.getMissKeyAmount());
                pkIntegrality.setMissingRatio(result.getMissKeyRate());
                //构建字段规范性数据
                normativity.setTotal(fieldIntegrality.getActualNum());
                isNullNormativityDetail.setIllegalNum(result.getReqFieldMismatchAmount());
                isNullNormativityDetail.setIllegalRatio(result.getReqFieldMismatchRate());
                typeNormativity.setIllegalNum(result.getFieldTypeMismatchAmount());
                typeNormativity.setIllegalRatio(result.getFieldTypeMismatchRate());
                lengthNormativity.setIllegalNum(result.getFieldLenMismatchAmount());
                lengthNormativity.setIllegalRatio(result.getFieldLenMismatchRate());
            }
        }catch (Exception e) {
            log.error("查询数据源结构校验结果异常", e);
            throw new BizException(e.getMessage());
        }
        List<NormativityOverview.Detail> list = Lists.newArrayList();
        list.add(isNullNormativityDetail);
        list.add(typeNormativity);
        list.add(lengthNormativity);
        normativity.setDetails(list);
        DatasourceValidResultVO resultVO = new DatasourceValidResultVO();
        resultVO.setDatasource(dataSource);
        resultVO.setField(fieldIntegrality);
        resultVO.setNormativity(normativity);
        resultVO.setTable(tableIntegrality);
        resultVO.setPk(pkIntegrality);
        return resultVO;
    }

    /**
     * 连接数据库
     *
     * @param url
     * @param username
     * @param password
     * @return
     */
    private Connection getDatabaseConnection(String dbType, String url, String username, String password) throws BizException {

        Connection conn = null;
        try {
            if (DbTypeConstant.MYSQL.equalsIgnoreCase(dbType)) {
                Class.forName(MYSQL_DRIVER);
            } else if (DbTypeConstant.ORACLE.equalsIgnoreCase(dbType)) {
                Class.forName(ORACLE_DRIVER);
            } else if (DbTypeConstant.SQL_SERVER.equalsIgnoreCase(dbType)) {
                Class.forName(SQLSERVER_DRIVER);
            } else {
                throw new BizException("未找到对应的数据库驱动");
            }
            conn = DriverManager.getConnection(url, username, password);
            log.info("连接{}数据库成功，地址:{}", dbType, url);
        } catch (ClassNotFoundException e) {
            log.error(String.format("未找到%s数据库驱动", dbType), e);
        } catch (SQLException e) {
            log.error(String.format("创建与%s的连接失败", dbType), e);
        }
        return conn;
    }

    private Double divide(int divisor, int dividend) {
        if (dividend == 0) {
            return null;
        } else {
            return NumberUtil.div(divisor, dividend, 4);
        }
    }

    /**
     * 根据需求，除数为0时评分为满分，所以计算时需要将Null转换成1.0
     *
     * @param num
     * @return
     */
    private Double null2One(Double num) {
        if (null == num) {
            return 1.0;
        }
        return num;
    }

    /**
     * 格式化百分数
     *
     * @param divisor  除数
     * @param dividend 被除数
     * @return ##.##%格式的百分数
     */
    private String formatRatio(int divisor, int dividend) {
        if (dividend == 0) {
            return "--";
        }
        return cn.hutool.core.util.NumberUtil.decimalFormat("#.##%", cn.hutool.core.util.NumberUtil.div(divisor, dividend, 4));
    }

    /**
     * 获取表数量完整性信息
     *
     * @param datasourceName
     * @param structList
     * @param datasetList
     * @return
     */
    private List<TableIntegralityDetail> getTableIntegralityDetails(String datasourceName, List<DatasourceStruct> structList,
                                                                    List<DatasetExtra> datasetList, Long dataSourceId, Long standardId) {
        //初始化数据源中包含的数据集信息
        List<TableIntegralityDetail> tableIntegralityDetails = Lists.newArrayList();
        //数据源中的表集合
        List<String> tableList = Lists.newArrayList();

        //根据数据源配置信息连接数据库并查得所有表
        try {
            for (DatasourceStruct struct : structList) {
                tableList.add(struct.getDatasetCode());
            }
            //遍历对比数据集中的表和数据源中的表
            loop:
            for (DatasetExtra dataSet : datasetList) {
                TableIntegralityDetail detail = new TableIntegralityDetail();
                detail.setTable(dataSet.getMetasetCode());
                detail.setCategoryName(dataSet.getCategoryName());
                detail.setDatasetName(dataSet.getMetasetName());
                detail.setStatus(1);
                detail.setSourceId(dataSourceId);
                detail.setStandardId(standardId);
                detail.setCreateTime(DateUtil.formatDateTime(new Date()));
                tableIntegralityDetails.add(detail);
                for (String tableName : tableList) {
                    if (dataSet.getMetasetCode().equalsIgnoreCase(tableName)) {
                        //将数据源中读取的表名设置为详情的表名，因为比对表是否存在时不区分大小写，如果使用数据集标准中的表名去查数据源中的字段时会存在查询不出的情况
                        detail.setTable(tableName);
                        detail.setStatus(EXIST);
                        continue loop;
                    }
                }
            }

        } catch (Exception e) {
            log.error("查询数据源[" + datasourceName + "]信息异常:", e);
            throw new BizException(String.format("查询数据源[%s]信息异常", datasourceName));
        }
        return tableIntegralityDetails;
    }

    /**
     * 获取数据表字段主键信息
     *
     * @param dataSource           数据源
     * @param structList           数据源结构信息列表
     * @param dataSetList          数据集信息列表
     * @param containedDataSetList 数据源中包含的数据集列表信息
     * @return
     * @throws BizException
     */
    private List<PkIntegralityDetail> getPkIntegralityDetailList(DataSource dataSource, List<DatasourceStruct> structList, List<DatasetExtra> dataSetList, List<TableIntegralityDetail> containedDataSetList) throws BizException {
        try {
            //拼接数据源中的表名和主键字段名
            Map<String, String> pkInDatasourceMap = Maps.newHashMap();
            structList = structList.stream().filter(t -> IS_KEY.equals(t.getIsKey())).collect(Collectors.toList());
            for (TableIntegralityDetail dataSet : containedDataSetList) {
                String joinedPkName = StringUtils.EMPTY;
                for (DatasourceStruct struct : structList) {
                    joinedPkName = String.join(",", joinedPkName, struct.getFieldCode());
                }
                pkInDatasourceMap.put(dataSet.getTable().toUpperCase(), joinedPkName);
            }

            //遍历比较数据源和数据集中同一张表的主键字段名，如果数据源该表包含所有的主键字段，则为符合规则，反之则为缺失主键。
            return comparePk(dataSource, pkInDatasourceMap, dataSetList);

        } catch (Exception e) {
            log.error("查询数据源[" + dataSource.getDataSourceName() + "]信息异常:", e);
            throw new BizException(String.format("查询数据源[%s]信息异常", dataSource.getDataSourceName()));
        }
    }

    /**
     * 获取数据表字段完整性信息
     *
     * @param dataSource            数据源
     * @param structList           数据源结构信息列表
     * @param fieldList            数据集中字段信息列表
     * @param containedDataSetList 数据源中包含的数据集列表信息
     * @return
     * @throws BizException
     */
    private Map<String, Object> getFieldMap(DataSource dataSource, List<DatasourceStruct> structList, List<FieldExtra> fieldList,
                                            List<TableIntegralityDetail> containedDataSetList) throws BizException {
        Map<String, Object> fieldMap = Maps.newHashMap();
        //存放数据源中所包含字段的规范性详情的List
        List<FieldNormativityDetail> containedFieldList = Lists.newArrayList();
        //数据源中的表字段规范性详情的集合
        List<FieldNormativityDetail> fieldDetailsInDatasource = Lists.newArrayList();

        //字段完整性列表
        List<FieldIntegralityDetail> fieldIntegralityDetailList = Lists.newArrayList();

        Long sourceId = dataSource.getId();
        Long standardId = dataSource.getStandardId();

        //遍历数据源中包含的数据集所包含的字段
        try {
            for (TableIntegralityDetail dataSet : containedDataSetList) {
                for (DatasourceStruct struct : structList) {
                    if(dataSet.getTable().equalsIgnoreCase(struct.getDatasetCode())){
                        FieldNormativityDetail fieldDetail = new FieldNormativityDetail();
                        fieldDetail.setFieldCode(struct.getFieldCode());
                        fieldDetail.setLengthOfDatasource(struct.getLength());
                        fieldDetail.setIsNullOfDatasource(struct.getIsNull());
                        fieldDetail.setTypeOfDatasource(struct.getFieldType());
                        fieldDetail.setSourceId(sourceId);
                        fieldDetail.setStandardId(standardId);
                        fieldDetail.setDatasetCode(dataSet.getTable());
                        fieldDetailsInDatasource.add(fieldDetail);
                    }
                }
            }

            loop:
            for (FieldExtra fieldInDataset : fieldList) {
                //构建字段完整性详情列表数据
                FieldIntegralityDetail fieldIntegralityDetail = new FieldIntegralityDetail();
                fieldIntegralityDetail.setCategoryName(fieldInDataset.getCategoryName());
                fieldIntegralityDetail.setDatasetName(fieldInDataset.getMetasetName());
                fieldIntegralityDetail.setDatasetCode(fieldInDataset.getMetasetCode());
                fieldIntegralityDetail.setFieldName(fieldInDataset.getMetadataName());
                fieldIntegralityDetail.setFieldCode(fieldInDataset.getFieldName());
                fieldIntegralityDetail.setStatus(NOT_EXIST);
                fieldIntegralityDetail.setSourceId(sourceId);
                fieldIntegralityDetail.setStandardId(standardId);
                fieldIntegralityDetail.setCreateTime(DateUtil.formatDateTime(new Date()));
                fieldIntegralityDetailList.add(fieldIntegralityDetail);
                for (FieldNormativityDetail fieldNormativityDetail : fieldDetailsInDatasource) {
                    //可能存在不同表名但是字段名相同的情况，所以在比较时要对比表名+字段名
                    if (!containedFieldList.contains(fieldNormativityDetail)
                        && fieldNormativityDetail.getDatasetCode().equalsIgnoreCase(fieldInDataset.getMetasetCode())
                        && fieldNormativityDetail.getFieldCode().equalsIgnoreCase(fieldInDataset.getFieldName())) {
                        fieldNormativityDetail.setCategoryName(fieldInDataset.getCategoryName());
                        fieldNormativityDetail.setDatasetName(fieldInDataset.getMetasetName());
                        fieldNormativityDetail.setFieldName(fieldInDataset.getMetadataName());
                        if(fieldInDataset.getMaxrange() != null){
                            fieldNormativityDetail.setLengthOfStandard(Double.valueOf(fieldInDataset.getMaxrange()).intValue());
                        }
                        fieldNormativityDetail.setStandardId(standardId);
                        fieldNormativityDetail.setSourceId(sourceId);
                        fieldNormativityDetail.setIsNullOfStandard(fieldInDataset.getIsNull());
                        fieldNormativityDetail.setTypeOfStandard(fieldInDataset.getMetadataType());
                        fieldNormativityDetail.setCreateTime(DateUtil.formatDateTime(new Date()));
                        containedFieldList.add(fieldNormativityDetail);
                        //将字段完整性状态设置为存在
                        fieldIntegralityDetail.setStatus(EXIST);
                        continue loop;
                    }
                }
            }

            fieldMap.put(CONTAINED_FIELD_KEY, containedFieldList);
            fieldMap.put(FIELD_INTEGRALITY_DETAIL_KEY, fieldIntegralityDetailList);
        } catch (Exception e) {
            log.error("查询数据源[" + dataSource.getDataSourceName() + "]信息异常:", e);
            throw new BizException(String.format("查询数据源[%s]信息异常", dataSource.getDataSourceName()));
        }
        return fieldMap;
    }

    /**
     * 对比数据源和数据集中的主键字段
     *
     * @param pkInDatasourceMap
     * @param dataSetList
     */
    private List<PkIntegralityDetail> comparePk(DataSource dataSource, Map<String, String> pkInDatasourceMap, List<DatasetExtra> dataSetList) throws BizException {
        List<PkIntegralityDetail> pkIntegralityDetailList = Lists.newArrayList();
        Long standardId = dataSource.getStandardId();
        Long sourceId = dataSource.getId();
        for (DatasetExtra dataSet : dataSetList) {
            PkIntegralityDetail detail = new PkIntegralityDetail();
            detail.setStatus(NOT_EXIST);
            detail.setPkField(dataSet.getPkField());
            detail.setSourceId(sourceId);
            detail.setStandardId(standardId);
            detail.setPkFieldDesc(dataSet.getPkFieldDesc());
            detail.setCategoryName(dataSet.getCategoryName());
            detail.setDatasetName(dataSet.getMetasetName());
            detail.setCreateTime(DateUtil.formatDateTime(new Date()));
            pkIntegralityDetailList.add(detail);
            List<String> pkListInDataSet = Arrays.asList(dataSet.getPkField().split(","));

            String pkFieldInDatasource = pkInDatasourceMap.get(dataSet.getMetasetCode().toUpperCase());
            if (StringUtils.isNotBlank(pkFieldInDatasource)) {
                List<String> pkListInDatasource = Arrays.asList(pkFieldInDatasource.split(","));
                if (CollUtil.containsAll(pkListInDatasource, pkListInDataSet)) {
                    detail.setStatus(EXIST);
                }
            }
        }

        return pkIntegralityDetailList;
    }
}
