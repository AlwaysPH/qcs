package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.DataSourceRequestDTO;
import com.gwi.qcs.model.dto.DataSourceResponseDTO;
import com.gwi.qcs.model.entity.DatasetExtra;
import com.gwi.qcs.model.mapper.mysql.DataSourceMapper;
import com.gwi.sm.entity.StandardVersionWithBLOBs;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: qcs
 * @description: 数据源配置
 * @author: liyoumou
 * @create: 2020-04-10 16:04
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class DataSourceService extends SuperServiceImpl<DataSourceMapper, DataSource> {

    public static final String STANDARD_ID = "standardId";
    public static final String STANDARD_NAME = "standardName";
    public static final String VERSION_CODE = "versionCode";
    public static final String PARENT_DATA_SET_CODE = "0";
    public static final String YI_LIAO = "基本医疗";

    @Resource
    private DatasourceDatasetService datasourceDatasetService;

    @Resource
    private DatasetService datasetService;

    @Autowired
    private DatasetFiledService datasetFiledService;

    @Resource
    private InstanceService instanceService;

    /**
     * 新增数据源
     *
     * @param dataSource
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> addDataSource(DataSource dataSource) {
        Map<String, Object> resultMap = Maps.newHashMap();
        Long standardId = dataSource.getStandardId();
        // 先判断当前新增的标准版本数据集信息是否已经落库
        instanceService.getSmData(standardId, standardId, CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        int insertCount = this.getBaseMapper().insert(dataSource);
        resultMap.put("id", dataSource.getId());
        resultMap.put("insertCount", insertCount);
        return resultMap;
    }

    /**
     * 修改数据源
     *
     * @param dataSource
     * @return
     */
    public Map<String, Object> updateDataSource(DataSource dataSource) {
        Map<String, Object> resultMap = Maps.newHashMap();
        int modifyCount = this.getBaseMapper().updateById(dataSource);
        resultMap.put("id", dataSource.getId());
        resultMap.put("insertCount", modifyCount);
        return resultMap;
    }

    public Boolean queryIsExistDataSource(Long dataSourceId, String dataSourceName, String dataSourceCode) {
        int queryCount = this.getBaseMapper().queryIsExistDataSource(dataSourceId, dataSourceName, dataSourceCode);
        return queryCount > 0;
    }

    /**
     * 删除数据源
     *
     * @param dataSourceId
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> deleteDataSource(Long dataSourceId) {
        Map<String, Object> resultMap = Maps.newHashMap();
        DataSource dataSource = this.getById(dataSourceId);

        Dataset dataset = new Dataset();
        dataset.setStandardId(dataSource.getStandardId());
        dataset.setSourceType(CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        datasetService.remove(new QueryWrapper<>(dataset));

        DatasetField datasetField = new DatasetField();
        datasetField.setStandardId(dataSource.getStandardId());
        datasetField.setSourceType(CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        datasetFiledService.remove(new QueryWrapper<>(datasetField));

        Integer deleteCount = this.getBaseMapper().deleteById(dataSourceId);
        datasourceDatasetService.getBaseMapper().deleteDataSourceDataSet(dataSourceId);
        resultMap.put("deleteCount", deleteCount);
        return resultMap;
    }

    /**
     * 启用停用数据源
     *
     * @param dataSourceId
     * @param status
     * @return
     */
    public Map<String, Object> handlerDataSource(Long dataSourceId, String status) {
        Map<String, Object> resultMap = Maps.newHashMap();
        Integer updateCount = this.getBaseMapper().updateDataSourceStatus(dataSourceId, status);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    /**
     * 根据数据源标识和数据源名称查询是否已经存在数据源
     *
     * @param dataSourceName
     * @returnqueryDataSourceList
     */
    public Boolean queryIsExistDataSource(String dataSourceName, String dataSourceCode) {
        int selectCount = this.getBaseMapper().countDataSourceByName(dataSourceName, dataSourceCode);
        return selectCount > 0;
    }

    /**
     * 获取数据源列表
     *
     * @return
     */
    public List<DataSource> queryDataSourceList() {
        DataSource dataSource = new DataSource();
        QueryWrapper<DataSource> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(dataSource);
        return this.getBaseMapper().selectList(queryWrapper);
    }

    /**
     * 根据数据源ID获取数据源包含的数据集的详细信息
     *
     * @param standardId
     * @param dataSourceId
     * @return
     */
    public Map<String, Object> queryDataSetTree(Long dataSourceId, Long standardId) {
        Map<String, Object> resultMap = Maps.newHashMap();
        List<Dataset> dataSets = datasetService.getByStandardId(standardId, CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        //获取数据源关联的数据集
        List<DataSourceDataset> dataSetList = datasourceDatasetService.getBaseMapper().getDataSet(dataSourceId);
        List<DataSourceResponseDTO> result = getNewTreeData(dataSetList, dataSets);
        resultMap.put("viewData", result);
        DataSource dataSource = this.getById(dataSourceId);
        StandardVersionWithBLOBs standardVersionInfo = new StandardVersionWithBLOBs();
        standardVersionInfo.setStandardCode(dataSource.getStandardCode());
        standardVersionInfo.setVersionCode(dataSource.getVersionCode());
        resultMap.put("standardVersionInfo", standardVersionInfo);
        return resultMap;
    }

    private List<DataSourceResponseDTO> getNewTreeData(List<DataSourceDataset> dataSetList, List<Dataset> datasetList) {
        List<DataSourceResponseDTO> result = new LinkedList<>();
        List<DataSourceResponseDTO> list = new LinkedList<>();
        if (CollectionUtils.isEmpty(datasetList)) {
            return result;
        }
        for (Dataset dataSet : datasetList) {
            DataSourceResponseDTO dataSourceResponseDTO = dataSet.toDataSourceResponseDTO();
            list.add(dataSourceResponseDTO);
        }
        for (DataSourceDataset dataSet : dataSetList) {
            Long dataSetId = dataSet.getDataSetId();
            List<DataSourceResponseDTO> dtoList = list.stream().filter(x -> x.getDataSetId().longValue() == dataSetId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(dtoList)) {
                continue;
            }
            DataSourceResponseDTO dto = dtoList.get(0);
            result.add(dto);
            recursive(dto, list, result);
        }
        return makeTree(result, PARENT_DATA_SET_CODE);
    }

    private void recursive(DataSourceResponseDTO dto, List<DataSourceResponseDTO> list, List<DataSourceResponseDTO> result) {
        DataSourceResponseDTO parentDto = list.stream().filter(y -> (y.getDataSetCode().equals(dto.getParentDataSetCode()) && "1".equals(y.getFlag()))).collect(Collectors.toList()).get(0);
        if (null != parentDto) {
            int size = result.stream().filter(x -> x.getDataSetId().longValue() == parentDto.getDataSetId().longValue()).collect(Collectors.toList()).size();
            if (size == 0) {
                result.add(parentDto);
            }
            //递归
            if (!PARENT_DATA_SET_CODE.equals(parentDto.getParentDataSetCode())) {
                recursive(parentDto, list, result);
            }
        }

    }

    /**
     * 获取数据集列表接口
     *
     * @param dataSourceId
     * @return
     */
    public Map<String, Object> processDataSets(Long dataSourceId) {
        DataSource dataSource = this.getById(dataSourceId);
        List<Dataset> datasetList = datasetService.getByStandardId(dataSource.getStandardId(), CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        Map<String, Object> resultMap = Maps.newHashMap();
        List<DataSourceResponseDTO> dataSourceResponseDTOs = new LinkedList<>();
        for (Dataset dataSet : datasetList) {
            DataSourceResponseDTO dataSourceResponseDTO = dataSet.toDataSourceResponseDTO();
            dataSourceResponseDTOs.add(dataSourceResponseDTO);
        }
        List<DataSourceDataset> checkedData = datasourceDatasetService.getBaseMapper().getCheckedDataSourceDataSets(dataSourceId);
        List<DataSourceResponseDTO> processData = makeTree(dataSourceResponseDTOs, PARENT_DATA_SET_CODE);
        resultMap.put("checkedData", checkedData);
        resultMap.put("viewData", processData);
        return resultMap;
    }

    /**
     * 配置数据源的相应数据集
     *
     * @param requestDTO
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> updateDataSourceDataSet(DataSourceRequestDTO requestDTO) {
        Map<String, Object> resultMap = Maps.newHashMap();
        datasourceDatasetService.getBaseMapper().deleteDataSourceDataSet(requestDTO.getDataSourceId());
        DataSource dataSource = this.getById(requestDTO.getDataSourceId());
        List<Dataset> originDatasetList = datasetService.getByStandardId(dataSource.getStandardId(), CommonConstant.SOURCE_TYPE_DATA_SOURCE);
        List<Long> idList = originDatasetList.stream().map(Dataset::getDataSetId).collect(Collectors.toList());
        List<DataSourceDataset> dataSourceDatasetList = requestDTO.toChooseDataSourceDataSets();
        for(int i = dataSourceDatasetList.size() - 1; i > -1; i--){
            DataSourceDataset dataSourceDataset = dataSourceDatasetList.get(i);
            if(!idList.contains(dataSourceDataset.getDataSetId())){
                dataSourceDatasetList.remove(dataSourceDataset);
            }
        }
        datasourceDatasetService.saveBatch(dataSourceDatasetList);
        resultMap.put("id", requestDTO.getDataSourceId());
        return resultMap;
    }

    /**
     * 处理更新版本
     *
     * @param dataSourceId
     * @param oldStandardId
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> processUpdateVersion(Long oldStandardId, Long dataSourceId) {
        Map<String, Object> data = instanceService.queryUpdataDataSets(oldStandardId);
        Map<String, Object> result = new HashMap<>();
        List<DataSourceResponseDTO> dataSourceResponseDTOs = new LinkedList<>();
        List<Dataset> newVersionDatasetList = new LinkedList<>();
        boolean versionIsLatest = (boolean) data.get("versionIslatest");
        Long latestStandardId = 0L;
        if (data.get(STANDARD_ID) != null) {
            latestStandardId = ((BigDecimal) data.get(STANDARD_ID)).longValue();
        }
        String latestStandardName = (String) data.get(STANDARD_NAME);
        result.put("versionIslatest", versionIsLatest);
        if (versionIsLatest) {
            result.put(STANDARD_ID, null);
            result.put(STANDARD_NAME, null);
            return result;
        } else {
            this.getBaseMapper().updateStandardId(latestStandardId, latestStandardName, dataSourceId, (String) data.get(VERSION_CODE));
        }
        result.put(STANDARD_ID, latestStandardId);
        result.put(STANDARD_NAME, latestStandardName);
        String sourceType = CommonConstant.SOURCE_TYPE_DATA_SOURCE;
        List<Dataset> originDatasetList = datasetService.getByStandardId(oldStandardId, sourceType);
        datasetService.remove(oldStandardId, sourceType);
        instanceService.getSmData(latestStandardId, latestStandardId, sourceType);
        List<Dataset> datasetList = datasetService.getByStandardId(latestStandardId, sourceType);
        for (Dataset dataset : datasetList) {
            newVersionDatasetList.add(dataset);
            DataSourceResponseDTO dataSourceResponseDTO = dataset.toDataSourceResponseDTO();
            dataSourceResponseDTOs.add(dataSourceResponseDTO);
        }
        List<Dataset> insertDatasetList = newVersionDatasetList.stream().filter(item -> !originDatasetList.stream().map(Dataset::getMetasetCode).collect(Collectors.toList()).contains(item.getMetasetCode())).collect(Collectors.toList());
        List<String> dataSetIds = insertDatasetList.stream().map(d -> d.getDataSetId().toString()).collect(Collectors.toList());
        for (DataSourceResponseDTO dataSourceResponseDTO : dataSourceResponseDTOs) {
            dataSourceResponseDTO.setIsdiff(InstanceService.isIn(dataSourceResponseDTO.getDataSetId().toString(), dataSetIds));
        }
        List<DataSourceResponseDTO> processData = makeTree(dataSourceResponseDTOs, PARENT_DATA_SET_CODE);
        List<DataSourceDataset> checkedData = datasourceDatasetService.getBaseMapper().getCheckedDataSourceDataSets(dataSourceId);
        result.put("checkedData", checkedData);
        result.put("processData", processData);
        return result;
    }

    public List<DatasetExtra> queryDataSetListByDatasource(Long dataSourceId, Long standardId) {
        return datasetService.getBaseMapper().queryDataSetListByDatasource(dataSourceId, standardId);
    }

    private static List<DataSourceResponseDTO> makeTree(List<DataSourceResponseDTO> departmentList, String parentDataSetCode) {
        //子类
        List<DataSourceResponseDTO> children = departmentList.stream().filter(x -> x.getParentDataSetCode().equals(parentDataSetCode)).collect(Collectors.toList());
        //后辈中的非子类
        List<DataSourceResponseDTO> successor = departmentList.stream().filter(x -> !x.getParentDataSetCode().equals(parentDataSetCode)).collect(Collectors.toList());
        children.forEach(x ->
            makeTree(successor, x.getDataSetCode()).forEach(
                y -> {
                    List<DataSourceResponseDTO> list = x.getChildren() == null ? new ArrayList<>() : x.getChildren();
                    list.add(y);
                    x.setChildren(list);
                }
            )
        );
        return children;
    }

    /**
     * 判断是否为基本医疗
     * @param sourceId
     * @return
     */
    public boolean checkIsYiLiaoBySourceId(String sourceId){
        QueryWrapper<DataSource> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("code", sourceId);
        DataSource dataSource = this.getBaseMapper().selectOne(queryWrapper);
        boolean isYiLiao = false;
        if(dataSource != null && YI_LIAO.equals(dataSource.getDataSourceName())){
            isYiLiao = true;
        }
        return isYiLiao;
    }

    /**
     * 获取质控实例的standardId
     * @param dataSource
     * @return
     */
    public Long getInstanceStandardId(DataSource dataSource){
        Long standardId = dataSource.getStandardId();
        Instance query = new Instance();
        query.setStandardCode(dataSource.getStandardCode());
        Instance instance = instanceService.getOne(new QueryWrapper<>(query));
        if(instance != null){
            standardId = instance.getStandardId();
        }
        return standardId;
    }
}
