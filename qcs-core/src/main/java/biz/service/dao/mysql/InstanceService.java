package biz.service.dao.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.greatwall.component.ccyl.redis.template.RedisRepository;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.*;
import com.gwi.qcs.model.entity.QcsInstanceRulePojo;
import com.gwi.qcs.model.mapper.mysql.InstanceMapper;
import com.gwi.sm.entity.StandardVersionWithBLOBs;
import com.gwi.sm.facade.SmFacadeApi;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.gwi.qcs.common.constant.CommonConstant.*;
import static com.gwi.qcs.service.api.ServiceVersion.STANDARD_MANAGE_VERSION;

@Service
@DS(DbTypeConstant.MYSQL)
public class InstanceService extends SuperServiceImpl<InstanceMapper, Instance> {
    /**
     * 顶层编码
     */
    public static final String TOP_CODE = "0";
    public static final String GET_DATASET = "0";

    @Autowired
    private RedisRepository redisRepository;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private DatasetFiledService datasetFiledService;

    @Autowired
    private InstanceDatasetService instanceDatasetService;

    @Autowired
    private DsmRefService dsmRefService;

    @Autowired
    private ScmDetailService scmDetailService;

    @Autowired
    private InstanceRuleService instanceRuleService;

    @Autowired
    private RuleDetailValService ruleDetailValService;

    @Autowired
    private DatasetInstanceRuleService datasetInstanceRuleService;

    @Reference(version = STANDARD_MANAGE_VERSION)
    private SmFacadeApi standardVersionService;

    public boolean qcsInstanceIsExists(String qcsInstanceName) {
        return baseMapper.getCount(qcsInstanceName) > 0;
    }

    public Instance getByStandardId(Long standardId){
        Instance instance = new Instance();
        instance.setStandardId(standardId);
        QueryWrapper<Instance> queryWrapper = new QueryWrapper<>(instance);
        return this.getOne(queryWrapper);
    }

    public Instance get(String standardCode, String versionCode){
        Instance instance = new Instance();
        instance.setStandardCode(standardCode);
        instance.setVersionCode(versionCode);
        QueryWrapper<Instance> queryWrapper = new QueryWrapper<>(instance);
        return this.getOne(queryWrapper);
    }

    @Transactional
    public Map<String, Object> addQcsInstance(QcsInstanceRequestDTO requestDTO){
        Integer count = 0;
        Map<String, Object> resultMap = new HashMap<>();
        Long standardId = requestDTO.getStandardId();

        //判断当前版本是否已有质控实例
        Instance instance = new Instance();
        instance.setStandardCode(requestDTO.getStandardCode());
        instance.setStandardName(requestDTO.getStandardName());
        Wrapper<Instance> queryWrapper = new QueryWrapper<>(instance);
        int versionCount = this.baseMapper.selectCount(queryWrapper);
        if (versionCount > 0) {
            resultMap.put("message", "当前版本已存在质控实例！");
            return resultMap;
        }

        // 先判断当前新增的标准版本数据集信息是否已经落库
        getSmData(standardId, standardId, CommonConstant.SOURCE_TYPE_INSTANCE);
        Instance qcsInstance = requestDTO.toQcsInstance();
        qcsInstance.setSmStandardId(qcsInstance.getStandardId());
        count = baseMapper.insert(qcsInstance);
        Long qcsInstanceId = qcsInstance.getId();
        List<InstanceDataSet> instanceDataSets = requestDTO.toQcsInstanceDataSet(qcsInstanceId);
        instanceDatasetService.saveBatch(instanceDataSets);
        instanceRuleService.generateRule(qcsInstanceId);
        resultMap.put("id", qcsInstanceId);
        resultMap.put("insertCount", count);
        return resultMap;
    }

    /**
     * 获取标准版本数据
     * @param smStandardId 数据源使用
     * @param standardId 质控实例使用
     * @param sourceType  通过数据源获取，还是通过实例获取
     */
    public void getSmData(Long smStandardId, Long standardId, String sourceType) {
        Long saveStandardId = standardId;
        if(CommonConstant.SOURCE_TYPE_DATA_SOURCE.equals(sourceType)){
            saveStandardId = smStandardId;
        }
        boolean isExistsInMysql = datasetIsExistsInMySql(saveStandardId, sourceType);
        if (!isExistsInMysql) {
            Set<String> keys = redisRepository.keys("cache:" + CommonConstant.REDIS_CACHE_STANDARD + ":");
            for(String key : keys){
                redisRepository.del(key);
            }
            // 根据standardId从标准管理获取标准版本信息
            StandardVersionWithBLOBs standardVersionInfo = querySmStandardVersionInfo(smStandardId);

            // 数据集
            List<Map<String, Object>> dataSets = querySmDataSetList(standardVersionInfo.getStandardCode(), standardVersionInfo.getVersionCode());
            List<Dataset> datasetList = new LinkedList<>();
            for (Map<String, Object> dataSetMap : dataSets) {
                Dataset dataSet = JSON.parseObject(JSON.toJSONString(dataSetMap), Dataset.class);
                dataSet.setStandardId(saveStandardId);
                dataSet.setSourceType(sourceType);
                datasetList.add(dataSet);
            }
            if (CollectionUtils.isNotEmpty(datasetList)) {
                datasetService.saveBatch(datasetList);
            }

            // 数据集的字段
            Map<String, Long> fieldIdMap = datasetFiledService.list(new LambdaQueryWrapper<DatasetField>()
                .eq(DatasetField::getSourceType, sourceType)
                .eq(DatasetField::getStandardId, saveStandardId)
                .select(DatasetField::getDatasetId, DatasetField::getMetadataCode, DatasetField::getId)).stream()
                .collect(Collectors.toMap(datasetField -> datasetField.getDatasetId() + datasetField.getMetadataCode(),
                    DatasetField::getId));
            List<Map<String, Object>> fields = queryFieldList(standardVersionInfo.getStandardCode(), standardVersionInfo.getVersionCode(), null);
            List<DatasetField> fieldList = new LinkedList<>();
            for (Map<String, Object> fieldMap : fields) {
                DatasetField field = JSON.parseObject(JSON.toJSONString(fieldMap), DatasetField.class);
                field.setFieldName(field.getFieldName());
                field.setStandardId(saveStandardId);
                field.setMetadataType((String)fieldMap.get("javaType"));
                field.setId(fieldIdMap.getOrDefault(field.getDatasetId() + field.getMetadataCode(), null));
                field.setSourceType(sourceType);
                fieldList.add(field);
            }
            if (CollectionUtils.isNotEmpty(fieldList)) {
                datasetFiledService.saveOrUpdateBatch(fieldList);
            }

            // 质控实例获取标准版本还需获取值域与关联性
            if(CommonConstant.SOURCE_TYPE_INSTANCE.equals(sourceType)){
                // 数据值域
                List<Map<String, Object>> rangeList = queryValueRangeList(standardVersionInfo.getStandardCode(), standardVersionInfo.getVersionCode());
                List<ScmDetail> scmDetailList = new LinkedList<>();
                for (Map<String, Object> rangeMap : rangeList) {
                    ScmDetail scmDetail = JSON.parseObject(JSON.toJSONString(rangeMap), ScmDetail.class);
                    scmDetail.setStandardId(saveStandardId);
                    scmDetailList.add(scmDetail);
                }
                if(CollectionUtils.isNotEmpty(scmDetailList)){
                    scmDetailService.saveBatch(scmDetailList);
                }

                // 数据关联
                List<Map<String, Object>> refMapList = queryDsmRefList(standardVersionInfo.getStandardCode(), standardVersionInfo.getVersionCode());
                List<DsmRef> refList = new LinkedList<>();
                for (Map<String, Object> rangeMap : refMapList) {
                    DsmRef dsmRef = JSON.parseObject(JSON.toJSONString(rangeMap), DsmRef.class);
                    dsmRef.setStandardId(saveStandardId);
                    refList.add(dsmRef);
                }
                if(CollectionUtils.isNotEmpty(refList)){
                    dsmRefService.saveBatch(refList);
                }
            }
        }
    }

    @Transactional
    public Map<String, Object> updateQcsInstance(QcsInstanceRequestDTO requestDTO) {
        Map<String, Object> resultMap = new HashMap<>();
        //查询当前质控实例是否存在正在执行的质控任务
        int count = baseMapper.getInstanceRunningTask(requestDTO.getQcsInstanceId());
        if (count > 0) {
            resultMap.put("message", "该实例存在正在执行的质控任务，不允许修改！");
            return resultMap;
        }
        Instance instance = this.getById(requestDTO.getQcsInstanceId());
        List<InstanceDataSet> instanceDataSets = requestDTO.getChooseQcsInstanceDataSets();
        List<InstanceDataSet> instanceDataSetList = new ArrayList<>();
        List<Dataset> datasetList = datasetService.getByStandardId(instance.getStandardId(), CommonConstant.SOURCE_TYPE_INSTANCE);
        List<Long> idList = datasetList.stream().map(Dataset::getDataSetId).collect(Collectors.toList());
        for(int i = instanceDataSets.size() - 1; i > -1; i--){
            InstanceDataSet instanceDataSet = instanceDataSets.get(i);
            if( ! idList.contains(instanceDataSet.getDataSetId())){
                instanceDataSets.remove(instanceDataSet);
            }
        }
        for (InstanceDataSet instanceDataSet : instanceDataSets) {
            if (instanceDatasetService.getBaseMapper().getDataSetCount(instanceDataSet) <= 0) {
                instanceDataSetList.add(instanceDataSet);
            }
        }
        List<Long> dataSetInQcsInstance = requestDTO.getDeleteQcsInstanceDataSets();
        if (CollectionUtils.isNotEmpty(instanceDataSetList) || CollectionUtils.isNotEmpty(dataSetInQcsInstance)) {
            if (CollectionUtils.isNotEmpty(instanceDataSetList)) {
                instanceDatasetService.saveBatch(instanceDataSetList);
            }
            if (CollectionUtils.isNotEmpty(dataSetInQcsInstance)) {
                dataSetInQcsInstance.forEach(ins -> {
                    Wrapper<InstanceDataSet> wapper = new UpdateWrapper<>();
                    ((UpdateWrapper<InstanceDataSet>) wapper).eq("INSTANCE_ID", requestDTO.getQcsInstanceId());
                    ((UpdateWrapper<InstanceDataSet>) wapper).eq("DATASET_ID", ins);
                    instanceDatasetService.getBaseMapper().delete(wapper);
                });
            }
            instanceRuleService.generateRule(instance.getId());
        }
        instance.setName(requestDTO.getQcsInstanceName());
        instance.setRemark(requestDTO.getRemark());
        instance.setStatus(requestDTO.getStatus());
        //判断需要更新的数据集及数据集下字段是否为空
        if (!StringUtils.EMPTY.equals(requestDTO.getInsertDataSet())) {
            JSONObject jsonObject = JSONObject.parseObject(requestDTO.getInsertDataSet());
            instance.setSmStandardId(jsonObject.getLong("standardId"));
            instance.setStandardName(jsonObject.getString("standardName"));
            instance.setVersionCode(jsonObject.getString("lastVersionCode"));
        }
        Integer modifyCount = baseMapper.updateById(instance);
        resultMap.put("id", requestDTO.getQcsInstanceId());
        resultMap.put("modifyCount", modifyCount);
        return resultMap;
    }

    /**
     * 从标准管理处获取更新信息
     *
     * @param standardId
     * @return
     */
    public Map<String, Object> queryUpdataDataSets(Long standardId) {
        Map<String, Object> resultMap = standardVersionService.queryStandardLastVersion(new BigDecimal(standardId));
        Map<String, Object> result = new HashMap<>();
        if (SUCCESS.equals((String) resultMap.get(RESULT_CODE))) {
            result = (Map<String, Object>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    /**
     * 处理版本更新信息
     *
     * @param qcsInstanceId
     * @return
     */
    @Transactional
    public Map<String, Object> processUpdateVersion(Long qcsInstanceId) {
        Instance instance = this.getById(qcsInstanceId);
        Long oldStandardId = instance.getSmStandardId();
        Map<String, Object> data = this.queryUpdataDataSets(oldStandardId);
        Map<String, Object> result = new HashMap<>();
        Boolean versionIslatest = (Boolean) data.get("versionIslatest");
        result.put("versionIslatest", versionIslatest);
        if (null != versionIslatest && versionIslatest) {
            result.put("standardId", null);
            result.put("standardName", null);
            return result;
        }
        Long latestStandardId = ((BigDecimal) data.get("standardId")).longValue();
        String latestStandardName = (String) data.get("standardName");

        String sourceType = CommonConstant.SOURCE_TYPE_INSTANCE;
        Long standardId = instance.getStandardId();
        //获取原数据集
        List<Dataset> originDatasetList = datasetService.getByStandardId(standardId, sourceType);
        //获取原数据集原始字段
        List<DatasetField> originFieldList = datasetFiledService.getByStandardId(standardId, sourceType);
        datasetService.remove(standardId, sourceType);
        this.getSmData(latestStandardId, standardId, sourceType);

        List<Dataset> newVersionDatasetList = datasetService.getByStandardId(standardId, sourceType);
        setNewVersionDatasetField(originDatasetList, newVersionDatasetList);
        List<DatasetField> fieldList = datasetFiledService.getByStandardId(standardId, sourceType);
        //获取需要新增数据集字段
        List<DatasetField> insertFieldList = fieldList.stream().filter(item -> !originFieldList.stream()
            .map(e -> e.getFieldName() + e.getMetasetCode())
            .collect(Collectors.toList())
            .contains(item.getFieldName() + item.getMetasetCode()))
            .collect(Collectors.toList());
        List<Dataset> insertDatasetList = newVersionDatasetList.stream().filter(item -> !originDatasetList.stream()
            .map(Dataset::getDataSetId).collect(Collectors.toList())
            .contains(item.getDataSetId())).collect(Collectors.toList());

        List<Dataset> resultList = new ArrayList<>();
        List<QcsInstanceResponseDTO> insertList = new ArrayList<>();
        //若存在新增数据集的字段，需返回新增字段的数据集
        for (DatasetField field : insertFieldList) {
            for (Dataset dataSet : newVersionDatasetList) {
                if (field.getDatasetId().longValue() == dataSet.getDataSetId().longValue()) {
                    //判断新增数据集是否存在
                    if (!insertDatasetList.contains(dataSet)) {
                        dataSet.setIsInsertFlag("1");
                        QcsInstanceResponseDTO qcsInstanceResponseDTO = dataSet.toQcsInstanceResponseDTO();
                        insertList.add(qcsInstanceResponseDTO);
                        resultList.add(dataSet);
                    }
                }
            }
        }
        for (Dataset set : insertDatasetList) {
            QcsInstanceResponseDTO qcsInstanceResponseDTO = set.toQcsInstanceResponseDTO();
            insertList.add(qcsInstanceResponseDTO);
            resultList.add(set);
        }

        List<Long> delDatasetIdList = originDatasetList.stream().filter(item -> !newVersionDatasetList.stream()
            .map(Dataset::getDataSetId).collect(Collectors.toList())
            .contains(item.getDataSetId())).map(Dataset::getDataSetId).collect(Collectors.toList());
        for(Long id : delDatasetIdList){
            InstanceDataSet query = new InstanceDataSet();
            query.setDataSetId(id);
            query.setInstanceId(instance.getId());
            QueryWrapper<InstanceDataSet> queryWrapper = new QueryWrapper<>(query);
            List<Long> instanceDataSetIdList = instanceDatasetService.list(queryWrapper).stream()
                .map(InstanceDataSet::getId).collect(Collectors.toList());
            for(Long instanceDataSetId : instanceDataSetIdList){
                QueryWrapper<DatasetInstanceRule> datasetInstanceRuleQueryWrapper = new QueryWrapper<>();
                datasetInstanceRuleQueryWrapper.eq("INSTANCE_DATASET_ID", instanceDataSetId);
                List<DatasetInstanceRule> datasetInstanceRuleList = datasetInstanceRuleService.list(datasetInstanceRuleQueryWrapper);
                List<String> idList = datasetInstanceRuleList.stream().map(DatasetInstanceRule::getInstanceRuleId).collect(Collectors.toList());
                for(String itemId : idList){
                    RuleDetailVal ruleDetailVal = new RuleDetailVal();
                    ruleDetailVal.setInstanceRuleId(itemId);
                    QueryWrapper<RuleDetailVal> ruleDetailValQueryWrapper = new QueryWrapper<>(ruleDetailVal);
                    ruleDetailValService.remove(ruleDetailValQueryWrapper);
                }
                instanceRuleService.removeByIds(idList);
            }
            instanceDatasetService.removeByIds(instanceDataSetIdList);
        }

        List<QcsInstanceRulePojo> checkedData = instanceDatasetService.getBaseMapper().getCheckedRuleData(qcsInstanceId);
        result.put("checkedData", checkedData);
        result.put("delDatasetIdList", delDatasetIdList);
        result.put("standardId", latestStandardId);
        result.put("standardName", latestStandardName);
        result.put("lastStandardCode", data.get("standardCode"));
        result.put("lastVersionCode", data.get("versionCode"));
        result.put("insertDataSetList", insertList);
        result.put("insertList", resultList);
        result.put("insertFieldList", insertFieldList);
        return result;
    }

    private void setNewVersionDatasetField(List<Dataset> originDatasetList, List<Dataset> newVersionDatasetList) {
        String resourceField = null;
        String orgCodeField = null;
        String uploadTimeField = null;
        String etranField = null;
        for(Dataset dataset : originDatasetList){
            if(StringUtils.isEmpty(resourceField)){
                resourceField = dataset.getResourceField();
            }
            if(StringUtils.isEmpty(orgCodeField)){
                orgCodeField = dataset.getOrgCodeField();
            }
            if(StringUtils.isEmpty(uploadTimeField)){
                uploadTimeField = dataset.getUploadTimeField();
            }
            if(StringUtils.isEmpty(etranField)){
                etranField = dataset.getEtranField();
            }
            if(StringUtils.isNotEmpty(resourceField) && StringUtils.isNotEmpty(orgCodeField)
                && StringUtils.isNotEmpty(uploadTimeField) && StringUtils.isNotEmpty(etranField)){
                break;
            }
        }
        for(Dataset dataset : newVersionDatasetList){
            dataset.setResourceField(resourceField);
            dataset.setOrgCodeField(orgCodeField);
            dataset.setUploadTimeField(uploadTimeField);
            dataset.setEtranField(etranField);
        }
        datasetService.saveOrUpdateBatch(newVersionDatasetList);
    }

    public List<QcsInstanceListResponseDTO> queryQcsInstanceList(String status) {
        List<QcsInstanceListResponseDTO> qcsInstanceList = baseMapper.queryQcsInstanceList(status);
        return qcsInstanceList;
    }

    public Map<String, Object> queryQcsInstanceDetailsForUpdate(Long qcsInstanceId) {
        Map<String, Object> resultMap = new HashMap<>();
        //获取当前质控实例信息
        Instance instance = baseMapper.selectById(qcsInstanceId);
        //调标准管理数据集列表接口
        List<Dataset> mapList = queryDataSetList(instance.getStandardCode(), instance.getVersionCode());
        List<QcsInstanceResponseDTO> processQcsInstanceResponseDTOs = processDataSets(mapList);
        List<QcsInstanceRulePojo> checkedData = instanceDatasetService.getBaseMapper().getCheckedRuleData(qcsInstanceId);
        resultMap.put("checkedData", checkedData);
        resultMap.put("viewData", processQcsInstanceResponseDTOs);
        return resultMap;
    }

    public List<QcsInstanceResponseDTO> queryQcsInstanceDetailsForRule(Long qcsInstanceId) {
        Map<String, Object> resultMap = new HashMap<>();
        //获取当前质控实例信息
        Instance instance = baseMapper.selectById(qcsInstanceId);
        //获取质控实例关联的数据集
        List<InstanceDataSet> dataSetList = baseMapper.getDataSet(qcsInstanceId);
        //调标准管理数据集列表接口
        List<Dataset> datasetList = queryDataSetList(instance.getStandardCode(), instance.getVersionCode());
        //在标准管理数据集列表中拿取关联的数据集，重新拼接树形结构
        List<QcsInstanceResponseDTO> result = getNewTreeData(dataSetList, datasetList, qcsInstanceId, instance.getStandardId());
        return result;
    }

    private List<QcsInstanceResponseDTO> getNewTreeData(List<InstanceDataSet> dataSetList, List<Dataset> datasetList, Long qcsInstanceId, Long standardId) {
        List<QcsInstanceResponseDTO> result = new LinkedList<>();
        List<QcsInstanceResponseDTO> list = new LinkedList<>();
        if (null == datasetList || datasetList.size() <= 0) {
            return result;
        }
        for (Dataset dataSet : datasetList) {
            QcsInstanceResponseDTO qcsInstanceResponseDTO = dataSet.toQcsInstanceResponseDTO();
            list.add(qcsInstanceResponseDTO);
        }
        for (InstanceDataSet dataSet : dataSetList) {
            Long dataSetId = dataSet.getDataSetId();
            //通过数据集ID获取数据集信息
            Dataset set = datasetService.getBaseMapper().getDataset(dataSetId, standardId, CommonConstant.SOURCE_TYPE_INSTANCE);
            //通过数据集ID和质控实例ID获取关联的规则数
            int count = baseMapper.getRuleCount(dataSet.getId(), qcsInstanceId);
            //通过实例ID和数据集ID获取实例数据集表主键ID
            InstanceDataSet instanceDataSet = instanceDatasetService.getBaseMapper().getData(qcsInstanceId, dataSetId);
            List<QcsInstanceResponseDTO> dtoList = list.stream().filter(x -> x.getDataSetId().longValue() == dataSetId).collect(Collectors.toList());
            if (null == dtoList || dtoList.size() <= 0) {
                continue;
            }
            QcsInstanceResponseDTO dto = dtoList.get(0);
            List<QcsInstanceResponseDTO> parentDtoList = list.stream().filter(y -> (y.getDataSetCode().equals(dto.getParentDataSetCode()) && "1".equals(y.getFlag()))).collect(Collectors.toList());
            dto.setDid(instanceDataSet.getId());
            if (CollectionUtils.isNotEmpty(parentDtoList)) {
                dto.setPDataSetName(parentDtoList.get(0).getDataSetName());
            }
            dto.setRuleCount(count);
            if (null != set) {
                dto.setResourceField(set.getResourceField());
                dto.setOrgCodeField(set.getOrgCodeField());
                dto.setUploadTimeField(set.getUploadTimeField());
                dto.setEtranField(set.getEtranField());
                dto.setRemark(set.getRemark());
            }
            result.add(dto);
            recursive(dto, list, result);
        }
        return makeTree(result, "0");
    }

    private void recursive(QcsInstanceResponseDTO dto, List<QcsInstanceResponseDTO> list, List<QcsInstanceResponseDTO> result) {
        QcsInstanceResponseDTO parentDto = list.stream().filter(y -> (y.getDataSetCode().equals(dto.getParentDataSetCode()) && "1".equals(y.getFlag()))).collect(Collectors.toList()).get(0);
        if (null != parentDto) {
            int size = result.stream().filter(x -> x.getDataSetId().longValue() == parentDto.getDataSetId().longValue()).collect(Collectors.toList()).size();
            if (size == 0) {
                result.add(parentDto);
            }
            //递归
            if (!TOP_CODE.equals(parentDto.getParentDataSetCode())) {
                recursive(parentDto, list, result);
            }
        }
    }

    @Transactional
    public Map<String, Object> deleteQcsInstance(Long qcsInstanceId, Long standardId) {
        Map<String, Object> resultMap = new HashMap<>();
        //查询当前质控实例是否存在对应的质控任务
        int count = baseMapper.getInstanceTask(qcsInstanceId);
        if (count > 0) {
            resultMap.put("message", "该实例存在质控任务，不允许删除！");
            return resultMap;
        }
        //查询当前质控实例中的数据集是否配置规则实例
        int ruleCount = baseMapper.getDataSetRuleCount(qcsInstanceId);
        if (ruleCount > 0) {
            resultMap.put("message", "该实例已配置规则实例，请先清除规则实例后再进行删除！");
            return resultMap;
        }
        Instance instance = this.getById(qcsInstanceId);
        Dataset dataset = new Dataset();
        dataset.setStandardId(instance.getStandardId());
        datasetService.remove(new QueryWrapper<>(dataset));

        DatasetField datasetField = new DatasetField();
        datasetField.setStandardId(instance.getStandardId());
        datasetFiledService.remove(new QueryWrapper<>(datasetField));

        DsmRef dsmRef = new DsmRef();
        dsmRef.setStandardId(instance.getStandardId());
        dsmRefService.remove(new QueryWrapper<>(dsmRef));

        ScmDetail scmDetail = new ScmDetail();
        scmDetail.setStandardId(instance.getStandardId());
        scmDetailService.remove(new QueryWrapper<>(scmDetail));

        Integer deleteCount = baseMapper.deleteById(qcsInstanceId);
        instanceDatasetService.getBaseMapper().deleteQcsInstanceDataSet(qcsInstanceId);
        resultMap.put("deleteCount", deleteCount);
        return resultMap;
    }

    public Map<String, Object> handleQcsInstance(String status, Long qcsInstanceId) {
        Map<String, Object> resultMap = new HashMap<>();
        /**停用当前实例**/
        if (TASK_RUN_STATUS.equals(status)) {
            //查询当前质控实例是否存在正在执行的质控任务
            int count = baseMapper.getInstanceRunningTask(qcsInstanceId);
            if (count > 0) {
                resultMap.put("message", "该实例存在正在执行的质控任务，不允许停用！");
                return resultMap;
            }
        }
        int updateCount = baseMapper.updateStatus(qcsInstanceId, status);
        baseMapper.updateQcsTaskStatus(qcsInstanceId);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    /**
     * 从标准管理获取标准版本列表信息
     *
     * @return
     */
    public List<StandardVersion> querySmStandardVersionList() {
        Map<String, Object> resultMap = standardVersionService.queryStandardVersionList();
        List<Map<String, Object>> result = new ArrayList<>();
        List<StandardVersion> standardVersions = new ArrayList<>();
        if (SUCCESS.equals((String) resultMap.get(RESULT_CODE))) {
            result = (List<Map<String, Object>>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        for (Map<String, Object> standardVersionMap : result) {
            StandardVersion standardVersion = JSON.parseObject(JSON.toJSONString(standardVersionMap), StandardVersion.class);
            standardVersions.add(standardVersion);
        }
        return standardVersions;
    }

    /**
     * 从本地获取标准版本列表信息
     *
     * @return
     */
    public List<StandardVersion> queryStandardVersionList() {
        QueryWrapper<Instance> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("status", CommonConstant.ENABLE_0);
        List<Instance> instanceList = this.list(queryWrapper);
        List<StandardVersion> standardVersionList = new ArrayList<>();
        for(Instance instance : instanceList){
            StandardVersion standardVersion = new StandardVersion();
            standardVersion.setId(instance.getStandardId());
            standardVersion.setVersionCode(instance.getVersionCode());
            standardVersion.setStandardCode(instance.getStandardCode());
            standardVersion.setStandardName(instance.getStandardName());
            standardVersion.setStatus(CommonConstant.ENABLE_0);
            standardVersionList.add(standardVersion);
        }
        return standardVersionList;
    }

    public StandardVersionWithBLOBs querySmStandardVersionInfo(Long standardId) {
        Map<String, Object> resultMap = standardVersionService.queryStandardVersionInfo(new BigDecimal(standardId));
        StandardVersionWithBLOBs result = new StandardVersionWithBLOBs();
        if (SUCCESS.equals((String) resultMap.get(RESULT_CODE))) {
            result = (StandardVersionWithBLOBs) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    /**
     * 根据standardCode和versionCode从标准管理获取数据集信息
     *
     * @param standardCode
     * @param versionCode
     * @return
     */
    public List<Map<String, Object>> querySmDataSetList(String standardCode, String versionCode) {
        Map<String, Object> resultMap = standardVersionService.queryDataSetList(standardCode, versionCode);
        List<Map<String, Object>> result = new ArrayList<>();
        if (SUCCESS.equals((String) resultMap.get(RESULT_CODE))) {
            result = (List<Map<String, Object>>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    public List<Dataset> queryDataSetList(String standardCode, String versionCode) {
        Instance instance = this.get(standardCode, versionCode);
        return datasetService.getByStandardId(instance.getStandardId(), CommonConstant.SOURCE_TYPE_INSTANCE);
    }

    /**
     * 根据standardCode,versionCode,metasetCode从标准管理获取字段信息
     *
     * @param standardCode
     * @param versionCode
     * @param metasetCode
     * @return
     */
    public List<Map<String, Object>> queryFieldList(String standardCode, String versionCode, String metasetCode) {
        Map<String, Object> resultMap = standardVersionService.queryFieldList(standardCode, versionCode, metasetCode);
        List<Map<String, Object>> result = new ArrayList<>();
        if (SUCCESS.equals((String) resultMap.get(RESULT_CODE))) {
            result = (List<Map<String, Object>>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    /**
     * 根据standardCode和versionCode从标准管理获取值域信息
     *
     * @param standardCode
     * @param versionCode
     * @return
     */
    public List<Map<String, Object>> queryValueRangeList(String standardCode, String versionCode) {
        Map<String, Object> resultMap = standardVersionService.queryValueRangeList(standardCode, versionCode);
        List<Map<String, Object>> result;
        if (SUCCESS.equals(resultMap.get(RESULT_CODE))) {
            result = (List<Map<String, Object>>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    /**
     * 根据standardCode和versionCode从标准管理获取值域信息
     *
     * @param standardCode
     * @param versionCode
     * @return
     */
    public List<Map<String, Object>> queryDsmRefList(String standardCode, String versionCode) {
        Map<String, Object> resultMap = standardVersionService.queryDsmRefList(standardCode, versionCode);
        List<Map<String, Object>> result;
        if (SUCCESS.equals(resultMap.get(RESULT_CODE))) {
            result = (List<Map<String, Object>>) resultMap.get("data");
        } else {
            throw new BizException("调用标准管理接口异常");
        }
        return result;
    }

    public List<QcsInstanceResponseDTO> processDataSets(List<Dataset> dataSets) {
        List<QcsInstanceResponseDTO> qcsInstanceResponseDTOs = new LinkedList<>();
        for (Dataset dataSet : dataSets) {
            QcsInstanceResponseDTO qcsInstanceResponseDTO = dataSet.toQcsInstanceResponseDTO();
            qcsInstanceResponseDTOs.add(qcsInstanceResponseDTO);
        }
        return makeTree(qcsInstanceResponseDTOs, "0");
    }

    public List<QcsInstanceResponseDTO> processSmDataSets(List<Map<String, Object>> dataSets) {
        List<QcsInstanceResponseDTO> qcsInstanceResponseDTOs = new LinkedList<>();
        for (Map<String, Object> dataSetMap : dataSets) {
            Dataset dataSet = JSON.parseObject(JSON.toJSONString(dataSetMap), Dataset.class);
            QcsInstanceResponseDTO qcsInstanceResponseDTO = dataSet.toQcsInstanceResponseDTO();
            qcsInstanceResponseDTOs.add(qcsInstanceResponseDTO);
        }
        return makeTree(qcsInstanceResponseDTOs, "0");
    }

    public Map<String, Object> modifyQcsInstance(Dataset dataSet) {
        Map<String, Object> resultMap = new HashMap<>();
        Long qcsInstanceId = instanceDatasetService.getBaseMapper().getQcsInstanceId(dataSet.getDataSetId());
        int count = baseMapper.getInstanceRunningTask(qcsInstanceId);
        if (count > 0) {
            resultMap.put("updateCount", 0);
            resultMap.put("message", "该实例存在正在执行的质控任务，不允许修改！");
            return resultMap;
        }
        datasetService.getBaseMapper().updateDataSet(dataSet);
        resultMap.put("updateCount", 1);
        return resultMap;
    }

    public boolean datasetIsExistsInMySql(Long standardId, String sourceType) {
        return datasetService.getBaseMapper().getCount(standardId, sourceType) > 0;
    }

    @Transactional
    public Map<String, Object> addInstanceRule(InstanceRuleRequestDTO instanceRuleDTO) {
        Map<String, Object> resultMap = new HashMap<>();
        int count = baseMapper.getInstanceRunningTask(instanceRuleDTO.getQcsInstanceId());
        if (count > 0) {
            resultMap.put("updateCount", 0);
            resultMap.put("message", "该实例存在正在执行的质控任务，不允许修改！");
            return resultMap;
        }
        InstanceRule instanceRule = instanceRuleDTO.getInstanceRule();
        InstanceRule instanceRuleQuery = new InstanceRule();
        instanceRuleQuery.setRuleId(instanceRule.getRuleId());
        instanceRuleQuery.setDatasetId(instanceRule.getDatasetId());
        instanceRuleQuery.setDatasetCode(instanceRule.getDatasetCode());
        instanceRuleQuery.setMetadataCode(instanceRule.getMetadataCode());
        count = instanceRuleService.count(new QueryWrapper<>(instanceRuleQuery));
        if(count > 0){
            resultMap.put("updateCount", 0);
            resultMap.put("message", "已存在相同的规则实例，不允许创建！");
            return resultMap;
        }
        instanceRuleService.getBaseMapper().insert(instanceRule);
        String instanceRuleId = instanceRule.getId();
        DatasetInstanceRule dataSetInstanceRule = new DatasetInstanceRule(instanceRuleDTO.getInstanceDatasetId(), instanceRuleId);
        Integer insertCount = datasetInstanceRuleService.getBaseMapper().insert(dataSetInstanceRule);
        //新增规则实例参数值添加
        addRuleDetailVal(instanceRuleDTO);
        resultMap.put("ruleInstanceId", instanceRuleId);
        resultMap.put("insertCount", insertCount);
        return resultMap;
    }

    private int deleteRuleDetailVal(InstanceRuleRequestDTO instanceRuleDTO) {
        int i = 0;
        if (instanceRuleDTO.getInstanceRule().getRuleDetailVals() != null) {
            Wrapper<RuleDetailVal> updateWrapper = new UpdateWrapper<>();
            ((UpdateWrapper<RuleDetailVal>) updateWrapper).eq("INSTANCE_RULE_ID", instanceRuleDTO.getInstanceRule().getId());
            i += ruleDetailValService.getBaseMapper().delete(updateWrapper);
        }
        return i;
    }

    private int addRuleDetailVal(InstanceRuleRequestDTO instanceRuleDTO) {
        AtomicInteger i = new AtomicInteger();
        AtomicInteger j = new AtomicInteger(0);
        if (instanceRuleDTO.getInstanceRule().getRuleDetailVals() != null) {
            instanceRuleDTO.getInstanceRule().getRuleDetailVals().forEach(val -> {
                j.addAndGet(1);
                val.setArgName("arg" + j.get());
                val.setInstanceRuleId(instanceRuleDTO.getInstanceRule().getId().toString());
                i.addAndGet(ruleDetailValService.getBaseMapper().insert(val));
            });
        }
        return i.get();
    }

    @Transactional
    public Map<String, Object> updateInstanceRule(InstanceRuleRequestDTO instanceRuleDTO) {
        Map<String, Object> resultMap = new HashMap<>();
        int count = baseMapper.getInstanceRunningTask(instanceRuleDTO.getQcsInstanceId());
        if (count > 0) {
            resultMap.put("updateCount", 0);
            resultMap.put("message", "该实例存在正在执行的质控任务，不允许修改！");
            return resultMap;
        }
        int updateCount = instanceRuleService.getBaseMapper().updateById(instanceRuleDTO.getInstanceRule());
        updateCount += deleteRuleDetailVal(instanceRuleDTO);
        updateCount += addRuleDetailVal(instanceRuleDTO);
        resultMap.put("ruleInstanceId", instanceRuleDTO.getInstanceRule().getId());
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    public List<InstanceRule> queryInstanceRuleList(QueryInstanceRuleDTO queryInstanceRuleDTO) {
        List<InstanceRule> instanceRuleResponseDTOS = instanceRuleService.getBaseMapper().queryQcsInstanceRuleList(queryInstanceRuleDTO);
        return instanceRuleResponseDTOS;
    }

    @Transactional
    public Map<String, Object> deleteRuleInstance(Long id) {
        Map<String, Object> resultMap = new HashMap<>();
        instanceRuleService.getBaseMapper().deleteById(id);
        Integer deleteCount = datasetInstanceRuleService.getBaseMapper().deleteDataSetInstanceRule(id);
        deleteInstanceRuleDetailVal(id);
        resultMap.put("deleteCount", deleteCount);
        return resultMap;
    }

    private void deleteInstanceRuleDetailVal(Long id) {
        Wrapper<RuleDetailVal> wrapper = new UpdateWrapper<>();
        ((UpdateWrapper<RuleDetailVal>) wrapper).eq("INSTANCE_RULE_ID", id);
        ruleDetailValService.getBaseMapper().delete(wrapper);
    }

    public Map<String, Object> handleRuleInstance(Long id, String status) {
        Map<String, Object> resultMap = new HashMap<>();
        Integer updateCount = instanceRuleService.getBaseMapper().updateStatus(id, status);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    public Map<String, Object> queryDataSetByChoose(Long qcsInstanceId, String argType, String dataSetId) {
        Map<String, Object> resultMap = new HashMap<>();
        List<Dataset> datasetList = new ArrayList<>();
        // 数据集
        if (StringUtils.equals(argType, GET_DATASET)) {
            datasetList = datasetService.getBaseMapper().queryDataSetDetails(qcsInstanceId, dataSetId);
        } else {
            datasetList = datasetService.getBaseMapper().queryDataSetsByQcsInstanceId(qcsInstanceId, dataSetId);
        }
        resultMap.put("dataSetList", datasetList);
        return resultMap;
    }

    private static List<QcsInstanceResponseDTO> makeTree(List<QcsInstanceResponseDTO> departmentList, String parentDataSetCode) {
        //子类
        List<QcsInstanceResponseDTO> children = departmentList.stream().filter(x -> x.getParentDataSetCode().equals(parentDataSetCode)).collect(Collectors.toList());

        //后辈中的非子类
        List<QcsInstanceResponseDTO> successor = departmentList.stream().filter(x -> !(x.getParentDataSetCode().equals(parentDataSetCode))).collect(Collectors.toList());

        children.forEach(x ->
            {
                makeTree(successor, x.getDataSetCode()).forEach(
                    y -> {
                        List<QcsInstanceResponseDTO> list = x.getChildDataSets() == null ? new ArrayList<>() : x.getChildDataSets();
                        list.add(y);
                        x.setChildDataSets(list);
                    }
                );
            }
        );
        return children;
    }

    public static boolean isIn(String substring, List<String> source) {
        if (source == null || source.size() == 0) {
            return false;
        }
        for (int i = 0; i < source.size(); i++) {
            String aSource = source.get(i);
            if (aSource.equals(substring)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, Object> checkInstanceRule(String id, String status) {
        Map<String, Object> map = new HashMap<>();
        int count = baseMapper.checkInstanceRule(id, status);
        map.put("checked", count <= 0);
        return map;
    }
}
