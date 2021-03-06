package biz.service.dao.mysql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.collect.Lists;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.core.biz.service.DmQueryService;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.InstanceVersionPojo;
import com.gwi.qcs.model.dto.QcsInstanceResponseDTO;
import com.gwi.qcs.model.mapper.mysql.*;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@DS(DbTypeConstant.MYSQL)
public class ScoreInstanceService extends SuperServiceImpl<ScoreInstanceMapper, ScoreInstance> {

    private Logger log = LoggerFactory.getLogger(ScoreInstanceService.class);

    @Autowired
    private ScoreInstanceMapper scoreInstanceMapper;

    @Autowired
    private TaskScoreInstanceMapper taskScoreInstanceMapper;

    @Autowired
    private ScoreInstanceRuleMapper scoreInstanceRuleMapper;

    @Autowired
    private ScoreDataSetMapper scoreDataSetMapper;

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    private InstanceMapper instanceMapper;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private DmQueryService dmQueryService;

    /**
     * ????????????????????????
     */
    private static final String IS_TREE = "1";

    private static final String ROW = "row";

    /**
     * ???????????????????????????
     */
    private List<ScoreInstance> bottomChildrenList = Lists.newArrayList();

    @Data
    public class QuerySourceIdOrOrg{
        List<String> orgList;
        String sourceId;
        boolean isOrg;
    }

    public QuerySourceIdOrOrg getQueryCondition(String id){
        QuerySourceIdOrOrg querySourceIdOrOrg = new QuerySourceIdOrOrg();
        querySourceIdOrOrg.setOrg(true);
        ScoreInstance scoreInstance = this.getById(id);
        if(CommonConstant.ORG_JBYL.contains(scoreInstance.getName())){
            LambdaQueryWrapper<ScoreInstance> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.in(ScoreInstance::getId, this.getChildrenId(Long.parseLong(id)));
            List<ScoreInstance> scoreInstanceList = this.list(queryWrapper);
            querySourceIdOrOrg.setOrgList(scoreInstanceList.stream().map(ScoreInstance::getOrgCode).collect(Collectors.toList()));
        }else{
            if(StringUtils.isNotEmpty(scoreInstance.getDatasourceId())){
                querySourceIdOrOrg.setOrg(false);
                querySourceIdOrOrg.setSourceId(scoreInstance.getDatasourceId());
            }else{
                querySourceIdOrOrg.setOrgList(Arrays.asList(scoreInstance.getOrgCode()));
            }
        }
        return querySourceIdOrOrg;
    }

    /***
     * ????????????????????????
     * @param scoreInstance
     * @return
     */
    public List<ScoreInstance> getScoreInstance(ScoreInstance scoreInstance) {
        List<ScoreInstance> resultList = new ArrayList<>();
        QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(scoreInstance);
        List<ScoreInstance> list = scoreInstanceMapper.selectList(queryWrapper);
        if (IS_TREE.equals(scoreInstance.getIsTree())) {
            if (CollectionUtils.isNotEmpty(list)) {
                resultList = getScoreInstanceTreeList(list, 0L);
            }
        } else {
            resultList = list;
        }
        return resultList;
    }

    /***
     *  ??????????????????
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject addScoreInstance(ScoreInstance scoreInstance) {
        JSONObject jsonObject = new JSONObject();
        if (null != scoreInstance.getParentId()) {
            //??????????????????????????????????????????????????????
            ScoreInstance instance = new ScoreInstance();
            instance.setParentId(scoreInstance.getParentId());
            Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
            List<ScoreInstance> childList = scoreInstanceMapper.selectList(queryWrapper);
            if (null != childList && childList.size() > 0) {
                for (ScoreInstance childInstance : childList) {
                    if (!scoreInstance.getIsCategory().equals(childInstance.getIsCategory())) {
                        //???????????????????????????????????????????????????????????????????????????????????????????????????
                        jsonObject.put("code", -1);
                        jsonObject.put("message", "????????????????????????????????????????????????????????????????????????????????????");
                        return jsonObject;
                    }
                }
            }
            //????????????????????????????????????????????????????????????????????????????????????
            ScoreInstance parentInstance = new ScoreInstance();
            parentInstance.setId(scoreInstance.getParentId());
            Wrapper<ScoreInstance> qw = new QueryWrapper<>(parentInstance);
            ScoreInstance pInstance = scoreInstanceMapper.selectOne(qw);
            if (CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(pInstance.getIsCategory())) {
                jsonObject.put("code", -1);
                jsonObject.put("message", "??????????????????????????????????????????????????????");
                return jsonObject;
            }
        }
        ScoreInstance instance = new ScoreInstance();
        instance.setName(scoreInstance.getName());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        List<ScoreInstance> list = scoreInstanceMapper.selectList(queryWrapper);
        if (CollectionUtils.isNotEmpty(list)) {
            jsonObject.put("code", -1);
            jsonObject.put("message", "?????????????????????????????????????????????");
            return jsonObject;
        }
        if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getOrgCode())) {
            try {
                List<String> iOrgCodes = Arrays.asList(scoreInstance.getOrgCode().split(","));
                List<String> orgCodeDms = new ArrayList<>();
                iOrgCodes.forEach(iOrgCode -> {
                    orgCodeDms.add(dmQueryService.getOrgByCode(iOrgCode).getOrgCode());
                });
                scoreInstance.setOrgCodes(iOrgCodes);
                scoreInstance.setOrgCodeDm(String.join(",", orgCodeDms));
            } catch (Exception e) {
                jsonObject.put("code", -1);
                jsonObject.put("message", "?????????????????????????????????????????????????????????????????????");
                return jsonObject;
            }
        }
        scoreInstanceMapper.insert(scoreInstance);
        //?????????????????????????????????????????????
        if (null != scoreInstance.getParentId()) {
            //?????????
            insertScoreInstanceDataSet(scoreInstance);
            //??????
            insertScoreInstanceRule(scoreInstance);
        }
        jsonObject.put("code", 0);
        jsonObject.put("id", scoreInstance.getId());
        return jsonObject;
    }

    private void insertScoreInstanceRule(ScoreInstance scoreInstance) {
        ScoreInstanceRule scoreInstanceRule = new ScoreInstanceRule();
        scoreInstanceRule.setScoreInstanceId(scoreInstance.getParentId());
        Wrapper<ScoreInstanceRule> ruleWrapper = new QueryWrapper<>(scoreInstanceRule);
        List<ScoreInstanceRule> ruleList = scoreInstanceRuleMapper.selectList(ruleWrapper);
        List<ScoreInstanceRule> list = new ArrayList<>();
        for (ScoreInstanceRule rule : ruleList) {
            ScoreInstanceRule instanceRule = new ScoreInstanceRule();
            instanceRule.setScoreInstanceId(scoreInstance.getId());
            instanceRule.setBizType(rule.getBizType());
            instanceRule.setBizId(rule.getBizId());
            instanceRule.setWeight(rule.getWeight());
            instanceRule.setScoreType(rule.getScoreType());
            list.add(instanceRule);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            scoreInstanceRuleMapper.save(list);
        }
    }

    private void insertScoreInstanceDataSet(ScoreInstance scoreInstance) {
        ScoreInstanceDataSet scoreInstanceDataSet = new ScoreInstanceDataSet();
        scoreInstanceDataSet.setScoreInstanceId(scoreInstance.getParentId());
        Wrapper<ScoreInstanceDataSet> dataSetWrapper = new QueryWrapper<>(scoreInstanceDataSet);
        List<ScoreInstanceDataSet> dataSetList = scoreDataSetMapper.selectList(dataSetWrapper);
        List<ScoreInstanceDataSet> list = new ArrayList<>();
        for (ScoreInstanceDataSet dataSet : dataSetList) {
            ScoreInstanceDataSet instanceDataSet = new ScoreInstanceDataSet();
            instanceDataSet.setScoreInstanceId(scoreInstance.getId());
            instanceDataSet.setDataSetId(dataSet.getDataSetId());
            list.add(instanceDataSet);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            scoreDataSetMapper.save(list);
        }
    }

    /***
     * ????????????????????????
     * @param scoreInstance
     * @return
     */
    public JSONObject updateScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        /**???????????????????????????????????????????????????????????????**/
        int count = taskMapper.getTaskRunningCount(scoreInstance.getId());
        if (count > 0) {
            result.put(ROW, row);
            result.put("message", "????????????????????????????????????????????????????????????????????????");
            return result;
        }
        //?????????????????????????????????????????????
        ScoreInstance instance = new ScoreInstance();
        instance.setParentId(scoreInstance.getId());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        int childCount = scoreInstanceMapper.selectCount(queryWrapper);
        if (childCount > 0) {
            result.put(ROW, row);
            result.put("message", "??????????????????????????????????????????????????????");
            return result;
        }

        //????????????????????????????????????????????????????????????????????????????????????
        QueryWrapper<ScoreInstance> wrapper = new QueryWrapper<>();
        wrapper.eq("id", scoreInstance.getParentId());
        ScoreInstance pInstance = scoreInstanceMapper.selectOne(wrapper);
        if (null != pInstance && CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(pInstance.getIsCategory())) {
            result.put(ROW, row);
            result.put("message", "??????????????????????????????????????????????????????");
            return result;
        }
        wrapper.clear();
        wrapper.eq("name", scoreInstance.getName());
        wrapper.ne("id", scoreInstance.getId());
        instance = scoreInstanceMapper.selectOne(wrapper);
        if (null != instance) {
            result.put(ROW, row);
            result.put("message", "??????????????????????????????");
            return result;
        }
        if (CommonConstant.SCORE_INSTANCE_SCORE_TAG_ORG.equals(scoreInstance.getScoreTag()) && StringUtils.isNotBlank(scoreInstance.getOrgCode())) {
            try {
                List<String> iOrgCodes = Arrays.asList(scoreInstance.getOrgCode().split(","));
                List<String> orgCodeDms = new ArrayList<>();
                iOrgCodes.forEach(iOrgCode -> {
                    orgCodeDms.add(dmQueryService.getOrgByCode(iOrgCode).getOrgCode());
                });
                scoreInstance.setOrgCodes(iOrgCodes);
                scoreInstance.setOrgCodeDm(String.join(",", orgCodeDms));
            } catch (Exception e) {
                result.put(ROW, row);
                result.put("message", "?????????????????????????????????????????????????????????????????????");
                return result;
            }
        }
        //????????????????????????
        row = scoreInstanceMapper.updateById(scoreInstance);
        result.put(ROW, row);
        return result;
    }

    /***
     * ??????????????????
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject deleteScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        //??????????????????????????????????????????????????????
        ScoreInstance instance = new ScoreInstance();
        instance.setParentId(scoreInstance.getId());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        int count = scoreInstanceMapper.selectCount(queryWrapper);
        if (count > 0) {
            result.put(ROW, row);
            result.put("message", "???????????????????????????????????????????????????????????????????????????????????????????????????");
            return result;
        }

        //????????????????????????????????????????????????
        TaskScoreInstance taskScoreInstance = new TaskScoreInstance();
        taskScoreInstance.setScoreInstanceId(scoreInstance.getId().toString());
        Wrapper<TaskScoreInstance> taskQueryWrapper = new QueryWrapper<>(taskScoreInstance);
        int taskCount = taskScoreInstanceMapper.selectCount(taskQueryWrapper);
        if (taskCount > 0) {
            result.put(ROW, row);
            result.put("message", "?????????????????????????????????????????????????????????");
            return result;
        }

        row = scoreInstanceMapper.deleteById(scoreInstance);
        scoreDataSetMapper.deleteByInstanceId(scoreInstance.getId());
        scoreInstanceRuleMapper.deleteByInstanceId(scoreInstance.getId());
        result.put(ROW, row);
        return result;
    }

    /***
     * ????????????????????????
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject switchScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        String status = scoreInstance.getStatus();
        List<ScoreInstance> list = new ArrayList<>();
        /**????????????????????????**/
        if (CommonConstant.TASK_RUN_STATUS.equals(status)) {
            /**???????????????????????????????????????????????????????????????**/
            int count = taskMapper.getTaskRunningCount(scoreInstance.getId());
            if (count > 0) {
                result.put(ROW, row);
                result.put("message", "????????????????????????????????????????????????????????????????????????");
                return result;
            } else {
                //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
                taskMapper.setTaskStatus(scoreInstance.getId(), "4");
            }
        } else {
            //??????????????????????????????????????????????????????
            ScoreInstance instance = new ScoreInstance();
            instance.setParentId(scoreInstance.getId());
            Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
            list = scoreInstanceMapper.selectList(queryWrapper);
        }
        list.add(scoreInstance);
        //????????????????????????
        row = scoreInstanceMapper.updateList(list, status);
        result.put(ROW, row);
        return result;
    }

    /***
     * ?????????????????????????????????
     * @param instanceDataSet
     * @return
     */
    public List<Map<String, Object>> getScoreDataSet(ScoreInstanceDataSet instanceDataSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        List<ScoreInstanceDataSet> dataSetList = scoreDataSetMapper.getDataSetList(instanceDataSet.getScoreInstanceId());
        dataSetList.stream().forEach(e -> {
            if (null != e) {
                Map<String, Object> map = new HashMap<>();
                map.put("dataSetId", e.getDataSetId());
                result.add(map);
            }
        });
        return result;
    }

    /***
     * ?????????????????????????????????
     * @param data
     * @return
     */
    @Transactional
    public JSONObject saveScoreDataSet(JSONArray data) {
        JSONObject result = new JSONObject();
        JSONObject jsonObject = data.getJSONObject(0);
        Long scoreInstanceId = jsonObject.getLong("scoreInstanceId");
        /**???????????????????????????????????????????????????????????????**/
        int count = taskMapper.getTaskRunningCount(scoreInstanceId);
        if (count > 0) {
            result.put(ROW, -1);
            result.put("message", "????????????????????????????????????????????????????????????????????????");
            return result;
        }

        /**??????????????????ID???????????????????????????**/
        //????????????????????????????????????
        List<ScoreInstance> list = getAllChild(scoreInstanceId);

        ScoreInstance sc = new ScoreInstance();
        sc.setId(scoreInstanceId);
        list.add(sc);
        //??????????????????????????????
        scoreDataSetMapper.deleteByScoreInstanceId(list);

        List<ScoreInstanceDataSet> ruleList = new ArrayList<>();
        //??????????????????????????????
        if (null != list && list.size() > 0) {
            for (ScoreInstance rule : list) {
                data.stream().forEach(childJson -> {
                    JSONObject dataJson = JSONObject.parseObject(childJson.toString());
                    ScoreInstanceDataSet dataSet = new ScoreInstanceDataSet();
                    dataSet.setScoreInstanceId(rule.getId());
                    dataSet.setDataSetId(dataJson.getLong("dataSetId"));
                    ruleList.add(dataSet);
                });
            }
        }
        int row = scoreDataSetMapper.save(ruleList);
        result.put(ROW, row);
        return result;
    }

    public List<ScoreInstance> getAllChild(Long scoreInstanceId) {
        List<ScoreInstance> result = new ArrayList<>();
        recursiveData(result, scoreInstanceId);
        return result;
    }

    private void recursiveData(List<ScoreInstance> result, Long scoreInstanceId) {
        List<ScoreInstance> childList = scoreInstanceMapper.getChild(scoreInstanceId);
        for (ScoreInstance instance : childList) {
            result.add(instance);
            if ("1".equals(instance.getIsCategory())) {
                recursiveData(result, instance.getId());
            }
        }
    }

    /***
     * ????????????????????????????????????
     * @param scoreInstanceRule
     * @return
     */
    public List<ScoreInstanceRule> getScoreRule(ScoreInstanceRule scoreInstanceRule) {
        Wrapper<ScoreInstanceRule> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<ScoreInstanceRule>) queryWrapper).setEntity(scoreInstanceRule);
        return scoreInstanceRuleMapper.selectList(queryWrapper);
    }

    /***
     * ????????????????????????????????????
     * @param data
     * @return
     */
    @Transactional
    public JSONObject saveScoreRule(JSONArray data) {
        JSONObject result = new JSONObject();
        JSONObject jsonObject = data.getJSONObject(0);
        Long scoreInstanceId = jsonObject.getLong("scoreInstanceId");

        /**???????????????????????????????????????????????????????????????**/
        int count = taskMapper.getTaskRunningCount(scoreInstanceId);
        if (count > 0) {
            result.put(ROW, -1);
            result.put("message", "????????????????????????????????????????????????????????????????????????");
            return result;
        }

        /**??????????????????ID?????????????????????????????????????????????????????????**/
        //????????????????????????????????????
        List<ScoreInstance> list = getAllChild(scoreInstanceId);

        //?????????????????????
        ScoreInstance sc = new ScoreInstance();
        sc.setId(scoreInstanceId);
        list.add(sc);
        scoreInstanceRuleMapper.deleteByScoreInstanceId(list);

        List<ScoreInstanceRule> ruleList = new ArrayList<>();
        //??????????????????????????????
        if (null != list && list.size() > 0) {
            for (ScoreInstance rule : list) {
                data.stream().forEach(childJson -> {
                    JSONObject dataJson = JSONObject.parseObject(childJson.toString());
                    ScoreInstanceRule childRule = new ScoreInstanceRule();
                    childRule.setScoreInstanceId(rule.getId());
                    childRule.setBizType(dataJson.getString("bizType"));
                    childRule.setBizId(dataJson.getLong("bizId"));
                    childRule.setWeight(dataJson.getLong("weight"));
                    childRule.setScoreType(dataJson.getString("scoreType"));
                    ruleList.add(childRule);
                });
            }
        }
        int row = scoreInstanceRuleMapper.save(ruleList);
        result.put(ROW, row);
        return result;
    }

    private List<ScoreInstance> getScoreInstanceTreeList(List<ScoreInstance> list, Long id) {
        List<ScoreInstance> finalTreeList = Lists.newArrayList();
        //???????????????
        List<ScoreInstance> fatherList = list.stream().filter(t -> t.getParentId() == 0).collect(Collectors.toList());
        //???????????????
        List<ScoreInstance> childrenList = list.stream().filter(t -> t.getParentId() != 0).collect(Collectors.toList());
        //???????????????????????????????????????
        for (ScoreInstance scoreInstance : fatherList) {
            finalTreeList.add(recursionList(scoreInstance, childrenList));
        }
        return finalTreeList;
    }

    private ScoreInstance recursionList(ScoreInstance scoreInstance, List<ScoreInstance> totalChildrenList) {
        //????????????????????????????????????????????????
        List<ScoreInstance> filteredChildrenList = totalChildrenList.stream().filter(t -> scoreInstance.getId().equals(t.getParentId())).collect(Collectors.toList());

        List<ScoreInstance> children = Lists.newArrayList();
        for (ScoreInstance child : filteredChildrenList) {
            //?????????????????????
            ScoreInstance grandChild = recursionList(child, totalChildrenList);
            children.add(grandChild);
        }
        scoreInstance.setChildren(children);
        return scoreInstance;
    }

    public List<InstanceVersionPojo> getInstanceVersion(Long scoreInstanceId) {
        List<InstanceVersionPojo> list = new ArrayList<>();
        if (null != scoreInstanceId) {
            list = instanceMapper.getParentInstanceVersion(scoreInstanceId);
        } else {
            list = instanceMapper.getInstanceVersion();
        }
        return list.stream().filter(e -> null != e).collect(Collectors.toList());
    }

    public List<QcsInstanceResponseDTO> getInstanceDataSet(InstanceVersionPojo instanceVersionPojo) {
        //????????????????????????????????????
        List<Dataset> list = instanceService.queryDataSetList(instanceVersionPojo.getStandardCode(), instanceVersionPojo.getVersionCode());
        //???????????????????????????????????????????????????
        List<Dataset> datasetList = instanceMapper.getDataByCode(instanceVersionPojo.getStandarId());
        return getNewTreeData(datasetList, list);
    }

    private List<QcsInstanceResponseDTO> getNewTreeData(List<Dataset> datasetList, List<Dataset> mapList) {
        List<QcsInstanceResponseDTO> result = new LinkedList<>();
        List<QcsInstanceResponseDTO> list = new LinkedList<>();
        if (null == mapList || mapList.size() <= 0) {
            return result;
        }
        for (Dataset dataset : mapList) {
            QcsInstanceResponseDTO qcsInstanceResponseDTO = dataset.toQcsInstanceResponseDTO();
            list.add(qcsInstanceResponseDTO);
        }
        for (Dataset dataSet : datasetList) {
            List<QcsInstanceResponseDTO> dtoList = list.stream().filter(x -> x.getDataSetCode().equals(dataSet.getMetasetCode())).collect(Collectors.toList());
            if (null == dtoList || dtoList.size() <= 0) {
                continue;
            }
            QcsInstanceResponseDTO dto = dtoList.get(0);
            QcsInstanceResponseDTO parentDto = list.stream().filter(y -> (y.getDataSetCode().equals(dto.getParentDataSetCode()) && "1".equals(y.getFlag()))).collect(Collectors.toList()).get(0);
            dto.setPDataSetName(parentDto.getDataSetName());
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
            //??????
            if (!CommonConstant.TOP_ID.toString().equals(parentDto.getParentDataSetCode())) {
                recursive(parentDto, list, result);
            }
        }
    }

    private static List<QcsInstanceResponseDTO> makeTree(List<QcsInstanceResponseDTO> departmentList, String parentDataSetCode) {
        //??????
        List<QcsInstanceResponseDTO> children = departmentList.stream().filter(x -> x.getParentDataSetCode().equals(parentDataSetCode)).collect(Collectors.toList());

        //?????????????????????
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

    /**
     * ?????????????????????????????????????????????
     *
     * @return
     */
    public List<ScoreInstance> getScoreObjectForSynthesizeReport() {
        List<ScoreInstance> resultList = new ArrayList<>();
        QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("status", CommonConstant.SCORE_INSTANCE_STATUS_USING);
        List<ScoreInstance> list = scoreInstanceMapper.selectList(queryWrapper);
        if (null != list && list.size() > 0) {
            resultList = getScoreInstanceTreeList(list, 0L);
        }
        return resultList;
    }

    /**
     * ??????????????????????????????
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    public IPage<ScoreInstance> pageLeveOne(Integer pageIndex, Integer pageSize) {
        QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("STATUS", CommonConstant.SCORE_INSTANCE_STATUS_USING);
        queryWrapper.eq("PARENT_ID", CommonConstant.LEVEL_ONE_PARENT_ID);
        queryWrapper.orderByDesc("ID");
        return scoreInstanceMapper.selectPage(new Page<>(pageIndex, pageSize), queryWrapper);
    }

    /**
     * ????????????????????????????????????????????????
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    public IPage<ScoreInstance> pageChildren(Integer pageIndex, Integer pageSize, Long scoreInstanceId) {
        QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("STATUS", CommonConstant.SCORE_INSTANCE_STATUS_USING);
        queryWrapper.eq("PARENT_ID", scoreInstanceId);
        queryWrapper.orderByAsc("ID");
        return scoreInstanceMapper.selectPage(new Page<>(pageIndex, pageSize), queryWrapper);
    }

    public List<ScoreInstance> getAllBottomChildren(Long id, String isCategory) {
        bottomChildrenList.clear();
        List<ScoreInstance> list = scoreInstanceMapper.selectList(null);
        //?????????
        Optional<ScoreInstance> father = list.stream().filter(t -> t.getId().equals(id)).findFirst();
        //???????????????
        List<ScoreInstance> childrenList = list.stream().filter(t -> t.getParentId() != 0).collect(Collectors.toList());
        //???????????????????????????????????????
        father.ifPresent(t -> recursionListForChildren(t, childrenList));
        if (StringUtils.isNotBlank(isCategory)) {
            bottomChildrenList = bottomChildrenList.stream().filter(t -> isCategory.equals(t.getIsCategory())).collect(Collectors.toList());
        }
        return bottomChildrenList;
    }

    private ScoreInstance recursionListForChildren(ScoreInstance scoreInstance, List<ScoreInstance> totalChildrenList) {
        //????????????????????????????????????????????????
        List<ScoreInstance> filteredChildrenList = totalChildrenList.stream().filter(t -> scoreInstance.getId().equals(t.getParentId())).collect(Collectors.toList());

        List<ScoreInstance> children = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(filteredChildrenList)) {
            for (ScoreInstance child : filteredChildrenList) {
                //?????????????????????
                ScoreInstance grandChild = recursionListForChildren(child, totalChildrenList);
                children.add(grandChild);
            }
        } else {
            bottomChildrenList.add(scoreInstance);
        }
        scoreInstance.setChildren(children);
        return scoreInstance;
    }

    /**
     * ??????????????????ID
     *
     * @param id
     * @return
     */
    public List<Long> getChildrenId(Long id) {
        return scoreInstanceMapper.getChildrenId(id);
    }

    /**
     * ??????????????????????????????
     *
     * @param id
     * @return
     */
    public List<String> getChildrenOrgCodeList(Long id) {
        List<Long> idList = this.getChildrenId(id);
        List<String> orgCodeList = new ArrayList<>();
        for (Long itemId : idList) {
            String orgCode = scoreInstanceMapper.selectById(itemId).getOrgCode();
            if (StringUtils.isNotEmpty(orgCode)) {
                orgCodeList.add(orgCode);
            }
        }
        return orgCodeList;
    }

    public List<String> getDatasetIdListByScoreId(Long scoreInstanceId) {
        QueryWrapper<ScoreInstanceDataSet> datasetQueryWrapper = new QueryWrapper<>();
        datasetQueryWrapper.eq("score_instance_id", scoreInstanceId);
        datasetQueryWrapper.select("dataset_id");
        return scoreDataSetMapper.selectObjs(datasetQueryWrapper)
            .stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.toList());
    }
}
