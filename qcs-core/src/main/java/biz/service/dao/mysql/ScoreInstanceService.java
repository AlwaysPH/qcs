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
     * 树形数据结构标识
     */
    private static final String IS_TREE = "1";

    private static final String ROW = "row";

    /**
     * 最底层的子节点集合
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
     * 获取评分对象列表
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
     *  新增评分对象
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject addScoreInstance(ScoreInstance scoreInstance) {
        JSONObject jsonObject = new JSONObject();
        if (null != scoreInstance.getParentId()) {
            //判断父节点下所有子节点的评分对象分类
            ScoreInstance instance = new ScoreInstance();
            instance.setParentId(scoreInstance.getParentId());
            Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
            List<ScoreInstance> childList = scoreInstanceMapper.selectList(queryWrapper);
            if (null != childList && childList.size() > 0) {
                for (ScoreInstance childInstance : childList) {
                    if (!scoreInstance.getIsCategory().equals(childInstance.getIsCategory())) {
                        //若当前评分对象和其他子节点评分对象的评分对象分类不一致，不允许入库
                        jsonObject.put("code", -1);
                        jsonObject.put("message", "当前评分对象与其他子节点评分对象分类不一致，不允许新增！");
                        return jsonObject;
                    }
                }
            }
            //判断父节点是否是评分对象分类，如果不是，则不允许添加下级
            ScoreInstance parentInstance = new ScoreInstance();
            parentInstance.setId(scoreInstance.getParentId());
            Wrapper<ScoreInstance> qw = new QueryWrapper<>(parentInstance);
            ScoreInstance pInstance = scoreInstanceMapper.selectOne(qw);
            if (CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(pInstance.getIsCategory())) {
                jsonObject.put("code", -1);
                jsonObject.put("message", "父节点不是评分对象分类，不允许新增！");
                return jsonObject;
            }
        }
        ScoreInstance instance = new ScoreInstance();
        instance.setName(scoreInstance.getName());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        List<ScoreInstance> list = scoreInstanceMapper.selectList(queryWrapper);
        if (CollectionUtils.isNotEmpty(list)) {
            jsonObject.put("code", -1);
            jsonObject.put("message", "存在评分对象相同，不允许新增！");
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
                jsonObject.put("message", "获取组织机构失败，请确认机构编码输入是否正确！");
                return jsonObject;
            }
        }
        scoreInstanceMapper.insert(scoreInstance);
        //获取父级节点配置的规则和数据集
        if (null != scoreInstance.getParentId()) {
            //数据集
            insertScoreInstanceDataSet(scoreInstance);
            //规则
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
     * 修改评分对象信息
     * @param scoreInstance
     * @return
     */
    public JSONObject updateScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        /**查询当前评分对象是否存在正在执行的评分任务**/
        int count = taskMapper.getTaskRunningCount(scoreInstance.getId());
        if (count > 0) {
            result.put(ROW, row);
            result.put("message", "当前评分对象存在正在执行的评分任务，不允许修改！");
            return result;
        }
        //查询当前评分对象是否存在子节点
        ScoreInstance instance = new ScoreInstance();
        instance.setParentId(scoreInstance.getId());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        int childCount = scoreInstanceMapper.selectCount(queryWrapper);
        if (childCount > 0) {
            result.put(ROW, row);
            result.put("message", "当前评分对象存在子节点，不允许修改！");
            return result;
        }

        //判断父节点是否是评分对象分类，如果不是，则不允许添加下级
        QueryWrapper<ScoreInstance> wrapper = new QueryWrapper<>();
        wrapper.eq("id", scoreInstance.getParentId());
        ScoreInstance pInstance = scoreInstanceMapper.selectOne(wrapper);
        if (null != pInstance && CommonConstant.SCORE_INSTANCE_IS_CATEGORY_FALSE.equals(pInstance.getIsCategory())) {
            result.put(ROW, row);
            result.put("message", "父节点不是评分对象分类，不允许修改！");
            return result;
        }
        wrapper.clear();
        wrapper.eq("name", scoreInstance.getName());
        wrapper.ne("id", scoreInstance.getId());
        instance = scoreInstanceMapper.selectOne(wrapper);
        if (null != instance) {
            result.put(ROW, row);
            result.put("message", "已有重名的评分对象！");
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
                result.put("message", "获取组织机构失败，请确认机构编码输入是否正确！");
                return result;
            }
        }
        //修改评分对象信息
        row = scoreInstanceMapper.updateById(scoreInstance);
        result.put(ROW, row);
        return result;
    }

    /***
     * 删除评分对象
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject deleteScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        //查询当前评分对象是否存在子类评分对象
        ScoreInstance instance = new ScoreInstance();
        instance.setParentId(scoreInstance.getId());
        Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
        int count = scoreInstanceMapper.selectCount(queryWrapper);
        if (count > 0) {
            result.put(ROW, row);
            result.put("message", "当前评分对象存在子节点，不允许删除！请先清除评分对象下的所有子节点");
            return result;
        }

        //查询当前评分对象是否存在评分任务
        TaskScoreInstance taskScoreInstance = new TaskScoreInstance();
        taskScoreInstance.setScoreInstanceId(scoreInstance.getId().toString());
        Wrapper<TaskScoreInstance> taskQueryWrapper = new QueryWrapper<>(taskScoreInstance);
        int taskCount = taskScoreInstanceMapper.selectCount(taskQueryWrapper);
        if (taskCount > 0) {
            result.put(ROW, row);
            result.put("message", "当前评分对象存在评分任务，不允许删除！");
            return result;
        }

        row = scoreInstanceMapper.deleteById(scoreInstance);
        scoreDataSetMapper.deleteByInstanceId(scoreInstance.getId());
        scoreInstanceRuleMapper.deleteByInstanceId(scoreInstance.getId());
        result.put(ROW, row);
        return result;
    }

    /***
     * 修改评分对象状态
     * @param scoreInstance
     * @return
     */
    @Transactional
    public JSONObject switchScoreInstance(ScoreInstance scoreInstance) {
        int row = 0;
        JSONObject result = new JSONObject();
        String status = scoreInstance.getStatus();
        List<ScoreInstance> list = new ArrayList<>();
        /**停用当前评分对象**/
        if (CommonConstant.TASK_RUN_STATUS.equals(status)) {
            /**查询当前评分对象是否存在正在执行的评分任务**/
            int count = taskMapper.getTaskRunningCount(scoreInstance.getId());
            if (count > 0) {
                result.put(ROW, row);
                result.put("message", "当前评分对象存在正在执行的评分任务，不允许修改！");
                return result;
            } else {
                //不存在正在执行的评分任务，当前评分对象上未执行和已暂停的评分任务自动变为已取消状态
                taskMapper.setTaskStatus(scoreInstance.getId(), "4");
            }
        } else {
            //判断父节点下所有子节点的评分对象分类
            ScoreInstance instance = new ScoreInstance();
            instance.setParentId(scoreInstance.getId());
            Wrapper<ScoreInstance> queryWrapper = new QueryWrapper<>(instance);
            list = scoreInstanceMapper.selectList(queryWrapper);
        }
        list.add(scoreInstance);
        //修改评分对象状态
        row = scoreInstanceMapper.updateList(list, status);
        result.put(ROW, row);
        return result;
    }

    /***
     * 获取评分对象关联数据集
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
     * 保存评分对象关联数据集
     * @param data
     * @return
     */
    @Transactional
    public JSONObject saveScoreDataSet(JSONArray data) {
        JSONObject result = new JSONObject();
        JSONObject jsonObject = data.getJSONObject(0);
        Long scoreInstanceId = jsonObject.getLong("scoreInstanceId");
        /**查询当前评分对象是否存在正在执行的评分任务**/
        int count = taskMapper.getTaskRunningCount(scoreInstanceId);
        if (count > 0) {
            result.put(ROW, -1);
            result.put("message", "当前评分对象存在正在执行的评分任务，不允许修改！");
            return result;
        }

        /**根据评分对象ID查询是否存在子节点**/
        //获取评分对象吓所有子节点
        List<ScoreInstance> list = getAllChild(scoreInstanceId);

        ScoreInstance sc = new ScoreInstance();
        sc.setId(scoreInstanceId);
        list.add(sc);
        //先删除原有关联数据集
        scoreDataSetMapper.deleteByScoreInstanceId(list);

        List<ScoreInstanceDataSet> ruleList = new ArrayList<>();
        //父子节点关联评分规则
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
     * 获取评分对象关联评分规则
     * @param scoreInstanceRule
     * @return
     */
    public List<ScoreInstanceRule> getScoreRule(ScoreInstanceRule scoreInstanceRule) {
        Wrapper<ScoreInstanceRule> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<ScoreInstanceRule>) queryWrapper).setEntity(scoreInstanceRule);
        return scoreInstanceRuleMapper.selectList(queryWrapper);
    }

    /***
     * 保存评分对象关联评分规则
     * @param data
     * @return
     */
    @Transactional
    public JSONObject saveScoreRule(JSONArray data) {
        JSONObject result = new JSONObject();
        JSONObject jsonObject = data.getJSONObject(0);
        Long scoreInstanceId = jsonObject.getLong("scoreInstanceId");

        /**查询当前评分对象是否存在正在执行的评分任务**/
        int count = taskMapper.getTaskRunningCount(scoreInstanceId);
        if (count > 0) {
            result.put(ROW, -1);
            result.put("message", "当前评分对象存在正在执行的评分任务，不允许修改！");
            return result;
        }

        /**根据评分对象ID查询是否存在子节点，且子节点为评分对象**/
        //获取评分对象吓所有子节点
        List<ScoreInstance> list = getAllChild(scoreInstanceId);

        //删除原关联规则
        ScoreInstance sc = new ScoreInstance();
        sc.setId(scoreInstanceId);
        list.add(sc);
        scoreInstanceRuleMapper.deleteByScoreInstanceId(list);

        List<ScoreInstanceRule> ruleList = new ArrayList<>();
        //父子节点关联评分规则
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
        //父节点列表
        List<ScoreInstance> fatherList = list.stream().filter(t -> t.getParentId() == 0).collect(Collectors.toList());
        //子节点列表
        List<ScoreInstance> childrenList = list.stream().filter(t -> t.getParentId() != 0).collect(Collectors.toList());
        //给每个父节点添加子节点信息
        for (ScoreInstance scoreInstance : fatherList) {
            finalTreeList.add(recursionList(scoreInstance, childrenList));
        }
        return finalTreeList;
    }

    private ScoreInstance recursionList(ScoreInstance scoreInstance, List<ScoreInstance> totalChildrenList) {
        //过滤出对应当前父节点的子节点列表
        List<ScoreInstance> filteredChildrenList = totalChildrenList.stream().filter(t -> scoreInstance.getId().equals(t.getParentId())).collect(Collectors.toList());

        List<ScoreInstance> children = Lists.newArrayList();
        for (ScoreInstance child : filteredChildrenList) {
            //递归获取子节点
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
        //调标准管理数据集列表接口
        List<Dataset> list = instanceService.queryDataSetList(instanceVersionPojo.getStandardCode(), instanceVersionPojo.getVersionCode());
        //获取当前版本的质控实例关联的数据集
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
            //递归
            if (!CommonConstant.TOP_ID.toString().equals(parentDto.getParentDataSetCode())) {
                recursive(parentDto, list, result);
            }
        }
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

    /**
     * 获取数据质量综合报告的评分对象
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
     * 分页查询一级评分对象
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
     * 分页查询子评分对象（或对象分类）
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
        //父节点
        Optional<ScoreInstance> father = list.stream().filter(t -> t.getId().equals(id)).findFirst();
        //子节点列表
        List<ScoreInstance> childrenList = list.stream().filter(t -> t.getParentId() != 0).collect(Collectors.toList());
        //给每个父节点添加子节点信息
        father.ifPresent(t -> recursionListForChildren(t, childrenList));
        if (StringUtils.isNotBlank(isCategory)) {
            bottomChildrenList = bottomChildrenList.stream().filter(t -> isCategory.equals(t.getIsCategory())).collect(Collectors.toList());
        }
        return bottomChildrenList;
    }

    private ScoreInstance recursionListForChildren(ScoreInstance scoreInstance, List<ScoreInstance> totalChildrenList) {
        //过滤出对应当前父节点的子节点列表
        List<ScoreInstance> filteredChildrenList = totalChildrenList.stream().filter(t -> scoreInstance.getId().equals(t.getParentId())).collect(Collectors.toList());

        List<ScoreInstance> children = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(filteredChildrenList)) {
            for (ScoreInstance child : filteredChildrenList) {
                //递归获取子节点
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
     * 获取所有子级ID
     *
     * @param id
     * @return
     */
    public List<Long> getChildrenId(Long id) {
        return scoreInstanceMapper.getChildrenId(id);
    }

    /**
     * 获取所有子级机构编码
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
