package biz.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateBetween;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.core.biz.schedule.QuartzJobManager;
import com.gwi.qcs.core.biz.schedule.task.DataQualityJobAbstract;
import com.gwi.qcs.core.biz.schedule.task.score.ScoringJobAbstract;
import com.gwi.qcs.core.biz.service.dao.mysql.InstanceService;
import com.gwi.qcs.core.biz.service.dao.mysql.SqlExecHistoryService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.dto.ContinueTaskRequest;
import com.gwi.qcs.model.entity.*;
import com.gwi.qcs.model.mapper.mysql.*;
import com.gwi.qcs.model.vo.TaskProgressResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.gwi.qcs.common.constant.CommonConstant.CURRENT_DAY_FLAG;

@Slf4j
@Service
public class TaskService extends SuperServiceImpl<TaskMapper, Task> {

    /**
     * 平台
     */
    private static final String PLATFORM = "1";

    @Value("${qcs.job.dataQuality.cron}")
    private String dataQualityCron;

    @Value("${qcs.job.scoring.cron}")
    private String dataScoreCron;

    @Value("${qcs.spark.master-url}")
    private String masterUrl;

    @Value("${qcs.task.parallel}")
    private boolean parallel;

    /**
     * SQL合并最大数量（防止SQL过长）
     */
    @Value("${qcs.job.dataQuality.mergeParMax}")
    private int mergeParMax;

    private static final String RULE_CATE_TYPE = "0";

    private static final String RULE_TYPE = PLATFORM;

    /**
     * 规范性
     */
    private static final String STANDARD_RULE = "standard";
    private static final String STANDARD_RULE_CHINESE = "规范性";

    /**
     * 一致性
     */
    private static final String CONSISTENCY_RULE = "consistency";
    private static final String CONSISTENCY_RULE_CHINESE = "一致性";

    /**
     * 完整性
     */
    private static final String COMPLETE_RULE = "complete";
    private static final String COMPLETE_RULE_CHINESE = "完整性";

    /**
     * 稳定性
     */
    private static final String STABILITY_RULE = "stability";
    private static final String STABILITY_RULE_CHINESE = "稳定性";

    /**
     * 关联性
     */
    private static final String RELEVANCE_RULE = "relevance";
    private static final String RELEVANCE_RULE_CHINESE = "关联性";

    /**
     * 及时性
     */
    private static final String TIMELINES_RULE = "timelines";
    private static final String TIMELINES_RULE_CHINESE = "及时性";

    private static final String WEIGHT = "weight";

    private static final String DATA = "data";

    private static final String SCORE_TYPE = "scoreType";

    @Resource
    private TaskDatasetMapper taskDatasetMapper;
    @Resource
    private TaskMapper taskMapper;
    @Resource
    private TaskRuleMapper taskRuleMapper;
    @Resource
    private TaskScoreInstanceMapper taskScoreinstanceMapper;

    @Autowired
    private ScoreInstanceMapper scoreInstanceMapper;

    @Autowired
    private ScoreDataSetMapper scoreDataSetMapper;

    @Autowired
    private ScoreInstanceRuleMapper scoreRuleMapper;

    @Autowired
    private RuleCategoryMapper ruleCategoryMapper;

    @Autowired
    private RuleMapper ruleMapper;

    @Autowired
    ParameterMapper parameterMapper;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private DatasetMapper dataSetMapper;

    @Autowired
    private DatasetFieldMapper datasetFieldMapper;

    @Autowired
    private InstanceRuleMapper instanceRuleMapper;

    @Autowired
    private InstanceDataSetMapper instanceDataSetMapper;

    @Autowired
    private TaskProgressService taskProgressService;

    @Autowired
    private SqlExecHistoryService execHistoryService;

    @Autowired
    private SparkLivyService sparkLivyService;

    @Transactional
    public int addTask(Task task) {
        task.setTaskCreatetime(DateUtil.now());
        int row = 0;
        row += taskMapper.insert(task);
        //如果是持续任务需要通过质控实例id查询出所有数据集和规则集
        if (task.getBizType().equals(CommonConstant.TASK_QUALITY) && task.getTaskType().equals(CommonConstant.TASK_CONTINUE_TYPE)) {
            Instance instance = instanceService.getBaseMapper().selectById(task.getBizId());
            List<Dataset> datasets = queryContinueTaskDatasetList(task, instance);
            List<TaskRule> instanceRules = queryContinueTaskRuleList(task, instance);
            List<TaskDataset> taskDatasets = new ArrayList<>();
            datasets.forEach(dataSet -> {
                TaskDataset taskDataset = new TaskDataset();
                taskDataset.setTaskId(task.getId());
                taskDataset.setDatasetId(dataSet.getDataSetId().toString());
                taskDatasets.add(taskDataset);
            });
            task.setDataSets(taskDatasets);
            task.setRules(instanceRules);
        }
        if (task.getRules() != null && !task.getRules().isEmpty()) {
            row += addTaskRule(task);
        }
        if (task.getDataSets() != null && !task.getDataSets().isEmpty()) {
            row += addTaskDataset(task);
        }
        if (task.getScoreinstances() != null && !task.getScoreinstances().isEmpty()) {
            row += addTaskScoreInstance(task);
        }
        return row;
    }

    private List<Dataset> queryContinueTaskDatasetList(Task task, Instance instance) {
        QueryWrapper<InstanceDataSet> instanceDataSetWrapper = new QueryWrapper<>();
        instanceDataSetWrapper.eq("INSTANCE_ID", instance.getId());
        List<InstanceDataSet> instanceDataSets = instanceDataSetMapper.selectList(instanceDataSetWrapper);

        List<String> dataSetIds = instanceDataSets.stream().map(instanceDataSet -> instanceDataSet.getDataSetId().toString()).collect(Collectors.toList());

        QueryWrapper<Dataset> dataSetWrapper = new QueryWrapper<>();
        dataSetWrapper.in("DATASET_ID", dataSetIds);
        dataSetWrapper.eq("SOURCE_TYPE", CommonConstant.SOURCE_TYPE_INSTANCE);
        return dataSetMapper.selectList(dataSetWrapper);
    }

    private List<TaskRule> queryContinueTaskRuleList(Task task, Instance instance) {
        return taskMapper.getInstanceRules(task);
    }

    private void checkAdd(Task task) throws ParseException {
        Wrapper<Task> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Task>) queryWrapper).eq("BIZ_ID", task.getBizId());
        ((QueryWrapper<Task>) queryWrapper).eq("BIZ_TYPE", task.getBizType());
        ((QueryWrapper<Task>) queryWrapper).eq("TASK_TYPE", task.getTaskType());
        ((QueryWrapper<Task>) queryWrapper).notIn("status", CommonConstant.TASK_CANCEL_STATUS);
        if (task.getTaskType().equals(CommonConstant.TASK_CONTINUE_TYPE)) {
            int count = taskMapper.selectCount(queryWrapper);
            if (count > 0) {
                throw new BizException("当前实例已存在一个持续任务，每个实例只允许存在一个持续任务！");
            }
        } else if (task.getTaskType().equals(CommonConstant.TASK_TEMP_TYPE)) {
            if (DateUtils.parseDate(task.getDataEndtime() + com.gwi.qcs.common.utils.DateUtils.DAY_END_TIME, new String[]{com.gwi.qcs.common.utils.DateUtils.DEFINE_YYYY_MM_DD_HH_MM_SS}).getTime() >= continuedTaskTime(task).getTime()) {
                throw new BizException("临时任务的数据区间已不能超过当前持续任务的完成区间！");
            }
        }
    }

    /**
     * 由毛佳兴提供数据接口
     *
     * @param task
     * @return
     */
    private Date continuedTaskTime(Task task) {
        return new Date();
    }

    private long parserDate(String dataStarttime) throws ParseException {
        return DateUtils.parseDate(dataStarttime, new String[]{"yyyy-MM-dd"}).getTime();
    }

    @Transactional
    public int updateTask(Task task) {
        int row = 0;
        row += taskMapper.updateById(task);
        if (task.getRules() != null && !task.getRules().isEmpty()) {
            row += deleteTaskRule(task);
            row += addTaskRule(task);
        }
        if (task.getDataSets() != null && !task.getDataSets().isEmpty()) {
            row += deleteTaskDataset(task);
            row += addTaskDataset(task);
        }
        if (task.getScoreinstances() != null && !task.getScoreinstances().isEmpty()) {
            row += deleteTaskScoreInstance(task);
            row += addTaskScoreInstance(task);
        }
        return row;
    }

    @Transactional
    public int deleteTask(Task task) {
        int row = 0;
        row += taskMapper.deleteById(task);
        row += deleteTaskRule(task);
        row += deleteTaskDataset(task);
        row += deleteTaskScoreInstance(task);
        return row;
    }

    public List<Task> getTaskList(Wrapper<Task> queryWrapper) {
        return taskMapper.selectList(queryWrapper);
    }

    public IPage<Task> page(Task toParameter, Integer pageIndex, Integer pageSize) throws ParseException {
        Page<Task> page = new Page<>(pageIndex, pageSize);
        QueryWrapper<Task> queryWrapper = new QueryWrapper<>();
        String[] taskType = toParameter.getTaskType().split(",");
        queryWrapper.in("TASK_TYPE", taskType);
        String[] status = toParameter.getStatus().split(",");
        queryWrapper.in("STATUS", status);
        String[] times = toParameter.getTaskCreatetime().split(",");
        if (times.length > 1) {
            queryWrapper.between("date(TASK_CREATETIME)", times[0], times[1]);
            toParameter.setTaskCreatetime(null);
        }
        toParameter.setTaskType(null);
        toParameter.setStatus(null);
        queryWrapper.setEntity(toParameter);
        queryWrapper.orderByDesc("TASK_CREATETIME");

        return taskMapper.selectPage(page, queryWrapper, Integer.parseInt(toParameter.getBizType()));
    }

    public Task queryTaskById(Task task) {
        return taskMapper.selectById(task);
    }

    public int switchTask(Task task) throws Exception {
        Task temp = new Task();
        switch (task.getStatus()) {
            case CommonConstant.TASK_RUN_STATUS:
                Task oldTask = taskMapper.selectById(task);
                runTask(oldTask);
                if (!oldTask.getStatus().equals(CommonConstant.TASK_SUSPEND_STATUS)) {
                    temp.setTaskStarttime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                }
                break;
            case CommonConstant.TASK_SUSPEND_STATUS:
                suspendTask(task);
                break;
            case CommonConstant.TASK_CANCEL_STATUS:
                cancelTask(task);
                break;
            default:
                throw new BizException("任务操作异常，请联系管理员！");
        }
        //只更新状态
        temp.setId(task.getId());
        temp.setStatus(task.getStatus());
        return taskMapper.updateById(temp);
    }

    private int deleteTaskScoreInstance(Task task) {
        TaskScoreInstance taskScoreinstance = new TaskScoreInstance();
        taskScoreinstance.setTaskId(task.getId());
        Wrapper<TaskScoreInstance> wrapper = new UpdateWrapper<>();
        ((UpdateWrapper<TaskScoreInstance>) wrapper).setEntity(taskScoreinstance);
        return taskScoreinstanceMapper.delete(wrapper);
    }

    private int deleteTaskDataset(Task task) {
        TaskDataset taskDataset = new TaskDataset();
        taskDataset.setTaskId(task.getId());
        Wrapper<TaskDataset> wrapper = new UpdateWrapper<>();
        ((UpdateWrapper<TaskDataset>) wrapper).setEntity(taskDataset);
        return taskDatasetMapper.delete(wrapper);
    }

    private int deleteTaskRule(Task task) {
        TaskRule taskRule = new TaskRule();
        taskRule.setTaskId(task.getId());
        Wrapper<TaskRule> wrapper = new UpdateWrapper<>();
        ((UpdateWrapper<TaskRule>) wrapper).setEntity(taskRule);
        return taskRuleMapper.delete(wrapper);
    }

    private int addTaskRule(Task task) {
        int row = 0;
        for (TaskRule rule : task.getRules()) {
            rule.setTaskId(task.getId());
            rule.setCreateBy(PLATFORM);
            row += taskRuleMapper.insert(rule);
        }
        return row;
    }

    private int addTaskScoreInstance(Task task) {
        int row = 0;
        for (TaskScoreInstance scoreinstance : task.getScoreinstances()) {
            scoreinstance.setTaskId(task.getId());
            scoreinstance.setCreateBy(PLATFORM);
            row += taskScoreinstanceMapper.insert(scoreinstance);
        }
        return row;
    }

    private int addTaskDataset(Task task) {
        int row = 0;
        for (TaskDataset dataSet : task.getDataSets()) {
            dataSet.setTaskId(task.getId());
            dataSet.setCreateBy(PLATFORM);
            row += taskDatasetMapper.insert(dataSet);
        }
        return row;
    }

    /**
     * 启动任务
     *
     * @param task
     */
    public void runTask(Task task) throws Exception {
        String jobGroupName = CommonConstant.QCS_JOB_GROUP;
        if (task.getBizType().equals(CommonConstant.TASK_QUALITY)) {
            if (task.getStatus().equals(CommonConstant.TASK_SUSPEND_STATUS)) {
                //恢复任务
                QuartzJobManager.getInstance().resumeJob(String.valueOf(task.getId()), jobGroupName);
            } else {
                Map map = JSON.parseObject(JSON.toJSONString(castQualityRealTaskByTaskDto(task)), Map.class);
                //创建之前先删除原有job
                QuartzJobManager.getInstance().deleteJob(String.valueOf(task.getId()), jobGroupName);
                //创建任务并按照执行计划执行
                QuartzJobManager.getInstance().addJob(DataQualityJobAbstract.class, task, jobGroupName, dataQualityCron, map);
            }
        } else if (task.getBizType().equals(CommonConstant.TASK_TEMPORARY)) {
            jobGroupName = CommonConstant.SCORE_JOB_GROUP;
            if (task.getStatus().equals(CommonConstant.TASK_SUSPEND_STATUS)) {
                //恢复任务
                QuartzJobManager.getInstance().resumeJob(String.valueOf(task.getId()), jobGroupName);
            } else {
                Map map = new HashMap();
                map.put(CommonConstant.SCORE_TASK_DATA, castScoreTaskByTaskDto(task));
                //创建之前先删除原有job
                QuartzJobManager.getInstance().deleteJob(String.valueOf(task.getId()), jobGroupName);
                //创建任务并按照执行计划执行
                QuartzJobManager.getInstance().addJob(ScoringJobAbstract.class, task, jobGroupName, dataScoreCron, map);
            }
        } else {

        }
        if (CommonConstant.TASK_CONTINUE_TYPE.equals(task.getTaskType())) {
            QuartzJobManager.getInstance().runJobNow(String.valueOf(task.getId()), jobGroupName);
        }
    }

    private List<ScoreJobDefinition> castScoreTaskByTaskDto(Task task1) {
        List<ScoreJobDefinition> list = new ArrayList<>();
        Task task = taskMapper.selectById(task1);
        ScoreInstance scoreInstance = scoreInstanceMapper.selectById(task.getBizId());
        //获取评分对象吓所有子节点
        List<ScoreInstance> childList = getAllChild(scoreInstance.getId());
        List<ScoreInstance> scoreChildList;
        if (CommonConstant.TASK_CONTINUE_TYPE.equals(task.getTaskType())) {
            // 持续任务则所有子级机构
            scoreChildList = childList;
        } else {
            //查询评分对象下哪些子节点勾选了评分
            List<TaskScoreInstance> taskScoreinstances = taskScoreinstanceMapper.getScoreInstance(task1.getId());
            scoreChildList = getScoreChildList(childList, taskScoreinstances);
        }
        scoreChildList.add(scoreInstance);
        //根据评分对象设置父类评分对象分类的数据源ID和机构编码
        setDataSourceAndOrgCode(scoreChildList);
        //获取ES max_result_window值
        Parameter parameter = new Parameter();
        parameter.setCode("ES_MAX_PAGESIZE");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        Integer esLimit = Integer.valueOf(parameterMapper.selectOne(queryWrapper).getValue());
        scoreChildList.stream().forEach(e -> {
            ScoreJobDefinition scoreJobDefinition = new ScoreJobDefinition();
            //获取关联数据集信息
            List<StandardDataSet> dataSetList = scoreDataSetMapper.getStandardDataSetList(e.getId());
            //获取关联规则
            List<ScoreInstanceRule> scoreInstanceRuleList = scoreRuleMapper.getScoreInstanceRule(e.getId());
            //构造规范性数据
            Map<String, Object> standardRuleMap = ruleDataList(scoreInstanceRuleList, STANDARD_RULE);
            //构造一致性数据
            Map<String, Object> consistencyMap = ruleDataList(scoreInstanceRuleList, CONSISTENCY_RULE);
            //构造完整性数据
            Map<String, Object> completeRuleMap = ruleDataList(scoreInstanceRuleList, COMPLETE_RULE);
            //构造稳定性数据
            Map<String, Object> stabilityRuleMap = ruleDataList(scoreInstanceRuleList, STABILITY_RULE);
            //构造关联性数据
            Map<String, Object> relevanceRuleMap = ruleDataList(scoreInstanceRuleList, RELEVANCE_RULE);
            //构造及时性数据
            Map<String, Object> timelinesRuleMap = ruleDataList(scoreInstanceRuleList, TIMELINES_RULE);

            scoreJobDefinition.setScoreTaskId(task.getId());
            if (StringUtils.isNotEmpty(e.getOrgCode())) {
                e.setOrgCodes(Arrays.asList(e.getOrgCode().split(",")));
            }
            if (StringUtils.isNotEmpty(e.getOrgCodeDm())) {
                e.setOrgCodeDms(Arrays.asList(e.getOrgCodeDm().split(",")));
            }
            scoreJobDefinition.setScoreInstance(e);
            scoreJobDefinition.setStandardRuleList((List<ScoreInstanceRule>) standardRuleMap.get("data"));
            scoreJobDefinition.setStandardWeight((Long) standardRuleMap.get(WEIGHT));
            scoreJobDefinition.setStandardScoreType((String) standardRuleMap.get(SCORE_TYPE));
            scoreJobDefinition.setConsistencyRuleList((List<ScoreInstanceRule>) consistencyMap.get("data"));
            scoreJobDefinition.setConsistencyWeight((Long) consistencyMap.get(WEIGHT));
            scoreJobDefinition.setConsistencyScoreType((String) consistencyMap.get(SCORE_TYPE));
            scoreJobDefinition.setCompleteRuleList((List<ScoreInstanceRule>) completeRuleMap.get("data"));
            scoreJobDefinition.setCompleteWeight((Long) completeRuleMap.get(WEIGHT));
            scoreJobDefinition.setCompleteScoreType((String) completeRuleMap.get(SCORE_TYPE));
            scoreJobDefinition.setStabilityRuleList((List<ScoreInstanceRule>) stabilityRuleMap.get("data"));
            scoreJobDefinition.setStabilityWeight((Long) stabilityRuleMap.get(WEIGHT));
            scoreJobDefinition.setStabilityScoreType((String) stabilityRuleMap.get(SCORE_TYPE));
            scoreJobDefinition.setRelevanceRuleList((List<ScoreInstanceRule>) relevanceRuleMap.get("data"));
            scoreJobDefinition.setRelevanceWeight((Long) relevanceRuleMap.get(WEIGHT));
            scoreJobDefinition.setRelevanceScoreType((String) relevanceRuleMap.get(SCORE_TYPE));
            scoreJobDefinition.setTimelinesRuleList((List<ScoreInstanceRule>) timelinesRuleMap.get("data"));
            scoreJobDefinition.setTimelinesWeight((Long) timelinesRuleMap.get(WEIGHT));
            scoreJobDefinition.setTimelinesScoreType((String) timelinesRuleMap.get(SCORE_TYPE));
            scoreJobDefinition.setStandardDataSetList(dataSetList);
            scoreJobDefinition.setStartTime(task1.getDataStarttime());
            scoreJobDefinition.setEndTime(task1.getDataEndtime());
            scoreJobDefinition.setIsParent(e.getIsParent());
            scoreJobDefinition.setCompleteAllRuleList(getCompleteAllRuleList(e.getId()));
            scoreJobDefinition.setUnbounded(CommonConstant.TASK_CONTINUE_TYPE.equals(task1.getTaskType()));
            scoreJobDefinition.setStandardCate(getCategoryData((List<ScoreInstanceRule>) standardRuleMap.get("data"), e.getId()));
            scoreJobDefinition.setConsistencyCate(getCategoryData((List<ScoreInstanceRule>) consistencyMap.get("data"), e.getId()));
            scoreJobDefinition.setCompleteCate(getCategoryData((List<ScoreInstanceRule>) completeRuleMap.get("data"), e.getId()));
            scoreJobDefinition.setStabilityCate(getCategoryData((List<ScoreInstanceRule>) stabilityRuleMap.get("data"), e.getId()));
            scoreJobDefinition.setRelevanceCate(getCategoryData((List<ScoreInstanceRule>) relevanceRuleMap.get("data"), e.getId()));
            scoreJobDefinition.setTimelinesCate(getCategoryData((List<ScoreInstanceRule>) timelinesRuleMap.get("data"), e.getId()));
            scoreJobDefinition.setEsLimit(esLimit);
            scoreJobDefinition.setTask(task);
            list.add(scoreJobDefinition);
        });
        return list;
    }

    private List<ScoreInstance> getScoreChildList(List<ScoreInstance> childList, List<TaskScoreInstance> taskScoreinstances) {
        List<ScoreInstance> result = new ArrayList<>();
        for (ScoreInstance scoreInstance : childList) {
            for (TaskScoreInstance taskScoreinstance : taskScoreinstances) {
                if (String.valueOf(scoreInstance.getId()).equals(taskScoreinstance.getScoreInstanceId())) {
                    result.add(scoreInstance);
                    break;
                }
            }
        }
        return result;
    }

    private void setDataSourceAndOrgCode(List<ScoreInstance> list) {
        //获取最顶级评分对象分类节点
        ScoreInstance parentInstance = list.stream().filter(e -> e.getParentId().longValue() == 0).collect(Collectors.toList()).get(0);
        parentInstance.setIsParent(true);
        //排除最顶级评分对象分类节点
        List<ScoreInstance> childList = list.stream().filter(e -> e.getParentId().longValue() != 0).collect(Collectors.toList());
        for (ScoreInstance instance : childList) {
            instance.setIsParent(false);
            //递归查询评分对象的数据源ID和机构编码
            StringBuilder sourceSB = new StringBuilder();
            StringBuilder codeSB = new StringBuilder();
            StringBuilder codeDMSB = new StringBuilder();
            getSourceIdAndOrgCode(instance, childList, sourceSB, codeSB, codeDMSB);
        }
        //设置最顶级评分对象分类节点
        setParentInstance(parentInstance, childList);
    }

    private void setParentInstance(ScoreInstance parentInstance, List<ScoreInstance> childList) {
        if (CollUtil.isNotEmpty(childList)) {
            Set<String> sourceSet = childList.stream().map(e -> e.getDatasourceId()).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            Set<String> codeSet = childList.stream().map(e -> e.getOrgCode()).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            Set<String> codeDMSet = childList.stream().map(e -> e.getOrgCodeDm()).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            if (sourceSet.size() > 0) {
                parentInstance.setDatasourceId(String.join(",", sourceSet));
            }
            if (codeSet.size() > 0) {
                parentInstance.setOrgCode(String.join(",", codeSet));
            }
            if (codeDMSet.size() > 0) {
                parentInstance.setOrgCodeDm(String.join(",", codeDMSet));
            }
        }

        childList.add(parentInstance);
    }

    private void getSourceIdAndOrgCode(ScoreInstance instance, List<ScoreInstance> childList, StringBuilder sourceSB, StringBuilder codeSB, StringBuilder codeDMSB) {
        List<ScoreInstance> list = childList.stream().filter(e -> instance.getId().equals(e.getParentId())).collect(Collectors.toList());
        if (null != list && list.size() > 0) {
            for (ScoreInstance childInstance : list) {
                if ("1".equals(childInstance.getIsCategory())) {
                    getSourceIdAndOrgCode(childInstance, list, sourceSB, codeSB, codeDMSB);
                } else {
                    if (StringUtils.isNotEmpty(childInstance.getDatasourceId())) {
                        sourceSB.append(",").append(childInstance.getDatasourceId());
                    }
                    if (StringUtils.isNotEmpty(childInstance.getOrgCode())) {
                        codeSB.append(",").append(childInstance.getOrgCode());
                    }
                    if (StringUtils.isNotEmpty(childInstance.getOrgCodeDm())) {
                        codeDMSB.append(",").append(childInstance.getOrgCodeDm());
                    }
                }
            }
            if (sourceSB.length() > 0) {
                sourceSB.deleteCharAt(0);
            }
            if (codeSB.length() > 0) {
                codeSB.deleteCharAt(0);
            }
            if (codeDMSB.length() > 0) {
                codeDMSB.deleteCharAt(0);
            }
            instance.setDatasourceId(sourceSB.toString());
            instance.setOrgCode(codeSB.toString());
            instance.setOrgCodeDm(codeDMSB.toString());
        }
    }

    private List<ScoreInstance> getAllChild(Long id) {
        List<ScoreInstance> result = new ArrayList<>();
        recursive(result, id);
        return result;
    }

    private void recursive(List<ScoreInstance> result, Long id) {
        List<ScoreInstance> childList = scoreInstanceMapper.getChild(id);
        for (ScoreInstance instance : childList) {
            result.add(instance);
            if ("1".equals(instance.getIsCategory())) {
                recursive(result, instance.getId());
            }
        }
    }

    private List<RuleCategory> getCategoryData(List<ScoreInstanceRule> data, Long scoreInstanceId) {
        if (null == data || data.size() <= 0) {
            return null;
        }
        List<String> ruleIdList = data.stream().map(e -> String.valueOf(e.getBizId())).collect(Collectors.toList());
        List<RuleCategory> ruleCategoryList = new ArrayList<>();
        for (String i : ruleIdList) {
            List<Rule> ruleList = new ArrayList<>();
            Rule rule = ruleMapper.selectById(i);
            ruleList.add(rule);
            RuleCategory ruleCategory = ruleCategoryMapper.selectById(rule.getCategoryId());
            ruleCategory.setRules(ruleList);
            ScoreInstanceRule scoreInstanceRule = scoreRuleMapper.getDataByCategoryId(ruleCategory.getId(), scoreInstanceId);
            ruleCategory.setScoreType(scoreInstanceRule.getScoreType());
            ruleCategory.setWeight(scoreInstanceRule.getWeight());
            ruleCategoryList.add(ruleCategory);
        }
        List<RuleCategory> result = new ArrayList<>();
        Map<String, List<RuleCategory>> map = ruleCategoryList.stream().collect(Collectors.groupingBy(a -> a.getId()));
        map.forEach((key, tlist) -> {
            if (tlist.size() > 1) {
                List<Rule> rules = new ArrayList<>();
                for (RuleCategory category : tlist) {
                    List<Rule> ruleList = category.getRules();
                    rules.addAll(ruleList);
                }
                RuleCategory ruleCategory = tlist.get(0);
                ruleCategory.setRules(rules);
                result.add(ruleCategory);
            } else {
                result.add(tlist.get(0));
            }
        });
        return result;
    }

    private List<RuleCategory> getCompleteAllRuleList(Long scoreInstanceId) {
        List<RuleCategory> result = new ArrayList<>();
        //获取所有规则分类
        Wrapper<RuleCategory> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) queryWrapper).orderByDesc("CREATE_AT");
        List<RuleCategory> ruleCategoryList = ruleCategoryMapper.selectList(queryWrapper);
        //获取完整性规则下所有规则分类
        List<RuleCategory> ruleCategoryList1 = ruleCategoryList.stream().filter(e -> COMPLETE_RULE_CHINESE.equals(e.getName())).collect(Collectors.toList());
        if (null == ruleCategoryList1 || ruleCategoryList1.size() <= 0) {
            return result;
        }
        RuleCategory ruleCategory = ruleCategoryList1.get(0);
        List<RuleCategory> cateChild = new ArrayList<>();
        orgRecursion(cateChild, ruleCategoryList, ruleCategory.getId());
        if (null == cateChild || cateChild.size() <= 0) {
            return result;
        }
        //获取所有规则分类对应的规则
        List<Rule> ruleList = ruleMapper.getRuleList(cateChild);
        Map<String, List<Rule>> list = ruleList.stream().collect(Collectors.groupingBy(Rule::getCategoryId));
        cateChild.stream().forEach(e -> {
            for (String key : list.keySet()) {
                if (e.getId().equals(key)) {
                    e.setRules(list.get(key));
                    ScoreInstanceRule scoreInstanceRule = scoreRuleMapper.getDataByCategoryId(key, scoreInstanceId);
                    if (null != scoreInstanceRule) {
                        e.setWeight(scoreInstanceRule.getWeight());
                        e.setScoreType(scoreInstanceRule.getScoreType());
                    }
                }
            }
        });
        //处理数据集上传率评分方式及权重数据
        for (RuleCategory category : cateChild) {
            if (StringUtils.isEmpty(category.getScoreType())) {
                ScoreInstanceRule scoreInstanceRule = scoreRuleMapper.getDataByCategoryId(category.getId(), scoreInstanceId);
                if (null != scoreInstanceRule) {
                    category.setWeight(scoreInstanceRule.getWeight());
                    category.setScoreType(scoreInstanceRule.getScoreType());
                }
            }
        }
        result = cateChild;
        return result;
    }

    private Map<String, Object> ruleDataList(List<ScoreInstanceRule> scoreInstanceRuleList, String type) {
        Map<String, Object> result = new HashMap<>();
        //获取规则
        List<ScoreInstanceRule> ruleList = scoreInstanceRuleList.stream().filter(e -> e.getBizType().equals(RULE_TYPE)).collect(Collectors.toList());
        //获取规则
        List<ScoreInstanceRule> ruleCateList = scoreInstanceRuleList.stream().filter(e -> e.getBizType().equals(RULE_CATE_TYPE)).collect(Collectors.toList());

        //获取所有规则分类
        Wrapper<RuleCategory> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) queryWrapper).orderByDesc("CREATE_AT");
        List<RuleCategory> ruleCategoryList = ruleCategoryMapper.selectList(queryWrapper);

        //获取六大规则分类的具体规则
        switch (type) {
            case STANDARD_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, STANDARD_RULE_CHINESE, ruleCateList);
                break;
            case CONSISTENCY_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, CONSISTENCY_RULE_CHINESE, ruleCateList);
                break;
            case COMPLETE_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, COMPLETE_RULE_CHINESE, ruleCateList);
                break;
            case STABILITY_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, STABILITY_RULE_CHINESE, ruleCateList);
                break;
            case RELEVANCE_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, RELEVANCE_RULE_CHINESE, ruleCateList);
                break;
            case TIMELINES_RULE:
                getCommonRules(ruleList, result, ruleCategoryList, TIMELINES_RULE_CHINESE, ruleCateList);
                break;
            default:
                result = result;
                break;
        }
        return result;
    }

    private void getCommonRules(List<ScoreInstanceRule> ruleList, Map<String, Object> result, List<RuleCategory> ruleCategoryList,
                                String name, List<ScoreInstanceRule> ruleCateList) {
        //获取对应规则下所有子类规则
        Map<String, Object> map = getChild(ruleCategoryList, name, ruleList, ruleCateList);
        if (map.size() > 0) {
            List<Rule> childList = (List<Rule>) map.get(DATA);
            Long weight = (Long) map.get(WEIGHT);
            String scoreType = (String) map.get(SCORE_TYPE);

            //获取对应规则分类下配置的具体规则
            getFinaliyResult(childList, ruleList, result, weight, scoreType);
        }

    }

    private Map<String, Object> getChild(List<RuleCategory> ruleCategoryList, String name, List<ScoreInstanceRule> ruleList, List<ScoreInstanceRule> ruleCateList) {
        Map<String, Object> map = new HashMap<>();
        List<Rule> result = new ArrayList<>();
        List<RuleCategory> cateChild = new ArrayList<>();
        List<RuleCategory> ruleCategoryList1 = ruleCategoryList.stream().filter(e -> name.equals(e.getName())).collect(Collectors.toList());
        if (null == ruleCategoryList1 || ruleCategoryList1.size() <= 0) {
            return map;
        }
        RuleCategory ruleCategory = ruleCategoryList1.get(0);
        orgRecursion(cateChild, ruleCategoryList, ruleCategory.getId());
        if (null == cateChild || cateChild.size() <= 0) {
            return map;
        }

        //获取规则分类的权重
        Long weight = 0L;
        String scoreType = "";
        for (ScoreInstanceRule instanceRule : ruleCateList) {
            String bid = String.valueOf(instanceRule.getBizId());
            //最上级规则分类节点
            if (bid.equals(ruleCategory.getId())) {
                weight = instanceRule.getWeight();
                scoreType = instanceRule.getScoreType();
            }
        }

        //获取所有规则分类对应的规则
        result = ruleMapper.getRuleList(cateChild);
        for (Rule rule : result) {
            rule.setParentCategoryId(ruleCategory.getId());
        }
        map.put(WEIGHT, weight);
        map.put(DATA, result);
        map.put(SCORE_TYPE, scoreType);
        return map;
    }

    public static void orgRecursion(List<RuleCategory> result, List<RuleCategory> ruleCategoryList, String pid) {
        for (RuleCategory category : ruleCategoryList) {
            if (category.getParentId() != null) {
                if (category.getParentId().equals(pid)) {
                    orgRecursion(result, ruleCategoryList, category.getId());
                    if (!category.getParentId().equals("0")) {
                        result.add(category);
                    }
                }
            }
        }
    }

    private void getFinaliyResult(List<Rule> childList, List<ScoreInstanceRule> ruleList, Map<String, Object> result, Long weight, String scoreType) {
        if (null != childList && childList.size() > 0) {
            List<ScoreInstanceRule> list = new ArrayList<>();
            for (Rule rule : childList) {
                for (ScoreInstanceRule instanceRule : ruleList) {
                    if (rule.getId().equals(String.valueOf(instanceRule.getBizId()))) {
                        instanceRule.setParentCategoryId(rule.getParentCategoryId());
                        list.add(instanceRule);
                    }
                }
            }
            result.put(WEIGHT, weight);
            result.put(DATA, list);
            result.put(SCORE_TYPE, scoreType);
        }
    }

    /**
     * 通过任务配置转换成可执行任务(质控任务)
     *
     * @param task1
     * @return
     */
    public JobDefinition castQualityRealTaskByTaskDto(Task task1) {
        Task task = taskMapper.selectById(task1);
        Instance instance = instanceService.getBaseMapper().selectById(task.getBizId());
        Map<String, String> extras = new HashMap<>();
        List<DatasetDescriptor> datasetDescriptors = getDatasetDescriptors(queryTaskDatasetList(task, instance), task);
        extras = castTaskExtras(task, instance);
        DataSourceDescriptor input = castInput(task);

        return JobDefinition.builder()
                .jobId(Long.valueOf(task.getId()))
                .input(input)
                .datasets(datasetDescriptors)
                .extras(extras)
                .taskStatus(task.getStatus())
                .instanceId(task.getBizId())
                .sourceRange(StrUtil.isNotEmpty(task.getResourceRange()) ? Arrays.asList(task.getResourceRange().split(",")) : ListUtil.empty())
                .orgCodeRange(StrUtil.isNotEmpty(task.getOrgcodeRange()) ? Arrays.asList(task.getOrgcodeRange().split(",")) : ListUtil.empty())
                .unbounded(task.getTaskType().equals(CommonConstant.TASK_CONTINUE_TYPE)).build();
    }

    private DataSourceDescriptor castInput(Task task) {
        Map<String, String> inputSetting = new HashMap<>();
        JSONObject json = JSON.parseObject(task.getDatasource());
        inputSetting.put(DataSourceDescriptor.HOSTS, json.getString(CommonConstant.T_DATASOURCE_HOST));
        inputSetting.put(DataSourceDescriptor.SCHEMA, json.getString(CommonConstant.T_DATASOURCE_SCHEMA));
        if (!ObjectUtils.isEmpty(json.get(CommonConstant.T_DATASOURCE_USERNAME)) && !ObjectUtils.isEmpty(json.get(CommonConstant.T_DATASOURCE_PASSWORD))) {
            inputSetting.put(DataSourceDescriptor.USERNAME, json.getString(CommonConstant.T_DATASOURCE_USERNAME));
            inputSetting.put(DataSourceDescriptor.PASSWORD, json.getString(CommonConstant.T_DATASOURCE_PASSWORD));
        }
        return new DataSourceDescriptor(DataSourceDescriptor.DataSourceType.Hive, inputSetting);
    }

    private List<Dataset> queryTaskDatasetList(Task task, Instance instance) {
        List<String> datasetIds = new ArrayList<>();
        task.getDataSets().forEach(dataset -> {
            datasetIds.add(dataset.getDatasetId());
        });
        QueryWrapper<Dataset> dataSetWrapper = new QueryWrapper<>();
        dataSetWrapper.eq("SOURCE_TYPE", CommonConstant.SOURCE_TYPE_INSTANCE);
        dataSetWrapper.in("DATASET_ID", datasetIds);
        return dataSetMapper.selectList(dataSetWrapper);
    }

    private Map<String, String> castTaskExtras(Task task, Instance instance) {
        Map<String, String> extras = new HashMap<>();
        extras.put(JobDefinition.STANDARD_ID, instance.getStandardId().toString());
        extras.put(JobDefinition.STANDARD_NAME, instance.getStandardName());
        extras.put(JobDefinition.countField(0), DatasetDescriptor.DATASET_METADATA_DATE_FILED);
        extras.put(JobDefinition.countValue(0), task.getDataStarttime());
        extras.put(JobDefinition.countSymbol(0), "time_range_start");
        if (StringUtils.isNotEmpty(task.getDataEndtime())) {
            extras.put(JobDefinition.countField(1), DatasetDescriptor.DATASET_METADATA_DATE_FILED);
            extras.put(JobDefinition.countValue(1), task.getDataEndtime());
            extras.put(JobDefinition.countSymbol(1), "time_range_end");
        }
        return extras;
    }


    private List<DatasetDescriptor> getDatasetDescriptors(List<Dataset> list, Task task) {
        List<DatasetDescriptor> datasetDescriptors = new ArrayList<>();
        list.forEach(dataSet -> {
            DatasetDescriptor datasetDescriptor = new DatasetDescriptor(dataSet.getDataSetId(), dataSet.getMetasetCode(), dataSet.getMetasetName());
            if (StringUtils.isEmpty(dataSet.getOrgCodeField())) {
                throw new BizException("数据集:[" + dataSet.getMetasetName() + "]机构编码字段为空，任务启动失败！");
            }
            if (StringUtils.isEmpty(dataSet.getUploadTimeField())) {
                throw new BizException("数据集:[" + dataSet.getMetasetName() + "]上传时间字段为空，任务启动失败！");
            }
            if (StringUtils.isEmpty(dataSet.getResourceField())) {
                throw new BizException("数据集:[" + dataSet.getMetasetName() + "]数据来源字段为空，任务启动失败！");
            }
            datasetDescriptor.putMetadata(DatasetDescriptor.DATASET_METADATA_ORG_FILED, dataSet.getOrgCodeField());
            datasetDescriptor.putMetadata(DatasetDescriptor.DATASET_METADATA_DATE_FILED, dataSet.getUploadTimeField());
            datasetDescriptor.putMetadata(DatasetDescriptor.DATASET_METADATA_SOURCE_FILED, dataSet.getResourceField());
            datasetDescriptor.putMetadata(DatasetDescriptor.DATASET_METADATA_STANDARD_ID, String.valueOf(dataSet.getStandardId()));
            datasetDescriptor.putMetadata(DatasetDescriptor.DATASET_METADATA_STANDARD_NAME, dataSet.getStandardName());
            List<DatasetField> datasetFields = getDataSetFieldList(dataSet);
            datasetFields.forEach(datasetField -> {
                DatasetItemDescriptor datasetItemDescriptor = new DatasetItemDescriptor();
                datasetItemDescriptor.setId(datasetField.getId().intValue());
                datasetItemDescriptor.setCode(datasetField.getFieldName());
                datasetItemDescriptor.setName(datasetField.getElementName());
                datasetItemDescriptor.setDesc(datasetField.getElementDesc());
                datasetItemDescriptor.setType(datasetField.getMetadataType());
                datasetItemDescriptor.setPrimaryKey((datasetField.getIsKey() != null && !datasetField.getIsKey().equals(CommonConstant.FIELD_STATUS_FALSE)));
                datasetDescriptor.putItem(datasetItemDescriptor);
            });
            List<TaskRule> taskRules = task.getRules();
            List<String> taskRuleids = new ArrayList<>();
            taskRules.forEach(taskRule -> {
                taskRuleids.add(taskRule.getRuleId());
            });
            List<InstanceRule> rules = instanceRuleMapper.getInstanceRuleListByDatasetId(dataSet.getDataSetId().toString(), taskRuleids);

            Map<String, List<InstanceRule>> groupRule = rules.stream().collect(Collectors.groupingBy(InstanceRule::getRuleCode));

            groupRule.forEach((ruleCode, instanceRules) -> {
                //合并规则SQL
                if (CommonUtil.isMergeSql(ruleCode)) {
                    List<List<InstanceRule>> partition = Lists.partition(instanceRules, mergeParMax);

                    for (List<InstanceRule> ruleList : partition) {
                        //取一条值域规则
                        Optional<InstanceRule> ruleOptional = ruleList.stream().findFirst();
                        if (ruleOptional.isPresent()) {
                            EvaluationDescriptor descriptor = buildDescriptorOnInstanceRule(ruleOptional.get());
                            descriptor.setMergeSql(true);

                            //将所有值域下arg参数，集成到一条值域规则中
                            List<List<EvaluationDescriptor.Arg>> args = Lists.newArrayList();
                            ruleList.forEach(instanceRule -> args.add(castArgs(instanceRule.getRuleDetailVals())));
                            descriptor.setArgs(args);
                            datasetDescriptor.putEvaluation(descriptor);
                        }
                    }

                } else {
                    instanceRules.forEach(rule -> {
                        datasetDescriptor.putEvaluation(buildDescriptorOnInstanceRule(rule));
                    });
                }
            });

            datasetDescriptors.add(datasetDescriptor);
        });
        return datasetDescriptors;
    }

    private EvaluationDescriptor buildDescriptorOnInstanceRule(InstanceRule rule) {
        EvaluationDescriptor eva = new EvaluationDescriptor();
        eva.setRuleId(Long.parseLong(rule.getRuleId()));
        eva.setRuleName(rule.getRuleName());
        eva.setRuleCode(rule.getRuleCode());
        eva.setCategoryId(Long.parseLong(rule.getRuleCategoryId()));
        eva.setCategoryName(rule.getRuleCategoryName());
        eva.setDataSetItemId(rule.getDatasetField() == null ? null : Long.parseLong(rule.getDatasetField()));
        eva.setType(rule.getRuleType().equals(CommonConstant.RULE_SQL) ? EvaluationDescriptor.EvaluationType.SQL : EvaluationDescriptor.EvaluationType.EXPR);
        eva.setTemplate(rule.getSqlTemp());
        eva.setRecommission(rule.getTwoCalc().equals(CommonConstant.TWO_CALC_TRUE));
        eva.setRecommissionTemplate(rule.getTwoCalcExpr());

        List<List<EvaluationDescriptor.Arg>> coverList = Lists.newArrayList();
        coverList.add(castArgs(rule.getRuleDetailVals()));
        eva.setArgs(coverList);
        return eva;
    }

    private List<EvaluationDescriptor.Arg> castArgs(List<RuleDetailVal> ruleDetailVals) {
        List<EvaluationDescriptor.Arg> list = new ArrayList<>();
        ruleDetailVals.forEach(val -> {
            EvaluationDescriptor.Arg arg = new EvaluationDescriptor.Arg();
            arg.setName(val.getArgName());
            //对参数类型为数据项时进行截取
            if (CommonConstant.VAL_TYPE_FIELD.equals(val.getType())) {
                arg.setValue(val.getVal().substring(val.getVal().indexOf("[") + 1, val.getVal().lastIndexOf("]")));
            } else {
                arg.setValue(val.getVal());
            }
            //字段是否可空
            arg.setNull(val.isNull());
            arg.setType(val.getType());
            list.add(arg);
        });
        return list;
    }

    private List<DatasetField> getDataSetFieldList(Dataset dataSet) {
        DatasetField datasetField = new DatasetField();
        datasetField.setDatasetId(dataSet.getDataSetId());
        QueryWrapper<DatasetField> wrapper = new QueryWrapper<>();
        wrapper.setEntity(datasetField);
        return datasetFieldMapper.selectList(wrapper);
    }

    /**
     * 取消任务
     *
     * @param dto
     */
    private void cancelTask(Task dto) throws Exception {
        Task task = taskMapper.selectById(dto);
        if (CommonConstant.TASK_QUALITY.equals(task.getBizType())) {
            //取消任务
            QuartzJobManager.getInstance().deleteJob(task.getId(), CommonConstant.QCS_JOB_GROUP);
            //中断正在执行的任务
            QuartzJobManager.getInstance().interruptJob(task.getId(), CommonConstant.QCS_JOB_GROUP);
        } else {
            //取消任务
            QuartzJobManager.getInstance().deleteJob(task.getId(), CommonConstant.SCORE_JOB_GROUP);
            //中断正在执行的任务
            QuartzJobManager.getInstance().interruptJob(task.getId(), CommonConstant.SCORE_JOB_GROUP);
        }
    }

    /**
     * 暂停任务
     *
     * @param task
     */
    public void suspendTask(Task task) throws Exception {
        Task saveTask = taskMapper.selectById(task);
        QuartzJobManager.getInstance().pauseJob(task.getId(), CommonConstant.QCS_JOB_GROUP);
        QuartzJobManager.getInstance().interruptJob(task.getId(), CommonConstant.QCS_JOB_GROUP);
        saveTask.setStatus(task.getStatus());
        taskMapper.updateById(saveTask);
    }

    public Map<String, String> checkTask(Task task) throws ParseException {
        Map<String, String> map = new HashMap<>();
        map.put("code", "true");
        Task finalTask = taskMapper.selectById(task);
        boolean isQuality = CommonConstant.TASK_QUALITY.equals(task.getBizType());
        //判断当前任务的质控实例是否存在正在执行的质控任务
        QueryWrapper<Task> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("BIZ_ID", task.getBizId());
        queryWrapper.eq("BIZ_TYPE", task.getBizType());
        queryWrapper.eq("status", CommonConstant.TASK_RUN_STATUS);
        int count = taskMapper.selectCount(queryWrapper);
        if (count > 0) {
            map.put("code", "false");
            map.put("isExcute", "0");
            map.put("msg", isQuality ? "该任务的质控实例存在正在执行的质控任务，不可启动该任务！"
                    : "该任务的评分对象存在正在执行的评分任务，不可启动该任务！");
            return map;
        }

        //质控任务
        if (EnumConstant.TaskStatus.EXEC_CANCEL.getValue() != Integer.parseInt(task.getStatus()) && !parallel && isQuality) {
            List<TaskProgress> taskProgresses = taskProgressService.getBaseMapper().selectPauseByInstanceIdAndExcludeTaskId(task.getBizId(), task.getId());
            if (CollUtil.isNotEmpty(taskProgresses)) {
                map.put("code", "false");
                map.put("isExcute", "0");
                map.put("msg", "当前实例存在暂停的任务，后台Spark还在执行，不可启动该任务！");
                return map;
            }
        }
        //判断质控任务的数据范围与同一质控实例上的已完成或已暂停的任务是否有重叠部分
        QueryWrapper<Task> instanceQueryWrapper = new QueryWrapper<>();
        instanceQueryWrapper.eq("BIZ_ID", task.getBizId());
        instanceQueryWrapper.eq("BIZ_TYPE", task.getBizType());
        instanceQueryWrapper.in("STATUS", new String[]{CommonConstant.TASK_COMPLETE_STATUS, CommonConstant.TASK_SUSPEND_STATUS});
        instanceQueryWrapper.notIn("ID", task.getId());
        List<Task> list = taskMapper.selectList(instanceQueryWrapper);
        boolean isQualityContinue = isQuality && CommonConstant.TASK_CONTINUE_TYPE.equals(task.getTaskType());
        if (isSame(list, finalTask) && !isQualityContinue) {
            map.put("code", "false");
            map.put("isExcute", "0");
            map.put("msg", isQuality ? "该任务与其它有效质控任务存在重叠部分，执行后将覆盖原有质控结果，确认要继续执行吗？"
                    : "该任务与其他有效评分任务存在重叠部分，执行后将覆盖原有评分结果，确认要继续执行吗？");
            return map;
        }
        return map;
    }

    private boolean isSame(List<Task> list, Task task) throws ParseException {
        Long oriStartTime = StringUtils.isNotEmpty(task.getDataStarttime()) ? parserDate(task.getDataStarttime()) : null;
        Long oriEndTime = StringUtils.isNotEmpty(task.getDataEndtime()) ? parserDate(task.getDataEndtime()) : null;
        for (Task sTask : list) {
            Long startTime = StringUtils.isNotEmpty(sTask.getDataStarttime()) ? parserDate(sTask.getDataStarttime()) : null;
            Long endTime = StringUtils.isNotEmpty(sTask.getDataEndtime()) ? parserDate(sTask.getDataEndtime()) : null;
            if (null != endTime) {
                if (null != oriEndTime) {
                    boolean isTrue = oriStartTime >= startTime && oriStartTime < endTime;
                    boolean isTrue2 = oriEndTime > startTime && oriEndTime <= endTime;
                    boolean isTrue3 = oriEndTime > startTime && oriEndTime > endTime && oriStartTime < endTime;
                    boolean isTrue4 = oriEndTime.equals(startTime) && oriEndTime.equals(endTime);
                    if (isTrue || isTrue2 || isTrue3 || isTrue4) {
                        return true;
                    }
                } else {
                    if (oriStartTime < endTime) {
                        return true;
                    }
                }
            } else {
                if (null != oriEndTime) {
                    if (oriStartTime >= startTime || oriEndTime > startTime) {
                        return true;
                    }
                } else {
                    if (oriStartTime >= startTime) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 前端查询任务进度信息
     *
     * @param taskId 任务id
     */
    public TaskProgressResp queryTaskProgressInfo(Long taskId) {
        TaskProgressResp.TaskProgressRespBuilder progressResp = TaskProgressResp.builder();

        QueryWrapper<Task> queryTaskWrapper = new QueryWrapper<>();
        Task task = new Task();
        task.setId(String.valueOf(taskId));
        queryTaskWrapper.setEntity(task);
        Task taskInfo = taskMapper.selectOne(queryTaskWrapper);

        //执行日期
        DateTime cycleDay;
        //执行进度
        int execProgress = 0;
        //当日执行进度
        int progressOfToday = 0;
        cn.hutool.json.JSONObject datasource = JSONUtil.parseObj(taskInfo.getDatasource());
        //主机地址
        String host = datasource.getStr("hosts");
        //数据库
        String schema = datasource.getStr("schema");

        TaskProgress progress = new TaskProgress();
        progress.setTaskId(taskId);
        List<TaskProgress> taskProgresses = taskProgressService.selectListByObject(progress);
        if (CollUtil.isNotEmpty(taskProgresses)) {
            Optional<TaskProgress> max = taskProgresses.stream().max(Comparator.comparing(TaskProgress::getCycleDay));
            cycleDay = max.map(value -> new DateTime(value.getCycleDay()))
                    .orElseGet(() ->
                            DateUtil.parseDate(taskInfo.getDataStarttime()));
        } else {
            //没有任务进度，取开始时间
            cycleDay = DateUtil.parseDate(taskInfo.getDataStarttime());
        }

        //设置任务信息
        progressResp.host(host)
                .schema(schema)
                .cycleDay(cycleDay.toDateStr())
                .taskType(taskInfo.getTaskType())
                .jobDes(taskInfo.getTaskDes());
        LambdaQueryWrapper<TaskProgress> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskProgress::getCycleDay, cycleDay);
        queryWrapper.eq(TaskProgress::getTaskId, taskId);
        TaskProgress taskProgress = taskProgressService.getOne(queryWrapper);
        if(taskProgress != null && StringUtils.isNotEmpty(taskProgress.getSparkTaskId())){
            progressResp.hiveUrl(masterUrl + "/cluster/app/" + taskProgress.getSparkTaskId());
        }

        EnumConstant.TaskStatus taskStatus = EnumConstant.TaskStatus.getEnum(Integer.parseInt(taskInfo.getStatus()));

        SqlExecHistory history = new SqlExecHistory();
        history.setTaskId(taskId);
        QueryWrapper<SqlExecHistory> wrapper = new QueryWrapper<>();
        wrapper.setEntity(history);
        List<SqlExecHistory> sqlExecHistories = execHistoryService.list(wrapper);

        if (CollUtil.isEmpty(sqlExecHistories)) {
            return progressResp
                    .execProgress(CommonConstant.TASK_CONTINUE_TYPE.equals(taskInfo.getTaskType()) ? "持续任务没有总进度" : StrUtil.format("{}%", execProgress))
                    .progressOfToday(StrUtil.format("{}%", progressOfToday))
                    .build();
        }

        //当日执行进度 = 当日已执行的SQL量 / 当日总SQL量 * 100
        List<SqlExecHistory> todayAllSql = sqlExecHistories.stream().filter((Predicate<SqlExecHistory>)
                sqlExecHistory -> {
                    return sqlExecHistory != null
                            && sqlExecHistory.getCycleDay().compareTo(cycleDay) == 0;
                }).collect(Collectors.toList());

        if (CollUtil.isNotEmpty(todayAllSql)) {
            progressOfToday = calProgress(todayAllSql, todayAllSql.size());
        }

        if (CommonConstant.TASK_TEMP_TYPE.equals(taskInfo.getTaskType())
            || CommonConstant.TASK_NEW_ORG_TYPE.equals(taskInfo.getTaskType())) {
            String startTime = taskInfo.getDataStarttime();
            String endTime = taskInfo.getDataEndtime();

            //计算任务间距日期； +1时加上当天
            long betweenDay = DateBetween.create(DateUtil.parse(startTime), DateUtil.parse(endTime)).between(DateUnit.DAY) + CURRENT_DAY_FLAG;
            execProgress = calProgress(sqlExecHistories, (int) (todayAllSql.size() * betweenDay));
        }

        StringBuilder execProgressBuilder = new StringBuilder();
        StringBuilder progressOfTodayBuilder = new StringBuilder();

        if (taskStatus == EnumConstant.TaskStatus.EXEC_SUCCEED) {
            progressOfTodayBuilder.append(CommonConstant.TASK_PROGRESS_COMPLETELY).append("%");
            execProgressBuilder.append(CommonConstant.TASK_PROGRESS_COMPLETELY).append("%");
        } else {
            if (progressOfToday == CommonConstant.TASK_PROGRESS_RATIO) {
                progressOfTodayBuilder.append(progressOfToday).append("%(SQL已执行完成,等待各个维度汇总...)");
            } else {
                progressOfTodayBuilder.append(progressOfToday).append("%");
            }

            if (execProgress == CommonConstant.TASK_PROGRESS_RATIO) {
                execProgressBuilder.append(execProgress).append("%(SQL已执行完成,等待各个维度汇总...)");
            } else {
                execProgressBuilder.append(execProgress).append("%");
            }
        }

        return progressResp
                .progressOfToday(progressOfTodayBuilder.toString())
                .execProgress(CommonConstant.TASK_CONTINUE_TYPE.equals(taskInfo.getTaskType()) ? "持续任务没有总进度"
                        : execProgressBuilder.toString()).build();
    }

    /**
     * 已执行的SQL量 / 当日总SQL量 * 100
     * 最大不超过99%，剩余的1%留给维度表写入
     *
     * @param sqlExecHistories
     * @return
     */
    private int calProgress(List<SqlExecHistory> sqlExecHistories, int sqlCount) {
        BigDecimal succeedCount = BigDecimal.valueOf(sqlExecHistories.stream().filter((Predicate<SqlExecHistory>) sqlExecHistory ->
                EnumConstant.TaskStatus.NOT_EXEC.getValue() != sqlExecHistory.getStatus()
                        && EnumConstant.TaskStatus.EXECUTING.getValue() != sqlExecHistory.getStatus()).count());
        return Math.min(succeedCount.divide(BigDecimal.valueOf(sqlCount), 2, BigDecimal.ROUND_HALF_UP)
                .multiply(BigDecimal.valueOf(100)).intValue(), CommonConstant.TASK_PROGRESS_RATIO);
    }

    /**
     * 启动持续任务
     *
     * @param continueTask
     */
    public void runContinueTask(ContinueTaskRequest continueTask) throws Exception {
        String jobGroupName = CommonConstant.QCS_JOB_GROUP;
        for (String taskId : continueTask.getTaskIds()) {
            Task task = taskMapper.getDataById(taskId);
            if (task == null) {
                continue;
            }
            Map<String, String> statusMap = checkTask(task);
            if(CommonConstant.FALSE.equals(String.valueOf(statusMap.get(CommonConstant.CODE)))){
                continue;
            }
            if (DateUtils.dataToStamp(continueTask.getCycleDay()) >= DateUtils.dataToStamp(DateUtil.today())) {
                continue;
            }
            TaskProgress taskProgress = taskProgressService.getBaseMapper().selectLastCycleDayByTaskId(Long.valueOf(task.getId()));
            if (taskProgress != null &&
                DateUtils.dataToStamp(continueTask.getCycleDay()) <= taskProgress.getCycleDay().getTime()) {
                continue;
            }
            boolean flag = (taskProgress != null) && (EnumConstant.TaskStatus.EXECUTING.getValue() == taskProgress.getStatus() || EnumConstant.TaskStatus.NOT_EXEC.getValue() == taskProgress.getStatus());
            if (flag) {
                continue;
            }
            if (taskProgress != null &&
                EnumConstant.TaskStatus.EXEC_PAUSE.getValue() == taskProgress.getStatus()) {
                //恢复任务
                QuartzJobManager.getInstance().resumeJob(String.valueOf(task.getId()), jobGroupName);
            } else {
                JobDefinition jobDefinition = castQualityRealTaskByTaskDto(task);
                jobDefinition.getExtras().put(JobDefinition.CONTINUE_CYCLEDAY, continueTask.getCycleDay());
                Map map = JSON.parseObject(JSON.toJSONString(jobDefinition), Map.class);
                //创建之前先删除原有job
                QuartzJobManager.getInstance().deleteJob(String.valueOf(task.getId()), jobGroupName);
                //创建任务并按照执行计划执行
                QuartzJobManager.getInstance().addJob(DataQualityJobAbstract.class, task, jobGroupName, dataQualityCron, map);
                QuartzJobManager.getInstance().runJobNow(String.valueOf(task.getId()), jobGroupName);
                //更新状态
                Task temp = new Task();
                temp.setId(task.getId());
                temp.setStatus(CommonConstant.TASK_RUN_STATUS);
                temp.setTaskStarttime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                taskMapper.updateById(temp);
            }
        }
    }
}

