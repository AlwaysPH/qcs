package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.mapper.mysql.*;
import com.gwi.qcs.service.api.dto.RuleCategoryListReq;
import com.gwi.qcs.service.api.dto.RuleCategoryListResp;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Service
@DS(DbTypeConstant.MYSQL)
public class RuleCategoryService extends SuperServiceImpl<RuleCategoryMapper, RuleCategory> {
    @Resource
    private RuleCategoryMapper ruleCategoryMapper;


    @Resource
    private RuleMapper ruleMapper;

    public String getNameById(String id, Map<String, String> map){
        if(! map.isEmpty()){
            return map.getOrDefault(id, CommonConstant.DASH);
        }
        map = this.list().stream().collect(Collectors.toMap(RuleCategory::getId, RuleCategory::getName));
        return getNameById(id, map);
    }

    /**
     * 获取所有分类规则
     *
     * @param ruleCategory
     * @return
     */
    public List<RuleCategory> getRuleCategory(RuleCategory ruleCategory) {
        Wrapper<RuleCategory> queryWrapper = new QueryWrapper<>();
        //使用${ew.customSqlSegment}会使queryWrapper.setEntity()方法失效???
        if (StringUtils.isNotEmpty(ruleCategory.getId())) {
            ((QueryWrapper<RuleCategory>) queryWrapper).eq("id", ruleCategory.getId());
        }
        if (StringUtils.isNotEmpty(ruleCategory.getName())) {
            ((QueryWrapper<RuleCategory>) queryWrapper).eq("name", ruleCategory.getName());
        }
        if (StringUtils.isNotEmpty(ruleCategory.getStatus())) {
            ((QueryWrapper<RuleCategory>) queryWrapper).eq("STATUS", ruleCategory.getStatus());
        }
        ((QueryWrapper<RuleCategory>) queryWrapper).orderByDesc("CREATE_AT");
        List<RuleCategory> list = ruleCategoryMapper.selectList(queryWrapper);
        List<RuleCategory> resultList = new ArrayList<>();
        if (list != null && !list.isEmpty()) {
            resultList = recursionList(list, "0");
        }
        return resultList;
    }

    public List<RuleCategoryListResp> getAllRule(RuleCategoryListReq ruleCategoryListReq){
        List<RuleCategory> ruleCategoryList = ruleCategoryMapper.getByScoreId(ruleCategoryListReq.getScoreInstanceId());
        if (CollectionUtils.isNotEmpty(ruleCategoryList)) {
            ruleCategoryList = recursionList(ruleCategoryList, "0");
        }
        return getChildren(ruleCategoryList);
    }

    private List<RuleCategoryListResp> getChildren(List<RuleCategory> ruleCategoryList){
        List<RuleCategoryListResp> ruleCategoryListRespList = new ArrayList<>();
        for(RuleCategory rule : ruleCategoryList){
            RuleCategoryListResp ruleCategoryListResp = new RuleCategoryListResp();
            ruleCategoryListResp.setParentId(rule.getParentId());
            ruleCategoryListResp.setName(rule.getName());
            ruleCategoryListResp.setId(rule.getId());
            List<RuleCategory> ruleList = rule.getChildren();
            ruleCategoryListResp.setChildren(CollectionUtils.isEmpty(ruleList) ? null : getChildren(ruleList));
            ruleCategoryListRespList.add(ruleCategoryListResp);
        }
        return ruleCategoryListRespList;
    }

    /**
     * 通过实例获取配置的规则分类
     *
     * @param instenesId
     * @return
     */
    public List<RuleCategory> getRuleCategory(String instenesId) {
        List<RuleCategory> resultList = new ArrayList<>();
        List<Rule> list = ruleCategoryMapper.queryQcsInstanceRuleList(instenesId);
        if (list != null && !list.isEmpty()) {
            List<RuleCategory> tresultList = new ArrayList<>();
            Set<String> rulesIds = new HashSet<>();
            for (Rule ins : list) {
                rulesIds.add(ins.getId());
            }
            List<Rule> rules = ruleMapper.selectBatchIds(rulesIds);
            Set<String> categoryIds = new HashSet<>();
            for (Rule rule : rules) {
                categoryIds.add(rule.getCategoryId());
            }
            tresultList = foundParent(categoryIds);
            resultList = recursionList(tresultList, "0");
        }
        return resultList;
    }

    /**
     * 通过子类反查所有父类
     *
     * @param categoryIds
     * @return
     */
    private List<RuleCategory> foundParent(Set<String> categoryIds) {
        List<RuleCategory> resultList = new ArrayList<>();
        Wrapper<RuleCategory> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) queryWrapper).in("ID", categoryIds);
        resultList = ruleCategoryMapper.selectList(queryWrapper);
        Set<String> categoryIdss = new HashSet<>();
        for (RuleCategory r : resultList) {
            if (r.getParentId() != null && !r.getParentId().equals("0") && !r.getParentId().equals("")) {
                categoryIdss.add(r.getParentId());
            }
        }
        if (!categoryIdss.isEmpty()){
            resultList.addAll(foundParent(categoryIdss));
        }
        return resultList;
    }

    /**
     * 4.2新增规则分类接口
     *
     * @param ruleCategory
     * @return
     */
    @Transactional
    public String addRuleCategory(RuleCategory ruleCategory) {
        if (ruleCategory.getParentId() == null) {
            ruleCategory.setParentId("0");
        }
        Wrapper<RuleCategory> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) queryWrapper).eq("NAME", ruleCategory.getName());
        long count = ruleCategoryMapper.selectCount(queryWrapper);
        if (count > 0) {
            throw new BizException("当前分类名称已被使用，请重新设置！");
        }
        ruleCategoryMapper.insert(ruleCategory);
        return ruleCategory.getId();
    }

    /**
     * 修改规则分类接口
     *
     * @param ruleCategory
     * @return
     */
    @Transactional
    public int updateRuleCategory(RuleCategory ruleCategory) {
        checkUpdateRuleCategory(ruleCategory);
        return ruleCategoryMapper.updateById(ruleCategory);
    }

    private void checkUpdateRuleCategory(RuleCategory ruleCategory) {
        Wrapper<RuleCategory> wrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) wrapper).notIn("ID", ruleCategory.getId());
        List<RuleCategory> list = ruleCategoryMapper.selectList(wrapper);
        list.forEach(ruleCategoryTemp -> {
            if (ruleCategoryTemp.getName().equals(ruleCategory.getName())) {
                throw new BizException("规则分类名称已存在！");
            }
        });
    }

    @Autowired
    private ScoreInstanceRuleMapper scoreInstanceRuleMapper;

    @Autowired
    private InstanceRuleMapper instanceRuleMapper;

    /**
     * 4.4删除规则分类接口
     *
     * @param id
     * @return
     */
    @Transactional

    public int deleteRuleCategory(String id) {
        RuleCategory rule = ruleCategoryMapper.selectById(id);
        rule.setParentId(id);
        Wrapper<RuleCategory> ruleCategoryQueryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) ruleCategoryQueryWrapper).eq("parent_id", id);
        int count = ruleCategoryMapper.selectCount(ruleCategoryQueryWrapper);
        if (count > 0) {
            throw new BizException("当前【" + rule.getName() + "】节点下有子节点，不允许删除，请先清除分类下的所有子分类和规则子项!");
        }
        Rule r = new Rule();
        r.setCategoryId(id);
        Wrapper<Rule> ruleQueryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) ruleQueryWrapper).setEntity(r);
        int count2 = ruleMapper.selectCount(ruleQueryWrapper);
        if (count2 > 0) {
            throw new BizException("当前【" + rule.getName() + "】分类下有规则子项，不允许删除!");
        }
        checkScoreInstanceRule(rule);
        checkInstanceRule(rule);
        return ruleCategoryMapper.deleteById(id);
    }

    private void checkInstanceRule(RuleCategory rule) {
        Wrapper<Rule> ruleWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) ruleWrapper).eq("CATEGORY_ID", rule.getId());
        List<Rule> rules = ruleMapper.selectList(ruleWrapper);
        List<String> ruleIds = new ArrayList<>();
        rules.forEach(rule1 -> {
            ruleIds.add(rule1.getId());
        });
        long count = 0L;
        if (ruleIds != null && !ruleIds.isEmpty()) {
            Wrapper<InstanceRule> wrapper = new QueryWrapper<>();
            ((QueryWrapper<InstanceRule>) wrapper).in("RULE_ID", ruleIds);
            count = instanceRuleMapper.selectCount(wrapper);
        }
        if (count > 0) {
            throw new BizException("该规则分类已被使用，不可进行此操作！");
        }
    }

    private void checkEffectChiled(String id) {
        RuleCategory rule = ruleCategoryMapper.selectById(id);
        rule.setParentId(id);
        Wrapper<RuleCategory> ruleCategoryQueryWrapper = new QueryWrapper<>();
        ((QueryWrapper<RuleCategory>) ruleCategoryQueryWrapper).eq("parent_id", id);
        ((QueryWrapper<RuleCategory>) ruleCategoryQueryWrapper).eq("status", CommonConstant.ENABLE_0);
        int count = ruleCategoryMapper.selectCount(ruleCategoryQueryWrapper);
        if (count > 0) {
            throw new BizException("当前【" + rule.getName() + "】节点下存在有效子节点，请先停用分类下的所有子分类和规则子项!");
        }
        Rule r = new Rule();
        r.setCategoryId(id);
        r.setStatus(CommonConstant.ENABLE_0);
        Wrapper<Rule> ruleQueryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) ruleQueryWrapper).setEntity(r);
        int count2 = ruleMapper.selectCount(ruleQueryWrapper);
        if (count2 > 0) {
            throw new BizException("当前【" + rule.getName() + "】分类下存在有效的规则子项，不允许停用!");
        }
    }

    private void checkScoreInstanceRule(RuleCategory rule) {
        Wrapper<Rule> ruleWrapper = new QueryWrapper<>();
        ((QueryWrapper<Rule>) ruleWrapper).eq("CATEGORY_ID", rule.getId());
        List<Rule> rules = ruleMapper.selectList(ruleWrapper);
        List<String> ruleIds = new ArrayList<>();
        rules.forEach(rule1 -> {
            ruleIds.add(rule1.getId());
        });
        Wrapper<ScoreInstanceRule> wrapper = new QueryWrapper<>();
        ((QueryWrapper<ScoreInstanceRule>) wrapper).eq("biz_type", 0);
        ((QueryWrapper<ScoreInstanceRule>) wrapper).eq("biz_id", rule.getId());
        long cout = 0L;
        cout += scoreInstanceRuleMapper.selectCount(wrapper);
        if (ruleIds != null && !ruleIds.isEmpty()) {
            Wrapper<ScoreInstanceRule> wrapper1 = new QueryWrapper<>();
            ((QueryWrapper<ScoreInstanceRule>) wrapper1).eq("biz_type", 1);
            ((QueryWrapper<ScoreInstanceRule>) wrapper1).in("biz_id", ruleIds);
            cout += scoreInstanceRuleMapper.selectCount(wrapper1);
        }
        if (cout > 0) {
            throw new BizException("该规则分类已被使用，不可进行此操作！");
        }
    }

    /**
     * 4.5修改规则分类状态接口
     *
     * @param ruleCategory
     * @return
     */
    @Transactional
    public int switchRuleCategory(RuleCategory ruleCategory) {
        if (ruleCategory.getStatus().equals(CommonConstant.DISABLE_1)) {
            checkEffectChiled(ruleCategory.getId());
            checkScoreInstanceRule(ruleCategory);
            checkInstanceRule(ruleCategory);
        }
        return ruleCategoryMapper.updateById(ruleCategory);
    }

    /**
     * 递归遍历父子关系
     *
     * @param list
     * @param id
     * @return
     */
    private static List<RuleCategory> recursionList(List<RuleCategory> list, String id) {
        List<RuleCategory> resultList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++){
            if (list.get(i).getParentId().equals(id)) {
                list.get(i).setChildren(recursionList(list, list.get(i).getId()));
                resultList.add(list.get(i));
            }
        }
        return resultList;
    }


}
