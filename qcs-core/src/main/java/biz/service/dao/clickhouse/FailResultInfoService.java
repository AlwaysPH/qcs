package biz.service.dao.clickhouse;

import cn.hutool.core.map.MapUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Maps;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.core.biz.service.dao.mysql.*;
import com.gwi.qcs.model.domain.clickhouse.FailResultInfo;
import com.gwi.qcs.model.entity.PreDataCondition;
import com.gwi.qcs.model.mapper.clickhouse.FailResultInfoMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * (FailResultInfo)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:35
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class FailResultInfoService extends AbstractSuperServiceImpl<FailResultInfoMapper, FailResultInfo> {

    @Autowired
    DatasetFiledService datasetFiledService;

    @Autowired
    DatasetService datasetService;

    @Autowired
    RuleService ruleService;

    @Autowired
    RuleCategoryService ruleCategoryService;

    @Autowired
    ParameterService parameterService;

    @Getter
    @Setter
    private Page<FailResultInfo> pageInfo;

    /**
     * 删除维度数据
     *
     * @param condition 条件
     */
    @Override
    public void delByPreDataCondition(PreDataCondition condition) {
        getBaseMapper().delByCycleDayAndDatasetIdAndStandardId(condition.getCycleDay(), condition.getDatasetIds(), condition.getStandardId());

        Map<String, Object> map = MapUtil
            .builder("CYCLE_DAY", (Object) condition.getCycleDay())
            .put("STANDARD_ID", condition.getStandardId())
            .build();
        querySleepByConditionMap(map);

    }


    /**
     * 根据datasetIds及日期范围查询(上层详情页)
     *
     * @return
     */
    public List<FailResultInfo> failResultInfos(FailResultInfo failResultInfo, List<String> datasetIds, String beginDate, String endDate, List<String> orgCodes, Page page) {
        if (StringUtils.isBlank(beginDate) || StringUtils.isBlank(endDate)) {
            return null;
        }
        if (page != null) {
            pageInfo = PageHelper.startPage(page.getPageNum(), page.getPageSize());
        }
        QueryWrapper<FailResultInfo> queryWrapper = new QueryWrapper<>(failResultInfo);
        queryWrapper.between("CYCLE_DAY", beginDate, endDate);
        if (datasetIds != null && !datasetIds.isEmpty()) {
            queryWrapper.in("DATASET_ID", datasetIds);
        }
        if(orgCodes != null && !orgCodes.isEmpty()){
            queryWrapper.in("ORG_CODE", orgCodes);
        }
        queryWrapper.orderByDesc("BIZ_TIME");
        //导出时获取最大导出数
        if(page == null){
            queryWrapper.last("limit " + parameterService.getMaxExportNum());
        }
        if(failResultInfo.getLimitOne()){
            queryWrapper.last("limit 1");
        }
        List<FailResultInfo> list = this.list(queryWrapper);
        return expandParamHandle(list, page == null);
    }


    private List<FailResultInfo> expandParamHandle(List<FailResultInfo> list, boolean isExport){
        Map<String, String> ruleNameMap = Maps.newHashMap();
        Map<String, String> ruleCategoryMap = Maps.newHashMap();
        if(!isExport){
            int maxCount = parameterService.getMaxErrorSize();
            if(maxCount < this.getPageInfo().getPageSize() * this.getPageInfo().getPageNum()){
                int size = list.size();
                int subNum = list.size() - maxCount % this.getPageInfo().getPageSize();
                for(int i = size - 1; i > size - subNum - 1; i--){
                    list.remove(list.get(i));
                }
            }
            this.getPageInfo().setTotal(Math.min(maxCount, this.getPageInfo().getTotal()));
        }
        for(FailResultInfo sample : list){
            sample.setDatasetItemCode(datasetFiledService.getCodeById(sample.getDatasetItemId()));
            sample.setDatasetCode(datasetService.getCodeById(sample.getDatasetId()));
            sample.setDatasetName(datasetService.getNameById(sample.getDatasetId()));
            sample.setRuleName(ruleService.getNameById(sample.getRuleId(), ruleNameMap));
            sample.setCategoryName(ruleCategoryService.getNameById(sample.getCategoryId(), ruleCategoryMap));
            sample.setFieldName(sample.getDatasetItemCode());
            sample.setMetaDataName(datasetFiledService.getNameById(sample.getDatasetItemId()));
            sample.setCreationTime(sample.getBizTime());
        }
        return list;
    }
}