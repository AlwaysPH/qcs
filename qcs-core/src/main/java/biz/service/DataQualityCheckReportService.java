package biz.service;

import cn.hutool.core.util.NumberUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.value.dto.common.CommonResponse;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.core.biz.service.dao.clickhouse.DataVolumeStatisticsService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.InstanceQualityService;
import com.gwi.qcs.core.biz.service.dao.mysql.RuleCategoryQualityService;
import com.gwi.qcs.model.domain.clickhouse.DataVolumeStatistics;
import com.gwi.qcs.model.domain.mysql.*;
import com.gwi.qcs.model.mapper.mysql.DatasetMapper;
import com.gwi.qcs.model.mapper.mysql.RuleCategoryMapper;
import com.gwi.qcs.model.mapper.mysql.RuleMapper;
import com.gwi.qcs.model.mapper.mysql.ScoreInstanceMapper;
import com.gwi.qcs.service.api.dto.OrgQualityReportReq;
import com.gwi.qcs.service.api.dto.OrgQualityReportResp;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DataQualityCheckReportService {

    /**
     * 最顶层级的id
     */
    private static final Long TOP_ID = 0L;

    private Logger log = LoggerFactory.getLogger(DataQualityCheckReportService.class);
    private static final Integer Q_DAY = 1;
    private static final Integer Q_WEEK = 2;
    private static final Integer Q_MONTH = 3;
    private static final String YMD = "yyyy-MM-dd";
    DecimalFormat df = new DecimalFormat("#0.00");
    @Resource
    private ScoreInstanceMapper scoreInstanceMapper;

    @Resource
    private RuleCategoryMapper ruleCategoryMapper;

    @Resource
    private DatasetMapper dataSetMapper;

    @Resource
    private InstanceQualityService instanceQualityService;

    @Resource
    private DatasetQualityService datasetQualityService;

    @Resource
    private RuleCategoryQualityService ruleCategoryQualityService;

    @Resource
    private DataVolumeStatisticsService dataVolumeStatisticsService;

    public CommonResponse<OrgQualityReportResp> getOrgQualityReport(OrgQualityReportReq orgQualityReportReq) {
        ScoreInstance scoreInstance = scoreInstanceMapper.selectById(orgQualityReportReq.getInstanceId());
        if (scoreInstance == null) {
            return CommonResponse.failed("未找到评分实例对象");
        }
        orgQualityReportReq.setOrgCode(scoreInstance.getOrgCode());
        List<InstanceQuality> initList = instanceQualityService.getInstanceQualityByDate(orgQualityReportReq, orgQualityReportReq.getQueryType());
        if (initList.isEmpty()) {
            return CommonResponse.succeed(new OrgQualityReportResp());
        }
        return CommonResponse.succeed(getByDay(orgQualityReportReq, initList, scoreInstance));
    }

    private OrgQualityReportResp getByDay(OrgQualityReportReq orgQualityReportReq, List<InstanceQuality> initList, ScoreInstance scoreInstance) {
        OrgQualityReportResp resp = new OrgQualityReportResp();
        InstanceQuality instanceQuality = initList.get(0);
        OrgQualityReportResp.OverallSituation overall = castOverallSituation(orgQualityReportReq, instanceQuality, scoreInstance);
        List<OrgQualityReportResp.UploadWaveSituation> uploadWaveSituation = new ArrayList<>();
        try {
            uploadWaveSituation = castUploadWaveSituation(orgQualityReportReq, instanceQuality);
        } catch (ParseException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        List<OrgQualityReportResp.QualityCheckRuleSituation> qualityCheckRule = castQualityCheckRuleSituation(orgQualityReportReq, instanceQuality).stream().limit(5).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(qualityCheckRule)) {
            overall.setProblemMostRuleName(qualityCheckRule.get(0).getRuleName());
        } else {
            overall.setProblemMostRuleName("-");
        }
        List<OrgQualityReportResp.DataSetCheckSituation> dataSetCheck = castDataSetCheckSituation(orgQualityReportReq, instanceQuality);
        if (!CollectionUtils.isEmpty(dataSetCheck)) {
            overall.setProblemMostDataSet(dataSetCheck.get(0).getTableName());
        } else {
            overall.setProblemMostDataSet("-");
        }
        String analyze = castAnalyze(overall, qualityCheckRule, dataSetCheck);
        resp.setAnalyze(analyze);
        resp.setDataSetCheck(dataSetCheck);
        resp.setOverall(overall);
        resp.setQualityCheckRule(qualityCheckRule);
        resp.setUploadWave(uploadWaveSituation);
        return resp;
    }

    /**
     * 综合分析
     *
     * @param overall
     * @param qualityCheckRule
     * @param dataSetCheck
     * @return
     */
    private String castAnalyze(OrgQualityReportResp.OverallSituation overall, List<OrgQualityReportResp.QualityCheckRuleSituation> qualityCheckRule, List<OrgQualityReportResp.DataSetCheckSituation> dataSetCheck) {
        StringBuffer sb = new StringBuffer();
        sb.append(overall.getDateRange());
        if (StringUtils.isNotBlank(overall.getOrgSize()) && !overall.getOrgSize().contains(CommonConstant.DASH)) {
            sb.append("，共对<span style=\"color:blue;\">");
            sb.append(overall.getOrgSize() + "</span>家机构进行了数据质控，您排名<span style=\"color:blue;\">");
            sb.append(overall.getTopBang() + "</span>名，");
        } else {
            sb.append("的数据质控报告，");
        }
        sb.append("从数据问题类型分析，问题主要发生在");
        List<String> ruleList = qualityCheckRule.stream().map(OrgQualityReportResp.QualityCheckRuleSituation::getRuleName).collect(Collectors.toList());
        sb.append(String.join("，", ruleList) + "等方面；从数据集分析，主要产生在");
        List<String> datasetList = dataSetCheck.stream().map(dataSetCheckSituation -> {
            return dataSetCheckSituation.getTableCode() + "（" + dataSetCheckSituation.getTableName() + "）";
        }).collect(Collectors.toList());
        sb.append(String.join("，", datasetList) + "。请尽快分析原因，尽快进行修改上传");
        return sb.toString();
    }

    /**
     * 数据集检查情况(问题数占比前5位)
     *
     * @param orgQualityReportReq
     * @param instanceQuality
     * @return
     */
    private List<OrgQualityReportResp.DataSetCheckSituation> castDataSetCheckSituation(OrgQualityReportReq orgQualityReportReq, InstanceQuality instanceQuality) {
        List<OrgQualityReportResp.DataSetCheckSituation> dataSetCheckSituations = new ArrayList<>();
        List<Dataset> datasetLists = dataSetMapper.selectList(new QueryWrapper<>());
        Map<Long, Dataset> datasetMap = datasetLists.stream().collect(Collectors.toMap(Dataset::getDataSetId, dataset -> dataset));
        List<DataSetQuality> datasetList = datasetQualityService.getDatasetQualityTop5(orgQualityReportReq, orgQualityReportReq.getQueryType());
        if (datasetList == null || datasetList.isEmpty()) {
            return dataSetCheckSituations;
        } else {
            for (DataSetQuality datasetQuality : datasetList) {
                OrgQualityReportResp.DataSetCheckSituation dataSetCheckSituation = new OrgQualityReportResp.DataSetCheckSituation();
                dataSetCheckSituation.setTableCode(datasetQuality.getDataSetCode());
                dataSetCheckSituation.setTableName(datasetMap.get(Long.parseLong(datasetQuality.getDataSetId())) == null ? "" : datasetMap.get(Long.parseLong(datasetQuality.getDataSetId())).getMetasetName());
                dataSetCheckSituation.setCheckProblemAmount(datasetQuality.getCheckFailTimes() + "");
                dataSetCheckSituation.setCheckTotalAmount(datasetQuality.getCheckTimes() + "");
                dataSetCheckSituation.setProblemRatio(datasetQuality.getCheckTimes() == null || datasetQuality.getCheckTimes().longValue() == 0L ? "-" : df.format(new BigDecimal(datasetQuality.getCheckFailTimes()).divide(new BigDecimal(datasetQuality.getCheckTimes()), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal("100")).doubleValue()) + "%");
                dataSetCheckSituations.add(dataSetCheckSituation);
            }
            return dataSetCheckSituations;
        }
    }

    @Resource
    private RuleMapper ruleMapper;

    /**
     * 质控规则检查情况(问题数占比前5位)
     *
     * @param orgQualityReportReq
     * @param instanceQuality
     * @return
     */
    private List<OrgQualityReportResp.QualityCheckRuleSituation> castQualityCheckRuleSituation(OrgQualityReportReq orgQualityReportReq, InstanceQuality instanceQuality) {
        List<OrgQualityReportResp.QualityCheckRuleSituation> qualityCheckRuleSituations = new ArrayList<>();
        List<Rule> ruleList = ruleMapper.selectByMap(new HashMap<>());
        Map<String, Rule> ruleMap = new HashMap<>();
        for (Rule rule : ruleList) {
            ruleMap.put(rule.getCategoryId(), rule);
        }
        List<RuleCategoryQuality> list = ruleCategoryQualityService.getRuleCategoryQualityTop5(orgQualityReportReq, orgQualityReportReq.getQueryType());
        List<RuleCategory> ruleCategoryList = ruleCategoryMapper.selectByMap(new HashMap<>());
        Map<String, RuleCategory> ruleCategoryMap = ruleCategoryList.stream().collect(Collectors.toMap(RuleCategory::getId, ruleCategory -> ruleCategory));
        if (list == null || list.isEmpty()) {
            return qualityCheckRuleSituations;
        } else {
            for (RuleCategoryQuality ruleCategoryQuality : list) {
                if (ruleMap.get(ruleCategoryQuality.getRuleCategoryId()) != null) {
                    OrgQualityReportResp.QualityCheckRuleSituation qualityCheckRuleSituation = new OrgQualityReportResp.QualityCheckRuleSituation();
                    qualityCheckRuleSituation.setRuleCategory(ruleCategoryMapper.getTopName(ruleCategoryQuality.getRuleCategoryId()));
                    qualityCheckRuleSituation.setRuleName(ruleCategoryMap.get(ruleCategoryQuality.getRuleCategoryId()).getName() + "");
                    qualityCheckRuleSituation.setCheckProblemAmount(ruleCategoryQuality.getCheckFailTimes() + "");
                    qualityCheckRuleSituation.setCheckTotalAmount(ruleCategoryQuality.getCheckTimes() + "");
                    qualityCheckRuleSituation.setProblemRatio(df.format(ruleCategoryQuality.getCheckFailTimesRate()) + "%");
                    qualityCheckRuleSituations.add(qualityCheckRuleSituation);
                }
            }
            return qualityCheckRuleSituations;
        }
    }

    /**
     * 获取数据上传波动情况
     *
     * @param orgQualityReportReq
     * @param instanceQuality
     * @return
     */
    private List<OrgQualityReportResp.UploadWaveSituation> castUploadWaveSituation(OrgQualityReportReq orgQualityReportReq, InstanceQuality instanceQuality) throws ParseException {
        List<OrgQualityReportResp.UploadWaveSituation> upload = new ArrayList<>();
        if (orgQualityReportReq.getQueryType() == 1) {
            return upload;
        }
        List<DataVolumeStatistics> uploadList = dataVolumeStatisticsService.getOrgUpload(orgQualityReportReq);
        Map<String, DataVolumeStatistics> dataMap = new HashMap<>();
        for (DataVolumeStatistics dataVolumeStatistics : uploadList) {
            dataMap.put(dataVolumeStatistics.getCycleDay(), dataVolumeStatistics);
        }
        for (String iDate = orgQualityReportReq.getStartTime(); !iDate.equals(DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(orgQualityReportReq.getEndTime(), YMD), 1), YMD)); iDate = DateFormatUtils.format(DateUtils.addDays(DateUtils.parseDate(iDate, YMD), 1), YMD)) {
            OrgQualityReportResp.UploadWaveSituation wave = new OrgQualityReportResp.UploadWaveSituation();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DateUtils.parseDate(iDate, YMD));
            int week = calendar.get(Calendar.DAY_OF_WEEK) - 1 == 0 ? 7 : (calendar.get(Calendar.DAY_OF_WEEK) - 1);
            wave.setType(week == 6 || week == 7 ? "1" : "0");
            wave.setDate(iDate);
            if (dataMap.get(iDate) == null) {
                wave.setUploadProblemDataSetNum("0");
            } else {
                wave.setUploadProblemDataSetNum(dataMap.get(iDate).getDataAmount() + "");
            }
            upload.add(wave);
        }
        return upload;
    }

    /**
     * 包装总体情况
     *
     * @param orgQualityReportReq
     * @param instanceQuality
     * @return
     */
    private OrgQualityReportResp.OverallSituation castOverallSituation(OrgQualityReportReq orgQualityReportReq, InstanceQuality instanceQuality,
                                                                       ScoreInstance scoreInstance) {
        OrgQualityReportResp.OverallSituation overall = new OrgQualityReportResp.OverallSituation();
        try {
            overall.setDateRange(orgQualityReportReq.getStartTime() + "至" + orgQualityReportReq.getEndTime());
            overall.setScore(instanceQuality.getScore() + "");
            QueryWrapper<ScoreInstance> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("parent_id", scoreInstance.getParentId());
            List<ScoreInstance> scoreInstances = scoreInstanceMapper.selectList(queryWrapper);
            if (orgQualityReportReq.getQueryType() != 1) {
                if (scoreInstances.size() == 1 || TOP_ID.equals(scoreInstance.getParentId())) {
                    // 如果是第一层级就不显示排名
                    overall.setScoreRank("-");
                    overall.setTopBang("-");
                    overall.setOrgSize("-");
                } else {
                    List<Long> instanceIds = scoreInstances.stream().map(ScoreInstance::getId).collect(Collectors.toList());
                    Integer topBang = instanceQualityService.getTheTopBang(instanceIds, scoreInstance.getId(), orgQualityReportReq, orgQualityReportReq.getQueryType());
                    overall.setTopBang(topBang + "");
                    overall.setOrgSize(scoreInstances.size() + "");
                    overall.setScoreRank(String.format("第%s名（共%s家机构参与排名）", topBang, scoreInstances.size()));
                }
            } else {
                overall.setTopBang("-");
                overall.setOrgSize("-");
                overall.setScoreRank("-");
            }
            overall.setUploadAmount(dataVolumeStatisticsService.getOrgUpload(orgQualityReportReq).stream().mapToLong(DataVolumeStatistics::getDataAmount).sum() + "");
            List<InstanceQuality> instanceQualities = instanceQualityService.getInstanceQualityByDate(orgQualityReportReq, orgQualityReportReq.getQueryType());
            List<InstanceQuality> lastDatas = instanceQualityService.getInstanceQualityByTopDate(orgQualityReportReq, orgQualityReportReq.getQueryType());
            RuleCategoryQuality rule = ruleCategoryQualityService.getMaxProblemRule(orgQualityReportReq, orgQualityReportReq.getQueryType());
            DataSetQuality pDataset = datasetQualityService.getMaxProblemDataset(orgQualityReportReq, orgQualityReportReq.getQueryType());
            if (instanceQualities != null && !instanceQualities.isEmpty() && instanceQualities.size() == 1) {
                InstanceQuality inst = instanceQualities.get(0);
                overall.setUploadProblemAmount(instanceQualities.get(0).getFailAmount() + "");
                overall.setCheckTotalAmount(instanceQualities.get(0).getFailAmountRate() + "%");
                if (Long.parseLong(overall.getUploadAmount()) == 0L) {
                    overall.setProblemRatio("-");
                } else {
                    overall.setProblemRatio(new BigDecimal(overall.getUploadProblemAmount()).divide(new BigDecimal(overall.getUploadAmount()), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(100)).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue() + "%");
                }
                if (lastDatas == null || lastDatas.isEmpty() || lastDatas.size() != 1) {
                    overall.setLastCheckProblemRatio("-");
                    overall.setLastProblemRatio("-");
                    overall.setProblemIncreaseRatio("-");
                    overall.setLastCheckProblemIncreaseRatio("-");
                } else {
                    InstanceQuality lastdata = lastDatas.get(0);
                    overall.setLastProblemRatio(df.format(lastdata.getFailAmountRate()) + "%");
                    overall.setLastCheckProblemRatio(df.format(lastdata.getCheckFailRate()) + "%");
                    overall.setProblemIncreaseRatio(getRoundRate(inst.getFailAmountRate(), lastdata.getFailAmountRate()));
                    overall.setLastCheckProblemIncreaseRatio(getRoundRate(inst.getCheckFailRate(), lastdata.getCheckFailRate()));
                }
                overall.setCheckTotalAmount(inst.getCheckTimes() + "");
                overall.setCheckProblemAmount(inst.getCheckFailTimes() + "");
                overall.setCheckProblemRatio(df.format(inst.getCheckFailRate()) + "%");
                overall.setShouldUploadDataSetNum(inst.getHopeDataSetAmount() + "");
                overall.setActualUploadDataSetNum(inst.getRealityDataSetAmount() + "");
                overall.setUploadProblemDataSetNum(inst.getMissDataSetAmount() + "");
                if (rule == null) {
                    overall.setProblemMostRuleName("-");
                } else {
                    RuleCategory ruleCategory = ruleCategoryMapper.selectById(rule.getRuleCategoryId());
                    overall.setProblemMostRuleName(ruleCategory.getName());
                }
                if (pDataset == null) {
                    overall.setProblemMostDataSet("-");
                } else {
                    QueryWrapper datasetWrapper = new QueryWrapper();
                    datasetWrapper.eq("DATASET_ID", pDataset.getDataSetId());
                    Dataset dataset = dataSetMapper.selectOne(datasetWrapper);
                    overall.setProblemMostDataSet(pDataset.getDataSetCode() + "（" + dataset.getMetasetName() + "）");
                }
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return overall;
    }

    private String getRoundRate(Double now, Double before) {
        return before != null && now != null && before != 0
                ? df.format(NumberUtil.round((now / before - 1) * 100, 2).doubleValue()) + "%" : "-";
    }
}