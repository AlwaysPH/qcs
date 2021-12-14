package biz.schedule.exception;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Lists;
import com.greatwall.component.ccyl.common.utils.DateUtil;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DqTaskConstant;
import com.gwi.qcs.common.constant.EnumConstant;
import com.gwi.qcs.common.utils.DateUtils;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.common.utils.StringUtil;
import com.gwi.qcs.core.biz.datasource.HiveDataSource;
import com.gwi.qcs.core.biz.schedule.AbstractQcsJob;
import com.gwi.qcs.core.biz.schedule.task.JobExecutor;
import com.gwi.qcs.core.biz.service.dao.clickhouse.OrgErrorService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetFiledService;
import com.gwi.qcs.core.biz.service.dao.mysql.DatasetService;
import com.gwi.qcs.core.biz.service.dao.mysql.ParameterService;
import com.gwi.qcs.core.biz.service.dao.mysql.TaskProgressService;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.domain.clickhouse.OrgError;
import com.gwi.qcs.model.domain.mysql.Dataset;
import com.gwi.qcs.model.domain.mysql.TaskProgress;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;


@Data
@Slf4j
@Component
public abstract class AbstractQueryExceptionJob extends AbstractQcsJob implements Callable<String> {

    private static int FETCH_SIZE = 10000;

    private List<String> childTableList;

    private Dataset childDataset;

    private HiveDataSource childHiveDataSource;

    private Date childStartDate;

    private boolean isFirst;

    @Autowired
    ParameterService parameterService;

    @Autowired
    TaskProgressService taskProgressService;

    @Autowired
    DatasetService datasetService;

    protected abstract AbstractQueryExceptionJob get();

    @Override
    protected final void executeJob(JobExecutionContext context)  throws Exception {
        String jobName = context.getJobDetail().getKey().getName();
        boolean localIsOrg = CommonConstant.ORG_EXCEPTION_JOB_ID.equals(jobName);
        // 异常数据的ID为1，异常机构的为2
        Long jobId = localIsOrg ? 1L : 2L;
        TaskProgress existTask = taskProgressService.getById(jobId);
        isFirst = existTask == null;
        Date startDate = taskProgressService.getTaskStartTime(jobId);
        HiveDataSource hiveDataSource = new HiveDataSource(parameterService.getParameterByCode("schedule_exception_hiveUrl"), null,
            parameterService.getParameterByCode("schedule_exception_hiveUser"),
            parameterService.getParameterByCode("schedule_exception_hivePassword"));
        TaskProgress taskProgress = new TaskProgress();
        taskProgress.setId(jobId);
        taskProgress.setJobType(localIsOrg ? DqTaskConstant.Task.ERROR_ORG : DqTaskConstant.Task.ERROR_DATA);
        taskProgress.setStatus(EnumConstant.TaskStatus.EXEC_SUCCEED.getValue());
        taskProgress.setTaskId(jobId);

        while (DateUtil.daysBetween(startDate, new Date()) >= 1){
            taskProgress.setCycleDay(startDate);
            List<String> tableList = getTableNameList(hiveDataSource);
            startJob(taskProgress, tableList, hiveDataSource);
            taskProgressService.saveOrUpdate(taskProgress);
            startDate = DateUtils.addDays(startDate, 1);
        }
    }

    private List<String> getTableNameList(HiveDataSource hiveDataSource) throws Exception{
        String databases = parameterService.getParameterByCode("schedule_exception_database");
        if(StringUtils.isEmpty(databases)){
            throw new Exception("异常数据查询，ODS中数据库的范围配置为空");
        }
        List<String> databaseList = StringUtil.splitByComma(databases);
        String tablePrefix = parameterService.getParameterByCode("schedule_exception_tablePrefix");
        List<String> tablePrefixList = StringUtil.splitByComma(tablePrefix);
        List<String> tableList = new ArrayList<>();
        for(String database : databaseList){
            for(String item : tablePrefixList){
                ResultSet resultSet = hiveDataSource.query("show tables from " + database + " like '" + item + "*' ");
                while (resultSet.next()) {
                    // 添加要查询的表
                    String tableName = resultSet.getString("tab_name");
                    tableList.add(database + "." + tableName);
                }
            }
        }
        return tableList;
    }

    private void startJob(TaskProgress taskProgress, List<String> tableList, HiveDataSource hiveDataSource) throws Exception{
        if(CollectionUtils.isEmpty(tableList)){
            throw new Exception("将要查询的表数量为空");
        }
        int threadCount = 8;
        JobExecutor jobExecutor = new JobExecutor(CommonConstant.SCHEDULE + CommonConstant.DASH
            + taskProgress.getId() + CommonConstant.DASH, threadCount);
        QueryWrapper<Dataset> queryWrapper = new QueryWrapper<>();
        queryWrapper.isNotNull("ORG_CODE_FIELD");
        Dataset dataset = datasetService.getOne(queryWrapper, false);
        List<List<String>> tableThreadList = Lists.partition(tableList, (tableList.size() + threadCount)/threadCount);
        List<Future<String>> futureList = new ArrayList<>();
        try {
            for(List<String> item : tableThreadList){
                AbstractQueryExceptionJob queryExceptionJob = this.get();
                queryExceptionJob.setChildTableList(item);
                queryExceptionJob.setChildDataset(dataset);
                queryExceptionJob.setFirst(isFirst);
                queryExceptionJob.setChildHiveDataSource(hiveDataSource);
                queryExceptionJob.setChildStartDate(taskProgress.getCycleDay());
                futureList.add(jobExecutor.startRun(queryExceptionJob));
            }
            for(Future<String> future : futureList){
                future.get();
            }
        } catch (Exception ex) {
            throw new Exception("定时运行异常查询出错", ex);
        } finally {
            jobExecutor.shutdown();
        }
    }

    //_________________________子类调用的方法_____________________

    protected String getPkValue(String tableName) throws Exception{
        DatasetFiledService datasetFiledService = SpringContextUtil.getBean(DatasetFiledService.class);
        Map<String, String> map = datasetFiledService.getKeysByTableName(tableName);
        String pkValue = StringUtils.EMPTY;
        if( map.isEmpty()){
            return pkValue;
        }
        pkValue = CommonUtil.getPkValue(map, false);
        return pkValue;
    }

    protected void query(String sql, String tableName, boolean isOrg) throws Exception{
        OrgErrorService orgErrorService = SpringContextUtil.getBean(OrgErrorService.class);
        List<OrgError> orgErrorList = new ArrayList<>();
        ResultSet resultSet = null;
        try{
            resultSet = this.getChildHiveDataSource().query(sql);
            int total = 0;
            while (resultSet.next()) {
                if(isOrg){
                    OrgError orgError = new OrgError();
                    orgError.setSourceId(resultSet.getString("data_source_id"));
                    orgError.setPkValue(resultSet.getString("pkValue"));
                    orgError.setUploadTime(DateUtils.parse(resultSet.getString("uploadTime")));
                    orgError.setDatasetCode(tableName);
                    orgError.setOrgCode(resultSet.getString("orgCode") == null ?
                        "null" : resultSet.getString("orgCode"));
                    orgErrorList.add(orgError);
                }else{

                }
                total++;
                if (total == FETCH_SIZE) {
                    if(isOrg){
                        orgErrorService.saveBatch(orgErrorList, FETCH_SIZE);
                        orgErrorList.clear();
                    }
                    total = 0;
                }
            }
            if(total > 0){
                if(isOrg){
                    orgErrorService.saveBatch(orgErrorList, FETCH_SIZE);
                }
            }
        }catch (Exception e){
            log.error("处理查询的数据失败： ", e);
        }finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                this.getChildHiveDataSource().closeConnection();
            } catch (SQLException e) {
                log.error("resultSet关闭出错!查询sql:{}", sql);
            }
        }
    }
}
