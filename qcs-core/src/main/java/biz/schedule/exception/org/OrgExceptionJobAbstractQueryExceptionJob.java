package biz.schedule.exception.org;

import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.schedule.exception.AbstractQueryExceptionJob;
import com.gwi.qcs.core.biz.service.dao.clickhouse.OrgErrorService;
import com.gwi.qcs.core.biz.utils.CommonUtil;
import com.gwi.qcs.model.domain.mysql.Dataset;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class OrgExceptionJobAbstractQueryExceptionJob extends AbstractQueryExceptionJob {

    @Override
    public String call() throws Exception {
        Dataset dataset = this.getChildDataset();
        String orgCode = dataset.getOrgCodeField();
        String sourceId = dataset.getResourceField();
        String uploadTimeField = dataset.getUploadTimeField();
        if(isFirst()){
            OrgErrorService orgErrorService = SpringContextUtil.getBean(OrgErrorService.class);
            orgErrorService.getBaseMapper().truncate();
        }
        for(String table : this.getChildTableList()){
            String sql = StringUtils.EMPTY;
            try{
                String tableName = StringUtils.upperCase(table.split("\\.")[1]);
                String pkValue = this.getPkValue(tableName);
                if(StringUtils.isNotEmpty(pkValue)){
                    sql = "select " + sourceId + " as data_source_id, " + uploadTimeField + " as uploadTime,  "
                        + pkValue + " as pkValue, " + orgCode + " as orgCode from " + table
                        + " where (" + orgCode + " is null or " + orgCode + " = '' or " + orgCode + " = '*') "
                        + (isFirst() ? StringUtils.EMPTY : " and " + CommonUtil.getDateCondition(this.getChildStartDate()));
                    this.query(sql, tableName, true);
                }
            }catch (Exception e){
                log.error("{} 表的定时任务查询语句： {}， 异常：", table, sql, e);
            }
        }
        return StringUtils.EMPTY;
    }

    @Override
    public OrgExceptionJobAbstractQueryExceptionJob get(){
        return new OrgExceptionJobAbstractQueryExceptionJob();
    }
}
