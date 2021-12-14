package biz.schedule.exception.data;

import com.gwi.qcs.common.utils.SpringContextUtil;
import com.gwi.qcs.core.biz.schedule.exception.AbstractQueryExceptionJob;
import com.gwi.qcs.core.biz.service.dao.clickhouse.UploadTimeErrorService;
import com.gwi.qcs.model.domain.mysql.Dataset;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DataExceptionJobAbstractQueryExceptionJob extends AbstractQueryExceptionJob {

    @Override
    public String call() throws Exception {
        UploadTimeErrorService uploadTimeErrorService = SpringContextUtil.getBean(UploadTimeErrorService.class);
        uploadTimeErrorService.getBaseMapper().truncate();
        Dataset dataset = this.getChildDataset();
        String sourceId = dataset.getResourceField();
        String extit = dataset.getUploadTimeField();
        for(String table : this.getChildTableList()){
            String sql = StringUtils.EMPTY;
            try{
                String[] nameArr = table.split("\\.");
                String tableName = StringUtils.upperCase(nameArr[1]);
                String pkValue = this.getPkValue(tableName);
                if(StringUtils.isNotEmpty(pkValue)){
                    sql = "select " + sourceId + " as data_source_id, " + extit + " as extit,  "
                        + pkValue + " as pkValue from " + table
                        + " where " + extit + " is null ";
                    this.query(sql, tableName, true);
                }
            }catch (Exception e){
                log.error("{} 表的定时任务查询语句： {}， 异常：", table, sql, e);
            }
        }
        return StringUtils.EMPTY;
    }

    public DataExceptionJobAbstractQueryExceptionJob get(){
        return new DataExceptionJobAbstractQueryExceptionJob();
    }
}
