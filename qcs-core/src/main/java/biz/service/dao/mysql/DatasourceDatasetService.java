package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.DataSourceDataset;
import com.gwi.qcs.model.mapper.mysql.DataSourceDatesetMapper;
import org.springframework.stereotype.Service;

/**
 * @author yanhan
 * @create 2020-08-16 14:48
 **/
@Service
@DS(DbTypeConstant.MYSQL)
public class DatasourceDatasetService extends SuperServiceImpl<DataSourceDatesetMapper, DataSourceDataset> {

}
