package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.InstanceDataSet;
import com.gwi.qcs.model.mapper.mysql.InstanceDataSetMapper;
import org.springframework.stereotype.Service;

@Service
@DS(DbTypeConstant.MYSQL)
public class InstanceDatasetService extends SuperServiceImpl<InstanceDataSetMapper, InstanceDataSet> {


}
