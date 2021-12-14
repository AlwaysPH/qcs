package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.DatasetInstanceRule;
import com.gwi.qcs.model.mapper.mysql.DatasetInstanceRuleMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author yanhan
 * @create 2020-08-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class DatasetInstanceRuleService extends SuperServiceImpl<DatasetInstanceRuleMapper, DatasetInstanceRule> {
}
