package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.RuleDetailVal;
import com.gwi.qcs.model.mapper.mysql.RuleDetailValMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author yanhan
 * @create 2020-08-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class RuleDetailValService extends SuperServiceImpl<RuleDetailValMapper, RuleDetailVal> {
}
