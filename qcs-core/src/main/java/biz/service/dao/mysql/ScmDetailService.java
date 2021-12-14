package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.ScmDetail;
import com.gwi.qcs.model.mapper.mysql.ScmDetailMapper;
import org.springframework.stereotype.Service;

/**
 * 质控同步标准版本中代码值表(ScmDetail)表服务接口
 *
 * @author easyCode
 * @since 2021-02-22 16:27:54
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class ScmDetailService extends SuperServiceImpl<ScmDetailMapper, ScmDetail> {

}