package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.DsmRef;
import com.gwi.qcs.model.mapper.mysql.DsmRefMapper;
import org.springframework.stereotype.Service;

/**
 * 数据集关联表，定义不同数据集字段关联关系（主表与子表关系）(DsmRef)表服务接口
 *
 * @author easyCode
 * @since 2021-02-22 16:26:58
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class DsmRefService extends SuperServiceImpl<DsmRefMapper, DsmRef> {

}