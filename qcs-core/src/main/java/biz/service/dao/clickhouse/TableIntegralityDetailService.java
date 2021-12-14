package biz.service.dao.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.TableIntegralityDetail;
import com.gwi.qcs.model.mapper.clickhouse.TableIntegralityDetailMapper;
import org.springframework.stereotype.Service;

/**
 * (TableIntegralityDetail)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:39
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class TableIntegralityDetailService extends SuperServiceImpl<TableIntegralityDetailMapper, TableIntegralityDetail> {

}