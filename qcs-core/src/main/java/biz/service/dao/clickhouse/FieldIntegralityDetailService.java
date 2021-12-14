package biz.service.dao.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.FieldIntegralityDetail;
import com.gwi.qcs.model.mapper.clickhouse.FieldIntegralityDetailMapper;
import org.springframework.stereotype.Service;

/**
 * (FieldIntegralityDetail)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:36
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class FieldIntegralityDetailService extends SuperServiceImpl<FieldIntegralityDetailMapper, FieldIntegralityDetail> {

}