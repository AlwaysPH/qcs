package biz.service.dao.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.DatasourceValidResult;
import com.gwi.qcs.model.mapper.clickhouse.DatasourceValidResultMapper;
import org.springframework.stereotype.Service;

/**
 * (DatasourceValidResult)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:22:34
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class DatasourceValidResultService extends SuperServiceImpl<DatasourceValidResultMapper, DatasourceValidResult> {

}