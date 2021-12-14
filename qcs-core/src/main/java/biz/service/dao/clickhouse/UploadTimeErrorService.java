package biz.service.dao.clickhouse;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.clickhouse.UploadTimeError;
import com.gwi.qcs.model.mapper.clickhouse.UploadTimeErrorMapper;
import org.springframework.stereotype.Service;

/**
 * (UploadTimeError)表服务接口
 *
 * @author easyCode
 * @since 2021-02-06 10:58:20
 */
@Service
@DS(DbTypeConstant.CLICKHOUSE)
public class UploadTimeErrorService extends SuperServiceImpl<UploadTimeErrorMapper, UploadTimeError> {

}