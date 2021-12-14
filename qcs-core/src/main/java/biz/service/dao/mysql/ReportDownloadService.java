package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.ReportDownload;
import com.gwi.qcs.model.mapper.mysql.ReportDownloadMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 综合报告批量下载
 */
@Service
@DS(DbTypeConstant.MYSQL)
public class ReportDownloadService extends SuperServiceImpl<ReportDownloadMapper, ReportDownload> {

    @Autowired
    private ReportDownloadMapper reportDownloadMapper;

    /**
     * 新增
     * @param qcsReportDownload
     * @return
     */
    public boolean add(ReportDownload qcsReportDownload) {
        qcsReportDownload.setIsGenerated(CommonConstant.IS_GENERATED_ING);
        return reportDownloadMapper.insert(qcsReportDownload) == 1;
    }

    /**
     * 报告生成后修改文件路径和状态
     *
     * @param qcsReportDownload
     * @return
     */
    public int updateForGenerated(ReportDownload qcsReportDownload) {
        ReportDownload qcsReportDownloadUpdate = new ReportDownload();
        qcsReportDownloadUpdate.setId(qcsReportDownload.getId());
        qcsReportDownloadUpdate.setFilePathPart(qcsReportDownload.getFilePathPart());
        qcsReportDownloadUpdate.setIsGenerated(qcsReportDownload.getIsGenerated());
        return reportDownloadMapper.updateById(qcsReportDownloadUpdate);
    }

    public IPage<ReportDownload> page(Integer pageIndex, Integer pageSize) {
        return reportDownloadMapper.selectPage(new Page<>(pageIndex, pageSize));
    }
}
