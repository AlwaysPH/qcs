package biz.service;

import com.gwi.qcs.livy.bean.YarnState;
import com.gwi.qcs.model.dto.DeleteDataRequest;

/**
 * livy服务接口
 *
 * @author ljl
 * @date 2021/3/3 11:51
 **/
public interface SparkLivyService {

    /**
     * 启动任务
     *
     * @param taskId     任务
     * @param jobType    任务类型
     * @param cycleDay   日期
     * @param standardId 标准版本ID
     * @return result
     */
    boolean startJob(String taskId, String cycleDay, String jobType, String standardId);

    /**
     * 启动删除任务
     *
     * @param deleteData
     * @return result
     */
    boolean startDelJob(DeleteDataRequest deleteData);

    /**
     * 停止掉任务
     *
     * @param sparkAppId sparkID
     * @return result
     */
    boolean killJob(String sparkAppId);

    /**
     * 任务状态
     *
     * @param sparkAppId sparkId
     * @return 集群状态
     */
    YarnState status(String sparkAppId);
}
