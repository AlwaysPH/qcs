package biz.properties;

import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.annotation.NacosConfigurationProperties;
import com.gwi.qcs.common.constant.DqTaskConstant;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * livy 调用spark参数配置
 *
 * @author ljl
 * @date 2021/2/26 14:19
 **/
@Data
@Configuration
@ConfigurationProperties(prefix = "qcs.spark")
@NacosConfigurationProperties(prefix = "qcs.spark", dataId = "qcs", type = ConfigType.YAML, autoRefreshed = true)
@Component
public class SparkLivyProperties {

    /**
     * livy地址
     */
    private String livyUrl;

    /**
     * spark程序名称
     */
    private String masterUrl;

    /**
     * spark程序名称
     */
    private String name;

    /**
     * spark程序启动main方法类
     */
    private String className;

    private String delClassName;

    /**
     * spark程序driver端运用内存
     */
    private int driverCores;

    /**
     * spark程序driver端运用内存
     */
    private String driverMemory;

    /**
     * spark程序任务启动executor端数量
     */
    private int executorCores;

    /**
     * spark程序每个executor端启动核数
     */
    private int numExecutors;

    /**
     * spark程序每个executor端执行内存
     */
    private String executorMemory;

    /**
     * 线程池个数
     */
    private String executorCount;

    /**
     * spark程序参数
     */
    private Map<String, String> conf;

    /**
     * spark程序任务依赖的jar路径HDFS
     */
    private String[] jars;

    /**
     * spark程序任务参数
     */
    private String[] args;

    /**
     * spark程序jar包地址,必须放在hdfs内
     */
    private String file;

    private String delFile;

    /**
     * 重试次数
     */
    private int retry = DqTaskConstant.SparkLivy.RETRY_TIMES;

    /**
     * spark 任务存活时间(单位小时,默认一天)，超过时间没有结束会结束掉本地轮训，任务直接失败
     */
    private int lifeCycle = 24;

    /**
     * 心跳检测线程每隔2分钟执行一次
     */
    private String heartbeatCron;

    /**
     * 任务检测线程执行cron
     */
    private String taskRepairCron;
}
