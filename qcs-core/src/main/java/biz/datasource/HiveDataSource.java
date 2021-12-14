package biz.datasource;

import com.google.common.collect.Maps;
import com.gwi.qcs.common.utils.SpringContextUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HiveDataSource extends BaseDatasource {

    /**
     * 五分钟
     */
    private static final int FIVE_MINUTE = 5 * 60 * 1000;

    private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private static final String HIVE_CONNECT_COUNT = "qcs.custom.hiveConnectCount";

    private static final String IS_DEVELOP = "qcs.custom.isDevelop";

    private static final String MAX_QUERY_SIZE = "data-quality.db-opeartion.max-query-size";
    /**
     * 开发模式
     */
    private static final String DEVELOP_MODE = "1";

    private final ThreadLocal<Resources> resources = new ThreadLocal<>(); //NOSONAR

    private static Map<String, List<Connection>> CONNECTION_MAP = Maps.newHashMap();

    private static Map<String, HikariDataSource> DATASOURCE_MAP = Maps.newHashMap();

    private int connectCount = 10;

    private boolean isDevelop = false;

    private int querySize = 50000;

    private int hiveExecutorMemory = 6554;

    private int hiveExecutorMemoryOverhead = 1638;

    private int hiveDriverMemory = 1638;

    private int hiveDriverMemoryOverhead = 410;

    static {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            log.error("未找到Hive驱动", e);
        }
    }

    public HiveDataSource(String host, String schema, String username, String password) {
        super(host, schema, username, password);
        Environment env = SpringContextUtil.getBean(Environment.class);
        String count = env.getProperty(HIVE_CONNECT_COUNT);
        if (StringUtils.isNotBlank(count)) {
            connectCount = Integer.parseInt(count);
        }
        String isDevelopStr = env.getProperty(IS_DEVELOP);
        if (DEVELOP_MODE.equals(isDevelopStr)) {
            isDevelop = true;
        }
        String querySizeStr = env.getProperty(MAX_QUERY_SIZE);
        if (StringUtils.isNotBlank(querySizeStr)) {
            querySize = Integer.parseInt(querySizeStr);
        }
        String hiveExecutorMemoryStr = env.getProperty("qcs.custom.hiveExecutorMemory");
        if (StringUtils.isNotBlank(hiveExecutorMemoryStr)) {
            hiveExecutorMemory = Integer.parseInt(hiveExecutorMemoryStr);
        }
        String hiveExecutorMemoryOverheadStr = env.getProperty("qcs.custom.hiveExecutorMemoryOverhead");
        if (StringUtils.isNotBlank(hiveExecutorMemoryOverheadStr)) {
            hiveExecutorMemoryOverhead = Integer.parseInt(hiveExecutorMemoryOverheadStr);
        }
        String hiveDriverMemoryStr = env.getProperty("qcs.custom.hiveDriverMemory");
        if (StringUtils.isNotBlank(hiveDriverMemoryStr)) {
            hiveDriverMemory = Integer.parseInt(hiveDriverMemoryStr);
        }
        String hiveDriverMemoryOverheadStr = env.getProperty("qcs.custom.hiveDriverMemoryOverhead");
        if (StringUtils.isNotBlank(hiveDriverMemoryOverheadStr)) {
            hiveDriverMemoryOverhead = Integer.parseInt(hiveDriverMemoryOverheadStr);
        }
    }

    @Override
    public ResultSet query(String sql) throws Exception {
        String key = buildDataSourceKey();
        Connection connection = getNewConnection(key);
        PreparedStatement statement;
        try {
            statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); // NOSONAR 流被返回不能关闭
        } catch (Exception e) {
            // 防止偶尔网络端口，刚好连接被存入map，之后就一直连接不上的问题
            log.warn("从map获取hive连接失败", e);
            CONNECTION_MAP.get(key).remove(connection);
            connection = getNewConnection(key);
            statement = connection.prepareStatement(sql); //NOSONAR 流被返回不能关闭
        }
        long startTime = System.currentTimeMillis();
        statement.execute("set spark.app.name = qcs");
        statement.execute("set spark.driver.memory = " + hiveDriverMemory + "m");
        statement.execute("set spark.yarn.driver.memoryOverhead = " + hiveDriverMemoryOverhead + "m");
        statement.execute("set spark.executor.memory = " + hiveExecutorMemory + "m");
        statement.execute("set spark.yarn.executor.memoryOverhead = " + hiveExecutorMemoryOverhead + "m");
        statement.setFetchSize(this.querySize);
        ResultSet resultSet = statement.executeQuery();
        long costTime = System.currentTimeMillis() - startTime;
        if (costTime > FIVE_MINUTE) {
            // 超过五分钟的打印warn
            log.warn("hive查询耗时：{}ms，语句：sql:{}", costTime, sql);
        } else {
            log.info("查询sql:{}", sql.replace("\\\n", ""));
        }
        resources.set(new Resources(connection, statement, resultSet));
        return resultSet;
    }

    private synchronized Connection getNewConnection(String key) throws SQLException {
        HikariDataSource hiveDataSource = null;
        boolean isCreate = true;
        if (DATASOURCE_MAP.containsKey(key)) {
            hiveDataSource = DATASOURCE_MAP.get(key);
            if (hiveDataSource.isClosed()) {
                DATASOURCE_MAP.remove(key);
            } else {
                isCreate = false;
            }
        }
        if (isCreate) {
            String url = "jdbc:hive2://" + host;
            String addStr = "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_zk?";
            if(isDevelop){
                addStr = "?hive.execution.engine=spark;mapred.job.queue.name=gw;";
            }
            addStr += "hive.exec.parallel=true;hive.exec.parallel.thread.number=8;useCursorFetch=true";
            url += addStr;

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(this.username);
            config.setPassword(this.password);
            config.setDriverClassName(DRIVER_CLASS);
            //连接超时时间，当前设置一天
            config.setConnectionTimeout(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
            config.setIdleTimeout(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
            config.setMaxLifetime(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
            config.setMinimumIdle(5);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            // 单条语句最大长度默认256，官方推荐2048
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.setMaximumPoolSize(connectCount);
            hiveDataSource = new HikariDataSource(config);
            DATASOURCE_MAP.put(key, hiveDataSource);
        }
        return hiveDataSource.getConnection();
    }

    @Override
    public void closeConnection() throws Exception {
        Resources resources = this.resources.get();
        if (resources != null) {
            try {
                if (resources.resultSet != null && !resources.resultSet.isClosed()) {
                    resources.resultSet.close();
                }

                if (resources.statement != null && !resources.statement.isClosed()) {
                    resources.statement.close();
                }
                if (resources.connection != null && !resources.connection.isClosed()) {
                    resources.connection.close();
                }
            } catch (Exception e) {
                log.warn("关闭hive查询资源失败： ", e);
            }
        }
    }

    private String buildDataSourceKey() {
        String keyUsername = StringUtils.isEmpty(this.username) ? StringUtils.EMPTY : this.username;
        String keyPassword = StringUtils.isEmpty(this.password) ? StringUtils.EMPTY : this.password;
        return host + "username=" + keyUsername + "password=" + keyPassword;
    }

    public static class Resources {
        private final Connection connection;
        private final Statement statement;
        private final ResultSet resultSet;

        public Resources(Connection connection, Statement statement, ResultSet resultSet) {
            this.connection = connection;
            this.statement = statement;
            this.resultSet = resultSet;
        }
    }
}
