package biz.datasource;

import java.sql.ResultSet;

/**
 * 基础数据源抽象类
 */
public abstract class BaseDatasource {

    protected final String host;

    protected final String schema;

    protected final String username;

    protected final String password;

    public BaseDatasource(String host, String schema, String username, String password) {
        this.host = host;
        this.schema = schema;
        this.username = username;
        this.password = password;
    }

    public BaseDatasource(String host, String schema) {
        this.host = host;
        this.schema = schema;
        this.username = null;
        this.password = null;
    }

    public BaseDatasource(String host) {
        this.host = host;
        this.schema = null;
        this.username = null;
        this.password = null;
    }

    public abstract ResultSet query(String sql) throws Exception;

    /**
     * 关闭连接
     * @throws Exception
     */
    public abstract void closeConnection() throws Exception;
}
