package com.alibaba.blink.datastreaming.datastream.canal;

import java.util.List;
import java.util.Map;

/**
 * Created by hzy
 *
 * @author hzy
 */
public class CanalJson {
    /**
     * 操作的类型，如：INSERT / UPDATE / DELETE。
     */
    private String type;
    /**
     * 操作的序列号。
     */
    private Long id;
    /**
     * 变更前的数据。
     */
    private List<Map<String, String>> old;
    /**
     * 当前操作的数据。
     */
    private List<Map<String, String>> data;
    /**
     * 数据库名称。
     */
    private String database;
    /**
     * schema
     */
    private String schema;
    /**
     * 表名。
     */
    private String table;
    /**
     * 字段数据类型名称(flink 类型)
     */
    private Map<String, String> mysqlType;
    /**
     * 字段 JDBC 数据类型。
     */
    private Map<String, Integer> sqlType;
    /**
     * 是否为 DDL 操作。
     */
    private Boolean isDdl;
    /**
     * 源端主键名称。
     */
    private List<String> pkNames;
    /**
     * 源端 SQL 执行的时间，13位Unix时间戳，单位为毫秒。
     */
    private Long es;
    /**
     * 操作发送的时间，13 位 Unix 时间戳，单位为毫秒。
     */
    private Long ts;
    /**
     * 源端执行的 DDL 语句。
     */
    private String sql;
    /**
     * 该消息为 DDL 时，携带的该表的元信息，如：主键，列。
     */
    private String tableChanges;

    private Map<String, Map<String, String>> tags;

    public Map<String, Map<String, String>> getTags() {
        return tags;
    }

    public void setTags(Map<String, Map<String, String>> tags) {
        this.tags = tags;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTableChanges() {
        return tableChanges;
    }

    public void setTableChanges(String tableChanges) {
        this.tableChanges = tableChanges;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
