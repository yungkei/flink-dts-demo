package com.alibaba.blink.datastreaming.datastream.canal;

import java.io.Serializable;
import java.util.Map;

public class CanalJsonWrapper implements Serializable {
    private static final long serialVersionUID = -5539833118000238345L;

    private String pk;
    private String database;
    private String table;
    private String operation;
    private CanalJson canalJson;
    private Map<String, String> tags;

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
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

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public CanalJson getCanalJson() {
        return canalJson;
    }

    public void setCanalJson(CanalJson canalJson) {
        this.canalJson = canalJson;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "CanalJsonWrapper{" +
                "pk='" + pk + '\'' +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", operation='" + operation + '\'' +
                ", canalJson=" + canalJson +
                ", tags=" + tags +
                '}';
    }
}
