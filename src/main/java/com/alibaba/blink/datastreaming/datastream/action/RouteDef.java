package com.alibaba.blink.datastreaming.datastream.action;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class RouteDef implements Serializable {
    private static final long serialVersionUID = -3216755435511898183L;
    private final String sourceDatabase;
    private final String targetDatabase;
    private final String includingTargetTables;
    private final String excludingTargetTables;
    private final String broadcastTables;
    private final List<FlatRouteDef> tableRouteDef;

    public RouteDef(String sourceTable, String sinkTable, String includingTargetTables, String excludingTargetTables, String broadcastTables, List<FlatRouteDef> tableRouteDef) {
        this.sourceDatabase = sourceTable;
        this.targetDatabase = sinkTable;
        this.includingTargetTables = includingTargetTables;
        this.excludingTargetTables = excludingTargetTables;
        this.broadcastTables = broadcastTables;
        this.tableRouteDef = tableRouteDef;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public String getIncludingTargetTables() {
        return includingTargetTables;
    }

    public String getExcludingTargetTables() {
        return excludingTargetTables;
    }

    public String getBroadcastTables() {
        return broadcastTables;
    }

    public List<FlatRouteDef> getTableRouteDef() {
        return tableRouteDef;
    }

    @Override
    public String toString() {
        return "RouteDef{" +
                "sourceDatabase='" + sourceDatabase + '\'' +
                ", targetDatabase='" + targetDatabase + '\'' +
                ", includingTargetTables='" + includingTargetTables + '\'' +
                ", excludingTargetTables='" + excludingTargetTables + '\'' +
                ", broadcastTables='" + broadcastTables + '\'' +
                ", tableRouteDef=" + tableRouteDef +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RouteDef routeDef = (RouteDef) o;
        return Objects.equals(sourceDatabase, routeDef.sourceDatabase) && Objects.equals(targetDatabase, routeDef.targetDatabase) && Objects.equals(includingTargetTables, routeDef.includingTargetTables) && Objects.equals(excludingTargetTables, routeDef.excludingTargetTables) && Objects.equals(broadcastTables, routeDef.broadcastTables) && Objects.equals(tableRouteDef, routeDef.tableRouteDef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceDatabase, targetDatabase, includingTargetTables, excludingTargetTables, broadcastTables, tableRouteDef);
    }
}
