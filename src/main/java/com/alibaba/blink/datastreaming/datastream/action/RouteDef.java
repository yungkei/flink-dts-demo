package com.alibaba.blink.datastreaming.datastream.action;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class RouteDef implements Serializable {
    private static final long serialVersionUID = 5046819613535805118L;

    private final String sourceTable;
    private final String sinkTable;
    @Nullable
    private final String description;

    public RouteDef(String sourceTable, String sinkTable, @Nullable String description) {
        this.sourceTable = sourceTable;
        this.sinkTable = sinkTable;
        this.description = description;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public String toString() {
        return "RouteDef{"
                + "sourceTable="
                + sourceTable
                + ", sinkTable="
                + sinkTable
                + ", description='"
                + description
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RouteDef routeDef = (RouteDef) o;
        return Objects.equals(sourceTable, routeDef.sourceTable)
                && Objects.equals(sinkTable, routeDef.sinkTable)
                && Objects.equals(description, routeDef.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, sinkTable, description);
    }
}
