package com.alibaba.blink.datastreaming.datastream.action;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class FlatRouteDef implements Serializable {
    private static final long serialVersionUID = 5046819613535805118L;

    private final String sourceTable;
    private final String targetTable;
    @Nullable
    private final String description;

    public FlatRouteDef(String sourceTable, String targetTable, @Nullable String description) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.description = description;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public String toString() {
        return "FlatRouteDef{"
                + "sourceTable="
                + sourceTable
                + ", targetTable="
                + targetTable
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
        FlatRouteDef flatRouteDef = (FlatRouteDef) o;
        return Objects.equals(sourceTable, flatRouteDef.sourceTable)
                && Objects.equals(targetTable, flatRouteDef.targetTable)
                && Objects.equals(description, flatRouteDef.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, targetTable, description);
    }
}
