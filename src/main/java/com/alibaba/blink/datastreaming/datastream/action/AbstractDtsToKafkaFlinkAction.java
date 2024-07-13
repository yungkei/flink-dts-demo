package com.alibaba.blink.datastreaming.datastream.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public abstract class AbstractDtsToKafkaFlinkAction extends AbstractFlinkAction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDtsToKafkaFlinkAction.class);
    private static final String DTS = "dts-config";
    private static final String KAFKA = "kafka-config";

    protected Integer mapParallelismConfig;
    protected Integer sinkParallelismConfig;
    protected String includingTablesConfig;
    protected String excludingTablesConfig;
    protected String extraPrimaryKeys;

    private static final String MAP_PARALLELISM = "map-parallelism";
    private static final String SINK_PARALLELISM = "sink-parallelism";
    private static final String EXTRA_COLUMN = "extra-column";
    protected HashMap<String, String> extraColumnConfig;
    public static final String INCLUDING_TABLES = "including_tables";
    public static final String EXCLUDING_TABLES = "excluding_tables";
    public static final String EXTRA_PRIMARYKEY = "extra-primarykeys";
    protected String enablePartitionUpdatePerform;
    public static final String ENABLE_PARTITION_UPDATE_PERFORM = "enablePartitionUpdatePerform";
    protected long partitionUpdatePerformsStateTtl;
    public static final String PARTITION_UPDATE_PERFORM_STATE_TTL = "partitionUpdatePerformsStateTtl";
    protected long partitionUpdatePerformsTimerTimeInternalMs;
    public static final String PARTITION_UPDATE_PERFORM_TIMER_TIME_INTERNAL_MS = "partitionUpdatePerformsTimerTimeInternalMs";
    protected int memoryStateMaxSize;
    public static final String MEMORY_STATE_MAX_SIZE = "memoryStateMaxSize";
    protected String enableDdl;
    private static final String ENABLE_DDL = "enable-ddl";
    protected String mapToString;
    private static final String MAP_TO_STRING = "map-to-string";

    @Override
    protected String getSourceIdentifier() {
        return DTS;
    }

    @Override
    protected String getSinkIdentifier() {
        return KAFKA;
    }

    @Override
    public abstract void run() throws Exception;

    @Override
    void setExtraConfig(String[] args) {
        setMapParallelismConfig(args);
        setSinkParallelismConfig(args);
        setExtraColumnConfig(args);
        setIncludingTablesConfig(args);
        setExcludingTablesConfig(args);
        setEnableDdl(args);
        setExtraPrimarykey(args);
        setEnablePartitionUpdatePerform(args);
        setPartitionUpdatePerformsStateTtl(args);
        setPartitionUpdatePerformsTimerTimeInternalMs(args);
        setMemoryStateMaxSize(args);
        setMapToString(args);
    }

    private void setEnableDdl(String[] args) {
        List<String> enableDdls = optionalConfigList(args, ENABLE_DDL, item -> item);
        if (enableDdls == null || enableDdls.isEmpty()) {
            this.enableDdl = "true";
        } else {
            this.enableDdl = enableDdls.get(0);
        }
    }

    private void setEnablePartitionUpdatePerform(String[] args) {
        List<String> enablePartitionUpdatePerforms = optionalConfigList(args, ENABLE_PARTITION_UPDATE_PERFORM, item -> item);
        if (enablePartitionUpdatePerforms == null || enablePartitionUpdatePerforms.isEmpty()) {
            this.enablePartitionUpdatePerform = "false";
        } else {
            this.enablePartitionUpdatePerform = enablePartitionUpdatePerforms.get(0);
        }
    }

    private void setPartitionUpdatePerformsStateTtl(String[] args) {
        List<Long> partitionUpdatePerformsStateTtls = optionalConfigList(args, PARTITION_UPDATE_PERFORM_STATE_TTL, item -> {
            if (item == null) {
                return 600L;
            }
            return Long.valueOf(item);
        });
        if (partitionUpdatePerformsStateTtls == null || partitionUpdatePerformsStateTtls.isEmpty()) {
            this.partitionUpdatePerformsStateTtl = 180L;
        } else {
            this.partitionUpdatePerformsStateTtl = partitionUpdatePerformsStateTtls.get(0);
        }
    }

    private void setPartitionUpdatePerformsTimerTimeInternalMs(String[] args) {
        List<Long> partitionUpdatePerformsTimerTimeInternalMss = optionalConfigList(args, PARTITION_UPDATE_PERFORM_TIMER_TIME_INTERNAL_MS, item -> {
            if (item == null) {
                return 10000L;
            }
            return Long.valueOf(item);
        });
        if (partitionUpdatePerformsTimerTimeInternalMss == null || partitionUpdatePerformsTimerTimeInternalMss.isEmpty()) {
            this.partitionUpdatePerformsTimerTimeInternalMs = 10000L;
        } else {
            this.partitionUpdatePerformsTimerTimeInternalMs = partitionUpdatePerformsTimerTimeInternalMss.get(0);
        }
    }

    private void setMemoryStateMaxSize(String[] args) {
        List<Integer> memoryStateMaxSizes = optionalConfigList(args, MEMORY_STATE_MAX_SIZE, item -> {
            if (item == null) {
                return 5 * 1024 * 1024;
            }
            return Integer.valueOf(item);
        });
        if (memoryStateMaxSizes == null || memoryStateMaxSizes.isEmpty()) {
            this.memoryStateMaxSize = 5 * 1024 * 1024;
        } else {
            this.memoryStateMaxSize = memoryStateMaxSizes.get(0);
        }
    }

    private void setMapToString(String[] args) {
        List<String> mapToStrings = optionalConfigList(args, MAP_TO_STRING, item -> item);
        if (mapToStrings == null || mapToStrings.isEmpty()) {
            this.mapToString = "false";
        } else {
            this.mapToString = mapToStrings.get(0);
        }
    }

    private void setExtraPrimarykey(String[] args) {
        List<String> extraPrimarykeys = optionalConfigList(args, EXTRA_PRIMARYKEY, item -> item);
        if (extraPrimarykeys == null || extraPrimarykeys.isEmpty()) {
            this.extraPrimaryKeys = "";
        } else {
            this.extraPrimaryKeys = extraPrimarykeys.get(0);
        }
    }

    private void setMapParallelismConfig(String[] args) {
        List<Integer> mapParallelisms = optionalConfigList(args, MAP_PARALLELISM, item -> Integer.valueOf(item));
        if (mapParallelisms == null || mapParallelisms.isEmpty()) {
            this.mapParallelismConfig = 1;
        } else {
            this.mapParallelismConfig = mapParallelisms.get(0);
        }
    }

    private void setSinkParallelismConfig(String[] args) {
        List<Integer> sinkParallelisms = optionalConfigList(args, SINK_PARALLELISM, item -> Integer.valueOf(item));
        if (sinkParallelisms == null || sinkParallelisms.isEmpty()) {
            this.sinkParallelismConfig = 1;
        } else {
            this.sinkParallelismConfig = sinkParallelisms.get(0);
        }
    }

    private void setExtraColumnConfig(String[] args) {
        this.extraColumnConfig = optionalConfigMap(args, EXTRA_COLUMN);
    }

    private void setIncludingTablesConfig(String[] args) {
        List<String> includingTables = optionalConfigList(args, INCLUDING_TABLES, item -> item);
        if (includingTables == null || includingTables.isEmpty()) {
            this.includingTablesConfig = ".*";
        } else {
            this.includingTablesConfig = includingTables.get(0);
        }
    }

    private void setExcludingTablesConfig(String[] args) {
        List<String> excludingTables = optionalConfigList(args, EXCLUDING_TABLES, item -> item);
        if (excludingTables == null || excludingTables.isEmpty()) {
            this.excludingTablesConfig = "";
        } else {
            this.excludingTablesConfig = excludingTables.get(0);
        }
    }

    private static boolean shouldMonitorTable(String tableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(tableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(tableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", tableName);
        }
        return shouldMonitor;
    }

    public static boolean shouldMonitorTable(String tableName, String includingTables, String excludingTables) {
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        return shouldMonitorTable(tableName, includingPattern, excludingPattern);
    }
}
