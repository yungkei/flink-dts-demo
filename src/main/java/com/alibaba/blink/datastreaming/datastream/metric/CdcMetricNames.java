package com.alibaba.blink.datastreaming.datastream.metric;

public class CdcMetricNames {
    public CdcMetricNames() {
    }

    public static final String CDC_RECORDS_PER_SECOND = "cdcRecordsPerSecond";
    public static final String INSERT_RECORDS_PER_SECOND = "insertRecordsPerSecond";
    public static final String DELETE_RECORDS_PER_SECOND = "deleteRecordsPerSecond";
    public static final String UPDATE_RECORDS_PER_SECOND = "updateRecordsPerSecond";
    public static final String DDL_RECORDS_PER_SECOND = "ddlRecordsPerSecond";
    public static final String DML_RECORDS_PER_SECOND = "dmlRecordsPerSecond";

    public static final String CDC_RECORDS_COUNT = "cdcRecordsCount";
    public static final String INSERT_RECORDS_COUNT = "insertRecordsCount";
    public static final String DELETE_RECORDS_COUNT = "deleteRecordsCount";
    public static final String UPDATE_RECORDS_COUNT = "updateRecordsCount";
    public static final String DDL_RECORDS_COUNT = "ddlRecordsCount";
    public static final String DML_RECORDS_COUNT = "dmlRecordsCount";

    public static final String CURRENT_EMIT_EVENT_TIME_LAG = "currentEmitEventTimeLag";

    public static final String MAX_EMIT_EVENT_TIME_LAG = "maxEmitEventTimeLag";
    public static final String MIN_EMIT_EVENT_TIME_LAG = "minEmitEventTimeLag";

    public static final String DRDS_PARTITION_UPDATE_PROCESS_MERGE_COUNT = "drdsPartitionUpdateProcessMergeCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_COUNT = "drdsPartitionUpdateProcessAddStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_REMOVE_STATE_COUNT = "drdsPartitionUpdateProcessRemoveStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_NOT_MATCH_STATE_COUNT = "drdsPartitionUpdateProcessNotMatchStateCount";

    public static final String DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT = "drdsCanalJsonWrapperValidateFailCount";
}
