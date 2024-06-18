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
    public static final String CURRENT_EMIT_EVENT_TIME_LAG = "currentEmitEventTimeLag";
}
