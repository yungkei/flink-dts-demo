package com.alibaba.blink.datastreaming.datastream.metric;

public class CdcMetricNames {
    public CdcMetricNames() {
    }

    public static final String MAX_DTS_READER_DELAY = "maxDtsReaderDelay";
    public static final String MIN_DTS_READER_DELAY = "minDtsReaderDelay";
    public static final String CDC_RECORDS_COUNT = "cdcRecordsCount";
    public static final String CDC_RECORDS_PER_SECOND = "cdcRecordsPerSecond";
    public static final String CURRENT_READER_THROUGHOUT_TIME_DELAY = "currentReaderThroughoutTimeDelay";
    public static final String CURRENT_READER_THROUGHOUT_TIMESTAMP = "currentReaderThroughoutTimestamp";
    public static final String MAX_KAFKA_WATERMARK_DELAY = "maxKafkaWatermarkDelay";
    public static final String MIN_KAFKA_WATERMARK_DELAY = "minKafkaWatermarkDelay";
    public static final String MAX_EVENT_WATERMARK_DELAY = "maxEventWatermarkDelay";
    public static final String MIN_EVENT_WATERMARK_DELAY = "minEventWatermarkDelay";

    // MATCH
    public static final String MATCHED_MAX_DTS_READER_DELAY = "matchedMaxDtsReaderDelay";
    public static final String MATCHED_MIN_DTS_READER_DELAY = "matchedMinDtsReaderDelay";
    public static final String MATCHED_CDC_RECORDS_COUNT = "matchedCdcRecordsCount";
    public static final String MATCHED_CDC_RECORDS_PER_SECOND = "matchedCdcRecordsPerSecond";
    public static final String MATCHED_CURRENT_READER_THROUGHOUT_TIME_DELAY = "matchedCurrentReaderThroughoutTimeDelay";
    public static final String MATCHED_CURRENT_READER_THROUGHOUT_TIMESTAMP = "matchedCurrentReaderThroughoutTimestamp";
    public static final String MATCHED_DML_RECORDS_COUNT = "matchedDmlRecordsCount";
    public static final String MATCHED_DDL_RECORDS_COUNT = "matchedDdlRecordsCount";
    public static final String MATCHED_UPDATE_RECORDS_COUNT = "matchedUpdateRecordsCount";
    public static final String MATCHED_INSERT_RECORDS_COUNT = "matchedInsertRecordsCount";
    public static final String MATCHED_DELETE_RECORDS_COUNT = "matchedDeleteRecordsCount";
    public static final String MATCHED_MAX_SOURCE_TIMESTAMP = "matchedMaxSourceTimestamp";
    public static final String MATCHED_MIN_SOURCE_TIMESTAMP = "matchedMinSourceTimestamp";
    public static final String MATCHED_MAX_KAFKA_WATERMARK_DELAY = "matchedMaxKafkaWatermarkDelay";
    public static final String MATCHED_MIN_KAFKA_WATERMARK_DELAY = "matchedMinKafkaWatermarkDelay";
    public static final String MATCHED_MAX_EVENT_WATERMARK_DELAY = "matchedMaxEventWatermarkDelay";
    public static final String MATCHED_MIN_EVENT_WATERMARK_DELAY = "matchedMinEventWatermarkDelay";

    public static final String DRDS_PARTITION_UPDATE_PROCESS_MERGE_COUNT = "drdsPartitionUpdateProcessMergeCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_COUNT = "drdsPartitionUpdateProcessAddStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_FAIL_COUNT = "drdsPartitionUpdateProcessAddStateFailCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_REMOVE_STATE_COUNT = "drdsPartitionUpdateProcessRemoveStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_NOT_MATCH_STATE_COUNT = "drdsPartitionUpdateProcessNotMatchStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_ON_TIMER_STATE_COUNT = "drdsPartitionUpdateProcessOnTimerStateCount";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_ON_TIMER_STATE_FAIL_COUNT = "drdsPartitionUpdateProcessOnTimerStateFailCount";
    public static final String DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT = "drdsCanalJsonWrapperValidateFailCount";

    public static final String DRDS_PARTITION_UPDATE_PROCESS_MAX_MATCH_EVENT_TIME_INTERVAL = "drdsPartitionUpdateProcessMaxMatchEventTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_MIN_MATCH_EVENT_TIME_INTERVAL = "drdsPartitionUpdateProcessMinMatchEventTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_CURRENT_MATCH_EVENT_TIME_INTERVAL = "drdsPartitionUpdateProcessCurrentMatchEventTimeInterval";

    public static final String DRDS_PARTITION_UPDATE_PROCESS_MAX_MATCH_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessMaxMatchProcessTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_MIN_MATCH_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessMinMatchProcessTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_CURRENT_MATCH_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessCurrentMatchProcessTimeInterval";

    public static final String DRDS_PARTITION_UPDATE_PROCESS_MAX_ON_TIMER_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessMaxOnTimerProcessTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_MIN_ON_TIMER_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessMinOnTimerProcessTimeInterval";
    public static final String DRDS_PARTITION_UPDATE_PROCESS_CURRENT_ON_TIMER_PROCESS_TIME_INTERVAL = "drdsPartitionUpdateProcessCurrentOnTimerProcessTimeInterval";

    public static long maxGaugeValue(long maxGaugeValue, long currentGaugeValue) {
        long value = maxGaugeValue;
        if (value == Long.MIN_VALUE) {
            return currentGaugeValue;
        }
        return value;
    }

    public static long minGaugeValue(long minGaugeValue, long currentGaugeValue) {
        long value = minGaugeValue;
        if (value == Long.MAX_VALUE) {
            return currentGaugeValue;
        }
        return value;
    }
}
