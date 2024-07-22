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
    public static final String MAX_EVENT_READER_THROUGHOUT_TIME_DELAY = "maxEventReaderThroughoutTimeDelay";

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

    public static final String MATCHED_MAX_EVENT_READER_THROUGHOUT_TIME_DELAY = "matchedMaxEventReaderThroughoutTimeDelay";
    public static final String ADD_STATE_COUNT = "addStateCount";
    public static final String FIRST_ADD_STATE_COUNT = "firstAddStateCount";
    public static final String AGAIN_ADD_STATE_COUNT = "againAddStateCount";
    public static final String REMOVE_STATE_COUNT = "removeStateCount";
    public static final String MATCHED_REMOVE_STATE_COUNT = "matchedRemoveStateCount";
    public static final String ON_TIMER_REMOVE_STATE_COUNT = "onTimerRemoveStateCount";
    public static final String UPDATE_EXPIRED_COUNT = "updateExpiredCount";
    public static final String MATCHED_HAVE_EXPIRED_COUNT = "matchedHaveExpiredCount";
    public static final String ON_TIMER_HAVE_EXPIRED_COUNT = "onTimerHaveExpiredCount";
    public static final String AGAIN_REGISTER_TIMER_COUNT = "againRegisterTimerCount";
    public static final String MERGE_INTO_UPDATE = "mergeIntoUpdateCount";
    public static final String NOT_MATCH_STATE_COUNT = "notMatchStateCount";
    public static final String DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT = "drdsCanalJsonWrapperValidateFailCount";
    public static final String OPERATION_HAVE_EXPIRED_COUNT = "operationHaveExpiredCount";

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
