package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonUtils;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.deserialize.ByteRecord;
import com.alibaba.blink.datastreaming.datastream.deserialize.JsonDtsRecord;
import com.alibaba.blink.datastreaming.datastream.metric.CdcMetricNames;
import com.alibaba.fastjson2.JSONObject;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordgenerator.AvroDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dts2CanalProcessFunction extends ProcessFunction<ByteRecord, CanalJsonWrapper> {
    private static final Logger LOG = LoggerFactory.getLogger(Dts2CanalProcessFunction.class);
    public static final long UNDEFINED = -1L;
    private transient long maxDtsReaderDelay = Long.MIN_VALUE;
    private transient long minDtsReaderDelay = Long.MAX_VALUE;
    private transient long currentDtsReaderDelay;
    private transient Counter cdcRecordsCount;
    private transient Meter cdcRecordsPerSecond;
    private transient long currentReaderThroughoutTimeDelay;
    private transient long currentReaderThroughoutTimestamp;
    private transient long currentKafkaWatermarkDelay;
    private transient long maxKafkaWatermarkDelay;
    public transient long minKafkaWatermarkDelay;
    private transient long currentEventWatermarkDelay;
    public transient long maxEventWatermarkDelay;
    public transient long minEventWatermarkDelay;

    private transient long matchedMaxDtsReaderDelay = Long.MIN_VALUE;
    private transient long matchedMinDtsReaderDelay = Long.MAX_VALUE;
    private transient long matchedCurrentDtsReaderDelay;
    private transient Counter matchedCdcRecordsCount;
    private transient Meter matchedCdcRecordsPerSecond;
    private transient long matchedCurrentReaderThroughoutTimeDelay;
    private transient long matchedCurrentReaderThroughoutTimestamp;
    private transient Counter matchedDdlRecordsCount;
    private transient Counter matchedDmlRecordsCount;
    private transient Counter matchedInsertRecordsCount;
    private transient Counter matchedUpdateRecordsCount;
    private transient Counter matchedDeleteRecordsCount;
    private transient long matchedCurrentSourceTimestamp;
    private transient long matchedMaxSourceTimestamp = Long.MIN_VALUE;
    private transient long matchedMinSourceTimestamp = Long.MAX_VALUE;
    private transient long matchedCurrentKafkaWatermarkDelay;
    private transient long matchedMaxKafkaWatermarkDelay = Long.MIN_VALUE;
    public transient long matchedMinKafkaWatermarkDelay = Long.MAX_VALUE;
    private transient long matchedCurrentEventWatermarkDelay;
    public transient long matchedMaxEventWatermarkDelay = Long.MIN_VALUE;
    public transient long matchedMinEventWatermarkDelay = Long.MAX_VALUE;

    private List<RouteDef> routeDefs;
    private HashMap<String, String> extraColumns;
    private String extraPrimaryKeys;
    private String includingTables;
    private String excludingTables;
    private String mapToString;
    private String enableDdl;
    private String dtsTopic;

    public void setRouteDefs(List<RouteDef> routeDefs) {
        this.routeDefs = routeDefs;
    }

    public void setExtraColumns(HashMap<String, String> extraColumns) {
        this.extraColumns = extraColumns;
    }

    public void setExtraPrimaryKeys(String extraPrimaryKeys) {
        this.extraPrimaryKeys = extraPrimaryKeys;
    }

    public void setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
    }

    public void setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
    }

    public void setMapToString(String mapToString) {
        this.mapToString = mapToString;
    }

    public void setEnableDdl(String enableDdl) {
        this.enableDdl = enableDdl;
    }

    public void setDtsTopic(String dtsTopic) {
        this.dtsTopic = dtsTopic;
    }

    @Override
    public void open(Configuration config) {
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MAX_DTS_READER_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(maxDtsReaderDelay, currentDtsReaderDelay);
            maxDtsReaderDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MIN_DTS_READER_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(minDtsReaderDelay, currentDtsReaderDelay);
            minDtsReaderDelay = Long.MAX_VALUE;
            return value;
        });
        this.cdcRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.CDC_RECORDS_COUNT);
        this.cdcRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.CDC_RECORDS_PER_SECOND, new MeterView(1));
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.CURRENT_READER_THROUGHOUT_TIME_DELAY, () -> this.currentReaderThroughoutTimeDelay);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.CURRENT_READER_THROUGHOUT_TIMESTAMP, () -> this.currentReaderThroughoutTimestamp);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MAX_KAFKA_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(maxKafkaWatermarkDelay, currentKafkaWatermarkDelay);
            maxKafkaWatermarkDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MIN_KAFKA_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(minKafkaWatermarkDelay, currentKafkaWatermarkDelay);
            minKafkaWatermarkDelay = Long.MAX_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MAX_EVENT_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(maxEventWatermarkDelay, currentEventWatermarkDelay);
            maxEventWatermarkDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MIN_EVENT_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(minEventWatermarkDelay, currentEventWatermarkDelay);
            minEventWatermarkDelay = Long.MAX_VALUE;
            return value;
        });

        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MAX_DTS_READER_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(matchedMaxDtsReaderDelay, matchedCurrentDtsReaderDelay);
            matchedMaxDtsReaderDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MIN_DTS_READER_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(matchedMinDtsReaderDelay, matchedCurrentDtsReaderDelay);
            matchedMinDtsReaderDelay = Long.MAX_VALUE;
            return value;
        });
        this.matchedCdcRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_CDC_RECORDS_COUNT);
        this.matchedCdcRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.MATCHED_CDC_RECORDS_PER_SECOND, new MeterView(1));
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_CURRENT_READER_THROUGHOUT_TIME_DELAY, () -> this.matchedCurrentReaderThroughoutTimeDelay);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_CURRENT_READER_THROUGHOUT_TIMESTAMP, () -> this.matchedCurrentReaderThroughoutTimestamp);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MAX_KAFKA_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(matchedMaxKafkaWatermarkDelay, matchedCurrentKafkaWatermarkDelay);
            matchedMaxKafkaWatermarkDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MIN_KAFKA_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(matchedMinKafkaWatermarkDelay, matchedCurrentKafkaWatermarkDelay);
            matchedMinKafkaWatermarkDelay = Long.MAX_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MAX_EVENT_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.maxGaugeValue(matchedMaxEventWatermarkDelay, matchedCurrentEventWatermarkDelay);
            matchedMaxEventWatermarkDelay = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MIN_EVENT_WATERMARK_DELAY, () -> {
            long value = CdcMetricNames.minGaugeValue(matchedMinEventWatermarkDelay, matchedCurrentEventWatermarkDelay);
            matchedMinEventWatermarkDelay = Long.MAX_VALUE;
            return value;
        });

        this.matchedDdlRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_DDL_RECORDS_COUNT);
        this.matchedDmlRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_DML_RECORDS_COUNT);
        this.matchedInsertRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_INSERT_RECORDS_COUNT);
        this.matchedDeleteRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_DELETE_RECORDS_COUNT);
        this.matchedUpdateRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_UPDATE_RECORDS_COUNT);

        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MAX_SOURCE_TIMESTAMP, () -> {
            long value = CdcMetricNames.maxGaugeValue(matchedMaxSourceTimestamp, matchedCurrentSourceTimestamp);
            matchedMaxSourceTimestamp = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MATCHED_MIN_SOURCE_TIMESTAMP, () -> {
            long value = CdcMetricNames.minGaugeValue(matchedMinSourceTimestamp, matchedCurrentSourceTimestamp);
            matchedMinSourceTimestamp = Long.MAX_VALUE;
            return value;
        });

    }

    private void setMetricGenericRecordsPerSecond(String type) {
        switch (type) {
            case "INSERT": {
                this.matchedInsertRecordsCount.inc();
                this.matchedDmlRecordsCount.inc();
                break;
            }
            case "DELETE": {
                this.matchedDeleteRecordsCount.inc();
                this.matchedDmlRecordsCount.inc();
                break;
            }
            case "UPDATE": {
                this.matchedUpdateRecordsCount.inc();
                this.matchedDmlRecordsCount.inc();
                break;
            }
            case "DDL": {
                this.matchedDdlRecordsCount.inc();
                break;
            }
        }
    }

    private void setInputMetric(JSONObject dtsRecord, Context context) {
        boolean hasSourceTimestamp = dtsRecord.getString("sourceTimestamp") != null;
        boolean hasReaderThroughoutTime = dtsRecord.getJSONObject("tags") != null && dtsRecord.getJSONObject("tags").getString("readerThroughoutTime") != null;

        try {
            if (hasSourceTimestamp && hasReaderThroughoutTime) {
                long sourceTimestamp = Long.valueOf(dtsRecord.getString("sourceTimestamp")) * 1000;
                long readerThroughoutTime = Long.valueOf(dtsRecord.getJSONObject("tags").getString("readerThroughoutTime"));
                this.currentDtsReaderDelay = readerThroughoutTime - sourceTimestamp;
                this.maxDtsReaderDelay = Math.max(currentDtsReaderDelay, maxDtsReaderDelay);
                this.minDtsReaderDelay = Math.max(currentDtsReaderDelay, minDtsReaderDelay);
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.MAX_DTS_READER_DELAY, e);
        }
        try {
            this.cdcRecordsCount.inc();
            this.cdcRecordsPerSecond.markEvent();
        } catch (Exception e) {
            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.CDC_RECORDS_PER_SECOND, e);
        }
        try {
            if (hasReaderThroughoutTime) {
                this.currentReaderThroughoutTimestamp = Long.valueOf(dtsRecord.getJSONObject("tags").getString("readerThroughoutTime"));
                this.currentReaderThroughoutTimeDelay = System.currentTimeMillis() - currentReaderThroughoutTimestamp;
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.CURRENT_READER_THROUGHOUT_TIME_DELAY, e);
        }
        try {
            if (hasSourceTimestamp) {
                long sourceTimestamp = Long.valueOf(dtsRecord.getString("sourceTimestamp")) * 1000;
                long watermark = context.timerService().currentWatermark();
                this.currentKafkaWatermarkDelay = sourceTimestamp - watermark;
                this.maxKafkaWatermarkDelay = Math.max(currentKafkaWatermarkDelay, maxKafkaWatermarkDelay);
                this.minKafkaWatermarkDelay = Math.max(currentKafkaWatermarkDelay, minKafkaWatermarkDelay);
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.MAX_KAFKA_WATERMARK_DELAY, e);
        }
        try {
            long eventTime = context.timestamp();
            long watermark = context.timerService().currentWatermark();
            this.currentEventWatermarkDelay = eventTime - watermark;
            this.maxEventWatermarkDelay = Math.max(currentEventWatermarkDelay, maxEventWatermarkDelay);
            this.minEventWatermarkDelay = Math.max(currentEventWatermarkDelay, minEventWatermarkDelay);
        } catch (Exception e) {
            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.MAX_EVENT_WATERMARK_DELAY, e);
        }

    }

    private void setOutputMetric(JSONObject dtsRecord, Context context) {
        boolean hasSourceTimestamp = dtsRecord.getString("sourceTimestamp") != null;
        boolean hasReaderThroughoutTime = dtsRecord.getJSONObject("tags") != null && dtsRecord.getJSONObject("tags").getString("readerThroughoutTime") != null;

        try {
            if (hasSourceTimestamp && hasReaderThroughoutTime) {
                long sourceTimestamp = Long.valueOf(dtsRecord.getString("sourceTimestamp")) * 1000;
                long readerThroughoutTime = Long.valueOf(dtsRecord.getJSONObject("tags").getString("readerThroughoutTime"));
                this.matchedCurrentDtsReaderDelay = readerThroughoutTime - sourceTimestamp;
                this.matchedMaxDtsReaderDelay = Math.max(matchedCurrentDtsReaderDelay, matchedMaxDtsReaderDelay);
                this.matchedMinDtsReaderDelay = Math.max(matchedCurrentDtsReaderDelay, matchedMinDtsReaderDelay);
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_MAX_DTS_READER_DELAY, e);
        }
        try {
            this.matchedCdcRecordsCount.inc();
            this.matchedCdcRecordsPerSecond.markEvent();
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_CDC_RECORDS_PER_SECOND, e);
        }
        try {
            if (hasReaderThroughoutTime) {
                this.matchedCurrentReaderThroughoutTimestamp = Long.valueOf(dtsRecord.getJSONObject("tags").getString("readerThroughoutTime"));
                this.matchedCurrentReaderThroughoutTimeDelay = System.currentTimeMillis() - matchedCurrentReaderThroughoutTimestamp;
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_CURRENT_READER_THROUGHOUT_TIME_DELAY, e);
        }
        try {
            if (hasSourceTimestamp) {
                long sourceTimestamp = Long.valueOf(dtsRecord.getString("sourceTimestamp")) * 1000;
                long watermark = context.timerService().currentWatermark();
                this.matchedCurrentKafkaWatermarkDelay = sourceTimestamp - watermark;
                this.matchedMaxKafkaWatermarkDelay = Math.max(matchedCurrentKafkaWatermarkDelay, matchedMaxKafkaWatermarkDelay);
                this.matchedMinKafkaWatermarkDelay = Math.max(matchedCurrentKafkaWatermarkDelay, matchedMinKafkaWatermarkDelay);
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_MAX_KAFKA_WATERMARK_DELAY, e);
        }
        try {
            long eventTime = context.timestamp();
            long watermark = context.timerService().currentWatermark();
            this.matchedCurrentEventWatermarkDelay = eventTime - watermark;
            this.matchedMaxEventWatermarkDelay = Math.max(matchedCurrentEventWatermarkDelay, matchedMaxEventWatermarkDelay);
            this.matchedMinEventWatermarkDelay = Math.max(matchedCurrentEventWatermarkDelay, matchedMinEventWatermarkDelay);
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_MAX_EVENT_WATERMARK_DELAY, e);
        }


        try {
            String operation = dtsRecord.getString("operation");
            setMetricGenericRecordsPerSecond(operation);
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_CDC_RECORDS_PER_SECOND, e);
        }

        try {
            if (hasSourceTimestamp) {
                long sourceTimestamp = Long.valueOf(dtsRecord.getString("sourceTimestamp")) * 1000;
                this.matchedCurrentSourceTimestamp = sourceTimestamp;
                this.matchedMaxSourceTimestamp = Math.max(matchedCurrentSourceTimestamp, matchedMaxSourceTimestamp);
                this.matchedMinSourceTimestamp = Math.min(matchedCurrentSourceTimestamp, matchedMinSourceTimestamp);
            }
        } catch (Exception e) {
            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.MATCHED_MAX_SOURCE_TIMESTAMP, e);
        }
    }

    @Override
    public void processElement(ByteRecord byteRecord, Context context, Collector<CanalJsonWrapper> out) throws Exception {
        long sourceInTime = System.currentTimeMillis();
        if (byteRecord != null) {
            JsonDtsRecord record;
            JSONObject dtsJson;
            try {
                record = new JsonDtsRecord(byteRecord.getBytes(), new AvroDeserializer());
                dtsJson = record.getJson();
            } catch (Exception e) {
                LOG.error("record.getJson error:", e);
                return;
            }
            setInputMetric(dtsJson, context);

            if (OperationType.INSERT == record.getOperationType() || OperationType.UPDATE == record.getOperationType() || OperationType.DELETE == record.getOperationType() || (OperationType.DDL == record.getOperationType() && "true".equals(enableDdl))) {
                String dtsObjectName = dtsJson.getString("objectName");
                if (!AbstractDtsToKafkaFlinkAction.shouldMonitorTable(dtsObjectName, includingTables, excludingTables)) {
                    return;
                }
                LOG.debug("Source table '{}' is included.", dtsObjectName);
                LOG.debug("dtsJson:{}", dtsJson);
                try {
                    CanalJson canalJson = CanalJsonUtils.convert(dtsJson, routeDefs, extraColumns, extraPrimaryKeys, mapToString);
                    if (canalJson == null) {
                        return;
                    }
                    LOG.debug("canalJson:{}", canalJson);
                    setOutputMetric(dtsJson, context);
                    CanalJsonWrapper canalJsonWrapper = DrdsCdcProcessFunction.createCanalJsonWrapper(canalJson);
                    long dts2canalOutTimestamp = System.currentTimeMillis();
                    Map<String, String> tags = canalJsonWrapper.getTags();
                    tags.put("sourceInTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(sourceInTime));
                    tags.put("dts2canalOutTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(dts2canalOutTimestamp));
                    tags.put("dtsTopic", dtsTopic);
                    canalJsonWrapper.setTags(tags);
                    out.collect(canalJsonWrapper);
                } catch (Exception ex) {
                    LOG.warn("parse dts {} to canal failed :", record, ex);
                }
            }
        }
    }
}
