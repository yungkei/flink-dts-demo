package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.metric.CdcMetricNames;
import com.google.common.hash.Hashing;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DrdsCdcProcessFunction extends KeyedProcessFunction<String, CanalJsonWrapper, CanalJsonWrapper> {
    public static final String MERGE_SUFFIX = "_MERGEINTO_UPDATE";
    public static final String NOMATCH_DROP_SUFFIX = "_NOMATCH_DROP";
    private static final Logger LOG = LoggerFactory.getLogger(DrdsCdcProcessFunction.class);
    private static final long serialVersionUID = 1L;
    private static final long TIME_INTERVAL_MS = 1000L;
    public static final String TRANSACTION_ID = "transactionId";
    public static final String TRANSACTION_INDEX = "transactionIndex";

    private transient MapState<String, CanalJsonWrapper> mapState;
    private transient Counter partitionUpdateProcessMergeCount;
    private transient Counter partitionUpdateProcessAddStateCount;
    private transient Counter partitionUpdateProcessAddStateFailCount;
    private transient Counter partitionUpdateProcessRemoveStateCount;
    private transient Counter partitionUpdateProcessNotMatchStateCount;
    private transient Counter partitionUpdateProcessOnTimerStateCount;
    private transient Counter partitionUpdateProcessOnTimerStateFailCount;
    private static Counter canalJsonWrapperValidateFailCount;
    private transient long partitionUpdateProcessMaxMatchEventTimeInterval = Long.MIN_VALUE;
    private transient long partitionUpdateProcessMinMatchEventTimeInterval = Long.MAX_VALUE;
    private transient long partitionUpdateProcessCurrentMatchEventTimeInterval;
    private transient long partitionUpdateProcessMaxMatchProcessTimeInterval = Long.MIN_VALUE;
    private transient long partitionUpdateProcessMinMatchProcessTimeInterval = Long.MAX_VALUE;
    private transient long partitionUpdateProcessCurrentMatchProcessTimeInterval;
    private transient long partitionUpdateProcessMaxOnTimerProcessTimeInterval = Long.MIN_VALUE;
    private transient long partitionUpdateProcessMinOnTimerProcessTimeInterval = Long.MAX_VALUE;
    private transient long partitionUpdateProcessCurrentOnTimerProcessTimeInterval;
    private ValueState<Long> previousMaxEventTimestamp;

    private long stateTtl = 180L;

    public void setStateTtl(long stateTtl) {
        this.stateTtl = stateTtl;
    }

    private long timerTimeInternalMs = TIME_INTERVAL_MS;

    public void setTimerTimeInternalMs(long timerTimeInternalMs) {
        this.timerTimeInternalMs = timerTimeInternalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(this.stateTtl)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).cleanupFullSnapshot().build();

        MapStateDescriptor<String, CanalJsonWrapper> descriptor = new MapStateDescriptor<>("drdsCdcState", String.class, CanalJsonWrapper.class);
//        descriptor.enableTimeToLive(ttlConfig);
        this.mapState = getRuntimeContext().getMapState(descriptor);
        ValueStateDescriptor<Long> descriptorPreviousMaxEventTimestamp = new ValueStateDescriptor<>("sortedConnectedWatermark2", Long.class);
        this.previousMaxEventTimestamp = getRuntimeContext().getState(descriptorPreviousMaxEventTimestamp);

        this.partitionUpdateProcessMergeCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MERGE_COUNT);
        this.partitionUpdateProcessAddStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_COUNT);
        this.partitionUpdateProcessAddStateFailCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_FAIL_COUNT);
        this.partitionUpdateProcessRemoveStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_REMOVE_STATE_COUNT);
        this.partitionUpdateProcessNotMatchStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_NOT_MATCH_STATE_COUNT);
        this.partitionUpdateProcessOnTimerStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_ON_TIMER_STATE_COUNT);
        this.partitionUpdateProcessOnTimerStateFailCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_ON_TIMER_STATE_FAIL_COUNT);
        canalJsonWrapperValidateFailCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_CURRENT_MATCH_EVENT_TIME_INTERVAL, () -> this.partitionUpdateProcessCurrentMatchEventTimeInterval);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MAX_MATCH_EVENT_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMaxMatchEventTimeInterval;
            if (value == Long.MIN_VALUE) {
                return partitionUpdateProcessCurrentMatchEventTimeInterval;
            }
            partitionUpdateProcessMaxMatchEventTimeInterval = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MIN_MATCH_EVENT_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMinMatchEventTimeInterval;
            if (value == Long.MAX_VALUE) {
                return partitionUpdateProcessCurrentMatchEventTimeInterval;
            }
            partitionUpdateProcessMinMatchEventTimeInterval = Long.MAX_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_CURRENT_MATCH_PROCESS_TIME_INTERVAL, () -> this.partitionUpdateProcessCurrentMatchProcessTimeInterval);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MAX_MATCH_PROCESS_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMaxMatchProcessTimeInterval;
            if (value == Long.MIN_VALUE) {
                return partitionUpdateProcessCurrentMatchProcessTimeInterval;
            }
            partitionUpdateProcessMaxMatchProcessTimeInterval = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MIN_MATCH_PROCESS_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMinMatchProcessTimeInterval;
            if (value == Long.MAX_VALUE) {
                return partitionUpdateProcessCurrentMatchProcessTimeInterval;
            }
            partitionUpdateProcessMinMatchProcessTimeInterval = Long.MAX_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_CURRENT_ON_TIMER_PROCESS_TIME_INTERVAL, () -> this.partitionUpdateProcessCurrentMatchProcessTimeInterval);
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MAX_ON_TIMER_PROCESS_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMaxOnTimerProcessTimeInterval;
            if (value == Long.MIN_VALUE) {
                return partitionUpdateProcessCurrentOnTimerProcessTimeInterval;
            }
            partitionUpdateProcessMaxOnTimerProcessTimeInterval = Long.MIN_VALUE;
            return value;
        });
        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MIN_ON_TIMER_PROCESS_TIME_INTERVAL, () -> {
            long value = partitionUpdateProcessMinOnTimerProcessTimeInterval;
            if (value == Long.MAX_VALUE) {
                return partitionUpdateProcessCurrentOnTimerProcessTimeInterval;
            }
            partitionUpdateProcessMinOnTimerProcessTimeInterval = Long.MAX_VALUE;
            return value;
        });
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<CanalJsonWrapper> out) throws Exception {
        String key = context.getCurrentKey();
        CanalJsonWrapper storeState = mapState.get(key);
        if (storeState != null) {
            partitionUpdateProcessOnTimerStateCount.inc();
            long processTimestamp;
            if (storeState.getTags() != null && storeState.getTags().get("processTimestamp") != null) {
                processTimestamp = Long.valueOf(storeState.getTags().get("processTimestamp"));
                this.partitionUpdateProcessCurrentOnTimerProcessTimeInterval = System.currentTimeMillis() - processTimestamp;
                this.partitionUpdateProcessMaxOnTimerProcessTimeInterval = Math.max(partitionUpdateProcessCurrentOnTimerProcessTimeInterval, partitionUpdateProcessMaxOnTimerProcessTimeInterval);
                this.partitionUpdateProcessMinOnTimerProcessTimeInterval = Math.min(partitionUpdateProcessCurrentOnTimerProcessTimeInterval, partitionUpdateProcessMinOnTimerProcessTimeInterval);
            }

            Map<String, String> storeTags = storeState.getTags();
            storeTags.put("stateOnTimerTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
            storeTags.put("partitionUpdateOutTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
            storeState.setTags(storeTags);
            out.collect(storeState);
            mapState.remove(key);
        } else {
            LOG.warn("onTimer storeState is Null");
            partitionUpdateProcessOnTimerStateFailCount.inc();
        }
    }

    @Override
    public void processElement(CanalJsonWrapper in, Context context, Collector<CanalJsonWrapper> out) throws Exception {
        Map<String, String> inTags = in.getTags();
        inTags.put("partitionUpdateInTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
        String stateKey = generateStateKey(in, true);
        if (mapState.contains(stateKey)) {
            CanalJsonWrapper stateValue = mapState.get(stateKey);
            if (shouldPerformState(in, stateValue)) {
                CanalJson canalJson = in.getCanalJson();
                CanalJson canalJsonState = stateValue.getCanalJson();

                long eventTimestamp = Long.valueOf(stateValue.getTags().get("eventTimestamp"));
                long processTimestamp = Long.valueOf(stateValue.getTags().get("processTimestamp"));
                long watermark = Long.valueOf(stateValue.getTags().get("watermark"));
                long eventTimer = Long.valueOf(stateValue.getTags().get("eventTimer"));
                inTags.put("eventTimestamp", String.valueOf(eventTimestamp));
                inTags.put("processTimestamp", String.valueOf(processTimestamp));
                inTags.put("watermark", String.valueOf(watermark));
                inTags.put("eventTimer", String.valueOf(eventTimer));

                inTags.put("stateRegisterTimerTime", stateValue.getTags().get("stateRegisterTimerTime"));
                inTags.put("stateMatchStateTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
                inTags.put("partitionUpdateOutTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));

                CanalJson canalJsonMerged = mergeState(canalJson, canalJsonState);
                CanalJsonWrapper canalJsonWrapperMerged = createCanalJsonWrapper(canalJsonMerged);
                canalJsonWrapperMerged.setTags(inTags);
                out.collect(canalJsonWrapperMerged);

                canalJsonState.setType(canalJsonState.getType() + MERGE_SUFFIX);
                CanalJsonWrapper canalJsonWrapperState = createCanalJsonWrapper(canalJsonState);
                canalJsonWrapperState.setTags(inTags);
                out.collect(canalJsonWrapperState);

                canalJson.setType(canalJson.getType() + MERGE_SUFFIX);
                CanalJsonWrapper canalJsonWrapper = createCanalJsonWrapper(canalJson);
                canalJsonWrapper.setTags(inTags);
                out.collect(canalJsonWrapper);

                partitionUpdateProcessMergeCount.inc();
                mapState.remove(stateKey);
                this.partitionUpdateProcessCurrentMatchEventTimeInterval = context.timestamp() - eventTimestamp;
                this.partitionUpdateProcessMaxMatchEventTimeInterval = Math.max(partitionUpdateProcessCurrentMatchEventTimeInterval, partitionUpdateProcessMaxMatchEventTimeInterval);
                this.partitionUpdateProcessMinMatchEventTimeInterval = Math.min(partitionUpdateProcessCurrentMatchEventTimeInterval, partitionUpdateProcessMinMatchEventTimeInterval);

                this.partitionUpdateProcessCurrentMatchProcessTimeInterval = System.currentTimeMillis() - processTimestamp;
                this.partitionUpdateProcessMaxMatchProcessTimeInterval = Math.max(partitionUpdateProcessCurrentMatchProcessTimeInterval, partitionUpdateProcessMaxMatchProcessTimeInterval);
                this.partitionUpdateProcessMinMatchProcessTimeInterval = Math.min(partitionUpdateProcessCurrentMatchProcessTimeInterval, partitionUpdateProcessMinMatchProcessTimeInterval);

                context.timerService().deleteEventTimeTimer(eventTimer);
                partitionUpdateProcessRemoveStateCount.inc();
            } else {
                long eventTimestamp = Long.valueOf(stateValue.getTags().get("eventTimestamp"));
                long processTimestamp = Long.valueOf(stateValue.getTags().get("processTimestamp"));
                long watermark = Long.valueOf(stateValue.getTags().get("watermark"));
                long eventTimer = Long.valueOf(stateValue.getTags().get("eventTimer"));
                inTags.put("stateRegisterTimerTime", stateValue.getTags().get("stateRegisterTimerTime"));
                inTags.put("partitionUpdateOutTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
                inTags.put("eventTimestamp", String.valueOf(eventTimestamp));
                inTags.put("processTimestamp", String.valueOf(processTimestamp));
                inTags.put("watermark", String.valueOf(watermark));
                inTags.put("eventTimer", String.valueOf(eventTimer));

                if ("rme_eqp_model".equalsIgnoreCase(in.getTable())) {
                    CanalJson canalJson = in.getCanalJson();
                    canalJson.setType(canalJson.getType() + MERGE_SUFFIX);
                    CanalJsonWrapper canalJsonWrapper = createCanalJsonWrapper(canalJson);
                    canalJsonWrapper.setTags(inTags);
                    out.collect(canalJsonWrapper);
                    out.collect(in);
                } else {
                    Long stateEventTimestamp = Long.valueOf(stateValue.getTags().get("eventTimestamp"));
                    Long currentEventTimestamp = context.timestamp();
                    if (stateEventTimestamp <= currentEventTimestamp) {
                        out.collect(stateValue);
                        mapState.put(stateKey, in);
                        context.timerService().registerEventTimeTimer(currentEventTimestamp + timerTimeInternalMs);
                    } else {
                        out.collect(in);
                    }
                    partitionUpdateProcessNotMatchStateCount.inc();
                }
            }
        } else {
            long eventTimestamp = context.timestamp();
            long processTimestamp = System.currentTimeMillis();
            long watermark = context.timerService().currentWatermark();
            long dynamicInterval = (this.previousMaxEventTimestamp.value() == null ? eventTimestamp : this.previousMaxEventTimestamp.value()) - watermark;
            long eventTimer = watermark + (dynamicInterval > 0 ? dynamicInterval : 0) + timerTimeInternalMs;
            inTags.put("eventTimestamp", String.valueOf(eventTimestamp));
            inTags.put("processTimestamp", String.valueOf(processTimestamp));
            inTags.put("stateRegisterTimerTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
            inTags.put("watermark", String.valueOf(watermark));
            inTags.put("eventTimer", String.valueOf(eventTimer));
            in.setTags(inTags);
            mapState.put(stateKey, in);
            partitionUpdateProcessAddStateCount.inc();
            try {
                context.timerService().registerEventTimeTimer(eventTimer);
            } catch (Exception e) {
                LOG.warn("register eventTimeTimer failed", e);
                partitionUpdateProcessAddStateFailCount.inc();
                out.collect(in);
            }
        }

        this.refreshPreviousMaxEventTimestamp(context.timestamp());
    }

    private void refreshPreviousMaxEventTimestamp(long eventTimestamp) throws IOException {
        if (this.previousMaxEventTimestamp.value() == null) {
            this.previousMaxEventTimestamp.update(eventTimestamp);
        } else {
            if (eventTimestamp > this.previousMaxEventTimestamp.value().longValue()) {
                this.previousMaxEventTimestamp.update(eventTimestamp);
            }
        }
    }

    public static boolean shouldAdditionalProcess(CanalJsonWrapper canalJsonWrapper) {
        if ("DDL".equalsIgnoreCase(canalJsonWrapper.getOperation())) {
            return false;
        }
        if (!validateCanalJsonWrapper(canalJsonWrapper)) {
            canalJsonWrapperValidateFailCount.inc();
            LOG.warn("canalJsonWrapperValidateFail, canalJsonWrapper:{}", canalJsonWrapper);
            return false;
        }
        Map<String, String> tags = canalJsonWrapper.getTags();
        String traceType = tags.get(TRANSACTION_INDEX);
        String operation = canalJsonWrapper.getOperation();
        if ("insert".equalsIgnoreCase(operation) && ("3".equals(traceType) || "2".equalsIgnoreCase(traceType)) || ("delete".equalsIgnoreCase(operation) && ("1".equals(traceType) || "2".equals(traceType)))) {
            return true;
        }
        if (canalJsonWrapper.getTags() == null || canalJsonWrapper.getTags().get(TRANSACTION_ID) == null) {
            return false;
        }
        return false;

    }

    public static boolean validateCanalJsonWrapper(CanalJsonWrapper canalJsonWrapper) {
        if (StringUtils.isBlank(canalJsonWrapper.getPk()) || StringUtils.isBlank(canalJsonWrapper.getOperation()) || StringUtils.isBlank(canalJsonWrapper.getDatabase()) || StringUtils.isBlank(canalJsonWrapper.getTable()) || canalJsonWrapper.getCanalJson() == null) {
            return false;
        }
        if (StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_ID)) || StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_INDEX))) {
            return false;
        }

        return true;
    }

    public static CanalJsonWrapper createCanalJsonWrapper(CanalJson canalJson) {
        CanalJsonWrapper canalJsonWrapper = new CanalJsonWrapper();
        canalJsonWrapper.setCanalJson(canalJson);
        canalJsonWrapper.setOperation(canalJson.getType());
        canalJsonWrapper.setDatabase(canalJson.getDatabase());
        canalJsonWrapper.setTable(canalJson.getTable());

        StringBuilder pkSb = new StringBuilder();
        List<String> pkNames = canalJson.getPkNames();
        if (pkNames != null && !pkNames.isEmpty() && canalJson.getData() != null && !canalJson.getData().isEmpty()) {
            Map<String, String> dataObject = canalJson.getData().get(0);
            if (dataObject != null) {
                for (String pkName : pkNames) {
                    pkSb.append(dataObject.get(pkName));
                }
            }
        }
        canalJsonWrapper.setPk(pkSb.toString());

        Map<String, String> canalWrapperTags = new HashMap<>();
        Map<String, Map<String, String>> canalJsonTags = canalJson.getTags();
        if (canalJsonTags != null) {
            Map<String, String> dts = canalJsonTags.get("dts");
            if (dts != null && dts.containsKey("traceid")) {
                String traceId = dts.getOrDefault("traceid", "");
                String traceRegex = "DRDS /.*/(.*){1}/(.*){1}//.?";
                Pattern tracePattern = Pattern.compile(traceRegex);
                Matcher traceMatcher = tracePattern.matcher(traceId);
                if (traceMatcher.find()) {
                    canalWrapperTags.put(TRANSACTION_ID, traceMatcher.group(1));
                    canalWrapperTags.put(TRANSACTION_INDEX, traceMatcher.group(2));
                }
            }
        }
        canalJsonWrapper.setTags(canalWrapperTags);
        return canalJsonWrapper;
    }


    public static String generateStateKey(CanalJsonWrapper canalJsonWrapper, boolean withTraceId) {
        StringBuilder sb = new StringBuilder();
        sb.append(canalJsonWrapper.getDatabase());
        sb.append(canalJsonWrapper.getTable());
        sb.append(canalJsonWrapper.getPk());
        if (withTraceId) {
            sb.append(canalJsonWrapper.getTags().get(TRANSACTION_ID));
        }
        return String.valueOf(Hashing.murmur3_128().hashString(sb.toString(), StandardCharsets.UTF_8).asLong());
    }

    public boolean shouldPerformState(CanalJsonWrapper current, CanalJsonWrapper store) {
        String operation = current.getOperation();
        String operationStored = store.getOperation();
        if (("insert".equalsIgnoreCase(operation) && "delete".equalsIgnoreCase(operationStored)) || ("delete".equalsIgnoreCase(operation) && "insert".equalsIgnoreCase(operationStored))) {
            return true;
        }
        return false;
    }

    public CanalJson mergeState(CanalJson current, CanalJson store) throws Exception {
        CanalJson canalJsonMerged = new CanalJson();
        if ("insert".equalsIgnoreCase(current.getType()) && "delete".equalsIgnoreCase(store.getType())) {
            BeanUtils.copyProperties(canalJsonMerged, current);
            canalJsonMerged.setOld(store.getData());
        } else if ("delete".equalsIgnoreCase(current.getType()) && "insert".equalsIgnoreCase(store.getType())) {
            BeanUtils.copyProperties(canalJsonMerged, store);
            canalJsonMerged.setOld(current.getData());
        }
        canalJsonMerged.setType("UPDATE");
        return canalJsonMerged;
    }

}
