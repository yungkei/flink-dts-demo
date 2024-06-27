package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.metric.CdcMetricNames;
import com.google.common.hash.Hashing;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DrdsCdcProcessFunction extends KeyedProcessFunction<String, CanalJsonWrapper, CanalJsonWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(DrdsCdcProcessFunction.class);
    private static final long serialVersionUID = 1L;
    private static final long TIME_INTERVAL_MS = 10000L;
    public static final String TRANSACTION_ID = "transactionId";
    public static final String TRANSACTION_INDEX = "transactionIndex";

    private transient MapState<String, CanalJsonWrapper> mapState;
    private transient Counter partitionUpdateProcessMergeCount;
    private transient Counter partitionUpdateProcessAddStateCount;
    private transient Counter partitionUpdateProcessRemoveStateCount;
    private transient Counter partitionUpdateProcessNotMatchStateCount;

    private static Counter canalJsonWrapperValidateFailCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(6000)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).cleanupFullSnapshot().build();

        MapStateDescriptor<String, CanalJsonWrapper> descriptor = new MapStateDescriptor<>("cdcState", String.class, CanalJsonWrapper.class);
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);

        this.partitionUpdateProcessMergeCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_MERGE_COUNT);
        this.partitionUpdateProcessAddStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_ADD_STATE_COUNT);
        this.partitionUpdateProcessRemoveStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_REMOVE_STATE_COUNT);
        this.partitionUpdateProcessNotMatchStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_PARTITION_UPDATE_PROCESS_NOT_MATCH_STATE_COUNT);
        canalJsonWrapperValidateFailCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<CanalJsonWrapper> out) throws Exception {
        String key = context.getCurrentKey();
        CanalJsonWrapper storeState = mapState.get(key);
        if (storeState != null) {
            out.collect(storeState);
            mapState.remove(key);
        }
    }

    @Override
    public void processElement(CanalJsonWrapper in, Context context, Collector<CanalJsonWrapper> out) throws Exception {
        String stateKey = generateStateKey(in);
        if (mapState.contains(stateKey)) {
            CanalJsonWrapper stateValue = mapState.get(stateKey);
            if (shouldPerformState(in, stateValue)) {
                CanalJson canalJson = in.getCanalJson();
                CanalJson canalJsonState = stateValue.getCanalJson();

                CanalJson canalJsonMerged = mergeState(canalJson, canalJsonState);
                out.collect(createCanalJsonWrapper(canalJsonMerged));

                canalJsonState.setType(canalJsonState.getType() + "Null");
                out.collect(createCanalJsonWrapper(canalJsonState));

                canalJson.setType(canalJson.getType() + "Null");
                out.collect(createCanalJsonWrapper(canalJson));

                partitionUpdateProcessMergeCount.inc();

                mapState.remove(stateKey);
                partitionUpdateProcessRemoveStateCount.inc();
            } else {
                out.collect(stateValue);
                partitionUpdateProcessNotMatchStateCount.inc();
            }
        } else {
            mapState.put(stateKey, in);
            partitionUpdateProcessAddStateCount.inc();
            try {
                context.timerService().registerEventTimeTimer(context.timestamp() + TIME_INTERVAL_MS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean shouldAdditionalProcess(CanalJsonWrapper canalJsonWrapper) {
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
        return false;

    }

    public static boolean validateCanalJsonWrapper(CanalJsonWrapper canalJsonWrapper) {
        if (StringUtils.isBlank(canalJsonWrapper.getPk()) ||
                StringUtils.isBlank(canalJsonWrapper.getOperation())
                || StringUtils.isBlank(canalJsonWrapper.getDatabase())
                || StringUtils.isBlank(canalJsonWrapper.getTable())
                || canalJsonWrapper.getCanalJson() == null) {
            return false;
        }
        if (StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_ID)) ||
                StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_INDEX))) {
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
        if (!pkNames.isEmpty() && !canalJson.getData().isEmpty()) {
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
                String traceRegex = "DRDS /.*/(\\w*){1}/(\\d){1}//.?";
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


    public static String generateStateKey(CanalJsonWrapper canalJsonWrapper) {
        StringBuilder sb = new StringBuilder();
        sb.append(canalJsonWrapper.getDatabase());
        sb.append(canalJsonWrapper.getTable());
        sb.append(canalJsonWrapper.getPk());
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
        } else if ("delete".equalsIgnoreCase(store.getType()) && "insert".equalsIgnoreCase(store.getType())) {
            BeanUtils.copyProperties(canalJsonMerged, store);
            canalJsonMerged.setOld(current.getData());
        }
        canalJsonMerged.setType("UPDATE");
        return canalJsonMerged;
    }

}
