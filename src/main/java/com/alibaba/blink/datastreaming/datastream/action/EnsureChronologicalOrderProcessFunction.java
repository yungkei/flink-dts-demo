package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.metric.CdcMetricNames;
import com.alibaba.fastjson2.JSON;
import com.google.common.hash.Hashing;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnsureChronologicalOrderProcessFunction extends KeyedProcessFunction<String, CanalJsonWrapper, CanalJsonWrapper> {
    private static final Logger LOG = LoggerFactory.getLogger(EnsureChronologicalOrderProcessFunction.class);
    private static final long TIME_INTERVAL_MS = 10000L;
    public static final String MERGE_SUFFIX = "_MERGEINTO_UPDATE";
    public static final String HAS_EXPIRED_SUFFIX = "_HAS_EXPIRED";
    public static final String TRANSACTION_ID = "transactionId";
    public static final String TRANSACTION_INDEX = "transactionIndex";
    private transient MapState<String, LinkedHashMap<String, CanalJsonWrapper>> mapState;

    private transient Counter addStateCount;
    private transient Counter firstAddStateCount;
    private transient Counter againAddStateCount;
    private transient Counter removeStateCount;
    private transient Counter matchedRemoveStateCount;
    private transient Counter onTimerRemoveStateCount;
    private transient Counter mergeIntoUpdateCount;
    private transient Counter updateExpiredCount;
    private transient Counter onTimerHaveExpiredCount;
    private transient Counter matchedHaveExpiredCount;
    private transient Counter againRegisterTimerCount;
    private transient Counter notMatchStateCount;
    private static Counter canalJsonWrapperValidateFailCount;
    private transient Counter operationHaveExpiredCount;

    private long timerTimeInternalMs = TIME_INTERVAL_MS;

    public void setTimerTimeInternalMs(long timerTimeInternalMs) {
        this.timerTimeInternalMs = timerTimeInternalMs;
    }

    private List<RouteDef> routeDefs;

    public void setRouteDefs(List<RouteDef> routeDefs) {
        this.routeDefs = routeDefs;
    }

    @Override
    public void open(Configuration parameters) {
        registryState();
        registryMetric();
    }

    private void registryState() {
        MapStateDescriptor<String, LinkedHashMap<String, CanalJsonWrapper>> descriptor = new MapStateDescriptor<>("ensureChronologicalOrderState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<LinkedHashMap<String, CanalJsonWrapper>>() {
        }));
        this.mapState = getRuntimeContext().getMapState(descriptor);
    }

    private void registryMetric() {
        this.addStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.ADD_STATE_COUNT);
        this.firstAddStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.FIRST_ADD_STATE_COUNT);
        this.againAddStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.AGAIN_ADD_STATE_COUNT);

        this.removeStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.REMOVE_STATE_COUNT);
        this.matchedRemoveStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_REMOVE_STATE_COUNT);
        this.onTimerRemoveStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.ON_TIMER_REMOVE_STATE_COUNT);

        this.updateExpiredCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.UPDATE_EXPIRED_COUNT);
        this.matchedHaveExpiredCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MATCHED_HAVE_EXPIRED_COUNT);
        this.onTimerHaveExpiredCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.ON_TIMER_HAVE_EXPIRED_COUNT);

        this.mergeIntoUpdateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.MERGE_INTO_UPDATE);
        this.againRegisterTimerCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.AGAIN_REGISTER_TIMER_COUNT);

        this.notMatchStateCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.NOT_MATCH_STATE_COUNT);

        canalJsonWrapperValidateFailCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DRDS_CANAL_JSON_WRAPPER_VALIDATE_FAIL_COUNT);
        this.operationHaveExpiredCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.OPERATION_HAVE_EXPIRED_COUNT);

    }

    @Override
    public void processElement(CanalJsonWrapper in, Context context, Collector<CanalJsonWrapper> out) throws Exception {
        String stateKey = context.getCurrentKey();
        Map<String, String> inTags = in.getTags();
        inTags.put("watermark", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(context.timerService().currentWatermark()));
        in.setTags(inTags);

        if (shouldPartitionUpdateProcess(in)) {
            if (mapState.contains(stateKey)) {
                Map<String, CanalJsonWrapper> store = mapState.get(stateKey);
                if (store.containsKey(in.getTags().get(TRANSACTION_ID))) {
                    CanalJsonWrapper match = store.get(in.getTags().get(TRANSACTION_ID));
                    if (checkStoreMergeMatched(in, match)) {
                        if (checkMatchedHasExpired(match, in)) {
                            handleElementMergeStateItemHasExpired(context, out, in, match);
                        } else {
                            handleElementMergeStateItemHasNoExpired(context, out, in, match);
                        }
                        handleStateRemoveStateItem(context, match);
                    } else {
                        notMatchStateCount.inc();
                        out.collect(in);
                    }
                } else {
                    handleElementStoreStateItem(context, stateKey, in);
                    againAddStateCount.inc();
                }
            } else {
                LinkedHashMap<String, CanalJsonWrapper> store = new LinkedHashMap<>();
                mapState.put(stateKey, store);
                handleElementStoreStateItem(context, stateKey, in);
                context.timerService().registerEventTimeTimer(generateRegisterTimer(context));
                firstAddStateCount.inc();
            }
        } else {
            if (mapState.contains(stateKey)) {
                handleElementUpdateStateItemExpired(stateKey, in);
            }
            out.collect(in);
        }
    }

    private long generateRegisterTimer(Context context) {
        return context.timestamp() + timerTimeInternalMs;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<CanalJsonWrapper> out) throws Exception {
        String stateKey = context.getCurrentKey();
        HashMap<String, CanalJsonWrapper> store = mapState.get(stateKey);
        if (store == null) {
            LOG.warn("stateKey:{},stateValue:{}", stateKey, store);
        }
        LinkedHashMap<String, CanalJsonWrapper> storeAfter = new LinkedHashMap<>();
        long minEventTimer = Long.MAX_VALUE;
        for (Map.Entry<String, CanalJsonWrapper> item : store.entrySet()) {
            String itemKey = item.getKey();
            CanalJsonWrapper storeItem = item.getValue();
            long eventTimer = Long.valueOf(storeItem.getTags().get("registerTimer"));
            if (timestamp >= eventTimer) {
                Map<String, String> storeItemTags = storeItem.getTags();
                storeItemTags.put("stateOnTimerProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
                storeItem.setTags(storeItemTags);
                if (checkOnTimerHasExpired(storeItem)) {
                    String operation = storeItem.getOperation();
                    String operationAfter = operation + HAS_EXPIRED_SUFFIX;
                    CanalJsonWrapper storeItemAfter = updateCanalJsonWrapperOperation(storeItem, operationAfter);
                    out.collect(storeItemAfter);
                    onTimerHaveExpiredCount.inc();
                } else {
                    out.collect(storeItem);
                }
                onTimerRemoveStateCount.inc();
                removeStateCount.inc();

            } else {
                minEventTimer = Math.min(minEventTimer, eventTimer);
                storeAfter.put(itemKey, storeItem);
            }
        }
        if (storeAfter.size() == 0) {
            mapState.remove(stateKey);
        } else {
            mapState.put(stateKey, storeAfter);
            context.timerService().registerEventTimeTimer(minEventTimer);
            LOG.info("againRegisterTime, stateKey:{},store:{}, storeAfter:{}", stateKey, JSON.toJSONString(store), JSON.toJSONString(storeAfter));
            againRegisterTimerCount.inc();
        }
    }

    public boolean shouldPartitionUpdateProcess(CanalJsonWrapper canalJsonWrapper) {
        if ("DDL".equalsIgnoreCase(canalJsonWrapper.getOperation())) {
            return false;
        }
        if (!validateCanalJsonWrapper(canalJsonWrapper)) {
            LOG.warn("canalJsonWrapperValidateFail, canalJsonWrapper:{}", canalJsonWrapper);
            canalJsonWrapperValidateFailCount.inc();
            return false;
        }
        if (isBroadcastTable(canalJsonWrapper, this.routeDefs)) {
            return false;
        }
        Map<String, String> tags = canalJsonWrapper.getTags();
        String traceType = tags.get(TRANSACTION_INDEX);
        String operation = canalJsonWrapper.getOperation();
        return ("insert".equalsIgnoreCase(operation) && ("3".equals(traceType) || "2".equalsIgnoreCase(traceType)) || ("delete".equalsIgnoreCase(operation) && ("1".equals(traceType) || "2".equals(traceType))));
    }

    public static boolean validateCanalJsonWrapper(CanalJsonWrapper canalJsonWrapper) {
        if (StringUtils.isBlank(canalJsonWrapper.getPk()) || StringUtils.isBlank(canalJsonWrapper.getOperation()) || StringUtils.isBlank(canalJsonWrapper.getDatabase()) || StringUtils.isBlank(canalJsonWrapper.getTable()) || canalJsonWrapper.getCanalJson() == null) {
            return false;
        }
        if (StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_ID)) || StringUtils.isBlank(canalJsonWrapper.getTags().get(TRANSACTION_INDEX))) {
            return false;
        }
        if (canalJsonWrapper.getEventTime() == null) {
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
        canalJsonWrapper.setEventTime(canalJson.getTs());

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

    public static CanalJsonWrapper updateCanalJsonWrapperOperation(CanalJsonWrapper canalJsonWrapper, String operation) {
        Map<String, String> sourceTags = canalJsonWrapper.getTags();
        CanalJson canalJson = canalJsonWrapper.getCanalJson();
        canalJson.setType(operation);
        CanalJsonWrapper canalJsonWrapperAfter = createCanalJsonWrapper(canalJson);
        canalJsonWrapperAfter.setTags(sourceTags);
        return canalJsonWrapper;
    }

    public static String generateStateKey(CanalJsonWrapper canalJsonWrapper) {
        StringBuilder sb = new StringBuilder();
        sb.append(canalJsonWrapper.getDatabase());
        sb.append(canalJsonWrapper.getTable());
        sb.append(canalJsonWrapper.getPk());
        return String.valueOf(Hashing.murmur3_128().hashString(sb.toString(), StandardCharsets.UTF_8).asLong());
    }

    public boolean checkStoreMergeMatched(CanalJsonWrapper current, CanalJsonWrapper store) {
        String operation = current.getOperation();
        String operationStored = store.getOperation();
        if (("insert".equalsIgnoreCase(operation) && "delete".equalsIgnoreCase(operationStored)) || ("delete".equalsIgnoreCase(operation) && "insert".equalsIgnoreCase(operationStored))) {
            return true;
        }
        return false;
    }

    private boolean checkOnTimerHasExpired(CanalJsonWrapper canalJsonWrapper) {
        String idExpired = canalJsonWrapper.getTags().get("idExpired");
        if (StringUtils.isBlank(idExpired)) {
            return false;
        }
        return true;
    }

    private boolean checkMatchedHasExpired(CanalJsonWrapper store, CanalJsonWrapper in) {
        String idExpired = store.getTags().get("idExpired");
        if (StringUtils.isBlank(idExpired)) {
            return false;
        }
        if (store.getTags().get("dtsTopic").equalsIgnoreCase(in.getTags().get("dtsTopic"))) {
            return false;
        } else {
            if ("DELETE".equalsIgnoreCase(store.getOperation())) {
                return false;
            } else {
                String esExpired = store.getTags().get("esExpired");
                if (store.getCanalJson().getEs().longValue() > Long.valueOf(esExpired).longValue()) {
                    return false;
                }
                if (store.getCanalJson().getEs().longValue() < Long.valueOf(esExpired).longValue()) {
                    return true;
                }
                Map<String, Long> topicMap = JSON.parseObject(idExpired, Map.class);
                String topic = store.getTags().get("dtsTopic");
                if (topicMap.containsKey(topic)) {
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    private String generateTagsIdExpired(CanalJsonWrapper canalJsonWrapper) {
        String idExpired = canalJsonWrapper.getTags().get("idExpired");
        String topic = canalJsonWrapper.getTags().get("dtsTopic");
        Long opId = canalJsonWrapper.getCanalJson().getId();
        if (StringUtils.isBlank(idExpired)) {
            Map<String, Long> topicMap = new HashMap<>();
            topicMap.put(topic, opId);
            return JSON.toJSONString(topicMap);
        } else {
            Map<String, Long> topicMap = JSON.parseObject(idExpired, Map.class);
            if (topicMap.containsKey(topic)) {
                long maxOpId = Math.max(opId, topicMap.get(topic).longValue());
                topicMap.put(topic, maxOpId);
            } else {
                topicMap.put(topic, opId);
            }
            return JSON.toJSONString(topicMap);
        }
    }

    private void handleElementUpdateStateItemExpired(String stateKey, CanalJsonWrapper in) throws Exception {
        LinkedHashMap<String, CanalJsonWrapper> store = mapState.get(stateKey);
        for (Map.Entry<String, CanalJsonWrapper> item : store.entrySet()) {
            String key = item.getKey();
            CanalJsonWrapper storeValue = item.getValue();

            if (in.getCanalJson().getEs().longValue() >= Long.valueOf(storeValue.getTags().get("esExpired")).longValue()) {
                String esExpired = String.valueOf(Math.max(in.getCanalJson().getEs(), Long.valueOf(storeValue.getTags().get("esExpired"))));
                Map<String, String> storeTags = storeValue.getTags();
                String idExpiredValue = generateTagsIdExpired(in);
                storeTags.put("esExpired", esExpired);
                storeTags.put("idExpired", idExpiredValue);
                storeTags.put("stateExpiredProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
                storeValue.setTags(storeTags);
                store.put(key, storeValue);
            } else {
                String operationAfter = in.getOperation() + HAS_EXPIRED_SUFFIX;
                updateCanalJsonWrapperOperation(in, operationAfter);
                operationHaveExpiredCount.inc();
            }
        }
        updateExpiredCount.inc();
        mapState.put(stateKey, store);
    }

    private void handleElementStoreStateItem(Context context, String stateKey, CanalJsonWrapper in) throws Exception {
        Map<String, String> inTags = in.getTags();
        LinkedHashMap<String, CanalJsonWrapper> store = mapState.get(stateKey);
        String transactionId = inTags.get(TRANSACTION_ID);
        inTags.put("esExpired", "0");
        inTags.put("idExpired", "");
        inTags.put("registerTimer", String.valueOf(generateRegisterTimer(context)));
        inTags.put("stateRegisterProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        in.setTags(inTags);
        store.put(transactionId, in);
        mapState.put(stateKey, store);
        addStateCount.inc();
    }

    private void handleStateRemoveStateItem(Context context, CanalJsonWrapper store) throws Exception {
        String stateKey = context.getCurrentKey();
        LinkedHashMap<String, CanalJsonWrapper> stateValue = mapState.get(stateKey);
        stateValue.remove(store.getTags().get(TRANSACTION_ID));
        long storeRegisterTimer = Long.valueOf(store.getTags().get("registerTimer"));
        context.timerService().deleteEventTimeTimer(storeRegisterTimer);
        if (stateValue.size() == 0) {
            mapState.remove(stateKey);
        } else {
            mapState.put(stateKey, stateValue);
            long minEventTimer = Long.MAX_VALUE;
            for (Map.Entry<String, CanalJsonWrapper> item : stateValue.entrySet()) {
                CanalJsonWrapper storeItem = item.getValue();
                long eventTimer = Long.valueOf(storeItem.getTags().get("registerTimer"));
                minEventTimer = Math.min(eventTimer, minEventTimer);
            }
            context.timerService().registerEventTimeTimer(minEventTimer);
        }
        removeStateCount.inc();
        matchedRemoveStateCount.inc();
    }

    private void handleElementMergeStateItemHasNoExpired(Context context, Collector<CanalJsonWrapper> out, CanalJsonWrapper in, CanalJsonWrapper store) throws Exception {
        CanalJsonWrapper canalJsonWrapperMerged = mergeState(in, store);
        Map<String, String> mergedTags = canalJsonWrapperMerged.getTags();
        mergedTags.put("stateMatchedProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        canalJsonWrapperMerged.setTags(mergedTags);
        out.collect(canalJsonWrapperMerged);

        String storeOperation = store.getOperation();
        String storeOperationAfter = storeOperation + MERGE_SUFFIX;
        CanalJsonWrapper canalJsonWrapperState = updateCanalJsonWrapperOperation(store, storeOperationAfter);
        Map<String, String> stateTags = canalJsonWrapperState.getTags();
        stateTags.put("stateMatchedProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        canalJsonWrapperState.setTags(stateTags);
        out.collect(canalJsonWrapperState);

        String inOperation = in.getOperation();
        String inOperationAfter = inOperation + MERGE_SUFFIX;
        CanalJsonWrapper canalJsonWrapper = updateCanalJsonWrapperOperation(in, inOperationAfter);
        Map<String, String> inTags = canalJsonWrapper.getTags();
        inTags.put("stateMatchedProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        canalJsonWrapper.setTags(inTags);
        out.collect(canalJsonWrapper);

        mergeIntoUpdateCount.inc();
    }

    private void handleElementMergeStateItemHasExpired(Context context, Collector<CanalJsonWrapper> out, CanalJsonWrapper in, CanalJsonWrapper store) throws Exception {
        String storeOperation = store.getOperation();
        String storeOperationAfter = storeOperation + HAS_EXPIRED_SUFFIX;
        CanalJsonWrapper canalJsonWrapperState = updateCanalJsonWrapperOperation(store, storeOperationAfter);
        Map<String, String> stateTags = canalJsonWrapperState.getTags();
        stateTags.put("stateMatchedProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        canalJsonWrapperState.setTags(stateTags);
        out.collect(canalJsonWrapperState);

        String inOperation = in.getOperation();
        String inOperationAfter = inOperation + HAS_EXPIRED_SUFFIX;
        CanalJsonWrapper canalJsonWrapper = updateCanalJsonWrapperOperation(in, inOperationAfter);
        Map<String, String> inTags = canalJsonWrapper.getTags();
        stateTags.put("stateMatchedProcessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()));
        canalJsonWrapper.setTags(inTags);
        out.collect(canalJsonWrapper);

        matchedHaveExpiredCount.inc();
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

    public CanalJsonWrapper mergeState(CanalJsonWrapper current, CanalJsonWrapper store) throws Exception {
        CanalJson canalJsonCurrent = current.getCanalJson();
        CanalJson canalJsonStore = store.getCanalJson();
        CanalJson canalJsonMerged = mergeState(canalJsonCurrent, canalJsonStore);
        CanalJsonWrapper canalJsonMergedWrapper = createCanalJsonWrapper(canalJsonMerged);
        if ("insert".equalsIgnoreCase(current.getOperation()) && "delete".equalsIgnoreCase(store.getOperation())) {
            canalJsonMergedWrapper.setTags(current.getTags());
        } else if ("delete".equalsIgnoreCase(current.getOperation()) && "insert".equalsIgnoreCase(store.getOperation())) {
            canalJsonMergedWrapper.setTags(store.getTags());
        }
        return canalJsonMergedWrapper;
    }

    private boolean isBroadcastTable(CanalJsonWrapper canalJsonWrapper, List<RouteDef> routeDefs) {
        if (routeDefs == null || routeDefs.isEmpty()) {
            return false;
        }
        String database = canalJsonWrapper.getDatabase();
        String table = canalJsonWrapper.getTable();
        for (int i = 0; i < routeDefs.size(); i++) {
            RouteDef routeDef = routeDefs.get(i);
            String checkDatabase = routeDef.getTargetDatabase();
            if (database.equalsIgnoreCase(checkDatabase)) {
                String checkBroadcastTables = routeDef.getBroadcastTables();
                Pattern checkBroadcastTablesPattern = Pattern.compile(checkBroadcastTables);
                boolean isBroadcastTable = checkBroadcastTablesPattern.matcher(table).matches();
                if (isBroadcastTable) {
                    return true;
                }
            }
        }
        return false;
    }

    private void printLog(String msg, String stateKey, CanalJsonWrapper wrapper) throws Exception {
        LOG.info("{}-stateKey:{},wrapper:{},state:{}", msg, stateKey, JSON.toJSONString(wrapper), JSON.toJSONString(this.mapState.get(stateKey)));
    }

}
