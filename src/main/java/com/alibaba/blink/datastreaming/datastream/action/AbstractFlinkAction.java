package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

public abstract class AbstractFlinkAction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkAction.class);

    protected HashMap<String, String> sourceConfig;
    protected HashMap<String, String> sinkConfig;
    protected List<RouteDef> routeConfig;
    protected String jobName;
    private static final String ROUTE = "route";
    private static final String ROUTE_SOURCE_DATABASE_KEY = "source-database";
    private static final String ROUTE_TARGET_DATABASE_KEY = "target-database";
    private static final String INCLUDING_TARGET_TABLES_KEY = "including-target-tables";
    private static final String EXCLUDING_TARGET_TABLES_KEY = "excluding-target-tables";
    private static final String TABLE_ROUTE_KEY = "table-route";
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_TARGET_TABLE_KEY = "target-table";
    private static final String ROUTE_DESCRIPTION_KEY = "description";
    private static final String JOB_NAME = "job-name";

    protected abstract String getSourceIdentifier();

    protected abstract String getSinkIdentifier();

    public void create(String[] args) {
        setSourceConfig(args, getSourceIdentifier());
        setSinkConfig(args, getSinkIdentifier());
        setRouteConfig(args);
        setExtraConfig(args);
        setJobName(args);
    }

    abstract void setExtraConfig(String[] args);

    protected void setSourceConfig(String[] args, String key) {
        this.sourceConfig = optionalConfigMap(args, key);
    }

    protected void setSinkConfig(String[] args, String key) {
        this.sinkConfig = optionalConfigMap(args, key);
    }

    protected void setRouteConfig(String[] args) {
        this.routeConfig = optionalConfigList(args, ROUTE, item -> toRouteDef(item));
    }

    protected void setJobName(String[] args) {
        List<String> mapParallelisms = optionalConfigList(args, JOB_NAME, item -> item);
        if (mapParallelisms == null || mapParallelisms.isEmpty()) {
            this.jobName = "default";
        } else {
            this.jobName = mapParallelisms.get(0);
        }
    }

    protected HashMap<String, String> optionalConfigMap(String[] args, String key) {
        MultipleParameterTool mpTool = MultipleParameterTool.fromArgs(args);
        Collection<String> kvCollection = mpTool.getMultiParameter(key);
        HashMap<String, String> configMap = new HashMap<>();
        parseKeyValueString(configMap, kvCollection);
        return configMap;
    }

    private static void parseKeyValueString(Map<String, String> map, Collection<String> kvCollection) {
        if (kvCollection == null || kvCollection.isEmpty()) {
            return;
        }
        kvCollection.stream().forEach(kvString -> {
            String[] kv = kvString.split("=", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(String.format("Invalid key-value string '%s'. Please use format 'key=value'", kvString));
            }
            map.put(kv[0].trim(), kv[1].trim());
        });
    }

    protected <T> List<T> optionalConfigList(String[] args, String key, Function<String, T> action) {
        MultipleParameterTool mpTool = MultipleParameterTool.fromArgs(args);
        Collection<String> kvCollection = mpTool.getMultiParameter(key);
        List<T> result = new ArrayList<>();
        Optional.ofNullable(kvCollection).ifPresent(node -> node.forEach(route -> result.add(action.apply(route))));
        return result;
    }

    private RouteDef toRouteDef(String jsonString) {
        JSONObject jsonObject;
        try {
            jsonObject = JSONObject.parseObject(jsonString);
        } catch (Exception e) {
            LOG.warn("toRouteDef parseObject error:", e);
            return new RouteDef("", "", "", "", Collections.emptyList());
        }
        String sourceDatabase = jsonObject.getOrDefault(ROUTE_SOURCE_DATABASE_KEY, "").toString();
        String targetDatabase = jsonObject.getOrDefault(ROUTE_TARGET_DATABASE_KEY, "").toString();
        String includingTargetTables = jsonObject.getOrDefault(INCLUDING_TARGET_TABLES_KEY, "").toString();
        String excludingTargetTables = jsonObject.getOrDefault(EXCLUDING_TARGET_TABLES_KEY, "").toString();
        JSONArray tableRouteJSONArray = jsonObject.getJSONArray(TABLE_ROUTE_KEY);
        List<FlatRouteDef> flatRouteDefs = new ArrayList<>();
        if (tableRouteJSONArray != null && !tableRouteJSONArray.isEmpty()) {
            for (int i = 0; i < tableRouteJSONArray.size(); i++) {
                JSONObject tableRouteJSONObject = tableRouteJSONArray.getJSONObject(i);
                String sourceTable = tableRouteJSONObject.getOrDefault(ROUTE_SOURCE_TABLE_KEY, "").toString();
                String targetTable = tableRouteJSONObject.getOrDefault(ROUTE_TARGET_TABLE_KEY, "").toString();
                String description = tableRouteJSONObject.getOrDefault(ROUTE_DESCRIPTION_KEY, "").toString();
                flatRouteDefs.add(new FlatRouteDef(sourceTable, targetTable, description));
            }
        }
        return new RouteDef(sourceDatabase, targetDatabase, includingTargetTables, excludingTargetTables, flatRouteDefs);
    }

    public static boolean matchWithTablePattern(Pattern sourcePattern, String source) {
        return sourcePattern.matcher(source).matches();
    }

    public static String convertTableNameIfMatched(String sourceMathRegex, String source, String sink) {
        String[] regexArray = sourceMathRegex.split(",");
        for (int i = 0; i < regexArray.length; i++) {
            Pattern pattern = Pattern.compile(regexArray[0]);
            boolean isMatched = matchWithTablePattern(pattern, source);

            if (isMatched) {
                return sink;
            }
        }
        return source;
    }

    public static String convertTableNameIfMatched(List<FlatRouteDef> flatRouteDefs, String source) {
        if (flatRouteDefs == null || flatRouteDefs.isEmpty()) {
            return source;
        }

        for (int i = 0; i < flatRouteDefs.size(); i++) {
            FlatRouteDef flatRouteDef = flatRouteDefs.get(i);

            Pattern pattern = Pattern.compile(flatRouteDef.getSourceTable());
            boolean isMatched = matchWithTablePattern(pattern, source);

            if (isMatched && !StringUtils.isBlank(flatRouteDef.getTargetTable())) {
                return flatRouteDef.getTargetTable();
            }
        }
        return source;
    }

    public static List<FlatRouteDef> flatRouteDefs(List<RouteDef> routeDefs) {
        List<FlatRouteDef> flatRouteDefs = new ArrayList<>(300);
        if (routeDefs == null || routeDefs.isEmpty()) {
            return flatRouteDefs;
        }
        for (int i = 0; i < routeDefs.size(); i++) {
            RouteDef routeDef = routeDefs.get(i);
            String sourceDatabase = routeDef.getSourceDatabase();
            String targetDatabase = routeDef.getTargetDatabase();
            List<FlatRouteDef> tableRouteDefs = routeDef.getTableRouteDef();
            if (tableRouteDefs == null || tableRouteDefs.isEmpty()) {
                continue;
            }
            for (int j = 0; j < tableRouteDefs.size(); j++) {
                FlatRouteDef tableRouteDef = tableRouteDefs.get(j);
                String sourceTable = tableRouteDef.getSourceTable();
                String targetTable = tableRouteDef.getTargetTable();
                Optional<String> description = tableRouteDef.getDescription();
                String sourceFlatTable = sourceDatabase + "." + sourceTable;
                String targetFlatTable = targetDatabase + "." + targetTable;
                flatRouteDefs.add(new FlatRouteDef(sourceFlatTable, targetFlatTable, description.isPresent() ? description.get() : null));
            }
        }
        return flatRouteDefs;
    }

    protected abstract void run() throws Exception;
}
