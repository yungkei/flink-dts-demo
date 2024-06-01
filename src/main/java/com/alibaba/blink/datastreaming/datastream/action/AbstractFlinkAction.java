package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.fastjson.JSONObject;
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
    private static final String ROUTE = "route";
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_DESCRIPTION_KEY = "description";

    protected abstract String getSourceIdentifier();

    protected abstract String getSinkIdentifier();

    public void create(String[] args) {
        setSourceConfig(args, getSourceIdentifier());
        setSinkConfig(args, getSinkIdentifier());
        setRouteConfig(args);
        setExtraConfig(args);
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
            return new RouteDef("", "", "");
        }
        String sourceTable = jsonObject.getOrDefault(ROUTE_SOURCE_TABLE_KEY, "").toString();
        String sinkTable = jsonObject.getOrDefault(ROUTE_SINK_TABLE_KEY, "").toString();
        String description = jsonObject.getOrDefault(ROUTE_DESCRIPTION_KEY, "").toString();
        return new RouteDef(sourceTable, sinkTable, description);
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

    public static String convertTableNameIfMatched(List<RouteDef> routeDefs, String source) {
        if (routeDefs == null || routeDefs.isEmpty()) {
            return source;
        }

        for (int i = 0; i < routeDefs.size(); i++) {
            RouteDef routeDef = routeDefs.get(i);

            Pattern pattern = Pattern.compile(routeDef.getSourceTable());
            boolean isMatched = matchWithTablePattern(pattern, source);

            if (isMatched && !StringUtils.isBlank(routeDef.getSinkTable())) {
                return routeDef.getSinkTable();
            }
        }
        return source;
    }

    protected abstract void run() throws Exception;
}
