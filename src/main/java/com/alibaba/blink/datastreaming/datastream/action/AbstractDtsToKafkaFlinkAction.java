package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public abstract class AbstractDtsToKafkaFlinkAction extends AbstractFlinkAction {
    private static final String DTS = "dts-config";
    private static final String KAFKA = "kafka-config";
    private static final String ROUTE = "route";

    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_DESCRIPTION_KEY = "description";

    protected List<RouteDef> routeConfig;

    @Override
    protected String getSourceIdentifier() {
        return DTS;
    }

    @Override
    protected String getSinkIdentifier() {
        return KAFKA;
    }

    @Override
    public abstract void run() throws Exception;

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

            if (isMatched || !StringUtils.isBlank(routeDef.getSinkTable())) {
                return routeDef.getSinkTable();
            }
        }
        return source;
    }

    @Override
    void setExtraConfig(String[] args) {
        this.routeConfig = optionalConfigMap(args, ROUTE);
    }

    private List<RouteDef> optionalConfigMap(String[] args, String key) {
        MultipleParameterTool mpTool = MultipleParameterTool.fromArgs(args);
        Collection<String> kvCollection = mpTool.getMultiParameter(key);
        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(kvCollection).ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));
        return routeDefs;
    }

    private RouteDef toRouteDef(String jsonString) {
        JSONObject jsonObject;
        try {
            jsonObject = JSONObject.parseObject(jsonString);
        } catch (Exception e) {
            System.out.println(e);
            return new RouteDef("", "", "");
        }
        String sourceTable = jsonObject.getOrDefault(ROUTE_SOURCE_TABLE_KEY, "").toString();
        String sinkTable = jsonObject.getOrDefault(ROUTE_SINK_TABLE_KEY, "").toString();
        String description = jsonObject.getOrDefault(ROUTE_DESCRIPTION_KEY, "").toString();
        return new RouteDef(sourceTable, sinkTable, description);
    }
}
