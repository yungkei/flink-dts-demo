package com.alibaba.blink.datastreaming.datastream.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractFlinkAction {
    protected HashMap<String, String> sourceConfig;

    protected HashMap<String, String> sinkConfig;

    protected abstract String getSourceIdentifier();

    protected abstract String getSinkIdentifier();

    public void create(String[] args) {
        setSourceConfig(args, getSourceIdentifier());
        setSinkConfig(args, getSinkIdentifier());
        setExtraConfig(args);
    }

    abstract void setExtraConfig(String[] args);

    protected void setSourceConfig(String[] args, String key) {
        this.sourceConfig = optionalConfigMap(args, key);
    }

    protected void setSinkConfig(String[] args, String key) {
        this.sinkConfig = optionalConfigMap(args, key);
    }

    private HashMap<String, String> optionalConfigMap(String[] args, String key) {
        MultipleParameterTool mpTool = MultipleParameterTool.fromArgs(args);
        Collection<String> kvCollection = mpTool.getMultiParameter(key);
        HashMap<String, String> configMap = new HashMap<>();
        parseKeyValueString(configMap, kvCollection);
        return configMap;
    }

    private static void parseKeyValueString(Map<String, String> map, Collection<String> kvCollection) {
        kvCollection.stream().forEach(kvString -> {
            String[] kv = kvString.split("=", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(String.format("Invalid key-value string '%s'. Please use format 'key=value'", kvString));
            }
            map.put(kv[0].trim(), kv[1].trim());
        });
    }

    protected abstract void run() throws Exception;
}
