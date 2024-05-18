package com.alibaba.blink.datastreaming.datastream.action;

public abstract class AbstractDtsToKafkaFlinkAction extends AbstractFlinkAction {
    private static final String DTS = "dts-config";
    private static final String KAFKA = "kafka-config";

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

    @Override
    void setExtraConfig(String[] args) {
    }
}
