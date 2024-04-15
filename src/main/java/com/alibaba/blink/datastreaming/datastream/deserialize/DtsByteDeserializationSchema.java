package com.alibaba.blink.datastreaming.datastream.deserialize;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class DtsByteDeserializationSchema extends AbstractDeserializationSchema<ByteRecord> {
    @Override
    public ByteRecord deserialize(byte[] message) {
        return new ByteRecord(message);
    }
}
