package com.alibaba.blink.datastreaming.datastream.deserialize;

import java.io.Serializable;

public class ByteRecord  implements Serializable {
    private byte[] bytes;

    public byte[] getBytes() {
        return bytes;
    }

    public ByteRecord(byte[] bytes) {
        this.bytes = bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
}
