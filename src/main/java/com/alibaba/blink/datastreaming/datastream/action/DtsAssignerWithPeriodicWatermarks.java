package com.alibaba.blink.datastreaming.datastream.action;

import com.alibaba.blink.datastreaming.datastream.deserialize.ByteRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.time.Duration;

public class DtsAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ByteRecord> {
    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public DtsAssignerWithPeriodicWatermarks(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
    }

    @Override
    public long extractTimestamp(ByteRecord element, long recordTimestamp) {
        this.maxTimestamp = Math.max(maxTimestamp, recordTimestamp);
        return this.maxTimestamp;
    }
}
