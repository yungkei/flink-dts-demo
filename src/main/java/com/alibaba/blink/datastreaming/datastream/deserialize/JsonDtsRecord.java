package com.alibaba.blink.datastreaming.datastream.deserialize;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.flink.connectors.dts.formats.internal.record.impl.UserAvroRecordParser;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import com.aliyun.dts.subscribe.clients.recordgenerator.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Function;

/**
 * @Author: ningqy
 * @Date: 2022/8/13 21:47
 * @Description: 将avro的数据格式解析成json数据
 */
public class JsonDtsRecord implements UserRecord {

    private final Record avroRecord;

    private RecordSchema recordSchema;
    private RowImage beforeImage;
    private RowImage afterImage;

    public JsonDtsRecord(ConsumerRecord<byte[], byte[]> record1, AvroDeserializer deserializer) {
        this.avroRecord = deserializer.deserialize(record1.value());
        this.recordSchema = getSchema();
        this.beforeImage = getBeforeImage();
        this.afterImage = getAfterImage();
    }
    public JsonDtsRecord(byte[] message, AvroDeserializer deserializer) {
        this.avroRecord = deserializer.deserialize(message);
        this.recordSchema = getSchema();
        this.beforeImage = getBeforeImage();
        this.afterImage = getAfterImage();
    }
    public Record getRecord() {
        return avroRecord;
    }

    private <R> R callAvroRecordMethod(Function<? super Record, ? extends R> method) {
        return method.apply(avroRecord);
    }

    @Override
    public long getId() {
        return callAvroRecordMethod(Record::getId);
    }

    @Override
    public long getSourceTimestamp() {
        return callAvroRecordMethod(Record::getSourceTimestamp);
    }

    @Override
    public OperationType getOperationType() {
        return callAvroRecordMethod(UserAvroRecordParser::getOperationType);
    }

    @Override
    public RecordSchema getSchema() {
        return callAvroRecordMethod((avroRecord1) -> {
            if (recordSchema == null) {
                recordSchema = UserAvroRecordParser.getRecordSchema(avroRecord1);
            }
            return recordSchema;
        });
    }

    @Override
    public RowImage getBeforeImage() {
        if (null == beforeImage) {
            beforeImage = callAvroRecordMethod(record1 -> UserAvroRecordParser.getRowImage(recordSchema, record1, true));
        }
        return beforeImage;
    }

    @Override
    public RowImage getAfterImage() {
        if (null == afterImage) {
            afterImage = callAvroRecordMethod(record1 -> UserAvroRecordParser.getRowImage(recordSchema, record1, false));
        }
        return afterImage;
    }

    @Override
    public String toString() {

        JSONObject jsonObject = JSON.parseObject(avroRecord.toString());
        if(null == beforeImage)
            jsonObject.put("beforeImages",null);
        else
            jsonObject.put("beforeImages", JSON.parseObject(beforeImage.toString()));
        if(null == afterImage)
            jsonObject.put("afterImages",null);
        else
            jsonObject.put("afterImages", JSON.parseObject(afterImage.toString()));
        return jsonObject.toString();
    }
     public JSONObject getJson() {
         JSONObject jsonObject = JSON.parseObject(avroRecord.toString());
         if(null == beforeImage)
             jsonObject.put("beforeImages",null);
         else
             jsonObject.put("beforeImages", JSON.parseObject(beforeImage.toString()));
         if(null == afterImage)
             jsonObject.put("afterImages",null);
         else
             jsonObject.put("afterImages", JSON.parseObject(afterImage.toString()));
        return jsonObject;
    }
}
