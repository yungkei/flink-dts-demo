package com.alibaba.blink.datastreaming.action;

import com.alibaba.blink.datastreaming.datastream.Dts2CanalJson;
import com.alibaba.blink.datastreaming.datastream.action.AbstractDtsToKafkaFlinkAction;
import com.alibaba.blink.datastreaming.datastream.action.FlatRouteDef;
import com.alibaba.blink.datastreaming.datastream.action.RouteDef;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonUtils;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.vividsolutions.jts.util.Assert;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.*;

public class DtsToKafkaFlinkActionTest {

    private static JSONObject dtsRecord = JSON.parseObject("{\"version\": 0, \"id\": 1858644, \"sourceTimestamp\": 1712905965, \"sourcePosition\": \"2924526@143\", \"safeSourcePosition\": \"2924372@143\", \"sourceTxid\": \"0\", \"source\": {\"sourceType\": \"MySQL\", \"version\": \"8.0.34\"}, \"operation\": \"INSERT\", \"objectName\": \"test_drds_hzy_pyii_0005.sample_order_real_aeje_11\", \"processTimestamps\": null, \"tags\": {\"thread_id\": \"2533976\", \"readerThroughoutTime\": \"1712905965384\", \"l_tb_name\": \"sample_order_real\", \"pk_uk_info\": \"{\\\"PRIMARY\\\":[\\\"id\\\"]}\", \"metaVersion\": \"827452312\", \"server_id\": \"2814094928\", \"l_db_name\": \"test_drds_hzy\"}, \"fields\": [{\"name\": \"id\", \"dataTypeNumber\": 3}, {\"name\": \"seller_id\", \"dataTypeNumber\": 3}, {\"name\": \"trade_id\", \"dataTypeNumber\": 3}, {\"name\": \"buyer_id\", \"dataTypeNumber\": 3}, {\"name\": \"buyer_nick\", \"dataTypeNumber\": 253}], \"beforeImages\": null, \"afterImages\": [{\"precision\": 4, \"value\": \"10000\"}, {\"precision\": 4, \"value\": \"11\"}, {\"precision\": 4, \"value\": \"122\"}, {\"precision\": 4, \"value\": \"33\"}, {\"charset\": \"utf8mb3\", \"value\": \"aaas\"}]}");

    public static void main(String[] args) throws Exception {
        AbstractDtsToKafkaFlinkAction flinkAction = new AbstractDtsToKafkaFlinkAction() {
            @Override
            public void run() throws Exception {
                System.out.println(this.sourceConfig);
                System.out.println(this.sinkConfig);
                System.out.println(this.routeConfig);
                String dtsRecordTable = dtsRecord.getString("objectName");

                List<RouteDef> routeDefs = this.routeConfig;
                List<FlatRouteDef> flatRouteDefs = AbstractDtsToKafkaFlinkAction.flatRouteDefs(routeDefs);
                String sinkTableName = convertTableNameIfMatched(flatRouteDefs, dtsRecordTable);
                System.out.println(sinkTableName);
                Assert.equals("test_drds_hzy_pyii.sample_order_real_aeje", sinkTableName);

                HashMap<String, String> extraColumns = this.extraColumnConfig;
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(10000);

                // flink main program
//                DataStream<String> input = env.addSource(
//                                new FlinkDtsRawConsumer(this.sourceConfig.get("broker-url"),
//                                        this.sourceConfig.get("topic"),
//                                        this.sourceConfig.get("sid"),
//                                        this.sourceConfig.get("group"),
//                                        this.sourceConfig.get("user"),
//                                        this.sourceConfig.get("password"),
//                                        Integer.valueOf("1712905892"),
//                                        new DtsByteDeserializationSchema(), null))
//                        .map((MapFunction<ByteRecord, String>) byteRecord -> {
//                            if (byteRecord != null) {
//                                JsonDtsRecord record = new JsonDtsRecord(byteRecord.getBytes(), new AvroDeserializer());
//                                if (OperationType.INSERT == record.getOperationType() || OperationType.UPDATE == record.getOperationType() || OperationType.DELETE == record.getOperationType() || OperationType.DDL == record.getOperationType()) {
//                                    try {
//                                        return JSON.toJSONString(CanalJsonUtils.convert(record.getJson(), routeDefs));
//                                    } catch (Exception ex) {
//
//                                    }
//
//                                } else {
//                                    return null;
//                                }
//                            }
//                            return null;
//                        }).filter((FilterFunction<String>) record -> {
//                            if (record == null || record.isEmpty()) {
//                                return false;
//                            } else {
//                                return true;
//                            }
//                        });

                DataStream<String> input = env.fromCollection(Collections.singleton(dtsRecord.toString()))
                        .map(dtsRecord -> {
                            if (dtsRecord != null) {
                                return JSON.toJSONString(CanalJsonUtils.convert(JSONObject.parseObject(dtsRecord), routeDefs, extraColumns, null));
                            }
                            return null;
                        }).filter((FilterFunction<String>) record -> {
                            if (record == null || record.isEmpty()) {
                                return false;
                            } else {
                                return true;
                            }
                        });

                Properties sinkProperties = new Properties();
                String kafkaBootstrapServers = this.sinkConfig.get("url");
                if (StringUtils.isBlank(kafkaBootstrapServers)) {
                    kafkaBootstrapServers = "127.0.0.1:9092";
                }
                sinkProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);

                FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("dts_sink", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), sinkProperties, Optional.of(new FlinkKafkaPartitioner<String>() {
                    @Override
                    public int partition(String value, byte[] key, byte[] valueSerialized, String targetTopic, int[] partitions) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String operationType = jsonObject.getString("type");
                        if (operationType == null) {
                            return 0;
                        }
                        if (operationType.equals("INSERT") || operationType.equals("UPDATE") || operationType.equals("DELETE")) {
                            return Dts2CanalJson.calculatePartition(jsonObject, partitions.length);
                        } else {
                            return 0;
                        }
                    }
                }));

                input.addSink(kafkaProducer).setParallelism(1);

                env.execute("Dts to Kafka Canal");

            }
        };
        flinkAction.create(args);

        flinkAction.run();
    }
}
