package com.alibaba.blink.datastreaming.datastream;

import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonUtils;
import com.alibaba.blink.datastreaming.datastream.deserialize.ByteRecord;
import com.alibaba.blink.datastreaming.datastream.deserialize.DtsByteDeserializationSchema;
import com.alibaba.blink.datastreaming.datastream.deserialize.JsonDtsRecord;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSON;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordgenerator.AvroDeserializer;
import com.google.common.hash.Hashing;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/** DtsExample. */
public class DtsJsonExampleTest {
    private static final Logger LOG = LoggerFactory.getLogger(DtsJsonExampleTest.class);
    public static void main(String[] args) throws Exception {
        // parse input arguments
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

//        String configFilePath = parameterTool.get("configFile");

        Properties sinkProperties = new Properties();

        /*将平台页面中设置的参数值加载到Properties对象中。*/
//        properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));
        Configuration conf = new Configuration();

        //specified a checkpoint to restore the program
//        conf.set(
//                SavepointConfigOptions.SAVEPOINT_PATH,
//                "file:///tmp/checkpoints/dts-checkpoint/b3572cdb686e6c7e2855400a3361850f/chk-225");

        conf.setLong("akka.ask.timeout", 3000000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        AvroDeserializer avroDeserializer = new AvroDeserializer();

        DataStream<String> input = env.addSource(
                new FlinkDtsRawConsumer(
                        "dts-cn-shanghai.aliyuncs.com:18001",
                        "rm_uf66393mka3m7dhoc",
                        "dtsi9nd3682277z2bn#dtstmkq370g273r334",
                        "dtsi9nd3682277z2bn",
                        "testdts",
                        "Dataphin123",
                        Integer.valueOf("1712905892"),
                        new DtsByteDeserializationSchema(),
                        null))

                .map(new MapFunction<ByteRecord,String>() {
                         @Override
                         public String map(ByteRecord byteRecord) throws Exception {
                             if (byteRecord != null) {
                                 JsonDtsRecord record = new JsonDtsRecord(byteRecord.getBytes(),new AvroDeserializer());
                                 if (OperationType.INSERT == record.getOperationType()
                                         || OperationType.UPDATE == record.getOperationType()
                                         || OperationType.DELETE == record.getOperationType()
                                         || OperationType.DDL == record.getOperationType()
                                 ) {
                                     try {
                                         return JSON.toJSONString(CanalJsonUtils.convert(record.getJson()));
                                     }catch (Exception ex) {
                                         LOG.warn("parse dts {} to canal failed :",record,ex);
                                     }

                                 } else {
                                     return null;
                                 }
                             }
                             return null;
                         }
                     }
                )
                .filter(
                        new FilterFunction<String>() {
                            @Override
                            public boolean filter(String record) throws Exception {
                                if(record == null || record.isEmpty()) {
                                    return false;
                                }else {
                                    return true;
                                }
                            }
                        })
                ;
        sinkProperties.setProperty("bootstrap.servers", "alikafka-post-cn-bl03oub1s002-1-vpc.alikafka.aliyuncs.com:9092,alikafka-post-cn-bl03oub1s002-2-vpc.alikafka.aliyuncs.com:9092,alikafka-post-cn-bl03oub1s002-3-vpc.alikafka.aliyuncs.com:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "dts_sink",
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                sinkProperties,
                Optional.of(new FlinkKafkaPartitioner<String>() {

                    @Override
                    public int partition(String value, byte[] key, byte[] valueSerialized, String targetTopic, int[] partitions) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String operationType = jsonObject.getString("type");
                        if(operationType.equals("INSERT") || operationType.equals("UPDATE") || operationType.equals("DELETE") ) {
                            return calculatePartition(jsonObject, partitions.length);
                        } else {
                            return 0;
                        }


                    }
                })
        );

        input.addSink(kafkaProducer).setParallelism(2);

        env.execute("Dts to Kafka Canal");
    }
    public static int calculatePartition(JSONObject canalJson, int totalPartitions) {

        String database = canalJson.getString("database");
        String table = canalJson.getString("table");
        List<String> pkNames = canalJson.getJSONArray("pkNames").toJavaList(String.class);

        StringBuilder sb = new StringBuilder();
        sb.append(database).append(table);
        if (!pkNames.isEmpty() && canalJson.getJSONArray("data").size() > 0) {
            JSONObject dataObject = canalJson.getJSONArray("data").getJSONObject(0);
            for (String pkName : pkNames) {
                sb.append(dataObject.getString(pkName));
            }
        }

        // 使用 Murmur3 哈希函数计算哈希值。
        int partitionHash = Hashing.murmur3_32().hashString(sb.toString(), StandardCharsets.UTF_8).asInt();
        int partition = Math.abs(partitionHash % totalPartitions);
        return partition;
    }

}
