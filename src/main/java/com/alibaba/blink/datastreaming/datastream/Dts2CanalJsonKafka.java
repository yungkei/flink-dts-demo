package com.alibaba.blink.datastreaming.datastream;

import com.alibaba.blink.datastreaming.datastream.action.AbstractDtsToKafkaFlinkAction;
import com.alibaba.blink.datastreaming.datastream.action.RouteDef;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonUtils;
import com.alibaba.blink.datastreaming.datastream.deserialize.ByteRecord;
import com.alibaba.blink.datastreaming.datastream.deserialize.DtsByteDeserializationSchema;
import com.alibaba.blink.datastreaming.datastream.deserialize.JsonDtsRecord;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordgenerator.AvroDeserializer;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class Dts2CanalJsonKafka {
    private static final Logger LOG = LoggerFactory.getLogger(Dts2CanalJsonKafka.class);

    public static void main(String[] args) throws Exception {
        LOG.info("flink:Dts2CanalJsonKafka");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AbstractDtsToKafkaFlinkAction flinkAction = new AbstractDtsToKafkaFlinkAction() {
            @Override
            public void run() throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(10000);
                List<RouteDef> routeDefs = this.routeConfig;
                HashMap<String, String> extraColumns = this.extraColumnConfig;
                String includingTables = this.includingTablesConfig;
                String excludingTables = this.excludingTablesConfig;
                Integer mapParallelism = this.mapParallelismConfig;
                Integer sinkParallelism = this.sinkParallelismConfig;
                String prov = extraColumns.getOrDefault("prov", "");
                String enableDdl = this.enableDdl;
                String jobName = this.jobName;

                LOG.info("dts.broker-url:{}", this.sourceConfig.get("broker-url"));
                LOG.info("dts.topic:{}", this.sourceConfig.get("topic"));
                LOG.info("dts.sid:{}", this.sourceConfig.get("sid"));
                LOG.info("dts.group:{}", this.sourceConfig.get("group"));
                LOG.info("dts.user:{}", this.sourceConfig.get("user"));
                LOG.info("dts.password:{}", this.sourceConfig.get("password"));
                LOG.info("dts.startupOffsetsTimestamp:{}", this.sourceConfig.get("startupOffsetsTimestamp"));

                LOG.info("includingTables:{}", includingTables);
                LOG.info("excludingTables:{}", excludingTables);
                LOG.info("mapParallelism:{}", mapParallelism);
                LOG.info("sinkParallelism:{}", sinkParallelism);
                LOG.info("prov:{}", prov);

                DataStream<String> input = env.addSource(new FlinkDtsRawConsumer(this.sourceConfig.get("broker-url"), this.sourceConfig.get("topic"), this.sourceConfig.get("sid"), this.sourceConfig.get("group"), this.sourceConfig.get("user"), this.sourceConfig.get("password"), Long.parseLong(this.sourceConfig.get("startupOffsetsTimestamp")), new DtsByteDeserializationSchema(), null)).name("Dts").flatMap((FlatMapFunction<ByteRecord, String>) (byteRecord, out) -> {
                    if (byteRecord != null) {
                        JsonDtsRecord record = new JsonDtsRecord(byteRecord.getBytes(), new AvroDeserializer());
                        JSONObject dtsJson;
                        try {
                            dtsJson = record.getJson();
                        } catch (Exception e) {
                            LOG.error("record.getJson error:", e);
                            return;
                        }

                        if (OperationType.INSERT == record.getOperationType() || OperationType.UPDATE == record.getOperationType() || OperationType.DELETE == record.getOperationType() || (OperationType.DDL == record.getOperationType() && "true".equals(enableDdl))) {
                            String dtsObjectName = dtsJson.getString("objectName");
                            if (!shouldMonitorTable(dtsObjectName, includingTables, excludingTables)) {
                                return;
                            }
                            LOG.debug("Source table '{}' is included.", dtsObjectName);
                            LOG.debug("dtsJson:{}", dtsJson);
                            try {
                                String canalJson = JSON.toJSONString(CanalJsonUtils.convert(dtsJson, routeDefs, extraColumns));
                                LOG.debug("canalJson:{}", canalJson);
                                out.collect(canalJson);
                            } catch (Exception ex) {
                                LOG.warn("parse dts {} to canal failed :", record, ex);
                            }
                        }
                    }
                }).returns(Types.STRING).setParallelism(mapParallelism);

                Properties sinkProperties = new Properties();
                String kafkaBootstrapServers = this.sinkConfig.get("bootstrap.servers");
                if (StringUtils.isBlank(kafkaBootstrapServers)) {
                    kafkaBootstrapServers = "127.0.0.1:9092";
                }
                String kafkaTopic = this.sinkConfig.getOrDefault("topic", "");
                String kafkaGroupId = this.sinkConfig.getOrDefault("group.id", "dts2kafka");
                String kafkaSecurityProtocol = this.sinkConfig.getOrDefault("security.protocol", "SASL_PLAINTEXT");
                String kafkaSaslMechanism = this.sinkConfig.getOrDefault("sasl.mechanism", "GSSAPI");
                String kafkaSaslKerberosServiceName = this.sinkConfig.getOrDefault("sasl.kerberos.service.name", "kafka");
                String kafkaSaslJaasConfig = this.sinkConfig.getOrDefault("sasl.jaas.config", "");

                LOG.info("kafka.bootstrap.servers:{}", kafkaBootstrapServers);
                LOG.info("kafka.topic:{}", kafkaTopic);
                LOG.info("kafka.group.id:{}", kafkaGroupId);
                LOG.info("kafka.security.protocol:{}", kafkaSecurityProtocol);
                LOG.info("kafka.sasl.mechanism:{}", kafkaSaslMechanism);
                LOG.info("kafka.sasl.kerberos.service.name:{}", kafkaSaslKerberosServiceName);
                LOG.info("kafka.sasl.jaas.config:{}", kafkaSaslJaasConfig);

                if (StringUtils.isBlank(kafkaTopic)) {
                    input.addSink(new PrintSinkFunction<>()).setParallelism(sinkParallelism);
                    env.execute("Dts2Canal");
                    return;
                }

                sinkProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
                sinkProperties.setProperty("group.id", kafkaGroupId);
                sinkProperties.setProperty("security.protocol", kafkaSecurityProtocol);
                sinkProperties.setProperty("sasl.mechanism", kafkaSaslMechanism);
                sinkProperties.setProperty("sasl.kerberos.service.name", kafkaSaslKerberosServiceName);
                if (StringUtils.isNotBlank(kafkaSaslJaasConfig)) {
                    sinkProperties.setProperty("sasl.jaas.config", kafkaSaslJaasConfig);
                }
                FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(kafkaTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), sinkProperties, Optional.of(new FlinkKafkaPartitioner<String>() {
                    @Override
                    public int partition(String value, byte[] key, byte[] valueSerialized, String targetTopic, int[] partitions) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String operationType = jsonObject.getString("type");
                        if (operationType.equals("INSERT") || operationType.equals("UPDATE") || operationType.equals("DELETE")) {
                            return calculatePartition(jsonObject, partitions.length, prov);
                        } else {
                            return 0;
                        }
                    }
                }));

                input.addSink(kafkaProducer).name("Kafka").setParallelism(sinkParallelism);

                env.execute(jobName);
            }
        };
        flinkAction.create(args);

        flinkAction.run();

    }

    public static int calculatePartition(JSONObject canalJson, int totalPartitions, String prov) {
        String database = canalJson.getString("database");
        String table = canalJson.getString("table");
        List<String> pkNames = canalJson.getJSONArray("pkNames").toJavaList(String.class);

        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(prov)) {
            sb.append(prov);
        }
        sb.append(database).append(table);
        if (!pkNames.isEmpty() && !canalJson.getJSONArray("data").isEmpty()) {
            JSONObject dataObject = canalJson.getJSONArray("data").getJSONObject(0);
            if (dataObject != null) {
                for (String pkName : pkNames) {
                    sb.append(dataObject.getString(pkName));
                }
            }
        }

        int partitionHash = Hashing.murmur3_32().hashString(sb.toString(), StandardCharsets.UTF_8).asInt();
        int partition = Math.abs(partitionHash % totalPartitions);
        return partition;
    }

}
