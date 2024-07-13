package com.alibaba.blink.datastreaming.datastream;

import com.alibaba.blink.datastreaming.datastream.action.*;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.deserialize.DtsByteDeserializationSchema;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Dts2CanalJsonKafka {
    private static final Logger LOG = LoggerFactory.getLogger(Dts2CanalJsonKafka.class);

    public static void main(String[] args) throws Exception {
        LOG.info("flink:Dts2CanalJsonKafka");

        AbstractDtsToKafkaFlinkAction flinkAction = new AbstractDtsToKafkaFlinkAction() {
            @Override
            public void run() throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                List<RouteDef> routeDefs = this.routeConfig;
                HashMap<String, String> extraColumns = this.extraColumnConfig;
                String includingTables = this.includingTablesConfig;
                String excludingTables = this.excludingTablesConfig;
                Integer mapParallelism = this.mapParallelismConfig;
                Integer sinkParallelism = this.sinkParallelismConfig;
                String prov = extraColumns.getOrDefault("prov", "");
                String enableDdl = this.enableDdl;
                String jobName = this.jobName;
                String currentExtraPrimaryKeys = this.extraPrimaryKeys;
                String enablePartitionUpdatePerform = this.enablePartitionUpdatePerform;
                long partitionUpdatePerformsStateTtl = this.partitionUpdatePerformsStateTtl;
                long partitionUpdatePerformsTimerTimeInternalMs = this.partitionUpdatePerformsTimerTimeInternalMs;
                int memoryStateMaxSize = this.memoryStateMaxSize;
                String mapToString = this.mapToString;

                String dtsBrokerUrl = this.sourceConfig.get("broker-url");
                String dtsTopic = this.sourceConfig.get("topic");
                String dtsSid = this.sourceConfig.get("sid");
                String dtsGroup = this.sourceConfig.get("group");
                String dtsUser = this.sourceConfig.get("user");
                String dtsPassword = this.sourceConfig.get("password");
                String dtsStartupOffsetsTimestamp = this.sourceConfig.get("startupOffsetsTimestamp");

                LOG.info("dts.broker-url:{}", dtsBrokerUrl);
                LOG.info("dts.topic:{}", dtsTopic);
                LOG.info("dts.sid:{}", dtsSid);
                LOG.info("dts.group:{}", dtsGroup);
                LOG.info("dts.user:{}", dtsUser);
                LOG.info("dts.password:{}", dtsPassword);
                LOG.info("dts.startupOffsetsTimestamp:{}", dtsStartupOffsetsTimestamp);

                LOG.info("includingTables:{}", includingTables);
                LOG.info("excludingTables:{}", excludingTables);
                LOG.info("mapParallelism:{}", mapParallelism);
                LOG.info("sinkParallelism:{}", sinkParallelism);
                LOG.info("prov:{}", prov);
                LOG.info("enablePartitionUpdatePerform:{}", enablePartitionUpdatePerform);
                LOG.info("partitionUpdatePerformsStateTtl:{}", partitionUpdatePerformsStateTtl);
                LOG.info("partitionUpdatePerformsTimerTimeInternalMs:{}", partitionUpdatePerformsTimerTimeInternalMs);
                LOG.info("memoryStateMaxSize:{}", memoryStateMaxSize);
                LOG.info("mapToString:{}", mapToString);

                Properties sinkProperties = new Properties();
                String kafkaBootstrapServers = this.sinkConfig.getOrDefault("bootstrap.servers", "");
                String kafkaTopic = this.sinkConfig.getOrDefault("topic", "");
                String kafkaGroupId = this.sinkConfig.getOrDefault("group.id", "dts2kafka");
                String kafkaSecurityProtocol = this.sinkConfig.getOrDefault("security.protocol", "SASL_PLAINTEXT");
                String kafkaSaslMechanism = this.sinkConfig.getOrDefault("sasl.mechanism", "GSSAPI");
                String kafkaSaslKerberosServiceName = this.sinkConfig.getOrDefault("sasl.kerberos.service.name", "kafka");
                String kafkaSaslJaasConfig = this.sinkConfig.getOrDefault("sasl.jaas.config", "");
                String kafkaBatchSize = this.sinkConfig.getOrDefault("batch.size", "");
                String kafkaLingerMs = this.sinkConfig.getOrDefault("linger.ms", "");
                String kafkaBufferMemory = this.sinkConfig.getOrDefault("buffer.memory", "");
                String kafkaRetries = this.sinkConfig.getOrDefault("retries", "");
                String kafkaMaxInFlightRequestPerConnection = this.sinkConfig.getOrDefault("max.in.flight.requests.per.connection", "");
                String kafkaPartition = this.sinkConfig.get("partition");

                LOG.info("kafka.bootstrap.servers:{}", kafkaBootstrapServers);
                LOG.info("kafka.topic:{}", kafkaTopic);
                LOG.info("kafka.group.id:{}", kafkaGroupId);
                LOG.info("kafka.security.protocol:{}", kafkaSecurityProtocol);
                LOG.info("kafka.sasl.mechanism:{}", kafkaSaslMechanism);
                LOG.info("kafka.sasl.kerberos.service.name:{}", kafkaSaslKerberosServiceName);
                LOG.info("kafka.sasl.jaas.config:{}", kafkaSaslJaasConfig);
                LOG.info("kafka.batch.size:{}", kafkaBatchSize);
                LOG.info("kafka.linger.ms:{}", kafkaLingerMs);
                LOG.info("kafka.buffer.memory:{}", kafkaBufferMemory);
                LOG.info("kafka.partition:{}", kafkaPartition);

                if (memoryStateMaxSize > 0L) {
                    StateBackend backend = new MemoryStateBackend(memoryStateMaxSize, false);
                    env.setStateBackend(backend);
                }

                if (this.dynamicSourceConfig == null || dynamicSourceConfig.isEmpty()) {
                    throw new Exception("sourceConfig is empty");
                }
                HashMap<String, String> sourceConfig0 = dynamicSourceConfig.get(0);
                String dtsName0 = sourceConfig0.get("name");
                String dtsBrokerUrl0 = sourceConfig0.get("broker-url");
                String dtsTopic0 = sourceConfig0.get("topic");
                String dtsSid0 = sourceConfig0.get("sid");
                String dtsGroup0 = sourceConfig0.get("group");
                String dtsUser0 = sourceConfig0.get("user");
                String dtsPassword0 = sourceConfig0.get("password");
                String dtsStartupOffsetsTimestamp0 = sourceConfig0.get("startupOffsetsTimestamp");
                LOG.info("Dts{}-config:{}", dtsName0, sourceConfig0);

                Dts2CanalProcessFunction dts2CanalProcessFunction0 = new Dts2CanalProcessFunction();
                dts2CanalProcessFunction0.setRouteDefs(routeDefs);
                dts2CanalProcessFunction0.setExtraColumns(extraColumns);
                dts2CanalProcessFunction0.setExtraPrimaryKeys(currentExtraPrimaryKeys);
                dts2CanalProcessFunction0.setIncludingTables(includingTables);
                dts2CanalProcessFunction0.setExcludingTables(excludingTables);
                dts2CanalProcessFunction0.setEnableDdl(enableDdl);
                dts2CanalProcessFunction0.setMapToString(mapToString);
                dts2CanalProcessFunction0.setDtsTopic(dtsTopic0);
                DataStream<CanalJsonWrapper> input = env.addSource(new FlinkDtsRawConsumer(dtsBrokerUrl0, dtsTopic0, dtsSid0, dtsGroup0, dtsUser0, dtsPassword0, Long.parseLong(dtsStartupOffsetsTimestamp0), new DtsByteDeserializationSchema(), null).assignTimestampsAndWatermarks(new DtsAssignerWithPeriodicWatermarks(Duration.ofSeconds(0)))).setParallelism(1).name(dtsName0).process(dts2CanalProcessFunction0).name(dtsName0 + "ToCanal").setParallelism(mapParallelism);
                for (int i = 1; i < dynamicSourceConfig.size(); i++) {
                    HashMap<String, String> sourceConfigN = dynamicSourceConfig.get(i);
                    String dtsNameN = sourceConfigN.get("name");
                    String dtsBrokerUrlN = sourceConfigN.get("broker-url");
                    String dtsTopicN = sourceConfigN.get("topic");
                    String dtsSidN = sourceConfigN.get("sid");
                    String dtsGroupN = sourceConfigN.get("group");
                    String dtsUserN = sourceConfigN.get("user");
                    String dtsPasswordN = sourceConfigN.get("password");
                    String dtsStartupOffsetsTimestampN = sourceConfigN.get("startupOffsetsTimestamp");
                    LOG.info("Dts{}-config:{}", dtsNameN, sourceConfigN);
                    Dts2CanalProcessFunction dts2CanalProcessFunctionN = new Dts2CanalProcessFunction();
                    dts2CanalProcessFunctionN.setRouteDefs(routeDefs);
                    dts2CanalProcessFunctionN.setExtraColumns(extraColumns);
                    dts2CanalProcessFunctionN.setExtraPrimaryKeys(currentExtraPrimaryKeys);
                    dts2CanalProcessFunctionN.setIncludingTables(includingTables);
                    dts2CanalProcessFunctionN.setExcludingTables(excludingTables);
                    dts2CanalProcessFunctionN.setEnableDdl(enableDdl);
                    dts2CanalProcessFunctionN.setMapToString(mapToString);
                    dts2CanalProcessFunctionN.setDtsTopic(dtsTopicN);
                    input = input.union(env.addSource(new FlinkDtsRawConsumer(dtsBrokerUrlN, dtsTopicN, dtsSidN, dtsGroupN, dtsUserN, dtsPasswordN, Long.parseLong(dtsStartupOffsetsTimestampN), new DtsByteDeserializationSchema(), null).assignTimestampsAndWatermarks(new DtsAssignerWithPeriodicWatermarks(Duration.ofSeconds(0)))).setParallelism(1).name(dtsNameN).process(dts2CanalProcessFunctionN).name(dtsNameN + "ToCanal").setParallelism(mapParallelism));
                }

                if ("true".equalsIgnoreCase(enablePartitionUpdatePerform)) {
                    final OutputTag<CanalJsonWrapper> stateOutputTag = new OutputTag<CanalJsonWrapper>("state-output") {
                    };
                    final OutputTag<CanalJsonWrapper> passThroughOutputTag = new OutputTag<CanalJsonWrapper>("pass-through-output") {
                    };

                    SingleOutputStreamOperator<CanalJsonWrapper> mainDataStream = input.process(new ProcessFunction<CanalJsonWrapper, CanalJsonWrapper>() {
                        @Override
                        public void processElement(CanalJsonWrapper s, ProcessFunction<CanalJsonWrapper, CanalJsonWrapper>.Context context, Collector<CanalJsonWrapper> collector) {
                            if (DrdsCdcProcessFunction.shouldAdditionalProcess(s)) {
                                context.output(stateOutputTag, s);
                            } else {
                                context.output(passThroughOutputTag, s);
                            }
                        }
                    }).name("KeyBy");

                    DataStream<CanalJsonWrapper> passThroughStream = mainDataStream.getSideOutput(passThroughOutputTag);
                    DrdsCdcProcessFunction drdsCdcProcessFunction = new DrdsCdcProcessFunction();
                    drdsCdcProcessFunction.setStateTtl(partitionUpdatePerformsStateTtl);
                    drdsCdcProcessFunction.setTimerTimeInternalMs(partitionUpdatePerformsTimerTimeInternalMs);
                    DataStream<CanalJsonWrapper> stateStream = mainDataStream.getSideOutput(stateOutputTag).keyBy(value -> DrdsCdcProcessFunction.generateStateKey(value, true)).process(drdsCdcProcessFunction).name("PartitionUpdatePerform").setParallelism(1);
                    input = stateStream.union(passThroughStream);
                }

                DataStream<String> output = input.map(item -> {
                    CanalJson canalJson = item.getCanalJson();
                    Map<String, String> canalJsonWrapperTags = item.getTags();
                    Map<String, Map<String, String>> canalJsonTags = canalJson.getTags();
                    if (canalJsonTags == null) {
                        canalJsonTags = new HashMap<>();
                    }

                    Map<String, String> dtsTags = canalJsonTags.get("dts");
                    if (dtsTags == null) {
                        dtsTags = new HashMap<>();
                    }
                    if (canalJsonWrapperTags != null) {
                        dtsTags.put("dtsTopic", canalJsonWrapperTags.get("dtsTopic"));
                    }
                    canalJsonTags.put("dts", dtsTags);

                    Map<String, String> subscribeTags = canalJsonTags.get("subscribe");
                    if (subscribeTags == null) {
                        subscribeTags = new HashMap<>();
                    }
                    if (canalJsonWrapperTags != null) {
                        subscribeTags.put("kafkaSinkTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(System.currentTimeMillis()));
                        subscribeTags.put("sourceInTime", canalJsonWrapperTags.get("sourceInTime"));
                        subscribeTags.put("dts2canalOutTime", canalJsonWrapperTags.get("dts2canalOutTime"));
                        subscribeTags.put("partitionUpdateInTime", canalJsonWrapperTags.get("partitionUpdateInTime"));
                        subscribeTags.put("stateRegisterTimerTime", canalJsonWrapperTags.get("stateRegisterTimerTime"));
                        subscribeTags.put("stateMatchStateTime", canalJsonWrapperTags.get("stateMatchStateTime"));
                        subscribeTags.put("stateOnTimerTime", canalJsonWrapperTags.get("stateOnTimerTime"));
                        subscribeTags.put("partitionUpdateOutTime", canalJsonWrapperTags.get("partitionUpdateOutTime"));

                        String registerTimerEventTimestamp = canalJsonWrapperTags.get("eventTimestamp");
                        String registerTimerProcessTimestamp = canalJsonWrapperTags.get("processTimestamp");
                        String registerTimerWatermark = canalJsonWrapperTags.get("watermark");
                        if (StringUtils.isNotBlank(registerTimerEventTimestamp)) {
                            subscribeTags.put("registerTimerEventTimestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(new Timestamp(Long.valueOf(registerTimerEventTimestamp))));
                        } else {
                            subscribeTags.put("registerTimerEventTimestamp", null);
                        }
                        if (StringUtils.isNotBlank(registerTimerProcessTimestamp)) {
                            subscribeTags.put("registerTimerProcessTimestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(new Timestamp(Long.valueOf(registerTimerProcessTimestamp))));
                        } else {
                            subscribeTags.put("registerTimerProcessTimestamp", null);
                        }
                        if (StringUtils.isNotBlank(registerTimerWatermark)) {
                            subscribeTags.put("registerTimerWatermark", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms").format(new Timestamp(Long.valueOf(registerTimerWatermark))));
                        } else {
                            subscribeTags.put("registerTimerWatermark", null);
                        }
                    }
                    canalJsonTags.put("subscribe", subscribeTags);
                    canalJson.setTags(canalJsonTags);
                    String itemAfter = JSON.toJSONString(canalJson, JSONWriter.Feature.WriteMapNullValue);
                    return itemAfter;
                }).returns(String.class).setParallelism(1);

                if (StringUtils.isBlank(kafkaTopic)) {
                    output.addSink(new PrintSinkFunction<>()).name("Print").setParallelism(sinkParallelism);
                    env.execute("dts2print");
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
                if (StringUtils.isNotBlank(kafkaBatchSize)) {
                    sinkProperties.setProperty("batch.size", kafkaBatchSize);
                }
                if (StringUtils.isNotBlank(kafkaLingerMs)) {
                    sinkProperties.setProperty("linger.ms", kafkaLingerMs);
                }
                if (StringUtils.isNotBlank(kafkaBufferMemory)) {
                    sinkProperties.setProperty("buffer.memory", kafkaBufferMemory);
                }
                if (StringUtils.isNotBlank(kafkaRetries)) {
                    sinkProperties.setProperty("retries", kafkaRetries);
                }
                if (StringUtils.isNotBlank(kafkaMaxInFlightRequestPerConnection)) {
                    sinkProperties.setProperty("max.in.flight.requests.per.connection", kafkaMaxInFlightRequestPerConnection);
                }
                FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(kafkaTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), sinkProperties, Optional.of(new FlinkKafkaPartitioner<String>() {
                    @Override
                    public int partition(String value, byte[] key, byte[] valueSerialized, String targetTopic, int[] partitions) {
                        if (kafkaPartition != null) {
                            return Integer.valueOf(kafkaPartition);
                        }
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String operationType = jsonObject.getString("type");
                        if (operationType.equals("INSERT") || operationType.equals("UPDATE") || operationType.equals("DELETE") || operationType.equals("INSERT" + DrdsCdcProcessFunction.MERGE_SUFFIX) || operationType.equals("DELETE" + DrdsCdcProcessFunction.MERGE_SUFFIX) || operationType.equals("INSERT" + DrdsCdcProcessFunction.NOMATCH_DROP_SUFFIX) || operationType.equals("DELETE" + DrdsCdcProcessFunction.NOMATCH_DROP_SUFFIX)) {
                            return calculatePartition(jsonObject, partitions.length, prov);
                        } else {
                            return 0;
                        }
                    }
                }));

                output.addSink(kafkaProducer).name("Kafka").setParallelism(sinkParallelism);

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
