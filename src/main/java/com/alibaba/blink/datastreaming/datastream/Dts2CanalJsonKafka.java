package com.alibaba.blink.datastreaming.datastream;

import com.alibaba.blink.datastreaming.datastream.action.AbstractDtsToKafkaFlinkAction;
import com.alibaba.blink.datastreaming.datastream.action.DrdsCdcProcessFunction;
import com.alibaba.blink.datastreaming.datastream.action.RouteDef;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJson;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonUtils;
import com.alibaba.blink.datastreaming.datastream.canal.CanalJsonWrapper;
import com.alibaba.blink.datastreaming.datastream.deserialize.ByteRecord;
import com.alibaba.blink.datastreaming.datastream.deserialize.DtsByteDeserializationSchema;
import com.alibaba.blink.datastreaming.datastream.deserialize.JsonDtsRecord;
import com.alibaba.blink.datastreaming.datastream.metric.CdcMetricNames;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordgenerator.AvroDeserializer;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
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

                DataStream<CanalJsonWrapper> input = env.addSource(new FlinkDtsRawConsumer(this.sourceConfig.get("broker-url"), this.sourceConfig.get("topic"), this.sourceConfig.get("sid"), this.sourceConfig.get("group"), this.sourceConfig.get("user"), this.sourceConfig.get("password"), Long.parseLong(this.sourceConfig.get("startupOffsetsTimestamp")), new DtsByteDeserializationSchema(), null)).name("Dts").flatMap(new RichFlatMapFunction<ByteRecord, CanalJsonWrapper>() {
                    public static final long UNDEFINED = -1L;
                    private transient long currentEmitEventTimeLag;
                    private transient long maxEmitEventTimeLag = Long.MIN_VALUE;
                    private transient long minEmitEventTimeLag = Long.MAX_VALUE;
                    private transient Meter cdcRecordsPerSecond;
                    private transient Meter insertRecordsPerSecond;
                    private transient Meter deleteRecordsPerSecond;
                    private transient Meter updateRecordsPerSecond;
                    private transient Meter ddlRecordsPerSecond;
                    private transient Meter dmlRecordsPerSecond;

                    private transient Counter cdcRecordsCount;
                    private transient Counter insertRecordsCount;
                    private transient Counter updateRecordsCount;
                    private transient Counter deleteRecordsCount;
                    private transient Counter ddlRecordsCount;
                    private transient Counter dmlRecordsCount;

                    @Override
                    public void open(Configuration config) {
                        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.CURRENT_EMIT_EVENT_TIME_LAG, () -> this.currentEmitEventTimeLag);
                        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MAX_EMIT_EVENT_TIME_LAG, new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                long value = maxEmitEventTimeLag;
                                if (value == Long.MIN_VALUE) {
                                    return currentEmitEventTimeLag;
                                }
                                maxEmitEventTimeLag = Long.MIN_VALUE;
                                return value;
                            }
                        });
                        getRuntimeContext().getMetricGroup().gauge(CdcMetricNames.MIN_EMIT_EVENT_TIME_LAG, new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                long value = minEmitEventTimeLag;
                                if (value == Long.MAX_VALUE) {
                                    return currentEmitEventTimeLag;
                                }
                                minEmitEventTimeLag = Long.MAX_VALUE;
                                return value;
                            }
                        });

                        this.cdcRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.CDC_RECORDS_PER_SECOND, new MeterView(1));
                        this.insertRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.INSERT_RECORDS_PER_SECOND, new MeterView(1));
                        this.deleteRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.DELETE_RECORDS_PER_SECOND, new MeterView(1));
                        this.updateRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.UPDATE_RECORDS_PER_SECOND, new MeterView(1));
                        this.ddlRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.DDL_RECORDS_PER_SECOND, new MeterView(1));
                        this.dmlRecordsPerSecond = getRuntimeContext().getMetricGroup().meter(CdcMetricNames.DML_RECORDS_PER_SECOND, new MeterView(1));

                        this.cdcRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.CDC_RECORDS_COUNT);
                        this.insertRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.INSERT_RECORDS_COUNT);
                        this.deleteRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DELETE_RECORDS_COUNT);
                        this.updateRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.UPDATE_RECORDS_COUNT);
                        this.ddlRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DDL_RECORDS_COUNT);
                        this.dmlRecordsCount = getRuntimeContext().getMetricGroup().counter(CdcMetricNames.DML_RECORDS_COUNT);

                    }

                    private void setMetricGenericRecordsPerSecond(String type) {
                        switch (type) {
                            case "INSERT": {
                                this.insertRecordsPerSecond.markEvent();
                                this.insertRecordsCount.inc();
                                this.dmlRecordsPerSecond.markEvent();
                                this.dmlRecordsCount.inc();
                                break;
                            }
                            case "DELETE": {
                                this.deleteRecordsPerSecond.markEvent();
                                this.deleteRecordsCount.inc();
                                this.dmlRecordsPerSecond.markEvent();
                                this.dmlRecordsCount.inc();
                                break;
                            }
                            case "UPDATE": {
                                this.updateRecordsPerSecond.markEvent();
                                this.updateRecordsCount.inc();
                                this.dmlRecordsPerSecond.markEvent();
                                this.dmlRecordsCount.inc();
                                break;
                            }
                            case "DDL": {
                                this.ddlRecordsPerSecond.markEvent();
                                this.ddlRecordsCount.inc();
                                break;
                            }
                        }
                    }

                    private void setInputMetric(JSONObject dtsRecord) {
                        try {
                            this.cdcRecordsPerSecond.markEvent();
                            this.cdcRecordsCount.inc();
                        } catch (Exception e) {
                            LOG.warn("flatmap.setMetric-{} error:", CdcMetricNames.CDC_RECORDS_PER_SECOND, e);
                        }
                        try {
                            String opTsString = dtsRecord.getString("sourceTimestamp");
                            if (opTsString != null) {
                                long opTs = new Timestamp(Long.valueOf(opTsString)).getTime();
                                this.currentEmitEventTimeLag = opTs != TimestampAssigner.NO_TIMESTAMP ? System.currentTimeMillis() - opTs * 1000 : UNDEFINED;
                                this.maxEmitEventTimeLag = Math.max(currentEmitEventTimeLag, maxEmitEventTimeLag);
                                this.minEmitEventTimeLag = Math.min(currentEmitEventTimeLag, minEmitEventTimeLag);

                            }
                        } catch (Exception e) {
                            LOG.warn("flatmap.setInputMetric-{} error:", CdcMetricNames.CURRENT_EMIT_EVENT_TIME_LAG, e);
                        }
                    }

                    private void setOutputMetric(JSONObject dtsRecord) {
                        try {
                            String operation = dtsRecord.getString("operation");
                            setMetricGenericRecordsPerSecond(operation);
                        } catch (Exception e) {
                            LOG.warn("flatmap.setOutputMetric-{} error:", CdcMetricNames.CDC_RECORDS_PER_SECOND, e);
                        }
                    }

                    @Override
                    public void flatMap(ByteRecord byteRecord, Collector<CanalJsonWrapper> out) throws Exception {
                        if (byteRecord != null) {
                            JsonDtsRecord record;
                            JSONObject dtsJson;
                            try {
                                record = new JsonDtsRecord(byteRecord.getBytes(), new AvroDeserializer());
                                dtsJson = record.getJson();
                            } catch (Exception e) {
                                LOG.error("record.getJson error:", e);
                                return;
                            }
                            setInputMetric(dtsJson);

                            if (OperationType.INSERT == record.getOperationType() || OperationType.UPDATE == record.getOperationType() || OperationType.DELETE == record.getOperationType() || (OperationType.DDL == record.getOperationType() && "true".equals(enableDdl))) {
                                String dtsObjectName = dtsJson.getString("objectName");
                                if (!shouldMonitorTable(dtsObjectName, includingTables, excludingTables)) {
                                    return;
                                }
                                LOG.debug("Source table '{}' is included.", dtsObjectName);
                                LOG.debug("dtsJson:{}", dtsJson);
                                try {
                                    CanalJson canalJson = CanalJsonUtils.convert(dtsJson, routeDefs, extraColumns, currentExtraPrimaryKeys);
                                    if (canalJson == null) {
                                        return;
                                    }
                                    LOG.debug("canalJson:{}", canalJson);
                                    setOutputMetric(dtsJson);
                                    out.collect(DrdsCdcProcessFunction.createCanalJsonWrapper(canalJson));
                                } catch (Exception ex) {
                                    LOG.warn("parse dts {} to canal failed :", record, ex);
                                }
                            }
                        }
                    }
                }).setParallelism(mapParallelism);

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
                    });

                    DataStream<CanalJsonWrapper> passThroughStream = mainDataStream.getSideOutput(passThroughOutputTag);
                    DataStream<CanalJsonWrapper> stateStream = mainDataStream.getSideOutput(stateOutputTag)
                            .keyBy(value -> DrdsCdcProcessFunction.generateStateKey(value))
                            .process(new DrdsCdcProcessFunction());
                    input = stateStream.union(passThroughStream);
                }

                DataStream<String> output = input.map(item -> JSON.toJSONString(item.getCanalJson(), JSONWriter.Feature.WriteMapNullValue)).returns(String.class);

                if (StringUtils.isBlank(kafkaTopic)) {
                    output.addSink(new PrintSinkFunction<>()).name("Print").setParallelism(sinkParallelism);
                    env.execute("Dts2Canal-" + jobName);
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
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String operationType = jsonObject.getString("type");
                        if (operationType.equals("INSERT") || operationType.equals("UPDATE") || operationType.equals("DELETE")) {
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
