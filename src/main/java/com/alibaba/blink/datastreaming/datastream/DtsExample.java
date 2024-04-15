package com.alibaba.blink.datastreaming.datastream;

import com.alibaba.flink.connectors.dts.FlinkDtsConsumer;
import com.alibaba.flink.connectors.dts.FlinkDtsKafkaConsumer;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.raw.DtsRecordDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class DtsExample {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String configFilePath = parameterTool.get("configFile");
        /*创建一个Properties对象，用于保存在平台中设置的相关参数值。*/
        Properties properties = new Properties();

        /*将平台页面中设置的参数值加载到Properties对象中。*/
        properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream input = env.addSource(
                new FlinkDtsRawConsumer(
                        (String) properties.get("broker-url"),
                        (String) properties.get("topic"),
                        (String) properties.get("sid"),
                        (String) properties.get("group"),
                        (String) properties.get("user"),
                        (String) properties.get("password"),
                        Integer.valueOf((String) properties.get("checkpoint")),
                        new DtsRecordDeserializationSchema(),
                        null))
                .filter(
                        new FilterFunction<DtsRecord>() {
                            @Override
                            public boolean filter(DtsRecord record) throws Exception {
                                if (OperationType.INSERT == record.getOperationType()
                                        || OperationType.UPDATE == record.getOperationType()
                                        || OperationType.DELETE == record.getOperationType()
                                        || OperationType.HEARTBEAT
                                        == record.getOperationType()) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }
                        })
                ;

        input.addSink(new PrintSinkFunction<>()).setParallelism(1);

        env.execute("Dts subscribe Example");
    }
}
