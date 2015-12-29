package org.apache.kafka.connect.samples;

import static org.apache.kafka.connect.samples.Util.resource;

import java.util.List;
import java.util.Properties;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.connect.samples.pojos.Pojo;

public class KafkaProducer {

    private String topic;
    private Properties producerConfig;
    private SerializationSchema<Pojo, byte[]> schema;

    public KafkaProducer(String topic, Properties producerConfig, SerializationSchema<Pojo, byte[]> schema) {
        this.topic = topic;
        this.producerConfig = producerConfig;
        this.schema = schema;
    }

    public void produce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(pojos()).addSink(new FlinkKafkaProducer<>(topic, schema, producerConfig));
        env.execute();
    }
    
    private List<Pojo> pojos() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //@formatter:off
        DataSource<Pojo> ds = env.readCsvFile(resource("data.txt").getPath())
           .ignoreFirstLine()
           .fieldDelimiter(",")
           .pojoType(Pojo.class, "givenName|middleInitial|surname|streetAddress|city|zipCode|emailAddress|telephoneNumber|guid|latitude|longitude|kilograms|centimeters|company|occupation".split("\\|"));
        //@formatter:on
        
        return ds.collect();
    }
}
