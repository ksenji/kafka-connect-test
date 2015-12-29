package org.apache.kafka.connect.samples;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.samples.Util.resource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.LogManager;

import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.apache.kafka.connect.samples.pojos.Pojo;

public class KafkaFileStreamConnectorTest {

    public static void main(String[] args) throws Exception {
        initLogging();

        produce();

        List<String> params = new ArrayList<String>();
        params.add(resource("connect-standalone.properties").getPath());
        params.add(resource("connect-console-sink.properties").getPath());

        ConnectStandalone.main(params.toArray(new String[params.size()]));
    }

    private static void initLogging() throws Exception {
        try (InputStream is = resource("logging.properties").openStream()) {
            LogManager.getLogManager().readConfiguration(is);
        }
    }

    private static void produce() throws Exception {
        Properties props = new Properties();
        ResourceBundle broker = ResourceBundle.getBundle("connect-standalone");
        props.put(BOOTSTRAP_SERVERS_CONFIG, broker.getString(BOOTSTRAP_SERVERS_CONFIG));

        ResourceBundle sink = ResourceBundle.getBundle("connect-console-sink");
        String topic = sink.getString("topics");

        new KafkaProducer(topic, props, new SerializationSchema<Pojo, byte[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public byte[] serialize(Pojo element) {
                return Util.convertor().fromConnectData(topic, Util.schema(), Util.toStruct(element));
            }
        }).produce();
    }

}
