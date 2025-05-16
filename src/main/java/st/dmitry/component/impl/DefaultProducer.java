package st.dmitry.component.impl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import st.dmitry.component.Producer;

import java.util.Properties;

public class DefaultProducer implements Producer {

    private static final Properties PROPERTIES = new Properties();

    private final String topic;
    private final KafkaProducer<String, String> producer;

    static {
        PROPERTIES.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        PROPERTIES.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        PROPERTIES.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        PROPERTIES.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        PROPERTIES.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    }

    public DefaultProducer(String bootstrapServer, String topic) {
        PROPERTIES.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.topic = (topic);

        this.producer = new KafkaProducer<>(PROPERTIES);
    }

    public void send(long key, String message) {
        producer.send(new ProducerRecord<>(topic, String.valueOf(key), message));
    }
}
