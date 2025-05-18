package st.dmitry.component.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.component.Consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class DefaultConsumer implements Consumer {

    private static final Logger log = LoggerFactory.getLogger(DefaultConsumer.class);
    private static final Properties PROPERTIES = new Properties();

    private final KafkaConsumer<String, String> consumer;
    private final int number;

    static {
        PROPERTIES.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka_group_id");
        PROPERTIES.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"); //consumer только с kafka 4.0.0
        PROPERTIES.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "512");
        PROPERTIES.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
        PROPERTIES.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        PROPERTIES.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        PROPERTIES.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Отключаем, если GROUP_PROTOCOL = 'consumer'
//        PROPERTIES.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
//        PROPERTIES.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
//        PROPERTIES.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        PROPERTIES.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        PROPERTIES.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    public DefaultConsumer(String bootstrapServer, String topic, int number) {
        PROPERTIES.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        this.number = number;
        this.consumer = new KafkaConsumer<>(PROPERTIES);
        consumer.subscribe(List.of(topic));
    }

    public void consume() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        log.info("Consumer#{} Message count: {}", number, records.count());
        records.forEach(record -> log.info("Consumer#{} Received: {}", number, record));
    }
}
