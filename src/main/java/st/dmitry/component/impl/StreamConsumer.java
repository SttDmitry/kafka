package st.dmitry.component.impl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.component.Consumer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public class StreamConsumer implements Consumer {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);
    private static final Properties PROPERTIES = new Properties();
    private static final int MAX_COUNT = 10;

    static {
        PROPERTIES.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-consumer");
        PROPERTIES.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    public StreamConsumer(String bootstrapServer, String topic, int number) {
        PROPERTIES.setProperty(StreamsConfig.STATE_DIR_CONFIG, getTempDirectory().toAbsolutePath().toString());
        PROPERTIES.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> messages = builder.stream(topic);

        KTable<String, Long> levelCount = messages
            .groupBy((level, message) -> level)
            .count();

        Map<String, String> environment = System.getenv();
        String streamOut = environment.getOrDefault("STREAM_OUT", "stream-out");
        levelCount.toStream()
            .filter((k, v) -> v >= MAX_COUNT)
            .peek((k, v) -> log.info("Consumer#{} There are {} messages with level {}", number, v, k))
            .to(streamOut, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, PROPERTIES);
        streams.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        streams.close();
    }

    public void consume() {
        //Messages consumed through Kafka Stream
    }

    private Path getTempDirectory() {
        try {
            return Files.createTempDirectory("kafka-streams");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
