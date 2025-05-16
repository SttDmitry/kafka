package st.dmitry.factory;


import st.dmitry.component.Consumer;
import st.dmitry.component.Producer;

import java.util.Map;

public abstract class KafkaFactory {

    protected String servers;
    protected String topic;

    {
        Map<String, String> environment = System.getenv();
        servers = environment.get("KAFKA_SERVERS");
        topic = environment.get("KAFKA_TOPIC");
    }

    public abstract Consumer createConsumer(int number);

    public abstract Producer createProducer();
}
