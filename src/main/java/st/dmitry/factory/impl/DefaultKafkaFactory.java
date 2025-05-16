package st.dmitry.factory.impl;

import st.dmitry.component.Consumer;
import st.dmitry.component.impl.DefaultConsumer;
import st.dmitry.component.impl.DefaultProducer;
import st.dmitry.component.Producer;
import st.dmitry.factory.KafkaFactory;

public class DefaultKafkaFactory extends KafkaFactory {

    @Override
    public Consumer createConsumer(int number) {
        return new DefaultConsumer(servers, topic, number);
    }

    @Override
    public Producer createProducer() {
        return new DefaultProducer(servers, topic);
    }
}
