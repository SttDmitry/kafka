package st.dmitry.component.impl;

import st.dmitry.factory.impl.DefaultKafkaFactory;
import st.dmitry.factory.KafkaFactory;
import st.dmitry.factory.impl.StreamKafkaFactory;

public class FactoryManager {

    public static KafkaFactory getFactory(Type type) {
        return switch (type) {
            case DEFAULT -> new DefaultKafkaFactory();
            case STREAM -> new StreamKafkaFactory();
        };
    }

    public enum Type {
        DEFAULT,
        STREAM
    }
}
