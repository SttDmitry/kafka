package st.dmitry.factory;

import st.dmitry.component.Producer;
import st.dmitry.component.impl.DefaultConsumer;
import st.dmitry.component.impl.DefaultProducer;
import st.dmitry.component.impl.StreamConsumer;
import st.dmitry.schedule.ConsumerScheduleStrategy;
import st.dmitry.schedule.ScheduledConsumer;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public class KafkaFactory {

    private static final String SERVERS;
    private static final String TOPIC;

    static {
        Map<String, String> environment = System.getenv();
        SERVERS = environment.get("KAFKA_SERVERS");
        TOPIC = environment.get("KAFKA_TOPIC");
    }

    public static Producer getProducer() {
        return new DefaultProducer(SERVERS, TOPIC);
    }

    public static void scheduleConsumer(ScheduledExecutorService scheduler, ConsumerScheduleStrategy strategy, int times)
        throws InterruptedException {
        ConsumerScheduleStrategy.Type type = strategy.type();
        Function<Integer, ScheduledConsumer> consumerFunction = switch (type) {
            case DEFAULT -> (number) -> new DefaultConsumer(SERVERS, TOPIC, number);
            case STREAM -> (number) -> new StreamConsumer(SERVERS, TOPIC, number);
        };
        strategy.schedule(scheduler, consumerFunction, times);
    }
}
