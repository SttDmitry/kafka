package st.dmitry.schedule.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.schedule.ConsumerScheduleStrategy;
import st.dmitry.schedule.ScheduledConsumer;

import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public class StreamScheduleStrategy implements ConsumerScheduleStrategy {

    private static final Logger log = LoggerFactory.getLogger(StreamScheduleStrategy.class);

    @Override
    public void schedule(ScheduledExecutorService scheduler, Function<Integer, ScheduledConsumer> consumer, int times) {
        consumer.apply(times);
        log.info("Consumer#{} scheduled at {}", times, ZonedDateTime.now());
    }

    @Override
    public Type type() {
        return Type.STREAM;
    }
}
