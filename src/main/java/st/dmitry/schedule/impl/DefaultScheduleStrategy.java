package st.dmitry.schedule.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.component.Consumer;
import st.dmitry.schedule.ConsumerScheduleStrategy;
import st.dmitry.schedule.ScheduledConsumer;

import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DefaultScheduleStrategy implements ConsumerScheduleStrategy {

    private static final Logger log = LoggerFactory.getLogger(DefaultScheduleStrategy.class);

    @Override
    public void schedule(ScheduledExecutorService scheduler, Function<Integer, ScheduledConsumer> consumerFunction, int times) throws InterruptedException {
        for (int number = 0; number < times; number++) {
            ScheduledConsumer scheduledConsumer = consumerFunction.apply(number);
            if (scheduledConsumer instanceof Consumer consumer) {
                scheduler.scheduleAtFixedRate(consumer::consume, 300, 1000, TimeUnit.MILLISECONDS);
            }
            log.info("Consumer#{} scheduled at {}", number, ZonedDateTime.now());
            Thread.sleep(5000);
        }
    }

    @Override
    public Type type() {
        return Type.DEFAULT;
    }


}
