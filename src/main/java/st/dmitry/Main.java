package st.dmitry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.component.Producer;
import st.dmitry.factory.KafkaFactory;
import st.dmitry.schedule.ConsumerScheduleStrategy;
import st.dmitry.schedule.impl.DefaultScheduleStrategy;
import st.dmitry.schedule.impl.StreamScheduleStrategy;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final Map<ConsumerScheduleStrategy.Type, ConsumerScheduleStrategy> STRATEGIES = Map.of(
        ConsumerScheduleStrategy.Type.DEFAULT, new DefaultScheduleStrategy(),
        ConsumerScheduleStrategy.Type.STREAM, new StreamScheduleStrategy()
    );

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        scheduleProducer(scheduler);
        scheduleConsumers(scheduler, ConsumerScheduleStrategy.Type.STREAM, 2);

        Thread.sleep(30000);
        scheduler.shutdown();
        boolean awaited = scheduler.awaitTermination(1, TimeUnit.MINUTES);
        log.info("Awaited is {}. Shutting down...", awaited);
    }

    private static void scheduleProducer(ScheduledExecutorService scheduler) {
        Producer producer = KafkaFactory.getProducer();
        Runnable send = () -> producer.send(System.currentTimeMillis() % 10, "Message UUID: " + UUID.randomUUID());
        scheduler.scheduleAtFixedRate(send, 100, 300, TimeUnit.MILLISECONDS);
        log.info("Producer scheduled at {}", ZonedDateTime.now());
    }

    private static void scheduleConsumers(ScheduledExecutorService scheduler, ConsumerScheduleStrategy.Type type, int times)
        throws InterruptedException {
        ConsumerScheduleStrategy consumerScheduleStrategy = STRATEGIES.get(type);
        KafkaFactory.scheduleConsumer(scheduler, consumerScheduleStrategy, times);
    }
}