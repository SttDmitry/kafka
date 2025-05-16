package st.dmitry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st.dmitry.component.Consumer;
import st.dmitry.component.impl.FactoryManager;
import st.dmitry.component.Producer;
import st.dmitry.factory.KafkaFactory;

import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        KafkaFactory factory = FactoryManager.getFactory(FactoryManager.Type.STREAM);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        scheduleProducer(scheduler, factory);
        scheduleConsumers(scheduler, factory, 1);

        Thread.sleep(10000);
        scheduler.shutdown();
        boolean awaited = scheduler.awaitTermination(1, TimeUnit.MINUTES);
        log.info("Awaited is {}. Shutting down...", awaited);
    }

    private static void scheduleProducer(ScheduledExecutorService scheduler, KafkaFactory factory) {
        Producer producer = factory.createProducer();
//  На основе key - Kafka при помощи DefaultPartitioner выбирает необходимую партицию (хэш функция ключа % кол-во партиций)
//        long constantKey = System.currentTimeMillis() % 10;
//        log.info("Producer constant key: {}", constantKey);
//        Runnable send = () -> producer.send(constantKey, "Message UUID: " + UUID.randomUUID());
        Runnable send = () -> producer.send(System.currentTimeMillis() % 10, "Message UUID: " + UUID.randomUUID());
        scheduler.scheduleAtFixedRate(send, 100, 300, TimeUnit.MILLISECONDS);
        log.info("Producer scheduled at {}", ZonedDateTime.now());
    }

    private static void scheduleConsumers(ScheduledExecutorService scheduler, KafkaFactory factory, int times)
        throws InterruptedException {
        for (int number = 0; number < times; number++) {
            Consumer consumer = factory.createConsumer(number);
            scheduler.scheduleAtFixedRate(consumer::consume, 300, 1000, TimeUnit.MILLISECONDS);
            log.info("Consumer#{} scheduled at {}", number, ZonedDateTime.now());
            Thread.sleep(5000);
        }
    }
}