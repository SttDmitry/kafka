package st.dmitry.schedule;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public interface ConsumerScheduleStrategy {

    void schedule(ScheduledExecutorService scheduler, Function<Integer, ScheduledConsumer> consumer, int times) throws InterruptedException;

    Type type();

    enum Type {
        DEFAULT,
        STREAM
    }
}
