package CustomThreadPool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class CustomThreadFactory implements ThreadFactory {
    public static final Logger logger = Logger.getLogger(CustomThreadFactory.class.getName());
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private final String prefix;

    public CustomThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(prefix + "-" + COUNTER.incrementAndGet());
        logger.info(CustomThreadFactory.class.getName() + " Creating new thread: " + thread.getName());
        return thread;
    }
}
