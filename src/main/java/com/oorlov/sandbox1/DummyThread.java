package com.oorlov.sandbox1;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Logger;

public class DummyThread extends Thread {
    // Consts
    private static final int AMOUNT_TO_STOP = 0;
    private static final int DEFAULT_DELAY  = 10000;
    // Immutable objects
    private final Logger logger;
    private final ThreadPoolExecutor threadPool;
    // Mutable objects
    private boolean continueToRun = true;

    public DummyThread(Logger logger, ThreadPoolExecutor threadPool) {
        if (logger == null)
            throw new NullArgumentException("The input logger object can't be null.");

        if (threadPool == null)
            throw new NullArgumentException("The input thread pool object can't be null.");

        this.logger     = logger;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        while (continueToRun) {
            try {
                Thread.sleep(DEFAULT_DELAY);
                int total = threadPool.getActiveCount();

                if (total == AMOUNT_TO_STOP)
                    continueToRun = false;
            } catch (InterruptedException exception) {
                logger.error(exception);
                Thread.currentThread().interrupt();
            }
        }

        logger.info("Now, we shall terminate the dummy thread.");
        Thread.currentThread().interrupt();
    }
}
