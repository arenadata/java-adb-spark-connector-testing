package com.oorlov.sandbox1;
import java.util.concurrent.ThreadPoolExecutor;
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
        this.logger     = logger;
        this.threadPool = threadPool;
    }

    public void run() {
        while (continueToRun) {
            try {
                Thread.sleep(DEFAULT_DELAY);
                int total = threadPool.getActiveCount();

                if (total == AMOUNT_TO_STOP)
                    continueToRun = false;
            } catch (InterruptedException exception) {
                logger.error(exception);
            }
        }

        logger.info("Now, we shall terminate the dummy thread.");
        Thread.currentThread().interrupt();
    }
}
