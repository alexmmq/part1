import CustomThreadPool.CustomThreadPoolExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Runner {
    private static final Logger LOGGER = Logger.getLogger(Runner.class.getName());

    public static void main(String[] args) throws InterruptedException {
        CustomThreadPoolExecutor executor = new CustomThreadPoolExecutor(
                2,
                4,
                5,
                TimeUnit.SECONDS,
                5,
                1);


        Runnable imitationTask = () -> {
            try {
                Thread.sleep(2000);
                LOGGER.info("Task started: " + Thread.currentThread().getName());
                Thread.sleep(3000);
                LOGGER.info("Task finished: " + Thread.currentThread().getName());
            } catch (InterruptedException e) {
                LOGGER.severe("Task interrupted: " + e.getMessage());
            }
        };


        for (int i = 0; i < 5; i++) {
            executor.execute(imitationTask);
        }


        Thread.sleep(15000);

        executor.shutdown();

        LOGGER.info("Waiting for all tasks to finish...");
        executor.awaitTermination(30, TimeUnit.SECONDS);
        LOGGER.info("All tasks have been completed.");

        Runnable rejectedTask = () -> LOGGER.info("Rejected task attempted to be executed.");
        executor.execute(rejectedTask);
        // Эта задача будет отклонена, так как пул закрыт, выбросит Exception

        LOGGER.info("Testing rejection of tasks after shutdown.");
    }

}
