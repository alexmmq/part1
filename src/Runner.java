import CustomThreadPool.CustomThreadPoolExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Runner {
    public static void main(String[] args) {
        CustomThreadPoolExecutor executor = new CustomThreadPoolExecutor(2, 4, 5,
                TimeUnit.SECONDS, 5, 1);

        for(int i = 0; i < 5; i++){
            executor.execute(() ->
            {
                try {
                    Thread.sleep(1000);
                    System.out.println("Task execution started");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        try {
            Thread.sleep(20000);
            executor.shutdownNow();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
