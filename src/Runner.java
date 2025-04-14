import CustomThreadPool.CustomThreadPoolExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Runner {
    private static final Logger LOGGER = Logger.getLogger(Runner.class.getName());

    public static void main(String[] args) throws InterruptedException {
        // Инициализация пула потоков с заданными параметрами
        CustomThreadPoolExecutor executor = new CustomThreadPoolExecutor(
                2,          // corePoolSize
                4,          // maxPoolSize
                5,          // keepAliveTime в секундах
                TimeUnit.SECONDS, // единица измерения времени
                5,          // queueSize
                1           // minSpareThreads
        );

        // Создание имитационной задачи
        Runnable imitationTask = () -> {
            try {
                // Имитация выполнения задачи
                Thread.sleep(2000); // Задержка на 2 секунды
                LOGGER.info("Task started: " + Thread.currentThread().getName());
                Thread.sleep(3000); // Дополнительная задержка на 3 секунды
                LOGGER.info("Task finished: " + Thread.currentThread().getName());
            } catch (InterruptedException e) {
                LOGGER.severe("Task interrupted: " + e.getMessage());
            }
        };

        // Отправка нескольких задач в пул
        for (int i = 0; i < 5; i++) {
            executor.execute(imitationTask);
        }

        // Даем немного времени на выполнение задач
        Thread.sleep(15000);

        // Вызов shutdown(), чтобы завершить пул потоков
        executor.shutdown();

        // Проверка, что все задачи завершились
        LOGGER.info("Waiting for all tasks to finish...");
        executor.awaitTermination(30, TimeUnit.SECONDS);
        LOGGER.info("All tasks have been completed.");

        // Демонстрация отклонения задач, если пул закрыт
        Runnable rejectedTask = () -> LOGGER.info("Rejected task attempted to be executed.");
        executor.execute(rejectedTask); // Эта задача будет отклонена, так как пул закрыт

        LOGGER.info("Testing rejection of tasks after shutdown.");
    }

}
