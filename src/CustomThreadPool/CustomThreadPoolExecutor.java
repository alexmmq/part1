package CustomThreadPool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CustomThreadPoolExecutor implements CustomExecutor{
    public static final Logger logger = Logger.getLogger(CustomThreadPoolExecutor.class.getName());

    /**
     * Обозначаем переменные, необходимые для тонкой настройки нашей версии AbstractExecutorService
     * <ul>
     *     <li>corePoolSize — минимальное (базовое) количество потоков.
     *      <li>maxPoolSize — максимальное количество потоков.
     *      <li>keepAliveTime и timeUnit — время, в течение которого поток может простаивать до завершения и единицы измерения этого самого времени.
     *      <li>queueSize — ограничение на количество задач в очереди.
     *      <li>minSpareThreads — минимальное число «резервных» потоков, которые должны быть всегда доступны.
     * </ul>
     *
     * Если число свободных потоков падает ниже этого значения, пул должен создавать новые потоки, даже при невысокой нагрузке.
     */

    private volatile int corePoolSize;
    private volatile int maxPoolSize;

    // по умолчанию в ThreadPoolExecutor измерения ведутся в наносекундах, в нашем варианте также необходима
    // вариативность, сохраним возможность сохранения больших чисел - оставим long.
    private volatile long keepAliveTime;

    private volatile TimeUnit timeUnit;
    private volatile int queueSize;

    private volatile int minSpareThreads;

    /*

     */
    private final List<Worker> workerList;

    private final Queue<Runnable>[] workQueue;

    private final AtomicInteger active = new AtomicInteger();
    private volatile boolean isOff = false;
    private final CustomRejectHandler rejectedExecutionHandler;

    private final CustomThreadFactory myThreadFactory;




    public CustomThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
                                    TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        /*
        Обозначаем контейнеры для наших потоков, для каждого потока - отдельная очередь из задач
         */
        this.workerList = new ArrayList<>();
        this.workQueue = new LinkedList[corePoolSize];
        for(int i = 0; i < corePoolSize; i++){
            workQueue[i] = new LinkedBlockingQueue<>(queueSize);
        }

        /*
        Обозначаем собственную политику отказов и ThreadFactory.
         */
        this.rejectedExecutionHandler = new CustomRejectHandler();
        this.myThreadFactory = new CustomThreadFactory();
    }


    /**
     * Также реализуйте алгоритм балансировки задач — например, поступающие задачи могут распределяться
     * по принципу Round Robin между несколькими очередями, привязанными к разным рабочим потокам.
     * При желании можно усложнить задачу и реализовать распределение на основе
     * наименьшей загруженности очереди (Least Loaded), но это необязательно.
     * <p>Попробуем реализацию на основе наименьшей загруженности</p>
     */
    @Override
    public void execute(Runnable command) {
        synchronized (this) {
            //ищем самую незагруженную очередь
            int laziest = findLaziestQueue();

            //в случае удачи метод должен вернуть значение больше 0
            if(laziest > 0){

                //если метод offer не срабатывает - отправляем на обработку в CustomRejectedExecutionHandler
                if(!workQueue[laziest].offer(command)){
                    rejectedExecutionHandler.rejectedExecution(command, this);
                }
                logger.info("[" + logger.getName() + "] Task accepted into queue #" + laziest + ": " +
                        command.toString());

            } else{
                //очереди не могут принять больше задач, необходимо увеличение количества потоков
                //проверка достигнут ли максимальный размер пула
                if(active.get() < maxPoolSize) {
                    int index = workerList.size();
                    createWorker(index);
                    workQueue[index].offer(command);
                    logger.info("[" + logger.getName() + "] Task accepted into queue #" + laziest + ": " +
                            command.toString());
                } else{
                 rejectedExecutionHandler.rejectedExecution(command, this);
                }
            }
        }
    }

    private void createWorker(int index) {
    }

    private int findLaziestQueue() {
        int min = Integer.MAX_VALUE;
        int returnValue = -1;
        for(int i = 0; i < workQueue.length; i++) {
            int size = workQueue[i].size();
            if(size < min) {
                min = size;
                returnValue = i;
            }
        }
        return returnValue;
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void shutdownNow() {

    }

    private class Worker {
    }
}
