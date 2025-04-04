package CustomThreadPool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
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
    Используем в качестве примера оригинальный ThreadPoolExecutor, где также используется ReentrantLock
     */

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workersCondition = lock.newCondition();
    private final List<Worker> workerList;

    private final List<LinkedBlockingQueue<Runnable>> workQueue;

    private final AtomicInteger active = new AtomicInteger();
    private volatile boolean toShutDown = false;
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
        this.workQueue = new LinkedList<LinkedBlockingQueue<Runnable>>();
        for(int i = 0; i < corePoolSize; i++){
            workQueue.add(new LinkedBlockingQueue<>(queueSize));
        }

        /*
        Обозначаем собственную политику отказов и ThreadFactory.
         */
        this.rejectedExecutionHandler = new CustomRejectHandler();
        this.myThreadFactory = new CustomThreadFactory("ThreadFactory");
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
        lock.lock();
        try {
            //ищем самую незагруженную очередь
            int laziest = findLaziestQueue();

            //в случае удачи метод должен вернуть значение больше 0
            if(laziest > 0) {
                if (!workQueue.get(laziest).offer(command)) {
                    rejectedExecutionHandler.rejectedExecution(command, this);
                    return;
                }
                logger.info(logger.getName() + " Task accepted into queue #" + laziest + ": " +
                        command.toString());
            } else {
                //очереди не могут принять больше задач, необходимо увеличение количества потоков
                //проверка достигнут ли максимальный размер пула
                if(active.get() < maxPoolSize) {
                    int index = workerList.size();
                    createWorker(index);
                    workQueue.get(index).offer(command);
                    logger.info(logger.getName() + " Task accepted into queue #" + laziest + ": " +
                            command.toString());
                } else{
                 rejectedExecutionHandler.rejectedExecution(command, this);
                }
            }
        }
        finally{
            lock.unlock();
        }
    }

    private void createWorker(int index) {
        //TODO: реализовать создание потоков через фабрику
        Worker worker = new Worker(this, index);
        workerList.add(worker);
        worker.run();
        logger.info(myThreadFactory.getClass().getName() + " creating new thread: " + worker.getName());
    }


    private int findLaziestQueue() {
        int min = Integer.MAX_VALUE;
        int returnValue = -1;
        for(int i = 0; i < workQueue.size(); i++) {
            int size = workQueue.get(i).size();
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
        lock.lock();
        try {
            toShutDown = true;
            for(Worker worker: workerList) {
                worker.thread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdownNow() {
        synchronized (this) {
            toShutDown = true;
        }
    }

    private class Worker implements Runnable {
        private final CustomThreadPoolExecutor executor;
        private final int queueIndex;
        private Runnable currentTask;

        private Thread thread;

        public Worker(CustomThreadPoolExecutor executor, int queueIndex) {
            this.executor = executor;
            this.queueIndex = queueIndex;
            this.thread = myThreadFactory.newThread(this);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    currentTask = workQueue.get(queueIndex).take();
                    logger.info(Thread.currentThread().getName() + " executes task: " + currentTask.toString());
                    currentTask.run();

                    // Проверяем нужно ли завершить поток
                    if (executor.active.get() > minSpareThreads) {
                        return;
                        // Завершение работы потока
                    }
                } catch (InterruptedException e) {
                    logger.info(Thread.currentThread().getName() + " has been interrupted");
                    return;
                    // Прерывание работы потока
                } finally {
                    // Уменьшаем счетчик активных потоков
                    active.decrementAndGet();
                }
            }
        }

        public String getName() {
            return null;
        }
    }
}
