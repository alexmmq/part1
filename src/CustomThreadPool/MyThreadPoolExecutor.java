package CustomThreadPool;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MyThreadPoolExecutor implements CustomExecutor{

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
    
    private final int minSpareThreads;
    
    private final int queueSize;

    /**
     * Для сохранения состояния главного пула потоков используется atomic integer, включающий в себя две части:
     * <li>workerCount, представляющий количество действующих потоков</li>
     * <li>runState, представляющий состояние потоков - running, shutting down и т.п.</li>
     * workerCount - количество worker-потоков, которым дано разрешение стартовать, но не разрешена остановка.
     * Видимый пользователю размер пула определяется текущим размером сета workers.
     * Флаги контроля жизни потоков runState:
     * <ul>
     *     <li>RUNNING - принимает новые задания и обрабатывает задания в очереди;
     *     <li>SHUTDOWN - не принимает новые задания, но продолжает обработку заданий в очереди;
     *     <li>STOP - не принимает новые задания, не продолжает обработку заданий в очереди и останавливает задания в обработке;
     *     <li>TIDYING - все задания завершены, счетчик workerCount = 0, поток, переходящий в стадию TIDYING вызовет связанный метод terminated();
     *     <li>TERMINATED - метод terminated() был завершеню
     * </ul>
     */

    private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
    private static final int COUNT_BITS = Integer.SIZE - 3;
    private static final int COUNT_MASK = (1 << COUNT_BITS) - 1;

    // runState хранится в старших битах
    private static final int RUNNING    = -1 << COUNT_BITS;
    private static final int SHUTDOWN   =  0 << COUNT_BITS;
    private static final int STOP       =  1 << COUNT_BITS;
    private static final int TIDYING    =  2 << COUNT_BITS;
    private static final int TERMINATED =  3 << COUNT_BITS;

    // Процесс шифрования и считывания значения ctl
    private static int runStateOf(int c)     { return c & ~COUNT_MASK; }
    private static int workerCountOf(int c)  { return c & COUNT_MASK; }
    private static int ctlOf(int rs, int wc) { return rs | wc; }

    /*
    Использование данных значений не предполагает считывания значения ctl.
     */
    private static boolean runStateLessThan(int c, int s) {
        return c < s;
    }

    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    private static boolean isRunning(int c) {
        return c < SHUTDOWN;
    }

    /*
    Compare-And-Swap операция (увеличение) поля workerCount переменной ctl
     */
    private boolean compareAndIncrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect + 1);
    }

    /*
    Compare-And-Swap операция (уменьшение) поля workerCount переменной ctl
     */
    private boolean compareAndDecrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect - 1);
    }

    /*
    Вызов функции осуществляется только при внезапной остановке потока.
     */
    private void decrementWorkerCount() {
        ctl.addAndGet(-1);
    }

    /**
     * Главная очередь для хранения задач и передачи задач в рабочие потоки
     */
    private final BlockingQueue<Runnable> workQueue;

    /**
     * Главный Lock для доступа к сету рабочих потоков и относящихся к ним данных
     */
    private final ReentrantLock mainLock = new ReentrantLock();

    /**
     * Сет для хранения всех рабочих потоков, доступ возможен при получении mainLock
     */
    private final HashSet<Worker> workers = new HashSet<>();

    /**
     * Условие для поддержки флага awaitTermination
     */
    private final Condition termination = mainLock.newCondition();


    public MyThreadPoolExecutor(int corePoolSize,
                                int maxPoolSize,
                                long keepAliveTime, TimeUnit unit,
                                int minSpareThreads,
                                int queueSize, BlockingQueue<Runnable> workQueue) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.minSpareThreads = minSpareThreads;
        this.queueSize = queueSize;
        this.workQueue = workQueue;
    }

    @Override
    public void execute(Runnable command) {

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

    private final class Worker {
    }

    /**
     * Обработка отказов
     * При переполнении очереди и загруженности всех потоков необходимо реализовать механизм отказа
     * (например, с использованием RejectedExecutionHandler или собственного подхода), который определяет,
     *  как поступать с новой задачей (отклонять, выполнять в текущем потоке или применять иной способ обработки).
     *   Обязательно укажите, почему выбран именно этот подход и какие могут быть его недостатки.
     */


    /**
     * Распределение задач
     * Также реализуйте алгоритм балансировки задач — например, поступающие задачи могут распределяться по принципу
     * Round Robin между несколькими очередями, привязанными к разным рабочим потокам.
     *  При желании можно усложнить задачу и реализовать распределение на основе наименьшей загруженности
     *  очереди (Least Loaded), но это необязательно.
     */


    /**
     * Распределение задач
     * Также реализуйте алгоритм балансировки задач — например, поступающие задачи могут распределяться по принципу
     * Round Robin между несколькими очередями, привязанными к разным рабочим потокам.
     * <li><li/>При желании можно усложнить задачу и реализовать распределение на основе наименьшей загруженности
     *  очереди (Least Loaded), но это необязательно.
     */

    /**
     * Кастомизация компонентов
     * ThreadFactory — разработайте фабрику для создания потоков, которая будет присваивать потокам уникальные имена и
     * логировать события их создания и завершения.
     * Очереди задач — можно использовать несколько стандартных BlockingQueue (по одной на каждый поток) или создать
     *  собственную обертку с дополнительной логикой.
     * Worker (рабочий поток) должен:
     *<ul>
     *     <li>Обрабатывать задачи из закрепленной за ним очереди.
     *     <li>При отсутствии задач в течение времени, превышающего keepAliveTime, завершаться, если общее число
     *      потоков превышает corePoolSize.
     *      <li>Перед выполнением новой задачи проверять, что пул не находится в состоянии завершения (shutdown).
     *</ul>
     */

}
