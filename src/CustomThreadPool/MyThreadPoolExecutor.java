package CustomThreadPool;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import jdk.internal.vm.*;

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
    private volatile int queueSize;
    
    private volatile int minSpareThreads;

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


    /**
     * Класс, в основе содержащий сет для хранения потоков и предоставляющий удобный инструментарий при инициализации
     * пула потоков
     */
    private final SharedThreadContainer container;

    /**
     * Трекер максимального размера пула. Доступ при получении mainLock
     */
    private int largestPoolSize;

    /**
     * Счетчик завершенных задач. Обновление счетчика происходит при завершении рабочих потоков. Доступ при получении mainLock
     */
    private long completedTaskCount;

    /**
     *Factory для создания новых потоков, в нашем случае необходимо переопределить реализацию ThreadFactory
     * с возможностью логирования процесса создания потоков.
     */
    private volatile ThreadFactory threadFactory;

    /**
     * Способ обработки отказа, необходимо определить что делать с новой поступившей задачей
     */
    private volatile RejectedExecutionHandler handler;

    /**
     * При значении false базовые потоки остаются живыми. При значении true базовые потоки будут завершены по истечении
     * времени, переданного через keepAliveTime
     */

    private volatile boolean allowCoreThreadTimeOut;

    /*
    Присваиваем значение по умолчанию для применяемой политики отказа
     */
    private static final RejectedExecutionHandler defaultHandler = null;

    /**
     * Необходимое снятие ограничений для вызовов shutdown() и shutdownNow() - в случае инициации которых происходит
     * прерывание потока
     */
    private static final RuntimePermission shutdownPerm =
            new RuntimePermission("modifyThread");

    /**
     * Class Worker mainly maintains interrupt control state for
     * threads running tasks, along with other minor bookkeeping.
     * This class opportunistically extends AbstractQueuedSynchronizer
     * to simplify acquiring and releasing a lock surrounding each
     * task execution.  This protects against interrupts tha0t are
     * intended to wake up a worker thread waiting for a task from
     * instead interrupting a task being run.  We implement a simple
     * non-reentrant mutual exclusion lock rather than use
     * ReentrantLock because we do not want worker tasks to be able to
     * reacquire the lock when they invoke pool control methods like
     * setCorePoolSize.  Additionally, to suppress interrupts until
     * the thread actually starts running tasks, we initialize lock
     * state to a negative value, and clear it upon start (in
     * runWorker).
     */
    private final class Worker
            extends AbstractQueuedSynchronizer
            implements Runnable
    {
        /**
         * This class will never be serialized, but we provide a
         * serialVersionUID to suppress a javac warning.
         */
        private static final long serialVersionUID = 6138294804551838833L;

        /** Thread this worker is running in.  Null if factory fails. */
        @SuppressWarnings("serial") // Unlikely to be serializable
        final Thread thread;
        /** Initial task to run.  Possibly null. */
        @SuppressWarnings("serial") // Not statically typed as Serializable
                Runnable firstTask;
        /** Per-thread task counter */
        volatile long completedTasks;

        // TODO: switch to AbstractQueuedLongSynchronizer and move
        // completedTasks into the lock word.

        /**
         * Creates with given first task and thread from ThreadFactory.
         * @param firstTask the first task (null if none)
         */
        Worker(Runnable firstTask) {
            setState(-1); // inhibit interrupts until runWorker
            this.firstTask = firstTask;
            this.thread = getThreadFactory().newThread(this);
        }

        /** Delegates main run loop to outer runWorker. */
        public void run() {
            runWorker(this);
        }

        // Lock methods
        //
        // The value 0 represents the unlocked state.
        // The value 1 represents the locked state.

        protected boolean isHeldExclusively() {
            return getState() != 0;
        }

        protected boolean tryAcquire(int unused) {
            if (compareAndSetState(0, 1)) {
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
            return false;
        }

        protected boolean tryRelease(int unused) {
            setExclusiveOwnerThread(null);
            setState(0);
            return true;
        }

        public void lock()        { acquire(1); }
        public boolean tryLock()  { return tryAcquire(1); }
        public void unlock()      { release(1); }
        public boolean isLocked() { return isHeldExclusively(); }

        void interruptIfStarted() {
            Thread t;
            if (getState() >= 0 && (t = thread) != null && !t.isInterrupted()) {
                try {
                    t.interrupt();
                } catch (SecurityException ignore) {
                }
            }
        }
    }

    private ThreadFactory getThreadFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return null;
            }
        };
    }

    private void runWorker(Worker worker) {
        
    }

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
        container = null;
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
