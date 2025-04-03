package CustomThreadPool;

public interface CustomRejectedExecutionHandler {
    void rejectedExecution(Runnable r, CustomThreadPoolExecutor executor);
}
