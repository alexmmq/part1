package CustomThreadPool;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class CustomRejectHandler implements CustomRejectedExecutionHandler {
    //взята реализация из стандартного RejectedExecutionHandler, соответствует AbortPolicy()
    @Override
    public void rejectedExecution(Runnable r, CustomThreadPoolExecutor executor) {
        throw new RejectedExecutionException("Task " + r.toString() +
                " rejected from " +
                executor.toString());
    }
}
