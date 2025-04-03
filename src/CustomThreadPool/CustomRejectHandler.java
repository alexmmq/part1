package CustomThreadPool;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class CustomRejectHandler implements CustomRejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, CustomThreadPoolExecutor executor) {

    }
}
