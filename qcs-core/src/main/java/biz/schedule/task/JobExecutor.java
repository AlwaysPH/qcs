package biz.schedule.task;

import com.gwi.qcs.common.utils.ThreadPoolUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 定时线程执行器
 *
 * @param <T>
 * @author x
 */
public class JobExecutor<T extends Callable> {

    private final String threadName;

    private final Integer threadCounts;

    private final ExecutorService executor;

    public JobExecutor(String threadName, Integer threadCounts) {
        this.threadName = threadName;
        this.threadCounts = threadCounts;
        this.executor = ThreadPoolUtils.createCachedThreadPool(threadName, threadCounts);
    }

    public Future<T> startRun(Callable<T> t) {
        return executor.submit(t);
    }

    public void shutdown() {
        this.executor.shutdown();
    }

    public ExecutorService getExecutor() {
        return this.executor;
    }

}
