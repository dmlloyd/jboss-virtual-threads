package org.jboss.threads.virtual;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.jboss.threads.EnhancedQueueExecutor;

/**
 * A thread scheduler which runs virtual threads using a backing thread pool.
 * This pool may be assigned to specific CPU cores or may have a particular scheduling behavior with the OS.
 */
public final class PoolDispatcher extends Dispatcher {
    private final EnhancedQueueExecutor executor;

    PoolDispatcher(final Scheduler scheduler, final EnhancedQueueExecutor executor) {
        super(scheduler);
        this.executor = executor;
    }

    boolean dispatchNow(final Thread thread) {
        try {
            executor.execute(VirtualThreads.infoOf(thread).runnable());
            return true;
        } catch (RejectedExecutionException ignored) {
            return false;
        }
    }

    ScheduledFuture<?> dispatchLater(final Thread thread, final long nanos) {
        try {
            return executor.schedule(() -> LockSupport.unpark(thread), nanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException ignored) {
            return null;
        }
    }

    void initiateShutdown() {
        // let it exit in the background
        executor.shutdown();
    }

    public static Builder builder(Scheduler scheduler) {
        return new Builder(scheduler);
    }

    public static final class Builder {
        private final Scheduler scheduler;
        private final EnhancedQueueExecutor.Builder eb;

        Builder(final Scheduler scheduler) {
            this.scheduler = scheduler;
            this.eb = new EnhancedQueueExecutor.Builder();
            eb.setQueueLimited(false);
            eb.setRegisterMBean(false);
        }

        public PoolDispatcher build() {
            return new PoolDispatcher(scheduler, eb.build());
        }
    }
}
