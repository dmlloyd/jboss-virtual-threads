package org.jboss.threads.virtual;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simple scheduler for the JVM to consume.
 */
public final class Scheduler implements ScheduledExecutorService, Thread.VirtualThreadScheduler {
    private volatile Dispatcher defaultDispatcher;
    private final Thread.Builder.OfVirtual builder;

    /**
     * Construct a new instance.
     */
    public Scheduler() {
        builder = VirtualThreads.threadBuilder(this);
    }

    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        Thread vthread = VirtualThreads.threadOfScheduleCommand(command);
        return effectiveDispatcherOf(vthread).dispatchLater(vthread, unit.toNanos(delay));
    }

    public void execute(final Runnable command) {
        execute(VirtualThreads.threadOfExecuteCommand(command), command);
    }

    public void execute(Thread vthread, Runnable command) {
        effectiveDispatcherOf(vthread).dispatchNow(vthread);
    }

    private Dispatcher effectiveDispatcherOf(final Thread thread) {
        assert thread.isVirtual();
        Dispatcher dispatcher = VirtualThreads.dispatcherOf(thread);
        if (dispatcher == null && Thread.currentThread().isVirtual()) {
            dispatcher = VirtualThreads.dispatcherOf(Thread.currentThread());
            if (dispatcher != null) {
                if (dispatcher.tryRegister()) {
                    // thread has found its new home
                    VirtualThreads.setDispatcher(thread, dispatcher);
                } else {
                    // try something else
                    dispatcher = null;
                }
            }
        }
        if (dispatcher == null) {
            dispatcher = defaultDispatcher;
            if (dispatcher != null) {
                if (dispatcher.tryRegister()) {
                    // thread has found its new home
                    VirtualThreads.setDispatcher(thread, dispatcher);
                } else {
                    // try something else
                    dispatcher = null;
                }
            }
        }
        if (dispatcher == null) {
            // initially, no threads have dispatchers, so we will reach here directly if this is not set up
            throw new RejectedExecutionException("Default dispatcher has not been initialized");
        }
        return dispatcher;
    }

    public Dispatcher defaultDispatcher() {
        return defaultDispatcher;
    }

    public void setDefaultDispatcher(final Dispatcher defaultDispatcher) {
        this.defaultDispatcher = defaultDispatcher;
    }

    Thread.Builder.OfVirtual builder() {
        return builder;
    }

    // unimplemented methods

    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
    }

    public boolean isShutdown() {
        throw new UnsupportedOperationException();
    }

    public boolean isTerminated() {
        throw new UnsupportedOperationException();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    public <T> Future<T> submit(Runnable task, T result) {
        throw new UnsupportedOperationException();
    }

    public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException();
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
