package org.jboss.threads.virtual;

import java.lang.invoke.ConstantBootstraps;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * The attachment for virtual threads which tracks its scheduling state.
 */
final class ThreadInfo {
    private static final VarHandle delayHandle = ConstantBootstraps.fieldVarHandle(
        MethodHandles.lookup(),
        "delay",
        VarHandle.class,
        ThreadInfo.class,
        long.class
    );

    /**
     * The system nanos time when this task started waiting.
     * Accessed only by carrier threads.
     */
    long waitingSinceTime;
    /**
     * The current scheduler.
     */
    Dispatcher dispatcher;
    /**
     * The thread priority.
     */
    int priority = Thread.NORM_PRIORITY;
    /**
     * The nanosecond delay time for scheduling.
     * Accessed by carrier threads and virtual threads.
     */
    @SuppressWarnings("unused") // delayHandle
    long delay;
    /**
     * The runnable task to resume the virtual thread.
     */
    final Runnable runnable = this::executeContinuation;
    final Runnable runContinuation;
    volatile boolean yielded;

    ThreadInfo(final Dispatcher dispatcher, final Runnable runContinuation) {
        this.dispatcher = dispatcher;
        this.runContinuation = runContinuation;
    }

    /**
     * {@return the number of nanoseconds that this thread has been waiting for}
     * The higher the waiting-since time, the higher priority a thread will have.
     * This value may be negative if the wait time includes a delay.
     *
     * @param current the current time
     */
    long waitingSince(long current) {
        long delay = (long) delayHandle.getOpaque(this);
        // delay is always 0 or positive, so result may be negative
        return Math.max(0, current - waitingSinceTime) - delay;
    }

    Runnable runnable() {
        return runnable;
    }

    void executeContinuation() {
        try {
            runContinuation.run();
        } finally {
            if (! VirtualThreads.threadOfExecuteCommand(runContinuation).isAlive()) {
                dispatcher.unregister();
                dispatcher = null;
            } else {
                waitingSinceTime = System.nanoTime();
            }
            yielded = true;
        }
    }

    boolean yielded() {
        if (yielded) {
            yielded = false;
            return true;
        }
        return false;
    }

    int priority() {
        return priority;
    }
}
