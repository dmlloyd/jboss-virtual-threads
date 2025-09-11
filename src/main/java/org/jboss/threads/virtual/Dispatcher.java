package org.jboss.threads.virtual;

import static java.lang.Thread.currentThread;

import java.lang.invoke.ConstantBootstraps;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ScheduledFuture;

/**
 * A scheduling policy for managed virtual threads.
 */
public abstract class Dispatcher {
    private static final VarHandle stateHandle = ConstantBootstraps.fieldVarHandle(MethodHandles.lookup(), "state", VarHandle.class, Dispatcher.class, long.class);

    private static final long ST_TERMINATED = 1L << 63;

    private final Scheduler scheduler;

    volatile long state;

    Dispatcher(final Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Request that the current thread should run on the given thread dispatcher.
     * If the new dispatcher is different from the current dispatcher, the
     * thread will be immediately rescheduled as if {@link Thread#yield()}
     * had been called.
     *
     * @param dispatcher the dispatcher to run on (must not be {@code null})
     * @return {@code true} if the thread was rescheduled, or {@code false} if the thread is not managed
     *      or the dispatcher is terminated
     */
    public static boolean runOn(Dispatcher dispatcher) {
        Thread thr = currentThread();
        if (thr.isVirtual()) {
            Dispatcher oldVal = VirtualThreads.dispatcherOf(thr);
            if (oldVal != null) {
                if (oldVal != dispatcher) {
                    if (dispatcher.tryRegister()) {
                        oldVal.unregister();
                        // our current dispatcher might have been destroyed, so be very careful what we do next!
                        VirtualThreads.setDispatcher(thr, dispatcher);
                        Thread.yield();
                    } else {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Start a thread into this dispatcher.
     *
     * @param thread the thread to start (must not be {@code null})
     * @throws IllegalThreadStateException if the thread is already started
     * @throws IllegalStateException if this dispatcher is already terminated
     * @throws IllegalArgumentException if the thread is not virtual or is not managed by our scheduler
     */
    public void startThread(Thread thread) {
        if (thread.isVirtual()) {
            if (scheduler == VirtualThreads.schedulerOf(thread)) {
                if (tryRegister()) {
                    if (! thread.isAlive()) {
                        VirtualThreads.setDispatcher(thread, this);
                        thread.start();
                    } else {
                        throw new IllegalThreadStateException("Thread already started");
                    }
                } else {
                    throw new IllegalStateException("Dispatcher is terminated");
                }
            } else {
                throw new IllegalArgumentException("Thread is not managed by this scheduler");
            }
        } else {
            throw new IllegalArgumentException("Thread is not virtual");
        }
    }

    void unregister() {
        // decrement the counter, and initiate termination if needed
        long prevVal = (long) stateHandle.getAndAdd(this, -1L);
        if (prevVal == ST_TERMINATED + 1L) {
            // it was indeed the last
            initiateShutdown();
        }
    }

    boolean tryRegister() {
        // increment the counter if it is not zero && not terminated
        long prevVal = (long) stateHandle.getOpaque(this);
        for (;;) {
            if (prevVal == ST_TERMINATED) {
                return false;
            }
            long witness = (long) stateHandle.compareAndExchange(this, prevVal, prevVal + 1);
            if (witness == prevVal) {
                return true;
            }
            prevVal = witness;
        }
    }

    /**
     * Dispatch a thread immediately on this dispatcher.
     *
     * @param thread the thread to dispatch (must not be {@code null})
     * @return {@code true} if the thread was dispatched, or {@code false} if the dispatcher has been terminated
     */
    abstract boolean dispatchNow(Thread thread);

    /**
     * Dispatch a thread later on this dispatcher.
     * Terminating the dispatcher may leave incomplete tasks orphaned, or may cause them to execute immediately.
     *
     * @param thread the thread to dispatch (must not be {@code null})
     * @param nanos the number of nanoseconds to wait (> 0)
     * @return the scheduled future, or {@code null} if the dispatcher has been terminated
     */
    abstract ScheduledFuture<?> dispatchLater(Thread thread, long nanos);

    /**
     * {@return the scheduler of the current thread, or {@code null} if the current thread is not associated with a scheduler}
     */
    public static Dispatcher current() {
        Thread thr = currentThread();
        return thr.isVirtual() ? VirtualThreads.dispatcherOf(thr) : null;
    }

    /**
     * Indicate that there are no more threads assigned to this dispatcher, so we can start
     * shutting it down.
     */
    abstract void initiateShutdown();

    boolean isTerminated() {
        return false;
    }

    Scheduler scheduler() {
        return scheduler;
    }
}

