package org.jboss.threads.virtual;

import static org.jboss.threads.virtual.Messages.msg;

import java.lang.invoke.ConstantBootstraps;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.PriorityQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

/**
 *
 */
public final class EventLoopDispatcher extends Dispatcher {
    /**
     * The number of nanoseconds to offset the wait time by, per priority point.
     */
    private static final long PRIORITY_BIAS = 50_000L;
    private static final VarHandle stateHandle = ConstantBootstraps.fieldVarHandle(
        MethodHandles.lookup(),
        "state",
        VarHandle.class,
        EventLoopDispatcher.class,
        int.class
    );

    private static final int ST_INITIAL = 0;
    private static final int ST_RUNNING = 1;
    // when exit is requested *and* there are no more threads assigned to this dispatcher
    private static final int ST_EXIT_REQ = 2;
    // exit is complete
    private static final int ST_EXITED = 3;

    private final EventLoop eventLoop;
    private int state;
    /**
     * The event loop virtual thread.
     */
    private volatile Thread elvt;
    /**
     * The carrier thread.
     */
    private volatile Thread ct;
    /**
     * Set to {@code true} while the event loop is entered, or {@code false} when it is not entered.
     */
    private volatile boolean entered;
    /**
     * Comparison nanos.
     * This is not representative of the current time; rather, it's a somewhat-recent (but arbitrary)
     * sample of {@code System.nanoTime()}.
     * Update before each queue operation and this queue can run forever, as long as no task waits for more than ~138 years.
     * Such tasks might unexpectedly have a different priority.
     * But it is not likely to matter at that point.
     */
    private long cmpNanos;
    /**
     * Current nanoseconds.
     * This <em>is</em> a snapshot of the current time, taken immediately before draining the delay queue.
     */
    private long currentNanos;
    /**
     * The amount of time that the event loop should block in its current iteration.
     */
    private long blockingTime;
    /**
     * The shared/slow task queue, which allows tasks to be enqueued from outside of this thread.
     */
    private final LinkedBlockingQueue<Thread> sq = new LinkedBlockingQueue<>();
    /**
     * The task queue.
     * The queue is ordered by the amount of time that each entry (thread) has been waiting to run.
     */
    private final PriorityQueue<Thread> q = new PriorityQueue<>(this::compare);
    /**
     * The bulk remover for transferring {@code sq} to {@code q}.
     */
    private final Predicate<Thread> bulkRemover = q::add;
    /**
     * The delay queue for timed sleep (patched JDKs only).
     */
    private final DelayQueue<DelayedTask<?>> dq = new DelayQueue<>();

    EventLoopDispatcher(final Scheduler scheduler, final EventLoop eventLoop) {
        super(scheduler);
        this.eventLoop = eventLoop;
        elvt = VirtualThreads.newVirtualThread(this, () -> {
            long time;
            try {
                for (;;) {
                    VirtualThreads.clearUnpark();
                    VirtualThreads.pin();
                    entered = true;
                    try {
                        time = eventLoop.handleEvents();
                    } finally {
                        entered = false;
                        VirtualThreads.unpin();
                    }
                    if (state == ST_EXIT_REQ) {
                        return;
                    }
                    VirtualThreads.yieldNanos(time);
                }
            } catch (Throwable t) {
                // todo: log a warning
            } finally {
                gasState(ST_EXITED);
                try {
                    eventLoop.close();
                } catch (Throwable t) {
                    // todo: log a warning
                }
            }
        });
        elvt.start();
    }

    /**
     * {@return the thread running this event loop}
     */
    public Thread thread() {
        return elvt;
    }

    /**
     * Enter an event loop from this thread.
     * The event loop will run until it is requested to exit.
     *
     * @throws IllegalStateException if the event loop is already entered
     */
    public void run() {
        int witness = caxState(ST_INITIAL, ST_RUNNING);
        if (witness != ST_INITIAL) {
            throw msg.eventLoopEntered();
        }
        ct = Thread.currentThread();
        // initialize the wait-time comparison basis
        cmpNanos = System.nanoTime();
        for (;;) {
            // drain shared queue, hopefully somewhat efficiently
            sq.removeIf(bulkRemover);
            long waitTime = -1L;
            if (!dq.isEmpty()) {
                // process the delay queue
                currentNanos = System.nanoTime();
                DelayedTask<?> dt = dq.poll();
                while (dt != null) {
                    // this will indirectly cause entries to be added to `q`
                    dt.run();
                    dt = dq.poll();
                }
                // DelayQueue will let you "peek" at future items when `poll` returns `null`
                dt = dq.peek();
                if (dt != null) {
                    // set the max wait time to the amount of time before the next scheduled task
                    waitTime = Math.max(1L, dt.deadline - currentNanos);
                }
            }
            Thread removed = q.poll();
            if (removed == null) {
                if (state == ST_EXITED) {
                    return;
                }
                // not possible (the event loop cannot yield)
                LockSupport.park();
            } else {
                ThreadInfo info = VirtualThreads.infoOf(removed);
                if (removed == elvt) {
                    // configure the wait time
                    blockingTime = q.isEmpty() && state < ST_EXIT_REQ ? waitTime : 0;
                }
                // update for next q operation without hitting nanoTime over and over
                cmpNanos = info.waitingSinceTime;
                info.runnable().run();
            }
        }

    }

    @Override
    boolean dispatchNow(final Thread thread) {
        if (Thread.currentThread() == this.elvt) {
            // add to fast queue
            q.add(thread);
        } else {
            sq.add(thread);
            if (entered) {
                eventLoop.wakeup();
            }
        }
        return true;
    }

    @Override
    DelayedTask<Object> dispatchLater(final Thread thread, final long nanos) {
        // it is expected that this will only be called locally (from the carrier of the sleeper)
        DelayedTask<Object> dt = new DelayedTask<>(() -> LockSupport.unpark(thread), System.nanoTime() + nanos);
        dq.add(dt);
        return dt;
    }

    void initiateShutdown() {
        state = ST_EXIT_REQ;
        if (entered) {
            eventLoop.wakeup();
        }
    }

    // === private ===

    private int caxState(int expect, int update) {
        return (int) stateHandle.compareAndExchange(this, expect, update);
    }

    private int gasState(int newValue) {
        return (int) stateHandle.getAndSet(this, newValue);
    }

    long blockingTime() {
        return blockingTime;
    }

    /**
     * Compare the wait times of two tasks.
     * The task that has waited for the longest time is considered earlier than the task that has a shorter wait time.
     *
     * @param t1 the first thread scheduler (must not be {@code null})
     * @param t2 the second thread scheduler (must not be {@code null})
     * @return the comparison result
     */
    private int compare(Thread t1, Thread t2) {
        ThreadInfo o1 = VirtualThreads.infoOf(t1);
        ThreadInfo o2 = VirtualThreads.infoOf(t2);
        long cmpNanos = this.cmpNanos;

        // with priority, we have a potentially greater-than-64-bit number
        long w1 = o1.waitingSince(cmpNanos);
        int s1 = Thread.NORM_PRIORITY - o1.priority();
        w1 += s1 * PRIORITY_BIAS;

        long w2 = o2.waitingSince(cmpNanos);
        int s2 = Thread.NORM_PRIORITY - o2.priority();
        w2 += s2 * PRIORITY_BIAS;

        return Long.compare(w2, w1);
    }

    final class DelayedTask<V> implements ScheduledFuture<V>, Runnable {
        private final Runnable task;
        private final long deadline;

        private DelayedTask(final Runnable task, final long deadline) {
            this.task = task;
            this.deadline = deadline;
        }

        public long getDelay(final TimeUnit unit) {
            long currentNanos = EventLoopDispatcher.this.currentNanos;
            return unit.convert(deadline - currentNanos, TimeUnit.NANOSECONDS);
        }

        public int compareTo(final Delayed o) {
            if (o instanceof DelayedTask<?> dt) {
                long currentNanos = EventLoopDispatcher.this.currentNanos;
                return Long.compare(deadline - currentNanos, dt.deadline - currentNanos);
            } else {
                throw new IllegalStateException();
            }
        }

        public boolean cancel(final boolean mayInterruptIfRunning) {
            return false;
        }

        public boolean isCancelled() {
            // unsupported
            return false;
        }

        public boolean isDone() {
            // unsupported
            return false;
        }

        public V get() {
            throw new UnsupportedOperationException();
        }

        public V get(final long timeout, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        public void run() {
            task.run();
        }
    }
}
