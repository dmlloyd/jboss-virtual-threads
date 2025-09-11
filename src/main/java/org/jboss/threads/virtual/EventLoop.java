package org.jboss.threads.virtual;

import static org.jboss.threads.virtual.VirtualThreads.dispatcherOf;

import java.util.concurrent.locks.LockSupport;

/**
 * The event loop for an {@linkplain EventLoopDispatcher event loop thread scheduler}.
 * An event loop is a special kind of task that is allowed to pin and block the carrier thread
 * under the specific circumstance of waiting for an event to be runnable.
 */
public interface EventLoop {
    /**
     * Run one iteration of the event loop.
     * <b>This method is run while the thread is pinned, so {@code yield()} (and equivalents) should not be used.</b>
     * The event loop handler method should perform the following steps:
     * <ul>
     *     <li>Unpark any ready threads via {@link LockSupport#unpark(Thread)}</li>
     *     <li>If no threads are ready, call {@link #blockingTime()} to discover how long to block (if at all)
     *     and block (pinned) for up to that amount of time to unpark more threads</li>
     *     <li>Yield by returning the maximum number of nanoseconds to delay before rescheduling the event loop</li>
     * </ul>
     * If {@link #blockingTime()} returns {@code -1}, then wait until either a thread is ready
     * or until {@link #wakeup()} has been called, whichever happens first.
     * <p>
     * Care must be taken not to deadlock with any ready threads, since they cannot resume while this method runs.
     * In particular, this method must <b>avoid</b> loading or initializing new classes.
     * Any class loading or initialization should be done in the constructor or static initializer of the event loop
     * implementation class.
     *
     * @return the maximum number of nanoseconds to allow other ready threads to barge the ready queue (may be negative)
     */
    long handleEvents();

    /**
     * Forcibly awaken the event loop.
     * If {@link #handleEvents} is blocked, then this method will cause it to no longer be blocked.
     * This method may be called from any thread.
     */
    void wakeup();

    /**
     * Release any resources allocated by this event loop.
     * This method will only be called when {@link #handleEvents()} is not currently executing on any thread.
     */
    void close();

    /**
     * {@return the maximum blocking time (in nanoseconds) for the current event loop}
     * The blocking time is {@code 0} when no blocking should be done,
     * and is {@code -1} when the event loop should exit.
     * @throws IllegalStateException if called from outside an event loop
     */
    static long blockingTime() {
        Thread thr = Thread.currentThread();
        if (thr.isVirtual() && dispatcherOf(thr) instanceof EventLoopDispatcher elts) {
            return elts.blockingTime();
        } else {
            throw new IllegalStateException("Called from outside event loop");
        }
    }
}
