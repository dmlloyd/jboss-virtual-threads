package org.jboss.threads.virtual;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.LockSupport;

import io.smallrye.common.constraint.Assert;

/**
 * Virtual thread utilities.
 */
@SuppressWarnings("unused") // some stuff is reserved for later
public final class VirtualThreads {
    private static final MethodHandle getAttachment;
    private static final MethodHandle getCurrentCarrier;
    private static final MethodHandle caxAttachment;
    private static final MethodHandle getRunContinuation;
    private static final MethodHandle getThreadFromContinuation;
    private static final MethodHandle getThreadFromUnpark;
    private static final MethodHandle getScheduler;
    private static final MethodHandle pinContinuation;
    private static final MethodHandle unpinContinuation;

    private VirtualThreads() {}

    /**
     * Run the current thread on the given thread dispatcher.
     * If the thread is already running on the given dispatcher, this method is a no-op.
     *
     * @param dispatcher the thread dispatcher to run on (must not be {@code null})
     * @throws IllegalArgumentException if the given dispatcher has been shut down
     */
    public static void runOn(Dispatcher dispatcher) {
        Assert.checkNotNullParam("dispatcher", dispatcher);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            if (schedulerOf(thread) == dispatcher.scheduler()) {
                Dispatcher old = dispatcherOf(thread);
                if (old == dispatcher) {
                    return;
                }
            }
        }
        infoOf(thread).dispatcher = dispatcher;
    }

    public static void parkAndRunOn(Dispatcher dispatcher) {
        Assert.checkNotNullParam("dispatcher", dispatcher);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            if (schedulerOf(thread) == dispatcher.scheduler()) {
                Dispatcher old = dispatcherOf(thread);
                if (old == dispatcher) {
                    LockSupport.park();
                    return;
                }
                if (dispatcher.tryRegister()) {
                    // old cannot be null here
                    assert old != null;
                    old.unregister();
                    LockSupport.park();
                    return;
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

    public static void parkAndRunOn(Dispatcher dispatcher, Object blocker) {
        Assert.checkNotNullParam("dispatcher", dispatcher);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            if (schedulerOf(thread) == dispatcher.scheduler()) {
                Dispatcher old = dispatcherOf(thread);
                if (old == dispatcher) {
                    LockSupport.park(blocker);
                    return;
                }
                if (dispatcher.tryRegister()) {
                    // old cannot be null here
                    assert old != null;
                    old.unregister();
                    LockSupport.park(blocker);
                    return;
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

    public static void parkNanosAndRunOn(Dispatcher dispatcher, long nanos) {
        Assert.checkNotNullParam("dispatcher", dispatcher);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            if (schedulerOf(thread) == dispatcher.scheduler()) {
                Dispatcher old = dispatcherOf(thread);
                if (old == dispatcher) {
                    LockSupport.parkNanos(nanos);
                    return;
                }
                if (dispatcher.tryRegister()) {
                    // old cannot be null here
                    assert old != null;
                    old.unregister();
                    LockSupport.parkNanos(nanos);
                    return;
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

    public static void parkNanosAndRunOn(Dispatcher dispatcher, Object blocker, long nanos) {
        Assert.checkNotNullParam("dispatcher", dispatcher);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            if (schedulerOf(thread) == dispatcher.scheduler()) {
                Dispatcher old = dispatcherOf(thread);
                if (old == dispatcher) {
                    LockSupport.parkNanos(blocker, nanos);
                    return;
                }
                if (dispatcher.tryRegister()) {
                    // old cannot be null here
                    assert old != null;
                    old.unregister();
                    LockSupport.parkNanos(blocker, nanos);
                    return;
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

    /**
     * Yield execution to any task is already waiting or will start waiting within the next {@code nanos} nanoseconds.
     * If no tasks remain within the given criteria, the current thread will resume.
     *
     * @param nanos the number of nanoseconds to attempt to yield for
     */
    public static void yieldNanos(long nanos) {
        infoOf(Thread.currentThread()).delay = nanos;
        Thread.yield();
    }

    /**
     * {@return the priority of the current thread or virtual thread}
     */
    public static int currentPriority() {
        Thread thread = Thread.currentThread();
        if (thread.isVirtual() && schedulerOf(thread) instanceof Scheduler) {
            return infoOf(thread).priority;
        } else {
            return thread.getPriority();
        }
    }

    /**
     * Change the priority of the current virtual thread, if possible.
     *
     * @param newPriority the new virtual thread priority
     */
    public static void changePriority(int newPriority) {
        newPriority = Math.min(Math.max(newPriority, Thread.MIN_PRIORITY), Thread.MAX_PRIORITY);
        Thread thread = Thread.currentThread();
        if (thread.isVirtual()) {
            ThreadInfo info = infoOf(thread);
            int old = info.priority;
            if (newPriority != old) {
                // apply new priority
                info.priority = newPriority;
                Thread.yield();
            }
        } else {
            thread.setPriority(newPriority);
        }
    }

    /**
     * Clear the {@linkplain LockSupport#unpark(Thread) unpark permit} for the current thread.
     */
    public static void clearUnpark() {
        // change unpark permit from (0 or 1) to (1)
        LockSupport.unpark(Thread.currentThread());
        // change unpark permit from (1) to (0)
        LockSupport.park();
    }

    // === non-public ===

    static Thread threadOfExecuteCommand(Runnable runContinuation) {
        try {
            return (Thread) getThreadFromContinuation.invokeExact(runContinuation);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static Thread threadOfScheduleCommand(final Runnable command) {
        try {
            return (Thread) getThreadFromUnpark.invokeExact(command);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static Dispatcher dispatcherOf(Thread thread) {
        return thread.isVirtual() ? infoOf(thread).dispatcher : null;
    }

    static Runnable continuationOf(Thread thread) {
        // VirtualThread.runContinuation
        try {
            return (Runnable) getRunContinuation.invokeExact(thread);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static ThreadInfo infoOf(Thread thread) {
        if (! thread.isVirtual()) {
            throw new IllegalArgumentException("Thread is not a virtual thread");
        }
        try {
            ThreadInfo info = (ThreadInfo) getAttachment.invokeExact(thread);
            if (info == null) {
                info = new ThreadInfo(Dispatcher.current(), continuationOf(thread));
                ThreadInfo appearing = (ThreadInfo) caxAttachment.invoke(thread, null, info);
                if (appearing != null) {
                    return appearing;
                }
            }
            return info;
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static void setDispatcher(Thread thread, Dispatcher dispatcher) {
        infoOf(thread).dispatcher = dispatcher;
    }

    static Thread newVirtualThread(Dispatcher initial, Runnable task) {
        Thread thread = initial.scheduler().builder().unstarted(task);
        if (initial.tryRegister()) {
            setDispatcher(thread, initial);
            return thread;
        } else {
            throw new IllegalStateException("Dispatcher is terminated");
        }
    }

    static Thread.Builder.OfVirtual threadBuilder(Scheduler scheduler) {
        return Thread.ofVirtual().scheduler(scheduler);
    }

    static Executor schedulerOf(final Thread thread) {
        try {
            return (Executor) getScheduler.invokeExact(thread);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static void pin() {
        try {
            pinContinuation.invokeExact();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static void unpin() {
        try {
            unpinContinuation.invokeExact();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    static {
        MethodHandle ct;
        MethodHandle vtf;
        MethodHandle sg;
        MethodHandle cg;
        MethodHandle ga;
        MethodHandle caxa;
        MethodHandle gt;
        MethodHandle pin;
        MethodHandle unpin;
        try {
            MethodHandles.Lookup thr = MethodHandles.privateLookupIn(Thread.class, MethodHandles.lookup());
            ct = thr.findStatic(Thread.class, "currentCarrierThread", MethodType.methodType(Thread.class));
            Class<?> vtc = thr.findClass("java.lang.VirtualThread");
            MethodHandles.Lookup vthr = MethodHandles.privateLookupIn(vtc, MethodHandles.lookup());
            try {
                sg = vthr.findGetter(vtc, "scheduler", ScheduledExecutorService.class);
            } catch (NoSuchFieldException | NoSuchFieldError ignored) {
                // unpatched JDK
                sg = vthr.findGetter(vtc, "scheduler", Thread.VirtualThreadScheduler.class);
            }
            sg = sg.asType(MethodType.methodType(Executor.class, Thread.class));
            Class<?> rcc = thr.findClass("java.lang.VirtualThread$RunContinuation");
            cg = vthr.findGetter(vtc, "runContinuation", rcc).asType(MethodType.methodType(Runnable.class, Thread.class));
            ga = vthr.findGetter(vtc, "attachment", Object.class).asType(MethodType.methodType(ThreadInfo.class, Thread.class));
            caxa = vthr.findVarHandle(vtc, "attachment", Object.class).withInvokeExactBehavior().toMethodHandle(VarHandle.AccessMode.COMPARE_AND_EXCHANGE).asType(MethodType.methodType(Object.class, Thread.class, ThreadInfo.class, ThreadInfo.class));
            MethodHandles.Lookup rc = MethodHandles.privateLookupIn(rcc, MethodHandles.lookup());
            gt = rc.findVirtual(rcc, "virtualThread", MethodType.methodType(vtc)).asType(MethodType.methodType(Thread.class, Runnable.class));
            Class<?> contc = Class.forName("jdk.internal.vm.Continuation", false, null);
            MethodHandles.Lookup cont = MethodHandles.privateLookupIn(contc, MethodHandles.lookup());
            pin = cont.findStatic(contc, "pin", MethodType.methodType(void.class));
            unpin = cont.findStatic(contc, "unpin", MethodType.methodType(void.class));
        } catch (Throwable e) {
            // no good
            throw new InternalError("Cannot initialize virtual threads", e);
        }
        getCurrentCarrier = ct;
        getScheduler = sg;
        getRunContinuation = cg;
        getThreadFromContinuation = gt;
        getAttachment = ga;
        caxAttachment = caxa;
        getThreadFromUnpark = null; // todo
        pinContinuation = pin;
        unpinContinuation = unpin;
    }
}
