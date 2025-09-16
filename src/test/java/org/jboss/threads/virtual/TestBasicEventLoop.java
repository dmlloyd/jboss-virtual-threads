package org.jboss.threads.virtual;

import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

/**
 *
 */
public final class TestBasicEventLoop {
    public TestBasicEventLoop() {}

    @Test
    void testBasicEventLoop() throws Throwable {
        Scheduler scheduler = new Scheduler();
        var el = new EventLoop() {
            Thread thread;

            public long handleEvents() {
                long time = EventLoop.blockingTime();
                if (time == - 1) {
                    LockSupport.park();
                } else if (time > 0) {
                    LockSupport.parkNanos(time);
                }
                return 0;
            }

            public void wakeup() {
                LockSupport.unpark(thread);
            }

            public void close() {
                // no op
            }
        };
        EventLoopDispatcher eld = new EventLoopDispatcher(scheduler, el);
        el.thread = eld.thread();
        eld.startThread(scheduler.builder().unstarted(() -> System.out.println("HELLO WORLD!")));
        eld.startThread(scheduler.builder().unstarted(() -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                // ignored
            }
            System.out.println("HELLO WORLD 3!");
        }));
        eld.startThread(scheduler.builder().unstarted(() -> {
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                // ignored
            }
            eld.initiateShutdown();
        }));
        new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
            eld.startThread(scheduler.builder().unstarted(() -> System.out.println("HELLO WORLD 2!")));
        }).start();
        eld.run();
        System.out.println("GOODBYE!");
    }
}
