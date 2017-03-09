/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.concurrent;


import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscArrayQueue;
import sun.misc.Contended;

public class MonitoredEpollEventLoopGroup extends MultithreadEventLoopGroup
{

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MonitoredEpollEventLoopGroup.class);

    @Contended
    private SingleCoreEventLoop[] initLoops;

    @Contended
    private Thread[] initThreads;

    @Contended
    private final SingleCoreEventLoop[] eventLoops;

    @Contended
    private final Thread[] runningThreads;

    private final Thread monitorThread;

    private volatile boolean shutdown;

    public MonitoredEpollEventLoopGroup(int nThreads)
    {
        this(nThreads, new NettyRxScheduler.NettyRxThreadFactory(MonitoredEpollEventLoopGroup.class, Thread.MAX_PRIORITY));
    }

    /**
     * This constructor is called by jmh benchmarks, when using a custom executor.
     * It should only be called for testing, not production code, since it wires the NettyRxScheduler
     * as well, but unlike {@link org.apache.cassandra.service.CassandraDaemon#initializeTPC()},
     * it will not set any CPU affinity or epoll IO ratio.
     *
     * @param nThreads - the number of threads
     * @param name - the thread name (unused)
     */
    @VisibleForTesting
    public MonitoredEpollEventLoopGroup(int nThreads, String name)
    {
        this(nThreads, new NettyRxScheduler.NettyRxThreadFactory(MonitoredEpollEventLoopGroup.class, Thread.MAX_PRIORITY));

        CountDownLatch ready = new CountDownLatch(eventLoops.length);

        for (int i = 0; i < eventLoops.length; i++)
        {
            final int cpuId = i;
            EventLoop loop = eventLoops[i];
            eventLoops[i].schedule(() -> {
                try
                {
                    NettyRxScheduler.register(loop, cpuId);
                }
                catch (Exception ex)
                {
                    logger.error("Failed to initialize monitored epoll event loop group due to exception", ex);
                }
                finally
                {
                    ready.countDown();
                }
            }, 0, TimeUnit.SECONDS);
        }

        Uninterruptibles.awaitUninterruptibly(ready);
    }

    /**
     * We pass default args for the newChild() since we need to init the data lazily
     * @param nThreads
     * @param threadFactory
     * @param args
     */
    public MonitoredEpollEventLoopGroup(int nThreads, ThreadFactory threadFactory, Object... args)
    {
        super(nThreads, threadFactory, nThreads, threadFactory, new AtomicInteger(0), args);

        eventLoops = initLoops;
        runningThreads = initThreads;

        monitorThread = threadFactory.newThread(() -> {
            int length = eventLoops.length;

            while (!shutdown)
            {
                long nanoTime = SingleCoreEventLoop.nanoTime();
                for (int i = 0; i < length; i++)
                    eventLoops[i].checkQueues(nanoTime);

                LockSupport.parkNanos(1);
                ApproximateTime.tick();
            }

            for (int i = 0; i < length; i++)
                eventLoops[i].unpark();
        });

        monitorThread.setName("netty-monitor-event-loop-thread");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    @Override
    public void shutdown()
    {
        for (EventLoop loop : eventLoops)
            loop.shutdown();

        shutdown = true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        long start = System.nanoTime();
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

        monitorThread.join(timeoutNanos / 1000000, (int)(timeoutNanos % 1000000));
        if (monitorThread.isAlive())
            return false;

        for (EventLoop loop : eventLoops)
        {
            long elapsed = System.nanoTime() - start;
            timeoutNanos -= elapsed;
            if (timeoutNanos <= 0)
                return false;

            if (!loop.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS))
                return false;
        }

        return true;
    }

    private synchronized void maybeInit(int nThreads, ThreadFactory threadFactory)
    {
        if (initLoops != null)
            return;

        initLoops = new SingleCoreEventLoop[nThreads];
        initThreads = new Thread[nThreads];

        CountDownLatch ready = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            initLoops[i] = new SingleCoreEventLoop(this, new MonitorableExecutor(threadFactory, i), i, nThreads);

            //Start the loop which sets the Thread
            initLoops[i].submit(ready::countDown);
        }

        Uninterruptibles.awaitUninterruptibly(ready);
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert args.length >= 2 : args.length;
        maybeInit((int)args[0], (ThreadFactory)args[1]);

        int offset = ((AtomicInteger)args[2]).getAndIncrement();
        if (offset >= initLoops.length)
            throw new RuntimeException("Trying to allocate more children than passed to the group");

        return initLoops[offset];
    }

    private class MonitorableExecutor implements Executor
    {
        final int offset;
        final ThreadFactory threadFactory;

        MonitorableExecutor(ThreadFactory threadFactory, int offset)
        {
            this.threadFactory = threadFactory;
            this.offset = offset;
        }

        public void execute(Runnable command)
        {
            Thread t = threadFactory.newThread(command);
            t.setDaemon(true);
            initThreads[offset] = t;

            t.start();
        }
    }

    private enum CoreState
    {
        PARKED,
        WORKING
    }

    private static class SingleCoreEventLoop extends EpollEventLoop implements Runnable
    {
        private final MonitoredEpollEventLoopGroup parent;
        private final int threadOffset;

        private static final int busyExtraSpins =  1024 * 128;
        private static final int yieldExtraSpins = 1024 * 8;
        private static final int parkExtraSpins = 1024; // 1024 is ~50ms

        private volatile int pendingEpollEvents = 0;
        private volatile long delayedNanosDeadline = -1;

        @Contended
        private volatile CoreState state;

        @Contended
        private final MpscArrayQueue<Runnable> externalQueue;

        @Contended
        private final ArrayDeque<Runnable> internalQueue;

        private SingleCoreEventLoop(MonitoredEpollEventLoopGroup parent, Executor executor, int threadOffset, int totalCores)
        {
            super(parent, executor, 0,  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject());

            this.parent = parent;
            this.threadOffset = threadOffset;
            this.externalQueue = new MpscArrayQueue<>(1 << 16);
            this.internalQueue = new ArrayDeque<>(1 << 16);

            this.state = CoreState.WORKING;
        }

        public void run()
        {
            try
            {
                while (!parent.shutdown)
                {
                    //deal with spurious wakeups
                    if (state == CoreState.WORKING)
                    {
                        int spins = 0;
                        while (!parent.shutdown)
                        {
                            int drained = drain();
                            if (drained > 0 || ++spins < busyExtraSpins)
                            {
                                if (drained > 0)
                                    spins = 0;

                                continue;
                            }
                            else if (spins < busyExtraSpins + yieldExtraSpins)
                            {
                                Thread.yield();
                            }
                            else if (spins < busyExtraSpins + yieldExtraSpins + parkExtraSpins)
                            {
                                LockSupport.parkNanos(1);
                            }
                            else
                                break;
                        }
                    }

                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            return;
                        }
                    }

                    //Nothing todo; park
                    park();
                }
            }
            finally
            {

            }
        }

        private void park()
        {
            state = CoreState.PARKED;
            LockSupport.park();
        }

        private void unpark()
        {
            state = CoreState.WORKING;
            LockSupport.unpark(parent.runningThreads[threadOffset]);
        }

        private void checkQueues(long nanoTime)
        {
            delayedNanosDeadline = nanoTime;

            if (state == CoreState.PARKED && !isEmpty())
                unpark();
        }

        public void addTask(Runnable task)
        {
            Thread currentThread = Thread.currentThread();

            if (parent.runningThreads != null)
            {
                if (currentThread == parent.runningThreads[threadOffset])
                {
                    if (!internalQueue.offer(task))
                        throw new RuntimeException("Backpressure");
                }
                else
                {
                    if (!externalQueue.relaxedOffer(task))
                        throw new RuntimeException("Backpressure");
                }
            }
            else
            {
                task.run();
            }
        }

        int drain()
        {
            int processed = drainEpoll();
            return drainTasks() + processed;
        }

        int drainEpoll()
        {
            try
            {
                int t;

                if (this.pendingEpollEvents > 0)
                {
                    t = pendingEpollEvents;
                    pendingEpollEvents = 0;
                }
                else
                {
                    t = this.selectStrategy.calculateStrategy(this.selectNowSupplier, hasTasks());
                    switch (t)
                    {
                        case -2:
                            return 0;
                        case -1:
                            t = this.epollWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);
                            if (this.wakenUp == 1)
                            {
                                Native.eventFdWrite(this.eventFd.intValue(), 1L);
                            }
                        default:
                    }
                }

                if (t > 0)
                {
                    this.processReady(this.events, t);
                }

                if (this.allowGrowing && t == this.events.length())
                {
                    this.events.increase();
                }

                return Math.max(t, 0);
            }
            catch (Exception e)
            {
                logger.error("Unexpected exception in the selector loop.", e);

                // Prevent possible consecutive immediate failures that lead to
                // excessive CPU consumption.
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return 0;
            }
        }

        @Inline
        int drainTasks()
        {
            int processed = 0;

            try
            {
                if (delayedNanosDeadline > 0)
                {
                    fetchFromDelayedQueue(delayedNanosDeadline);
                    delayedNanosDeadline = -1;
                }

                Runnable r;
                while ((r = internalQueue.poll()) != null && processed < Short.MAX_VALUE)
                {
                    r.run();
                    ++processed;
                }

                while ((r = externalQueue.relaxedPoll()) != null && processed < Short.MAX_VALUE * 2)
                {
                    r.run();
                    ++processed;
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);

                logger.error("Task exception encountered: ", t);
                try
                {
                    RxJavaPlugins.getErrorHandler().accept(t);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            return processed;
        }

        @Override
        @Inline
        protected boolean hasTasks()
        {
            boolean hasTasks = internalQueue.peek() != null || externalQueue.relaxedPeek() != null;

            if (!hasTasks && delayedNanosDeadline > 0)
                hasTasks = hasScheduledTasks(delayedNanosDeadline);

            return hasTasks;
        }



        @Inline
        boolean isEmpty()
        {
            boolean empty = !hasTasks();

            try
            {
                int t;
                if (empty)
                {
                    t = this.epollWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);

                    if (t > 0)
                        pendingEpollEvents = t;
                    else
                        Native.eventFdWrite(this.eventFd.intValue(), 1L);

                    return t <= 0;
                }
            }
            catch (Exception e)
            {
                logger.error("Error selecting socket ", e);
            }

            return empty;
        }

        protected static long nanoTime()
        {
            return AbstractScheduledEventExecutor.nanoTime();
        }

        @Inline
        void fetchFromDelayedQueue(long nanoTime)
        {
            Runnable scheduledTask = pollScheduledTask(nanoTime);
            while (scheduledTask != null)
            {
                submit(scheduledTask);
                scheduledTask = pollScheduledTask(nanoTime);
            }
        }

    }
}
