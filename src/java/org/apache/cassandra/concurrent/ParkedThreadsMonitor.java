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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * This class is responsible for keeping tabs on threads that register as a {@link MonitorableThread}.
 * <p>
 * When a {@link MonitorableThread} parks itself this thread wakes it up when work is ready. This allows Threads to
 * (for example) use a non-blocking queue without constantly spinning.
 * <p>
 * The watcher will call {@link MonitorableThread#shouldUnpark(long)} and when that method returns true, unparks the
 * thread.  When the thread has no work left it parks itself.
 * <p>
 * It's upto the {@link MonitorableThread#shouldUnpark(long)} implementation to track its own state.
 */
public class ParkedThreadsMonitor
{
    public static final Supplier<ParkedThreadsMonitor> instance = Suppliers.memoize(ParkedThreadsMonitor::new);
    private static final Logger LOGGER = LoggerFactory.getLogger(ParkedThreadsMonitor.class);

    /*
     * NOTE: parkNanos seems to operate as follows (on my machine, expect differences):
     *  - 1 to 80ns   -> p50% is 400-500ns
     *  - 80 to 100ns -> some instability, flip between 500ns and 52us
     *  - > 100ns     -> p50% looks like 52us + parkTime
     */
    private static final long SLEEP_INTERVAL_NS = Long.getLong("dse.thread_monitor_sleep_nanos", 50000);
    private static final boolean AUTO_CALIBRATE =
        Boolean.parseBoolean(System.getProperty("dse.thread_monitor_auto_calibrate", "true")) &&
        SLEEP_INTERVAL_NS > 0;

    private static final Sleeper SLEEPER =
        AUTO_CALIBRATE ? new CalibratingSleeper(SLEEP_INTERVAL_NS) : new Sleeper();

    private final MpscUnboundedArrayQueue<Runnable> commands = new MpscUnboundedArrayQueue<>(128);
    private final ArrayList<MonitorableThread> monitoredThreads =
        new ArrayList<>(Runtime.getRuntime().availableProcessors() * 2);
    private final ArrayList<Runnable> loopActions = new ArrayList<>(4);
    private final Thread watcherThread;
    private volatile boolean shutdown;

    private ParkedThreadsMonitor()
    {
        shutdown = false;
        watcherThread = new Thread(this::run);
        watcherThread.setName("ParkedThreadsMonitor");
        watcherThread.setPriority(Thread.MAX_PRIORITY);
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    private void run()
    {
        final ArrayList<Runnable> loopActions = this.loopActions;
        final ArrayList<MonitorableThread> monitoredThreads = this.monitoredThreads;
        final MpscUnboundedArrayQueue<Runnable> commands = this.commands;
        while (!shutdown)
        {
            try
            {
                runCommands(commands);
                executeLoopActions(loopActions);
                monitorThreads(monitoredThreads);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                LOGGER.error("ParkedThreadsMonitor exception: ", t);
            }
            SLEEPER.sleep();
        }
        unparkOnShutdown(monitoredThreads);
    }

    private void monitorThreads(ArrayList<MonitorableThread> monitoredThreads)
    {
        final long nanoTime = System.nanoTime();
        for (int i = 0; i < monitoredThreads.size(); i++)
        {
            MonitorableThread thread = monitoredThreads.get(i);
            if (thread.shouldUnpark(nanoTime))
            {
                // TODO: add a counter we can track for unpark rate
                thread.unpark();
            }
        }
    }

    private void executeLoopActions(ArrayList<Runnable> loopActions)
    {
        for (int i = 0; i < loopActions.size(); i++)
        {
            Runnable action = loopActions.get(i);
            action.run();
        }
    }

    private void runCommands(MpscUnboundedArrayQueue<Runnable> commands)
    {
        // queue is almost always empty, so pre check makes sense
        if (!commands.isEmpty())
        {
            commands.drain(Runnable::run);
        }
    }

    private void unparkOnShutdown(ArrayList<MonitorableThread> monitoredThreads)
    {
        for (MonitorableThread thread : monitoredThreads)
            thread.unpark();
    }

    /**
     * Adds a collection of threads to monitor
     */
    public void addThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        threads.forEach(this::addThreadToMonitor);
    }

    /**
     * Adds a thread to monitor
     */
    public void addThreadToMonitor(MonitorableThread thread)
    {
        commands.offer(() -> monitoredThreads.add(thread));
    }

    /**
     * Removes a thread from monitoring
     */
    public void removeThreadToMonitor(MonitorableThread thread)
    {
        commands.offer(() -> monitoredThreads.remove(thread));
    }

    /**
     * Removes a collection of threads from monitoring
     */
    public void removeThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        threads.forEach(this::removeThreadToMonitor);
    }

    /**
     * Runs the specified action in each loop. Mainly used to avoid many threads doing the same work
     * over and over, example {@link org.apache.cassandra.db.monitoring.ApproximateTime}
     */
    public void addAction(Runnable action)
    {
        commands.offer(() -> loopActions.add(action));
    }

    public void shutdown()
    {
        shutdown = true;
    }

    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException
    {
        shutdown();
        watcherThread.join(timeUnit.toMillis(timeout));
        return !watcherThread.isAlive();
    }

    /**
     * Interface for threads that take their work from a non-blocking queue and
     * wish to be watched by the {@link ParkedThreadsMonitor}
     * <p>
     * When a Thread has no work todo it should park itself.
     */
    public static interface MonitorableThread
    {
        /**
         * What a MonitorableThread should use to track it's current state
         */
        enum ThreadState
        {
            PARKED,
            WORKING
        }

        /**
         * Will unpark a thread in a parked state. Called by {@link ParkedThreadsMonitor} AFTER
         * {@link #shouldUnpark(long)} returns true.
         */
        void unpark();

        /**
         * Called continuously by the {@link ParkedThreadsMonitor to decide if unpark() should be called on a
         * Thread.
         * <p>
         * Should return true IFF the thread is parked and there is work to be done when the thread is
         * unparked.
         */
        boolean shouldUnpark(long nanoTimeSinceStart);
    }

    static class Sleeper
    {
        void sleep()
        {
            if (SLEEP_INTERVAL_NS > 0)
                LockSupport.parkNanos(SLEEP_INTERVAL_NS);
        }
    }

    @VisibleForTesting
    public static class CalibratingSleeper extends Sleeper
    {
        final long targetNs;
        long calibratedSleepNs;
        long expSmoothedSleepTimeNs;
        int comparisonDelay;

        @VisibleForTesting
        public CalibratingSleeper(long targetNs)
        {
            this.targetNs = targetNs;
            this.calibratedSleepNs = targetNs;
            this.expSmoothedSleepTimeNs = 0;
        }

        @VisibleForTesting
        public void sleep()
        {
            long start = nanoTime();
            park();
            long sleptNs = nanoTime() - start;
            if (expSmoothedSleepTimeNs == 0)
                expSmoothedSleepTimeNs = sleptNs;
            else
                expSmoothedSleepTimeNs = (long) (0.001 * sleptNs + 0.999 * expSmoothedSleepTimeNs);

            // calibrate sleep time if we miss target by more than 10%, wait for at least 100 observations
            if (comparisonDelay < 100)
            {
                comparisonDelay++;
            }
            else if (expSmoothedSleepTimeNs > targetNs * 1.1)
            {
                calibratedSleepNs = (long) (calibratedSleepNs * 0.9) + 1;
                expSmoothedSleepTimeNs = 0;
                comparisonDelay = 0;
            }
            else if (expSmoothedSleepTimeNs < targetNs * 0.9)
            {
                calibratedSleepNs = (long) (calibratedSleepNs * 1.1);
                expSmoothedSleepTimeNs = 0;
                comparisonDelay = 0;
            }
        }

        @VisibleForTesting
        void park()
        {
            LockSupport.parkNanos(calibratedSleepNs);
        }

        @VisibleForTesting
        long nanoTime()
        {
            return System.nanoTime();
        }
    }
}