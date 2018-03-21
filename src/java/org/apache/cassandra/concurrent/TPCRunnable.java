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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;


/**
 * Runnable used to wrap TPC scheduled tasks.
 *
 * Takes care of setting up local context (ExecutorLocals) and keeping track of scheduled and completed tasks for
 * metrics.
 *
 * The TPCRunnable is usually constructed when a task is passed on to an executor which cannot execute it immediately,
 * but sometimes needs to be created explicitly to capture the current thread context (ExecutorLocals) when the task is
 * going to be executed by a shared signal (for example, see MemtableAllocator.whenBelowLimits), or when a request
 * is first initiated with specific ExecutorLocals.
 */
public class TPCRunnable implements Runnable
{
    private static final AtomicIntegerFieldUpdater<TPCRunnable> COMPLETION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(TPCRunnable.class, "completed");

    private final Runnable runnable;
    private final ExecutorLocals locals;
    private final TPCTaskType taskType;
    private final TPCMetrics metrics;

    // Used via COMPLETION_UPDATER above:
    @SuppressWarnings({"unused"})
    private volatile int completed;

    public TPCRunnable(Runnable runnable, ExecutorLocals locals, TPCTaskType taskType, int scheduledOn)
    {
        this.runnable = runnable;
        this.locals = locals;
        this.taskType = taskType;
        this.metrics = TPC.metrics(scheduledOn);

        metrics.scheduled(taskType);
    }

    public void run()
    {
        metrics.starting(taskType);

        ExecutorLocals.set(locals);

        try
        {
            // We don't expect this to throw, but we can't let the completed count miss any tasks because it has errored
            // out.
            runnable.run();
        }
        catch (Throwable t)
        {
            metrics.failed(taskType, t);
            throw t;
        }
        finally
        {
            if (COMPLETION_UPDATER.compareAndSet(this, 0, 1))
                metrics.completed(taskType);
            else
                throw new IllegalStateException("TPC task was cancelled while still running.");
        }
    }

    public void cancelled()
    {
        if (COMPLETION_UPDATER.compareAndSet(this, 0, 1))
            metrics.cancelled(taskType);
    }

    public void setPending()
    {
        metrics.pending(taskType, +1);
    }

    public void unsetPending()
    {
        metrics.pending(taskType, -1);
    }

    public boolean isPendable()
    {
        return taskType.pendable();
    }

    public boolean hasPriority()
    {
        return taskType.priority();
    }

    public boolean alwaysEnqueue()
    {
        return taskType.alwaysEnqueue();
    }

    public TPCTaskType taskType()
    {
        return taskType;
    }

    public void blocked()
    {
        metrics.blocked(taskType);
    }

    public static TPCRunnable wrap(Runnable runnable)
    {
        return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, TPC.getNumCores());
    }

    public static TPCRunnable wrap(Runnable runnable, int defaultCore)
    {
        return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, defaultCore);
    }

    public static TPCRunnable wrap(Runnable runnable, TPCTaskType defaultStage, int defaultCore)
    {
        return wrap(runnable, ExecutorLocals.create(), defaultStage, defaultCore);
    }

    public static TPCRunnable wrap(Runnable runnable,
                                   TPCTaskType defaultStage,
                                   StagedScheduler scheduler)
    {
        return wrap(runnable, ExecutorLocals.create(), defaultStage, scheduler.metricsCoreId());
    }

    public static TPCRunnable wrap(Runnable runnable,
                                   ExecutorLocals locals,
                                   TPCTaskType defaultStage,
                                   StagedScheduler scheduler)
    {
        return wrap(runnable, locals, defaultStage, scheduler.metricsCoreId());
    }

    public static TPCRunnable wrap(Runnable runnable,
                                   ExecutorLocals locals,
                                   TPCTaskType defaultStage,
                                   int defaultCore)
    {
        if (runnable instanceof TPCRunnable)
            return (TPCRunnable) runnable;

        return new TPCRunnable(runnable, locals, defaultStage, defaultCore);
    }
}
