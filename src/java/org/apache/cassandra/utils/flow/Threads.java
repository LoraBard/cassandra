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

package org.apache.cassandra.utils.flow;

import java.util.concurrent.Callable;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;

public class Threads
{
    static class RequestOnCore implements FlowSubscription
    {
        final int coreId;
        final FlowSubscription source;

        <T> RequestOnCore(FlowSubscriber<T> subscriber, int coreId, Flow<T> source) throws Exception
        {
            this.coreId = coreId;
            this.source = source.subscribe(subscriber);
        }

        public void request()
        {
            if (TPC.isOnCore(coreId))
                source.request();
            else
                TPC.getForCore(coreId).scheduleDirect(source::request);
        }

        public void close() throws Exception
        {
            // Close on the current thread to propagate exceptions
            source.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }
    }

    final static Flow.Operator<?, ?> REQUEST_ON_CORE[] = new Flow.Operator[TPC.getNumCores()];
    static
    {
        for (int i = 0; i < REQUEST_ON_CORE.length; ++i)
            REQUEST_ON_CORE[i] = constructRequestOnCore(i);
    }

    private static Flow.Operator<Object, Object> constructRequestOnCore(int coreId)
    {
        return (source, subscriber) -> new RequestOnCore(subscriber, coreId, source);
    }

    /**
     * Returns an operator to perform each request() on the given flow on the specified core thread.
     * If execution is already on this thread, the request is called directly, otherwise it is given to the scheduler
     * for async execution.
     */
    public static <T> Flow.Operator<T, T> requestOnCore(int coreId)
    {
        return (Flow.Operator<T, T>) REQUEST_ON_CORE[coreId];
    }

    static class RequestOn implements FlowSubscription
    {
        final Scheduler scheduler;
        final FlowSubscription source;

        <T> RequestOn(FlowSubscriber<T> subscriber, Scheduler scheduler, Flow<T> source) throws Exception
        {
            this.scheduler = scheduler;
            this.source = source.subscribe(subscriber);
        }

        public void request()
        {
            // TODO: If blocking is not a concern, recognizing we are already on an IO thread could boost perf.
            scheduler.scheduleDirect(source::request);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return source.addSubscriberChainFromSource(throwable);
        }
    }

    /**
     * Returns an operator to perform each request() on the given flow on the specified scheduler.
     * If we are already on that scheduler, whether the request is called directly or scheduled depends on the specific
     * scheduler.
     */
    public static <T> Flow.Operator<T, T> requestOn(Scheduler scheduler)
    {
        if (scheduler instanceof TPCScheduler)
            return requestOnCore(((TPCScheduler) scheduler).coreId());
        else if (scheduler == Schedulers.io())
            return requestOnIo();
        else
            return createRequestOn(scheduler);
    }

    private static <T> Flow.Operator<T, T> createRequestOn(Scheduler scheduler)
    {
        return (source, subscriber) -> new RequestOn(subscriber, scheduler, source);
    }

    static final Flow.Operator<?, ?> REQUEST_ON_IO = createRequestOn(Schedulers.io());

    /**
     * Returns an operator to perform each request() on the given flow on the IO scheduler.
     * Used for operations that can block (e.g. sync reads off disk).
     */
    public static <T> Flow.Operator<T, T> requestOnIo()
    {
        return (Flow.Operator<T, T>) REQUEST_ON_IO;
    }

    static class EvaluateOn<T> implements FlowSubscription
    {
        final FlowSubscriber<T> subscriber;
        final Callable<T> source;
        final int coreId;

        private volatile int requested = 0;
        EvaluateOn(FlowSubscriber<T> subscriber, Callable<T> source, int coreId)
        {
            this.subscriber = subscriber;
            this.source = source;
            this.coreId = coreId;
        }

        public void request()
        {
            switch (requested++)
            {
            case 0:
                if (TPC.isOnCore(coreId))
                    evaluate();
                else
                    TPC.getForCore(coreId).scheduleDirect(this::evaluate);
                break;
            default:
                // Assuming no need to switch threads for no work.
                subscriber.onComplete();
            }
        }

        private void evaluate()
        {
            try
            {
                T v = source.call();
                subscriber.onNext(v);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void close()
        {
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return Flow.wrapException(throwable, this);
        }

        public String toString()
        {
            return Flow.formatTrace("evaluateOn " + coreId, source, subscriber);
        }
    }

    /**
     * Returns a flow which represents the evaluation of the given callable on the specified core thread.
     * If execution is already on this thread, the evaluation is called directly, otherwise it is given to the scheduler
     * for async execution.
     */
    public static <T> Flow<T> evaluateOnCore(Callable<T> callable, int coreId)
    {
        return new Flow<T>()
        {
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                return new EvaluateOn<T>(subscriber, callable, coreId);
            }
        };
    }

    public static <T> Flow<T> deferOnCore(Callable<Flow<T>> source, int coreId)
    {
        return evaluateOnCore(source, coreId).flatMap(x -> x);
    }
}