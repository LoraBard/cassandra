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

import io.reactivex.functions.Function;

/**
 * Functionality for skipping over empty flows.
 * For usage example, see
 * {@link org.apache.cassandra.db.filter.RowFilter.CQLFilter#filter(org.apache.cassandra.utils.flow.CsFlow, org.apache.cassandra.schema.TableMetadata, int)}
 */
public class SkipEmpty
{
    public static <T> CsFlow<CsFlow<T>> skipEmpty(CsFlow<T> flow)
    {
        return new SkipEmptyContent<>(flow, x -> x);
    }

    public static <T, U> CsFlow<U> skipMapEmpty(CsFlow<T> flow, Function<CsFlow<T>, U> mapper)
    {
        return new SkipEmptyContent<>(flow, mapper);
    }

    /**
     * This is both flow and subscription. Done this way as we can only subscribe to these implementations once
     * and thus it doesn't make much sense to create subscription-specific instances.
     */
    static class SkipEmptyContent<T, U> extends CsFlow<U> implements CsSubscription
    {
        final Function<CsFlow<T>, U> mapper;
        CsSubscriber<U> subscriber;
        CsFlow<T> content;
        SkipEmptyContentSubscriber<T> child;

        private enum State
        {
            READY,
            SUBSCRIBED,
            REQUESTED,
            SUPPLIED,
            COMPLETED,
            CLOSED
        }
        State state = State.READY;

        public SkipEmptyContent(CsFlow<T> content, Function<CsFlow<T>, U> mapper)
        {
            this.content = content;
            this.mapper = mapper;
        }

        public CsSubscription subscribe(CsSubscriber<U> subscriber)
        {
            if (state != State.READY)
                throw new AssertionError("skipEmpty partitions can only be subscribed to once. State was " + state);

            this.subscriber = subscriber;
            state = State.SUBSCRIBED;
            return this;
        }

        public void request()
        {
            if (tryTransition(State.SUPPLIED, State.COMPLETED))
            {
                subscriber.onComplete();
                return;
            }
            if (!verifyTransition(State.SUBSCRIBED, State.REQUESTED))
                return;

            try
            {
                child = new SkipEmptyContentSubscriber(content, this);
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        public void close() throws Exception
        {
            switch (state)
            {
            case SUBSCRIBED:
            case SUPPLIED:
            case COMPLETED:
                state = State.CLOSED;
                break;
            default:
                throw new AssertionError("Unexpected state " + state + " while closing " + this);
            }
        }

        void onContent(CsFlow<T> child)
        {
            if (!verifyTransition(State.REQUESTED, State.SUPPLIED))
                return;

            U result;
            try
            {
                result = mapper.apply(child);
            }
            catch (Exception e)
            {
                onError(e);
                return;
            }

            subscriber.onNext(result);
        }

        void onEmpty()
        {
            if (!verifyTransition(State.REQUESTED, State.COMPLETED))
                return;

            subscriber.onComplete();
        }

        void onError(Throwable e)
        {
            state = state.COMPLETED;
            subscriber.onError(e);
        }

        boolean tryTransition(State from, State to)
        {
            if (state != from)
                return false;

            state = to;
            return true;
        }

        boolean verifyTransition(State from, State to)
        {
            if (tryTransition(from, to))
                return true;

            onError(new AssertionError("Incorrect state " + from + " to transition to " + to + " in " + this));
            return false;
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return child != null ? child.addSubscriberChainFromSource(throwable) : CsFlow.wrapException(throwable, this);
        }

        public String toString()
        {
            return CsFlow.formatTrace("skipEmpty", mapper, subscriber);
        }
    }

    /**
     * This is both flow and subscription. Done this way as we can only subscribe to these implementations once
     * and thus it doesn't make much sense to create subscription-specific instances.
     */
    private static class SkipEmptyContentSubscriber<T> extends CsFlow<T> implements CsSubscriber<T>, CsSubscription
    {
        final SkipEmptyContent parent;
        final CsSubscription upstream;

        CsSubscriber<T> downstream = null;
        T first = null;

        public SkipEmptyContentSubscriber(CsFlow<T> content,
                                          SkipEmptyContent parent) throws Exception
        {
            this.parent = parent;
            upstream = content.subscribe(this);
            upstream.request();
        }

        public CsSubscription subscribe(CsSubscriber<T> subscriber)
        {
            assert downstream == null : "skipEmpty content can only be subscribed to once, " + parent.toString();
            assert first != null;
            downstream = subscriber;

            return this;
        }

        public void request()
        {
            if (first != null)
            {
                T toReturn = first;
                first = null;
                downstream.onNext(toReturn);
            }
            else
                upstream.request();
        }

        public void close() throws Exception
        {
            upstream.close();
        }

        public void onNext(T item)
        {
            if (downstream != null)
                downstream.onNext(item);
            else
            {
                if (first != null)
                    parent.onError(new AssertionError("Got onNext twice with " + first + " and then " + item + " in " + parent
                                                                                                                        .toString()));
                first = item;
                parent.onContent(this);
            }
        }

        public void onComplete()
        {
            if (downstream != null)
                downstream.onComplete();
            else
            {
                // Empty flow. No one will subscribe to us now, so make sure our subscription is closed.
                try
                {
                    upstream.close();
                    parent.onEmpty();
                }
                catch (Exception e)
                {
                    parent.onError(e);
                }
            }
        }

        public void onError(Throwable t)
        {
            if (downstream != null)
                downstream.onError(t);
            else
            {
                // Error before first element. No one will subscribe to us now, so make sure our subscription is closed.
                try
                {
                    upstream.close();
                }
                catch (Throwable tt)
                {
                    t.addSuppressed(tt);
                }
                parent.onError(t);
            }
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return upstream.addSubscriberChainFromSource(throwable);
        }

        public String toString()
        {
            return CsFlow.formatTrace("skipEmpty", parent.mapper, downstream);
        }
    }
}
