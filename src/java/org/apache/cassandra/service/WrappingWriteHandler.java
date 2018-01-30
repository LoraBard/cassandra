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
package org.apache.cassandra.service;

import java.net.InetAddress;

import io.reactivex.Completable;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Response;

public abstract class WrappingWriteHandler extends WriteHandler
{
    protected final WriteHandler wrapped;

    protected WrappingWriteHandler(WriteHandler wrapped)
    {
        this.wrapped = wrapped;

        // We want this to complete when wrapped does so
        // we install a callback that will be executed when
        // wrapped completes, either with a result or exceptionally
        wrapped.whenComplete((x, t) -> {
            if (logger.isTraceEnabled())
                logger.trace("{}/{} - Completed", WrappingWriteHandler.this.hashCode(), wrapped.hashCode());

            if (t == null)
                complete(null);
            else
                completeExceptionally(t);
        });
        // We also want the other way around and that's why we override the complete methods below.
    }

    public WriteEndpoints endpoints()
    {
        return wrapped.endpoints();
    }

    public ConsistencyLevel consistencyLevel()
    {
        return wrapped.consistencyLevel();
    }

    public WriteType writeType()
    {
        return wrapped.writeType();
    }

    protected long queryStartNanos()
    {
        return wrapped.queryStartNanos();
    }

    @Override
    public Void get() throws WriteTimeoutException, WriteFailureException
    {
        return wrapped.get();
    }

    public Completable toObservable()
    {
        return wrapped.toObservable();
    }

    public void onResponse(Response<EmptyPayload> response)
    {
        wrapped.onResponse(response);
    }

    public void onFailure(FailureResponse<EmptyPayload> response)
    {
        wrapped.onFailure(response);
    }

    public void onTimeout(InetAddress host)
    {
        wrapped.onTimeout(host);
    }

    @Override
    public boolean complete(Void value)
    {
        if (!wrapped.isDone())
            wrapped.complete(value);
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex)
    {
        if (!wrapped.isDone())
            wrapped.completeExceptionally(ex);
        return super.completeExceptionally(ex);
    }
}
