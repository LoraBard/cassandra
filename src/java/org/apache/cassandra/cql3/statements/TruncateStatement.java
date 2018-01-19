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
package org.apache.cassandra.cql3.statements;

import java.util.concurrent.TimeoutException;

import io.reactivex.Single;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.flow.RxThreads;

public class TruncateStatement extends CFStatement implements CQLStatement
{
    public TruncateStatement(CFName name)
    {
        super(name);
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.TRUNCATE;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    @Override
    public void checkAccess(QueryState state)
    {
        state.checkTablePermission(keyspace(), columnFamily(), CorePermission.MODIFY);
    }

    public void validate(QueryState state) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), columnFamily());
    }

    public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        return executeInternal(state, options);
    }

    public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options)
    {
        return RxThreads.subscribeOnIo(
            Single.defer(() ->
                         {
                             try
                             {
                                 TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), columnFamily());
                                 if (metaData.isView())
                                     return Single.error(new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead"));

                                 StorageProxy.truncateBlocking(keyspace(), columnFamily());
                             }
                             catch (UnavailableException | TimeoutException e)
                             {
                                 return Single.error(new TruncateException(e));
                             }

                             return Single.just(new ResultMessage.Void());
                         }),
            TPCTaskType.TRUNCATE);
    }

    public StagedScheduler getScheduler()
    {
        return TPC.ioScheduler();
    }
}
