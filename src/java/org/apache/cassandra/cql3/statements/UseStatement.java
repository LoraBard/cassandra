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

import io.reactivex.Single;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class UseStatement extends ParsedStatement implements CQLStatement, KeyspaceStatement
{
    private final String keyspace;

    public UseStatement(String keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public String keyspace()
    {
        return keyspace;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.SET_KS;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    public void checkAccess(QueryState state)
    {
    }

    public void validate(QueryState state) throws InvalidRequestException
    {
    }

    public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException
    {
        state.getClientState().setKeyspace(keyspace);
        return Single.just(new ResultMessage.SetKeyspace(keyspace));
    }

    public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) throws InvalidRequestException
    {
        // In production, internal queries are exclusively on the system keyspace and 'use' is thus useless
        // but for some unit tests we need to set the keyspace (e.g. for tests with DROP INDEX)
        return execute(state, options, System.nanoTime());
    }

    public StagedScheduler getScheduler()
    {
        return TPC.ioScheduler();
    }
}
