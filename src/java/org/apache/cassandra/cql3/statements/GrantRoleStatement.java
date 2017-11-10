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

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantRoleStatement extends RoleManagementStatement
{
    public GrantRoleStatement(RoleName name, RoleName grantee)
    {
        super(name, grantee);
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.GRANT;
    }

    public Single<ResultMessage> execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        return Single.fromCallable(() -> {
            DatabaseDescriptor.getRoleManager().grantRole(state.getUser(), role, grantee);
            return new ResultMessage.Void();
        });
    }
}
