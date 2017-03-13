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


import io.reactivex.Maybe;
import io.reactivex.Single;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropTableStatement extends SchemaAlteringStatement
{
    private final boolean ifExists;

    public DropTableStatement(CFName name, boolean ifExists)
    {
        super(name);
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        try
        {
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), CorePermission.DROP);
        }
        catch (InvalidRequestException e)
        {
            if (!ifExists)
                throw e;
        }
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException
    {
        return Maybe.defer(() ->
                           {

                               KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace());
                               if (ksm == null)
                                   return error(String.format("Cannot drop table in unknown keyspace '%s'", keyspace()));

                               TableMetadata metadata = ksm.getTableOrViewNullable(columnFamily());
                               if (metadata != null)
                               {
                                   if (metadata.isView())
                                       return error("Cannot use DROP TABLE on Materialized View");

                                   boolean rejectDrop = false;
                                   StringBuilder messageBuilder = new StringBuilder();
                                   for (ViewMetadata def : ksm.views)
                                   {
                                       if (def.baseTableId.equals(metadata.id))
                                       {
                                           if (rejectDrop)
                                               messageBuilder.append(',');
                                           rejectDrop = true;
                                           messageBuilder.append(def.name);
                                       }
                                   }
                                   if (rejectDrop)
                                   {
                                       return error(String.format("Cannot drop table when materialized views still depend on it (%s.{%s})",
                                                                  keyspace(),
                                                                  messageBuilder.toString()));
                                   }
                               }

                               return MigrationManager.announceTableDrop(keyspace(), columnFamily(), isLocalOnly)
                                                      .andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily())))
                                                      .onErrorResumeNext(e ->
                                                                         {
                                                                             if (e instanceof ConfigurationException && ifExists)
                                                                                 return Maybe.empty();

                                                                             return Maybe.error(e);
                                                                         });
                           });
    }
}
