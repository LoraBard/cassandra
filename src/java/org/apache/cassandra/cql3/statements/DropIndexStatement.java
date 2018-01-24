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

import java.util.Optional;

import io.reactivex.Maybe;
import io.reactivex.Single;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public class DropIndexStatement extends SchemaAlteringStatement implements TableStatement
{
    public final String indexName;
    public final boolean ifExists;

    public DropIndexStatement(IndexName indexName, boolean ifExists)
    {
        super(indexName.getCfName());
        this.indexName = indexName.getIdx();
        this.ifExists = ifExists;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.DROP_INDEX;
    }

    public String columnFamily()
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace());
        if (ksm == null)
            return null;

        Optional<TableMetadata> indexedTable = ksm.findIndexedTable(indexName);
        return indexedTable.isPresent() ? indexedTable.get().name : null;
    }

    @Override
    public void checkAccess(QueryState state)
    {
        TableMetadata metadata = lookupIndexedTable();
        if (metadata == null)
            return;

        state.checkTablePermission(metadata.keyspace, metadata.name, CorePermission.ALTER);
    }

    public void validate(QueryState state)
    {
        // validated in lookupIndexedTable()
    }

    @Override
    public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException
    {
        return announceMigration(state, false).map(schemaChangeEvent -> new ResultMessage.SchemaChange(schemaChangeEvent))
                                              .cast(ResultMessage.class)
                                              .toSingle(new ResultMessage.Void());
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        TableMetadata current = lookupIndexedTable();
        if (current == null)
            return Maybe.empty();

        TableMetadata updated =
            current.unbuild()
                   .indexes(current.indexes.without(indexName))
                   .build();

        // Dropping an index is akin to updating the CF
        // Note that we shouldn't call columnFamily() at this point because the index has been dropped and the call to lookupIndexedTable()
        // in that method would now throw.
        Event.SchemaChange event = new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, current.keyspace, current.name);
        return MigrationManager.announceTableUpdate(updated, isLocalOnly).andThen(Maybe.just(event));
    }

    /**
     * The table for which the index should be dropped, or null if the index doesn't exist
     *
     * @return the metadata for the table containing the dropped index, or {@code null}
     * if the index to drop cannot be found but "IF EXISTS" is set on the statement.
     *
     * @throws InvalidRequestException if the index cannot be found and "IF EXISTS" is not
     * set on the statement.
     */
    private TableMetadata lookupIndexedTable()
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace());
        if (ksm == null)
            throw new KeyspaceNotDefinedException("Keyspace " + keyspace() + " does not exist");

        return ksm.findIndexedTable(indexName)
                  .orElseGet(() -> {
                      if (ifExists)
                          return null;

                      throw invalidRequest("Index '%s' could not be found in any of the tables of keyspace '%s'",
                                           indexName, keyspace());
                  });
    }
}
