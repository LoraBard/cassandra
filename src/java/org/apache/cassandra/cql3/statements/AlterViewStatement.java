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

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class AlterViewStatement extends SchemaAlteringStatement implements TableStatement
{
    private final TableAttributes attrs;

    public AlterViewStatement(CFName name, TableAttributes attrs)
    {
        super(name);
        this.attrs = attrs;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.UPDATE_VIEW;
    }

    public void checkAccess(QueryState state)
    {
        TableMetadataRef baseTable = View.findBaseTable(keyspace(), columnFamily());
        if (baseTable != null)
            state.checkTablePermission(keyspace(), baseTable.name, CorePermission.ALTER);
    }

    public void validate(QueryState state)
    {
        // validated in announceMigration()
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException
    {
        TableMetadata meta = Schema.instance.validateTable(keyspace(), columnFamily());
        if (!meta.isView())
            return error("Cannot use ALTER MATERIALIZED VIEW on Table");

        ViewMetadata current = Schema.instance.getView(keyspace(), columnFamily());

        if (attrs == null)
            return error("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");

        attrs.validate();

        TableParams params = attrs.asAlteredTableParams(current.metadata.params);
        if (params.compaction.klass().equals(DateTieredCompactionStrategy.class) &&
            !current.metadata.params.compaction.klass().equals(DateTieredCompactionStrategy.class))
        {
            DateTieredCompactionStrategy.deprecatedWarning(keyspace(), columnFamily());
        }

        if (params.gcGraceSeconds == 0)
        {
            return error("Cannot alter gc_grace_seconds of a materialized view to 0, since this " +
                         "value is used to TTL undelivered updates. Setting gc_grace_seconds too " +
                         "low might cause undelivered updates to expire before being replayed.");
        }

        if (params.defaultTimeToLive > 0)
        {
            throw new InvalidRequestException("Cannot set or alter default_time_to_live for a materialized view. " +
                                              "Data in a materialized view always expire at the same time than " +
                                              "the corresponding data in the parent table.");
        }

        ViewMetadata updated = current.copy(current.metadata.unbuild().params(params).build());

        return MigrationManager.announceViewUpdate(updated, isLocalOnly)
                               .andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED,
                                                                          Event.SchemaChange.Target.TABLE,
                                                                          keyspace(),
                                                                          columnFamily())));
    }

    public String toString()
    {
        return String.format("AlterViewStatement(name=%s)", cfName);
    }
}
