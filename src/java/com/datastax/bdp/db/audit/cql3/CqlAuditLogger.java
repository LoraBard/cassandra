/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit.cql3;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.AuditLogger;
import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import io.reactivex.Completable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;

public class CqlAuditLogger
{
    private static final Logger logger = LoggerFactory.getLogger(CqlAuditLogger.class);

    private final AuditableEventGenerator eventGenerator = new AuditableEventGenerator();
    private AuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();

    public Completable logEvents(List<AuditableEvent> events)
    {
        AuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();
        Completable result = null;

        for (AuditableEvent event : events)
        {
            Completable recordEvent = auditLogger.recordEvent(event);

            if (result == null)
                result = recordEvent;
            else
                result = result.andThen(recordEvent);
        }

        return result == null ? Completable.complete() : result;
    }

    public Completable logUnauthorizedAttempt(List<AuditableEvent> events, UnauthorizedException e)
    {
        Completable result = null;

        if (auditLogger.isEnabled() || auditLogger.forceAuditLogging())
            for (AuditableEvent event: events)
            {
                AuditableEvent.Builder builder = AuditableEvent.Builder.fromUnauthorizedException(event, e);

                Completable recordEvent = auditLogger.recordEvent(builder.build());

                if (result == null)
                    result = recordEvent;
                else
                    result = result.andThen(recordEvent);
            }

        return result;
    }

    public Completable logFailedQuery(List<AuditableEvent> events, CassandraException e)
    {
        // system property used to force audit logging functionality
        // in tests where  the full plugin system isn't initialized
        AuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();
        Completable result = null;

        if (auditLogger.isEnabled() || auditLogger.forceAuditLogging())
            for (AuditableEvent event: events)
            {
                AuditableEvent.Builder builder = AuditableEvent.Builder.fromEvent(event);
                builder.type(CoreAuditableEventType.REQUEST_FAILURE);
                String operation = event.getOperation();
                operation = e.getLocalizedMessage() + (operation != null ? " " + operation : "");
                builder.operation(operation);

                Completable recordEvent = auditLogger.recordEvent(builder.build());

                if (result == null)
                    result = recordEvent;
                else
                    result = result.andThen(recordEvent);
            }

        return result;
    }

    /**
     * Get AuditableEvents for a CQL statement
     *
     * @param statement
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @param boundNames
     * @return
     */
    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          String queryString,
                                          QueryState queryState,
                                          QueryOptions queryOptions,
                                          List<ColumnSpecification> boundNames)
    {
        return eventGenerator.getEvents(statement,
                                        queryState,
                                        queryString,
                                        queryOptions.getValues(),
                                        boundNames,
                                        queryOptions.getConsistency());
    }

    /**
     * Get AuditableEvents for a CQL BatchStatement
     *
     * @param batch
     * @param queryState
     * @param queryOptions
     * @return
     */
    public List<AuditableEvent> getEvents(BatchStatement batch,
                                          QueryState queryState,
                                          BatchQueryOptions queryOptions)
    {
        UUID batchId = UUID.randomUUID();
        List<Object> queryOrIdList = queryOptions.getQueryOrIdList();
        List<AuditableEvent> events = Lists.newArrayList();
        for (int i=0; i<queryOrIdList.size(); i++)
        {
            Object queryOrId = queryOrIdList.get(i);
            if (queryOrId instanceof String)
            {
                // regular statement
                // column specs for bind vars are not available, so we pass the
                // variables + an empty list of specs so we log the fact that
                // they exit, but can't be logged
                events.addAll(eventGenerator.getEvents(batch.getStatements().get(i),
                                                       queryState,
                                                       (String) queryOrId,
                                                       queryOptions.forStatement(i).getValues(),
                                                       Collections.<ColumnSpecification>emptyList(),
                                                       batchId,
                                                       queryOptions.getConsistency()));
            }
            else if (queryOrId instanceof MD5Digest)
            {
                // prepared statement
                // lookup the original prepared stmt from QP's cache
                // then use it as the key to fetch CQL string & column
                // specs for bind vars from our own cache
                CQLStatement prepared = null;
                ParsedStatement.Prepared preparedStatement = queryState.getClientState()
                                                                        .getCQLQueryHandler()
                                                                        .getPrepared((MD5Digest) queryOrId);
                if (preparedStatement != null)
                {
                    prepared = preparedStatement.statement;
                }
                else
                {
                    logger.warn(String.format("Prepared Statement [id=%s] is null! " +
                                    "This usually happens because the KS or CF was dropped between the " +
                                    "prepared statement creation and its retrieval from cache",
                                    queryOrId.toString()));
                }

                events.addAll(eventGenerator.getEvents(prepared,
                                                       queryState,
                                                       preparedStatement.rawCQLStatement,
                                                       queryOptions.forStatement(i).getValues(),
                                                       preparedStatement.boundNames,
                                                       batchId,
                                                       queryOptions.getConsistency()));
            }
        }
        return events;
    }

    /**
     * Get AuditableEvents for the act of preparing a statement.
     * Also caches metadata about the prepared statement for retrieval
     * at execution time. This consists of the original CQL string and
     * bind variable definitions required for audit logging at execution
     * time
     *
     * @param statement
     * @param queryString
     * @param clientState
     * @return
     */
    public List<AuditableEvent> getEventsForPrepare(CQLStatement statement,
                                    String queryString,
                                    ClientState clientState)
    {
        return eventGenerator.getEventsForPrepare(statement, clientState, queryString);
    }
}
