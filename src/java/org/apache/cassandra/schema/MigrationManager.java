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
package org.apache.cassandra.schema;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.internal.schedulers.ImmediateThinScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    private static final int MIGRATION_DELAY_IN_MS = 60000;

    private static final int MIGRATION_TASK_WAIT_IN_SECONDS = Integer.parseInt(System.getProperty("cassandra.migration_task_wait_in_seconds", "1"));

    private MigrationManager() {}

    public static void scheduleSchemaPull(InetAddress endpoint, EndpointState state)
    {
        VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);

        if (!endpoint.equals(FBUtilities.getBroadcastAddress()) && value != null)
            maybeScheduleSchemaPull(UUID.fromString(value.value), endpoint);
    }

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to apply locally.
     */
    private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddress endpoint)
    {
        UUID ourVersion = Schema.instance.getVersion();
        if ((ourVersion != null && Schema.instance.getVersion().equals(theirVersion)) || !shouldPullSchemaFrom(endpoint))
        {
            logger.debug("Not pulling schema from {} because versions match ({}/{}), or shouldPullSchemaFrom returned false",
                         endpoint, ourVersion, theirVersion);
            return;
        }

        if (SchemaConstants.emptyVersion.equals(Schema.instance.getVersion()) || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Submitting migration task for {}", endpoint);
            submitMigrationTask(endpoint);
        }
        else
        {
            // Include a delay to make sure we have a chance to apply any changes being
            // pushed out simultaneously. See CASSANDRA-5025
            Runnable runnable = () ->
            {
                // grab the latest version of the schema since it may have changed again since the initial scheduling
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (epState == null)
                {
                    logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                    return;
                }
                VersionedValue value = epState.getApplicationState(ApplicationState.SCHEMA);
                UUID currentVersion = UUID.fromString(value.value);
                if (Schema.instance.getVersion().equals(currentVersion))
                {
                    logger.debug("not submitting migration task for {} because our versions match", endpoint);
                    return;
                }
                logger.debug("submitting migration task for {}", endpoint);
                submitMigrationTask(endpoint);
            };
            ScheduledExecutors.nonPeriodicTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
    }

    private static Future<?> submitMigrationTask(InetAddress endpoint)
    {
        /*
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
    }

    static boolean shouldPullSchemaFrom(InetAddress endpoint)
    {
        /*
         * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
         * Don't request schema from fat clients
         */
        return MessagingService.instance().knowsVersion(endpoint)
                && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
    }

    public static boolean isReadyForBootstrap()
    {
        return MigrationTask.getInflightTasks().isEmpty();
    }

    public static void waitUntilReadyForBootstrap()
    {
        CountDownLatch completionLatch;
        while ((completionLatch = MigrationTask.getInflightTasks().poll()) != null)
        {
            try
            {
                if (!completionLatch.await(MIGRATION_TASK_WAIT_IN_SECONDS, TimeUnit.SECONDS))
                    logger.error("Migration task failed to complete");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.error("Migration task was interrupted");
            }
        }
    }

    public static Completable announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException
    {
        return announceNewKeyspace(ksm, false);
    }

    public static Completable announceNewKeyspace(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException
    {
        return announceNewKeyspace(ksm, FBUtilities.timestampMicros(), announceLocally);
    }

    public static Completable announceNewKeyspace(KeyspaceMetadata ksm, long timestamp, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     ksm.validate();

                                     if (Schema.instance.getKeyspaceMetadata(ksm.name) != null)
                                         return Completable.error(new AlreadyExistsException(ksm.name));

                                     logger.info("Create new Keyspace: {}", ksm);
                                     return announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
                                 });
    }

    public static Completable announceNewTable(TableMetadata cfm) throws ConfigurationException
    {
        return announceNewTable(cfm, false);
    }

    public static Completable announceNewTable(TableMetadata cfm, boolean announceLocally)
    {
        return announceNewTable(cfm, announceLocally, true);
    }

    /**
     * Announces the table even if the definition is already know locally.
     * This should generally be avoided but is used internally when we want to force the most up to date version of
     * a system table schema (Note that we don't know if the schema we force _is_ the most recent version or not, we
     * just rely on idempotency to basically ignore that announce if it's not. That's why we can't use announceUpdateColumnFamily,
     * it would for instance delete new columns if this is not called with the most up-to-date version)
     *
     * Note that this is only safe for system tables where we know the id is fixed and will be the same whatever version
     * of the definition is used.
     */
    public static Completable forceAnnounceNewTable(TableMetadata cfm)
    {
        return announceNewTable(cfm, false, false, 0);
    }

    private static Completable announceNewTable(TableMetadata cfm, boolean announceLocally, boolean throwOnDuplicate)
    {
        return announceNewTable(cfm, announceLocally, throwOnDuplicate, FBUtilities.timestampMicros());
    }

    private static Completable announceNewTable(TableMetadata cfm, boolean announceLocally, boolean throwOnDuplicate, long timestamp)
    {
        return Completable.defer(() ->
                                 {
                                     cfm.validate();

                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(cfm.keyspace);
                                     if (ksm == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.name, cfm.keyspace)));
                                         // If we have a table or a view which has the same name, we can't add a new one
                                     else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.name) != null)
                                         return Completable.error(new AlreadyExistsException(cfm.keyspace, cfm.name));

                                     logger.info("Create new table: {}", cfm.toDebugString());
                                     return announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, timestamp), announceLocally);
                                 });
    }

    public static Completable announceNewView(ViewMetadata view, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     view.metadata.validate();

                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(view.keyspace);
                                     if (ksm == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", view.name, view.keyspace)));
                                     else if (ksm.getTableOrViewNullable(view.name) != null)
                                         return Completable.error(new AlreadyExistsException(view.keyspace, view.name));

                                     logger.info("Create new view: {}", view);
                                     return announce(SchemaKeyspace.makeCreateViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceNewType(UserType newType, boolean announceLocally)
    {
        return announceNewType(newType, announceLocally, FBUtilities.timestampMicros());
    }

    public static Completable forceAnnounceNewType(UserType newType)
    {
        return announceNewType(newType, false, 0);
    }

    public static Completable announceNewType(UserType newType, boolean announceLocally, long timestamp)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(newType.keyspace);
        return announce(SchemaKeyspace.makeCreateTypeMutation(ksm, newType, timestamp), announceLocally);
    }

    public static Completable announceNewFunction(UDFunction udf, boolean announceLocally)
    {
        logger.info("Create scalar function '{}'", udf.name());
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
        return announce(SchemaKeyspace.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static Completable announceNewAggregate(UDAggregate udf, boolean announceLocally)
    {
        logger.info("Create aggregate function '{}'", udf.name());
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
        return announce(SchemaKeyspace.makeCreateAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static Completable announceKeyspaceUpdate(KeyspaceMetadata ksm) throws ConfigurationException
    {
        return announceKeyspaceUpdate(ksm, false);
    }

    public static Completable announceKeyspaceUpdate(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     ksm.validate();

                                     KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
                                     if (oldKsm == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name)));

                                     logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
                                     return announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceTableUpdate(TableMetadata tm) throws ConfigurationException
    {
        return announceTableUpdate(tm, false);
    }

    public static Completable announceTableUpdate(TableMetadata updated, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     updated.validate();

                                     TableMetadata current = Schema.instance.getTableMetadata(updated.keyspace, updated.name);
                                     if (current == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", updated.name, updated.keyspace)));

                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(current.keyspace);

                                     current.validateCompatibility(updated);

                                     logger.info("Update table '{}/{}' From {} To {}", current.keyspace, current.name, current.toDebugString(), updated.toDebugString());
                                     return announce(SchemaKeyspace.makeUpdateTableMutation(ksm, current, updated, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceViewUpdate(ViewMetadata view, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     view.metadata.validate();

                                     ViewMetadata oldView = Schema.instance.getView(view.keyspace, view.name);
                                     if (oldView == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot update non existing materialized view '%s' in keyspace '%s'.", view.name, view.keyspace)));

                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(view.keyspace);

                                     oldView.metadata.validateCompatibility(view.metadata);

                                     logger.info("Update view '{}/{}' From {} To {}", view.keyspace, view.name, oldView, view);
                                     return announce(SchemaKeyspace.makeUpdateViewMutation(ksm, oldView, view, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceTypeUpdate(UserType updatedType, boolean announceLocally)
    {
        logger.info("Update type '{}.{}' to {}", updatedType.keyspace, updatedType.getNameAsString(), updatedType);
        return announceNewType(updatedType, announceLocally);
    }

    public static Completable announceKeyspaceDrop(String ksName) throws ConfigurationException
    {
        return announceKeyspaceDrop(ksName, false);
    }

    public static Completable announceKeyspaceDrop(String ksName, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
                                     if (oldKsm == null)
                                        return Completable.error(new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName)));

                                     logger.info("Drop Keyspace '{}'", oldKsm.name);
                                     return announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceTableDrop(String ksName, String cfName) throws ConfigurationException
    {
        return announceTableDrop(ksName, cfName, false);
    }

    public static Completable announceTableDrop(String ksName, String cfName, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     TableMetadata tm = Schema.instance.getTableMetadata(ksName, cfName);
                                     if (tm == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName)));

                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);

                                     logger.info("Drop table '{}/{}'", tm.keyspace, tm.name);
                                     return announce(SchemaKeyspace.makeDropTableMutation(ksm, tm, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceViewDrop(String ksName, String viewName, boolean announceLocally) throws ConfigurationException
    {
        return Completable.defer(() ->
                                 {
                                     ViewMetadata view = Schema.instance.getView(ksName, viewName);
                                     if (view == null)
                                         return Completable.error(new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", viewName, ksName)));
                                     KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);

                                     logger.info("Drop table '{}/{}'", view.keyspace, view.name);
                                     return announce(SchemaKeyspace.makeDropViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
                                 });
    }

    public static Completable announceTypeDrop(UserType droppedType, boolean announceLocally)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(droppedType.keyspace);
        return announce(SchemaKeyspace.dropTypeFromSchemaMutation(ksm, droppedType, FBUtilities.timestampMicros()), announceLocally);
    }

    public static Completable announceFunctionDrop(UDFunction udf, boolean announceLocally)
    {
        logger.info("Drop scalar function overload '{}' args '{}'", udf.name(), udf.argTypes());
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
        return announce(SchemaKeyspace.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    public static Completable announceAggregateDrop(UDAggregate udf, boolean announceLocally)
    {
        logger.info("Drop aggregate function overload '{}' args '{}'", udf.name(), udf.argTypes());
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
        return announce(SchemaKeyspace.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static Completable announce(Mutation.SimpleBuilder schema, boolean announceLocally)
    {
        SchemaMigration migration = new SchemaMigration(Collections.singletonList(schema.build()));

        if (announceLocally)
            return Completable.fromRunnable(() -> Schema.instance.merge(migration))
                              .subscribeOn(TPC.isTPCThread() ? StageManager.getScheduler(Stage.MIGRATION) :
                                           ImmediateThinScheduler.INSTANCE);
        else
            return announce(migration);
    }

    private static void pushSchemaMutation(InetAddress endpoint, SchemaMigration schema)
    {
        MessagingService.instance().send(Verbs.SCHEMA.PUSH.newRequest(endpoint, schema));
    }

    // Returns a future on the local application of the schema
    private static Completable announce(final SchemaMigration schema)
    {
        return Completable.fromRunnable(() ->
                                        {
                                            Schema.instance.mergeAndAnnounceVersion(schema);

                                            for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
                                            {
                                                // only push schema to nodes with known and equal versions
                                                if (!endpoint.equals(FBUtilities.getBroadcastAddress()) &&
                                                    MessagingService.instance().knowsVersion(endpoint) &&
                                                    MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version)
                                                    pushSchemaMutation(endpoint, schema);
                                            }
                                        }).subscribeOn(TPC.isTPCThread() ? StageManager.getScheduler(Stage.MIGRATION) :
                                                       ImmediateThinScheduler.INSTANCE);
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     *
     * @param version The schema version to announce
     */
    static void passiveAnnounce(UUID version)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        logger.debug("Gossiping my schema version {}", version);
    }

    /**
     * Clear all locally stored schema information and reset schema to initial state.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     */
    public static void resetLocalSchema()
    {
        logger.info("Starting local schema reset...");

        logger.debug("Truncating schema tables...");

        SchemaKeyspace.truncate();

        logger.debug("Clearing local schema keyspace definitions...");

        Schema.instance.clear();

        Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
        liveEndpoints.remove(FBUtilities.getBroadcastAddress());

        // force migration if there are nodes around
        for (InetAddress node : liveEndpoints)
        {
            if (shouldPullSchemaFrom(node))
            {
                logger.debug("Requesting schema from {}", node);
                FBUtilities.waitOnFuture(submitMigrationTask(node));
                break;
            }
        }

        logger.info("Local schema reset is complete.");
    }

}
