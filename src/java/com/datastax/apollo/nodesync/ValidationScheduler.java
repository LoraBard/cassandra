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
package com.datastax.apollo.nodesync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

/**
 * Continuously schedules the segment validations to be ran (by an {@link ValidationExecutor}) for NodeSync.
 * <p>
 * A scheduler gets validation proposals ({@link ValidationProposal}) from a set of {@link ValidationProposer} and
 * prioritize them for execution. At a high level, it can be seen as a multiplexer of validation proposals where the
 * inputs are the configured {@link ValidationProposer} (at least one for each table with NodeSync enabled) and the
 * output is consumed by {@link ValidationExecutor} (which runs the corresponding validations).
 * <p>
 * Note that priority between proposals is not decided by this class but is rather directly implemented by the
 * comparison on {@link ValidationProposal}, so this class main job is to merge the proposal from multiple proposers
 * in a way that respects priority and is thread-safe (individual {@link ValidationProposer} are not thread-safe but
 * {@link ValidationExecutor} is multi-threaded).
 * <p>
 * This class also handles the default per-table {@link ContinuousTableValidationProposer} necessary to NodeSync,
 * creating one for every table with NodeSync enabled at startup and maintaining that set when table are created,
 * updated or removed.
 * <p>
 * This class and all it's publicly-visible methods <b>ARE</b> thread-safe.
 */
class ValidationScheduler extends SchemaChangeListener implements IEndpointLifecycleSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationScheduler.class);

    private final NodeSyncService service;
    /**
     * Sets of the active validation proposers. The fact it is a set is important to guarantee for instance that we don't
     * end up with 2 {@link ContinuousTableValidationProposer} object for the same table, see {@link ContinuousTableValidationProposer#equals(Object)}
     * (Note: we don't use the alternative of making this a map keyed by {@link TableMetadata} because 1) we want to
     * leave open the possibility to have non-table based proposers and 2) because while we don't want 2
     * {@link ContinuousTableValidationProposer} for the same table, we might want 2 object of different type of proposers
     * even if they are on the same table (to implement user-forced validation cleanly for instance)).
     */
    private final Set<ValidationProposer> proposers;
    private final PriorityQueue<ValidationProposal> proposalQueue;

    // Note: we rely on the lock being re-entrant in queueProposal() below
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasProposals = lock.newCondition();

    // Only volatile because only _updated_ within the lock
    private volatile long queuedProposals;

    private volatile boolean isShutdown;

    /**
     * The actions we perform on either schema changes or topology changes are costly-ish (they may involve reading
     * distributed system tables if we add/init a new proposer for instance), but by default the corresponding listener
     * methods are done on threads that may not expect those calls to take time. Typically, {@link IEndpointLifecycleSubscriber}
     * methods are executed on {@link Stage#GOSSIP} and we don't want to slow gossip. So we use a simple single thread
     * executor to offload the execution of those change.
     * Note that both schema and topology changes are basically very infrequent, and acting on them for NodeSync is not
     * performance critical either, so there is probably no reason to use more than one thread.
     */
    private final ExecutorService eventExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("ValidationSchedulerEventExecutor"));

    private final UserValidations userValidations;

    ValidationScheduler(NodeSyncService service)
    {
        this.service = service;
        this.proposers = new HashSet<>();
        this.proposalQueue = new PriorityQueue<>();
        this.userValidations = new UserValidations(this);
    }

    NodeSyncService service()
    {
        return service;
    }

    void createInitialProposers()
    {
        List<ContinuousTableValidationProposer> initialProposers = ContinuousTableValidationProposer.createAll(service);
        if (!initialProposers.isEmpty())
        {
            logger.debug("Adding NodeSync validation proposer for tables {}", toTableNamesString(initialProposers));
            addAll(initialProposers);
        }
    }

    private static String toTableNamesString(List<? extends AbstractValidationProposer> proposers)
    {
        return "{ " + Joiner.on(", ").join(Iterables.transform(proposers, AbstractValidationProposer::table)) + " }";
    }

    UserValidations userValidations()
    {
        return userValidations;
    }

    /**
     * Adds a new proposer to this scheduler.
     *
     * @param proposer the proposer to add.
     * @return {@code true} if the proposer was added, {@code false} if it was already present and was thus not added.
     */
    boolean add(ValidationProposer proposer)
    {
        lock.lock();
        try
        {
            return addProposer(proposer);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Adds new proposers to this scheduler. Any proposer that was already part of the scheduler will be ignored.
     *
     * @param proposers the proposers to add.
     * @return {@code true} if <b>any</b> of the proposers has been added, {@code false} if all the proposers were
     * already present in the scheduler and thus skipped.
     */
    boolean addAll(Collection<? extends ValidationProposer> proposers)
    {
        // Don't bother with the lock if we don't have to.
        if (proposers.size() == 0)
            return false;

        lock.lock();
        try
        {
            boolean addedAny = false;
            for (ValidationProposer proposer : proposers)
                addedAny |= addProposer(proposer);
            return addedAny;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Removes a proposer from this scheduler. This is a no-op if the scheduler didn't had the provided proposer.
     *
     * @param proposer the proposer to remove.
     * @return {@code true} if {@code proposer} was removed, {@code false} if the scheduler didn't had that proposer.
     */
    boolean remove(ValidationProposer proposer)
    {
        lock.lock();
        try
        {
            return removeProposer(proposer, true);
        }
        finally
        {
            lock.unlock();
        }
    }

    // *Must* only be called while holding the lock
    private boolean addProposer(ValidationProposer proposer)
    {
        // Don't initialize a proposer if it was a duplicate for a proposer we already had; this avoid duplicate work.
        boolean added = proposers.add(proposer);
        if (added)
        {
            proposer.init();
            requeue(proposer);
        }
        return added;
    }

    // *Must* only be called while holding the lock
    private boolean removeProposer(ValidationProposer proposer, boolean checkForQueuedProposals)
    {
        boolean removed = proposers.remove(proposer);
        if (removed && checkForQueuedProposals)
            proposalQueue.removeIf(p -> p.proposer().equals(proposer));
        return removed;
    }

    private void mapOnProposers(Function<ValidationProposer, ValidationProposer> fct)
    {
        lock.lock();
        try
        {
            List<ValidationProposer> toAdd = new ArrayList<>();
            List<ValidationProposer> toRemove = new ArrayList<>();
            for (ValidationProposer proposer : proposers)
            {
                ValidationProposer updated = fct.apply(proposer);
                if (updated == proposer)
                    continue;

                // Either updated is null, meaning we should remove the proposer, or it's an updated version and we
                // have to remove the current one and add the new one.
                toRemove.add(proposer);
                if (updated != null)
                    toAdd.add(updated);
            }

            // The order below don't matter much, but do the remove first so we don't grow proposalQueue unnecessarily
            toRemove.forEach(c -> removeProposer(c, true));
            toAdd.forEach(this::addProposer);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * How many proposers are currently active.
     */
    int proposersCount()
    {
        return proposers.size();
    }

    /**
     * How many proposals were queued (and so generated by proposers) since the scheduler was created.
     * <p>
     * The main use of this value is that if it don't increase for a given period of time, it means nothing has been
     * done by the scheduler.
     */
    long queuedProposals()
    {
        return queuedProposals;
    }

    /**
     * Returns the next validation to be ran.
     * <p>
     * This method <b>may</b> block if there is no validation to be done at the current time (including the case where
     * no table have NodeSync currently activated and so the scheduler has no proposers set) and this until a validation
     * has to be done (there is no timeout).
     *
     * @return a {@link Validator} corresponding to the next validation to be ran, blocking if necessary until a
     * validation has to be run.
     *
     * @throws ShutdownException if the scheduler has been shutdown.
     */
    Validator getNextValidation()
    {
        if (isShutdown)
            throw new ShutdownException();

        while (true)
        {
            ValidationProposal proposal = getProposal();
            // Note: activating a proposal is potentially costly so having that done outside the lock is very much
            // on purpose.
            Validator validator = proposal.activate();
            if (validator != null)
                return validator;
        }
    }

    /**
     * Shutdown this scheduler. The main effect of this call is to make any ongoing and future calls to
     * {@link #getNextValidation()} to throw a {@link ShutdownException}.
     */
    void shutdown()
    {
        // We need to set isShutdown, but also unblock any thread that is currently waiting on the hasProposals condition
        lock.lock();
        try
        {
            if (isShutdown)
                return;

            isShutdown = true;
            hasProposals.signalAll();
        }
        finally
        {
            lock.unlock();
        }

    }

    private ValidationProposal getProposal()
    {
        ValidationProposal proposal;
        lock.lock();
        try
        {
            while ((proposal = proposalQueue.poll()) == null)
            {
                if (isShutdown)
                    throw new ShutdownException();

                hasProposals.awaitUninterruptibly();
            }

            // Requeue before leaving the lock. This ensures that if the corresponding ValidationProposer has priority
            // over all others, all his proposals get taken before any other.
            requeue(proposal.proposer());
            return proposal;
        }
        finally
        {
            lock.unlock();
        }
    }

    // This *must* be called while holding the lock
    private void requeue(ValidationProposer proposer)
    {
        if (!proposer.supplyNextProposal(this::queueProposal))
        {
            // We know we don't any proposal for this proposer in the queue, hence the false below
            removeProposer(proposer, false);
        }
    }

    // This needs the lock but may or may not be called while already holding it, so we acquire said lock and rely
    // on re-entrancy if it was hold already.
    private void queueProposal(ValidationProposal proposal)
    {
        lock.lock();
        try
        {
            proposalQueue.add(proposal);
            ++queuedProposals;
            hasProposals.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    /*
     * IEndpointLifecycleSubscriber methods
     *
     * These method handle actions to be taken on topology changes.
     */

    public void onJoinCluster(InetAddress endpoint)
    {
        // On single node cluster, we don't create any proposers since nothing can be validated. So now that we have
        // another node, we should add the proposers for any NodeSync-enabled table. Note that this is not the only situation
        // where we could have 0 proposers, this could also be due to no table being NodeSync-enabled/no keyspace having RF>1.
        // Calling ContinuousTableValidationProposer.createAll() is harmless in those case however, it will simply return an
        // empty list. And since join events are pretty rare and createAll is pretty cheap particularly in those cases, ...
        if (proposersCount() == 0)
        {
            eventExecutor.execute(() -> {
                List<ContinuousTableValidationProposer> proposers = ContinuousTableValidationProposer.createAll(service);
                if (!proposers.isEmpty())
                {
                    logger.info("{} has joined the cluster: starting NodeSync validations on tables {} as consequence",
                                endpoint, toTableNamesString(proposers));
                    addAll(proposers);
                }
            });
        }
        else
        {
            // TODO: that mean some tokens will have changed/be added which might impact our local ranges and thus the
            // segment we should generate. So we could re-general our "cached" proposals to take new range into account,
            // but I'm not 100% sure if we "could" or truly "should": that is, while we still use the "old" segments,
            // the node might end up querying ranges that are not strict subsets of the range it owns, which should be
            // harmless-ish (slightly inefficient but as it's very temporary), but need to double-check.
        }
    }

    public void onLeaveCluster(InetAddress endpoint)
    {
        // TODO: same as for onJoinCluster regarding token changes. Aside from that, not sure if we want to bother about
        // the leaving node making us a single node cluster. In theory this would be cleaner, but 1) going back to a
        // single node cluster while having keyspace with RF > 1 is really an edge case while 2) I'm not sure the methods
        // from IEndpointLifecycleSubscribed are guaranteed to be serialized so I wouldn't want to risk having NodeSync
        // basically break (by not adding the proposers when we should) if there is a leave/join race.
    }

    public void onUp(InetAddress endpoint)
    {
        // Nothing to do for this for now (though we could imagine to look up and re-prioritize segment that have been somewhat
        // recently validated but only partially due to the absence of this node; not trivial to mix that properly with
        // normal prioritization though so not v1 material).
    }

    public void onDown(InetAddress endpoint)
    {
        // Nothing to do for this for now (but as for onUp, we could imagine de-prioritizing (a bit but probably not
        // totally) segments for which this is a replica, so we focus on other segments first, so that if the node is
        // back relatively quickly, we end up limiting the segment we only partially validate but without losing much
        // progress; similarly not v1 material).
    }

    public void onMove(InetAddress endpoint)
    {
        // TODO: same onJoin/onLeave
    }

    /*
     * SchemaChangeListener methods.
     *
     * We check for any event that implies some table proposer should be either added or removed.
     *
     * Note: we don't bother with onCreateKeyspace/onDropKeyspace because we'll get individual notifications for any
     * table that is affected by those.
     */

    public void onAlterKeyspace(String keyspace)
    {
        // We should handle RF changes from and to 1. Namely, if the RF is increased from 1, we should consider any
        // table from the table for inclusion, while if it's decreased to 1, we should remove all tables.
        // Note that we actually cannot know what was the RF before the ALTER, only what it is now (something we should
        // change, but that's another story), so we simply blindly add/remove tables depending on the current RF and
        // rely on the fact that ContinuousTableValidationProposer equality is solely based on the table (by design), and so
        // this will do the right thing.
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        // Shouldn't happen I suppose in practice, but could imagine a race between ALTER and DROP getting us this so
        // no point in failing
        if (ks == null)
            return;

        eventExecutor.execute(() -> {
            if (ks.getReplicationStrategy().getReplicationFactor() <= 1)
            {
                Set<TableMetadata> removed = new HashSet<>();
                ks.getColumnFamilyStores().forEach(s -> {
                    if (removeContinuousProposerFor(s.metadata()))
                        removed.add(s.metadata());
                });
                if (!removed.isEmpty())
                    logger.info("Stopping NodeSync validations on tables {} because keyspace {} is not replicated anymore",
                                removed, keyspace);
            }
            else
            {
                List<ContinuousTableValidationProposer> proposers = ContinuousTableValidationProposer.createForKeyspace(service, ks);
                if (addAll(proposers))
                {
                    // As mentioned above, it's absolutely possible the addition above was a complete no-op, but if it
                    // wasn't, that (almost surely, we could have raced with another change, but that's sufficiently
                    // unlikely that we ignore it for the purpose of logging) means the RF of the keyspace has just
                    // been increased.
                    logger.info("Starting NodeSync validations on tables {} following increase of the replication factor on {}",
                                toTableNamesString(proposers), keyspace);
                }
            }
        });
    }

    public void onCreateTable(String keyspace, String table)
    {
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
        // As always, protect against hypothetical race with a drop, no matter how unlikely this is.
        if (store == null)
            return;

        eventExecutor.execute(() ->
                              ContinuousTableValidationProposer.create(service, store)
                                                               .ifPresent(p -> {
                                                                   if (add(p))
                                                                       logger.info("Starting NodeSync validations on newly created table {}",
                                                                                   store.metadata());
                                                               }));
    }

    public void onAlterTable(String keyspace, String table, boolean affectsStatements)
    {
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
        // As always, protect against hypothetical race with a drop, no matter how unlikely this is.
        if (store == null)
            return;

        eventExecutor.execute(() -> {
            // We want to keep things as generic and flexible for future new implementations of ValidationProposer, so when a table
            // is updated, we let each proposer tell us exactly if it's affected and how.
            mapOnProposers(c -> c.onTableUpdate(store.metadata()));

            // On top of updating any existing proposer, we may have to create a new ContinuousTableValidationProposer if the alter
            // happens to have just turned NodeSync on for this table. As we can't really know what the alter did, we simply
            // blindly add a new proposer is NodeSync is enabled now and let that be a no-op if NodeSync was already on before (we
            // waste the proposer proposer on any other type of alter, which is a shame, but the right way to fix this is to
            // improve the listener interface and in the meantime, schema change are comparatively rare events so a little
            // inefficiently don't matter concretely (outside of hurting our feelings that is)).
            ContinuousTableValidationProposer.create(service, store).ifPresent(p -> {
                if (add(p))
                    logger.info("Starting NodeSync validations on table {} following user activation", store.metadata());
            });
        });
    }

    public void onDropTable(String keyspace, String table)
    {
        // Same on alter, we let proposer tell us if they are affected by the drop.
        eventExecutor.execute(() -> mapOnProposers(c -> c.onTableRemoval(keyspace, table)));
    }

    private boolean removeContinuousProposerFor(TableMetadata metadata)
    {
        return remove(ContinuousTableValidationProposer.dummyProposerFor(metadata));
    }

    /**
     * Thrown by {@link #getNextValidation()} when the scheduler has been shutdown.
     */
    static class ShutdownException extends RuntimeException {}
}
