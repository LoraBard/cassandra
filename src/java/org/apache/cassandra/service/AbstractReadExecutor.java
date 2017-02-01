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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ReadCommand command;
    protected final List<InetAddress> targetReplicas;
    protected final ReadCallback handler;
    protected final TraceState traceState;

    AbstractReadExecutor(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas, long queryStartNanoTime)
    {
        this.command = command;
        this.targetReplicas = targetReplicas;
        this.handler = new ReadCallback(new DigestResolver(keyspace, command, consistencyLevel, targetReplicas.size()), consistencyLevel, command, targetReplicas, queryStartNanoTime);
        this.traceState = Tracing.instance.get();

        // Set the digest version (if we request some digests). This is the smallest version amongst all our target replicas since new nodes
        // knows how to produce older digest but the reverse is not true.
        // TODO: we need this when talking with pre-3.0 nodes. So if we preserve the digest format moving forward, we can get rid of this once
        // we stop being compatible with pre-3.0 nodes.
        int digestVersion = MessagingService.current_version;
        for (InetAddress replica : targetReplicas)
            digestVersion = Math.min(digestVersion, MessagingService.instance().getVersion(replica));
        command.setDigestVersion(digestVersion);
    }

    protected Completable makeDataRequests(Iterable<InetAddress> endpoints)
    {
        return makeRequests(command, endpoints);
    }

    protected Completable makeDigestRequests(Iterable<InetAddress> endpoints)
    {
        return makeRequests(command.copy().setIsDigestQuery(true), endpoints);
    }

    private Completable makeRequests(ReadCommand readCommand, Iterable<InetAddress> endpoints)
    {

            boolean hasLocalEndpoint = false;

            for (InetAddress endpoint : endpoints)
            {
                if (StorageProxy.canDoLocalRequest(endpoint))
                {
                    hasLocalEndpoint = true;
                    continue;
                }

                if (traceState != null)
                    traceState.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);
                logger.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);
                MessageOut<ReadCommand> message = readCommand.createMessage();
                MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
            }

            // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
            if (hasLocalEndpoint)
            {
                //logger.trace("reading {} locally", readCommand.isDigestQuery() ? "digest" : "data");

                if (command instanceof SinglePartitionReadCommand)
                {
                    SinglePartitionReadCommand singleCommand = (SinglePartitionReadCommand) command;
                    Scheduler scheduler = NettyRxScheduler.getForKey(command.metadata().ksName, singleCommand.partitionKey(), false);
                    handler.setLocalCoreId(NettyRxScheduler.getCoreId());
                    return executeLocalRead().subscribeOn(scheduler);
                }
                else
                {
                    Scheduler scheduler = NettyRxScheduler.instance();
                    handler.setLocalCoreId(NettyRxScheduler.getCoreId());
                    return executeLocalRead().subscribeOn(scheduler);
                }
            }

            return Completable.complete();
    }

    private Completable executeLocalRead()
    {
        return Completable.defer(
        () ->
        {
            final long start = System.nanoTime();
            final long constructionTime = System.currentTimeMillis();
            MessagingService.Verb verb = MessagingService.Verb.READ;

            ReadExecutionController executionController = command.executionController();
            return command.executeLocally(executionController)
                          .map(iterator -> command.createResponse(iterator))
                          .flatMapCompletable(response ->
                                              {
                                                  executionController.close();

                                                  if (command.complete())
                                                  {
                                                      handler.response(response);
                                                  }
                                                  else
                                                  {
                                                      MessagingService.instance().incrementDroppedMessages(verb, System.currentTimeMillis() - constructionTime);
                                                      handler.onFailure(FBUtilities.getBroadcastAddress(), RequestFailureReason.UNKNOWN);
                                                  }

                                                  MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                                                  return Completable.complete();
                                              })
                          .onErrorResumeNext(t ->
                                             {
                                                 executionController.close();

                                                 if (t instanceof TombstoneOverwhelmingException)
                                                 {
                                                     handler.onFailure(FBUtilities.getBroadcastAddress(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                                                     logger.error(t.getMessage());
                                                     return Completable.complete();
                                                 }
                                                 else
                                                 {
                                                     handler.onFailure(FBUtilities.getBroadcastAddress(), RequestFailureReason.UNKNOWN);
                                                     return Completable.error(t);
                                                 }
                                             });
        });
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract Completable maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract Collection<InetAddress> getContactedReplicas();

    /**
     * send the initial set of requests
     */
    public abstract Completable executeAsync();

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public Single<PartitionIterator> get()
    {
        return handler.get();
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.partitionKey());
        // 11980: Excluding EACH_QUORUM reads from potential RR, so that we do not miscount DC responses
        ReadRepairDecision repairDecision = consistencyLevel == ConsistencyLevel.EACH_QUORUM
                                            ? ReadRepairDecision.NONE
                                            : command.metadata().newReadRepairDecision();
        List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, allReplicas, repairDecision);

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas);

        if (repairDecision != ReadRepairDecision.NONE)
        {
            Tracing.trace("Read-repair {}", repairDecision);
            ReadRepairMetrics.attempted.mark();
        }

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().cfId);
        SpeculativeRetryParam retry = cfs.metadata.params.speculativeRetry;

        // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
        // 11980: Disable speculative retry if using EACH_QUORUM in order to prevent miscounting DC responses
        if (retry.equals(SpeculativeRetryParam.NONE)
            || consistencyLevel == ConsistencyLevel.EACH_QUORUM
            || consistencyLevel.blockFor(keyspace) == allReplicas.size())
            return new NeverSpeculatingReadExecutor(keyspace, command, consistencyLevel, targetReplicas, queryStartNanoTime);

        if (targetReplicas.size() == allReplicas.size())
        {
            // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
            // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
            // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
            return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas, queryStartNanoTime);
        }

        // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
        // we might have to find a replacement that's not already in targetReplicas.
        if (repairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }
        targetReplicas.add(extraReplica);

        if (retry.equals(SpeculativeRetryParam.ALWAYS))
            return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas, queryStartNanoTime);
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas, queryStartNanoTime);
    }

    public static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public NeverSpeculatingReadExecutor(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas, long queryStartNanoTime)
        {
            super(keyspace, command, consistencyLevel, targetReplicas, queryStartNanoTime);
        }

        public Completable executeAsync()
        {
            Completable result = makeDataRequests(targetReplicas.subList(0, 1));
            if (targetReplicas.size() > 1)
                result = result.concatWith(makeDigestRequests(targetReplicas.subList(1, targetReplicas.size())));

            return result;
        }

        public Completable maybeTryAdditionalReplicas()
        {
            // no-op
            return Completable.complete();
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(Keyspace keyspace,
                                       ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas,
                                       long queryStartNanoTime)
        {
            super(keyspace, command, consistencyLevel, targetReplicas, queryStartNanoTime);
            this.cfs = cfs;
        }

        public Completable executeAsync()
        {
            // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
            // that the last replica in our list is "extra."
            List<InetAddress> initialReplicas = targetReplicas.subList(0, targetReplicas.size() - 1);

            Completable result;
            if (handler.blockfor < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                result = makeDataRequests(initialReplicas.subList(0, 2));
                if (initialReplicas.size() > 2)
                    result = result.concatWith(makeDigestRequests(initialReplicas.subList(2, initialReplicas.size())));
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                result = makeDataRequests(initialReplicas.subList(0, 1));
                if (initialReplicas.size() > 1)
                    result = result.concatWith(makeDigestRequests(initialReplicas.subList(1, initialReplicas.size())));
            }

            return result;
        }

        public Completable maybeTryAdditionalReplicas()
        {
            return Completable.defer(
            () ->
            {
                // no latency information, or we're overloaded
                if (cfs.keyspace.getReplicationStrategy().getReplicationFactor() == 1 ||
                    cfs.sampleLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                    return CompletableObserver::onComplete;

                NettyRxScheduler.instance().scheduleDirect(() ->
                                                           {
                                                               if (handler.hasValue())
                                                                   return;

                                                               // Could be waiting on the data, or on enough digests.
                                                               ReadCommand retryCommand = command;
                                                               if (handler.resolver.isDataPresent())
                                                                   retryCommand = command.copy().setIsDigestQuery(true);

                                                               InetAddress extraReplica = Iterables.getLast(targetReplicas);
                                                               if (traceState != null)
                                                                   traceState.trace("speculating read retry on {}", extraReplica);
                                                               logger.trace("speculating read retry on {}", extraReplica);
                                                               MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(), extraReplica, handler);
                                                               speculated = true;

                                                               cfs.metric.speculativeRetries.inc();
                                                           }, cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);

                return CompletableObserver::onComplete;
            });
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return speculated
                 ? targetReplicas
                 : targetReplicas.subList(0, targetReplicas.size() - 1);
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;

        public AlwaysSpeculatingReadExecutor(Keyspace keyspace,
                                             ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas,
                                             long queryStartNanoTime)
        {
            super(keyspace, command, consistencyLevel, targetReplicas, queryStartNanoTime);
            this.cfs = cfs;
        }

        public Completable maybeTryAdditionalReplicas()
        {
            // no-op
            return Completable.complete();
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }

        @Override
        public Completable executeAsync()
        {
            Completable result = makeDataRequests(targetReplicas.subList(0, targetReplicas.size() > 1 ? 2 : 1));
            if (targetReplicas.size() > 2)
                result = result.concatWith(makeDigestRequests(targetReplicas.subList(2, targetReplicas.size())));

            cfs.metric.speculativeRetries.inc();
            return result;
        }
    }
}
