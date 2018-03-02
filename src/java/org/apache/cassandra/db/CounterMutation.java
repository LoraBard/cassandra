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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import java.util.Objects;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;

import io.reactivex.Completable;
import io.reactivex.Single;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.WriteVerbs.WriteVersion;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.CoordinatedAction;
import org.apache.cassandra.utils.concurrent.ExecutableLock;
import org.apache.cassandra.utils.flow.RxThreads;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class CounterMutation implements IMutation, SchedulableMessage
{
    public static final Versioned<WriteVersion, Serializer<CounterMutation>> serializers = WriteVersion.versioned(CounterMutationSerializer::new);

    /**
     *   These are the striped locks that are shared by all threads that perform counter mutations. Locks are
     *   taken by metadata id, partition and clustering keys, and the counter column name,
     *   see {@link CounterMutation#getLocks()}.
     *   <p>
     *   We use semaphores rather than locks because the same thread may have several counter mutations
     *   ongoing at the same time in an asynchronous fashion. So reentrant locks would not protect against
     *   the same thread starting a new counter mutation before finishing a previous one on the same counter.
     *   <p>
     *   If we were sure that at any given time a partition key is handled by only a TPC thread, then we
     *   could make LOCKS smaller and thread local. However, there are cases when this is currently
     *   not true, such us before StorageService is initialized or when the topology changes.
     */
    private static final Striped<Semaphore> SEMAPHORES_STRIPED = Striped.semaphore(TPC.getNumCores() * 1024, 1);
    private static final ConcurrentMap<Semaphore, Pair<Long, ExecutableLock>> LOCKS = new ConcurrentHashMap<>();
    private static final AtomicLong LOCK_ID_GEN = new AtomicLong();

    private final Mutation mutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(Mutation mutation, ConsistencyLevel consistency)
    {
        this.mutation = mutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Collection<TableId> getTableIds()
    {
        return mutation.getTableIds();
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public DecoratedKey key()
    {
        return mutation.key();
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    /**
     * Applies the counter mutation, returns the result Mutation (for replication to other nodes).
     *
     * 1. Grabs the striped cell-level locks in the proper order
     * 2. Gets the current values of the counters-to-be-modified from the counter cache
     * 3. Reads the rest of the current values (cache misses) from the CF
     * 4. Writes the updated counter values
     * 5. Updates the counter cache
     * 6. Releases the lock(s)
     *
     * See CASSANDRA-4775 and CASSANDRA-6504 for further details.
     *
     * @return the applied resulting Mutation
     */
    public CompletableFuture<Mutation> applyCounterMutation()
    {
        return applyCounterMutation(System.currentTimeMillis());
    }

    public StagedScheduler getScheduler()
    {
        return mutation.getScheduler();
    }

    public TracingAwareExecutor getRequestExecutor()
    {
        return mutation.getRequestExecutor();
    }

    public TracingAwareExecutor getResponseExecutor()
    {
        return mutation.getResponseExecutor();
    }

    /**
     * Helper method for {@link #applyCounterMutation(long)}. Performs the actual work with the locks
     * available.
     *
     * @return a single that will complete when the mutation is applied
     */
    private Single<Mutation> applyCounterMutationInternal()
    {
        final Mutation result = new Mutation(getKeyspaceName(), key());
        Completable ret = Completable.concat(getPartitionUpdates()
                                            .stream()
                                            .map(this::processModifications)
                                            .map(single -> single.flatMapCompletable(upd -> Completable.fromRunnable(() -> result.add(upd))))
                                            .collect(Collectors.toList()));
        return RxThreads.subscribeOn(ret.andThen(result.applyAsync()).toSingleDefault(result),
                                     getScheduler(),
                                     TPCTaskType.COUNTER_ACQUIRE_LOCK);
    }

    public void apply()
    {
        TPCUtils.blockingGet(applyCounterMutation());
    }

    public Completable applyAsync()
    {
        return TPCUtils.toCompletable(applyCounterMutation().thenAccept(mutation -> {}));
    }

    /**
     * Update the views via the the supplied action by first acquiring locks over the given mutation updates;
     * each lock is acquired in order based on its id, so to avoid deadlocks.
     *
     * @return The future that will be completed when all locks are acquired and the action executed.
     * Locks are released after such future completes.
     */
    private CompletableFuture<Mutation> applyCounterMutation(long startTime)
    {
        Tracing.trace("Acquiring counter locks");
        SortedMap<Long, ExecutableLock> locks = getLocks();

        if (locks.isEmpty())
            return TPCUtils.toFuture(applyCounterMutationInternal()); // it can happen if the counter updates an unset value

        return TPCUtils.withLocks(locks,
                                  startTime,
                                  DatabaseDescriptor.getCounterWriteRpcTimeout(),
                                  () -> TPCUtils.toFuture(applyCounterMutationInternal()),
                                  err -> {
                                      Tracing.trace("Failed to acquire locks for counter mutation for longer than {} millis, giving up",
                                                    DatabaseDescriptor.getCounterWriteRpcTimeout());
                                      Keyspace keyspace = Keyspace.open(getKeyspaceName());
                                      return new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace));
                                  });
    }

    /**
     * Get locks for the given counter mutation partition updates in sorted order and into a map to get rid of duplicates
     * (which could arise due to underlying striping). IMPORTANT: the locks must be acquired in the provided order,
     * to avoid deadlocks.
     */
    private SortedMap<Long, ExecutableLock> getLocks()
    {
        SortedMap<Long, ExecutableLock> ret = new TreeMap<>(Comparator.naturalOrder());

        for (PartitionUpdate update : getPartitionUpdates())
        {
            for (Row row : update)
            {
                for (ColumnData data : row)
                {
                    int hash = Objects.hash(update.metadata().id, key(), row.clustering(), data.column());
                    Semaphore semaphore = SEMAPHORES_STRIPED.get(hash);
                    Pair<Long, ExecutableLock> p = LOCKS.computeIfAbsent(semaphore, s -> Pair.create(LOCK_ID_GEN.incrementAndGet(), new ExecutableLock(s)));
                    ret.put(p.left, p.right);
                }
            }
        }

        return ret;
    }

    private Single<PartitionUpdate> processModifications(PartitionUpdate changes)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changes.metadata().id);

        List<PartitionUpdate.CounterMark> marks = changes.collectCounterMarks();

        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", marks.size());
            updateWithCurrentValuesFromCache(marks, cfs);
            if (marks.isEmpty())
                return Single.just(changes);
        }

        Tracing.trace("Reading {} counter values from the CF", marks.size());
        return updateWithCurrentValuesFromCFS(marks, cfs).andThen(Single.fromCallable(() -> {
            // What's remain is new counters
            for (PartitionUpdate.CounterMark mark : marks)
                updateWithCurrentValue(mark, ClockAndCount.BLANK, cfs);
            return changes;
        }));
    }

    private void updateWithCurrentValue(PartitionUpdate.CounterMark mark, ClockAndCount currentValue, ColumnFamilyStore cfs)
    {
        long clock = Math.max(FBUtilities.timestampMicros(), currentValue.clock + 1L);
        long count = currentValue.count + CounterContext.instance().total(mark.value());

        mark.setValue(CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count));

        // Cache the newly updated value
        cfs.putCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path(), ClockAndCount.create(clock, count));
    }

    // Returns the count of cache misses.
    private void updateWithCurrentValuesFromCache(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        Iterator<PartitionUpdate.CounterMark> iter = marks.iterator();
        while (iter.hasNext())
        {
            PartitionUpdate.CounterMark mark = iter.next();
            ClockAndCount cached = cfs.getCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path());
            if (cached != null)
            {
                updateWithCurrentValue(mark, cached, cfs);
                iter.remove();
            }
        }
    }

    // Reads the missing current values from the CFS.
    private Completable updateWithCurrentValuesFromCFS(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        BTreeSet.Builder<Clustering> names = BTreeSet.builder(cfs.metadata().comparator);
        for (PartitionUpdate.CounterMark mark : marks)
        {
            if (mark.clustering() != Clustering.STATIC_CLUSTERING)
                names.add(mark.clustering());
            if (mark.path() == null)
                builder.add(mark.column());
            else
                builder.select(mark.column(), mark.path());
        }

        int nowInSec = FBUtilities.nowInSeconds();
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names.build(), false);
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, key(), builder.build(), filter);
        PeekingIterator<PartitionUpdate.CounterMark> markIter = Iterators.peekingIterator(marks.iterator());

        return Completable.using(() -> cmd.executionController(),
                                 controller -> cmd.deferredQuery(cfs, controller)
                                                  .flatMapCompletable(p ->
                                                  {
                                                      FlowablePartition partition = FlowablePartitions.filter(p, nowInSec);
                                                      updateForRow(markIter, partition.staticRow(), cfs);

                                                      return partition.content()
                                                                      .takeWhile((row) -> markIter.hasNext())
                                                                      .processToRxCompletable(row -> updateForRow(markIter, row, cfs));
                                                  }),
                                 controller -> controller.close());
    }

    private int compare(Clustering c1, Clustering c2, ColumnFamilyStore cfs)
    {
        if (c1 == Clustering.STATIC_CLUSTERING)
            return c2 == Clustering.STATIC_CLUSTERING ? 0 : -1;
        if (c2 == Clustering.STATIC_CLUSTERING)
            return 1;

        return cfs.getComparator().compare(c1, c2);
    }

    private void updateForRow(PeekingIterator<PartitionUpdate.CounterMark> markIter, Row row, ColumnFamilyStore cfs)
    {
        int cmp = 0;
        // If the mark is before the row, we have no value for this mark, just consume it
        while (markIter.hasNext() && (cmp = compare(markIter.peek().clustering(), row.clustering(), cfs)) < 0)
            markIter.next();

        if (!markIter.hasNext())
            return;

        while (cmp == 0)
        {
            PartitionUpdate.CounterMark mark = markIter.next();
            Cell cell = mark.path() == null ? row.getCell(mark.column()) : row.getCell(mark.column(), mark.path());
            if (cell != null)
            {
                updateWithCurrentValue(mark, CounterContext.instance().getLocalClockAndCount(cell.value()), cfs);
                markIter.remove();
            }
            if (!markIter.hasNext())
                return;

            cmp = compare(markIter.peek().clustering(), row.clustering(), cfs);
        }
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        return String.format("CounterMutation(%s, %s)", mutation.toString(shallow), consistency);
    }

    private static class CounterMutationSerializer extends VersionDependent<WriteVersion> implements Serializer<CounterMutation>
    {
        private CounterMutationSerializer(WriteVersion version)
        {
            super(version);
        }

        public void serialize(CounterMutation cm, DataOutputPlus out) throws IOException
        {
            Mutation.serializers.get(version).serialize(cm.mutation, out);
            out.writeUTF(cm.consistency.name());
        }

        public CounterMutation deserialize(DataInputPlus in) throws IOException
        {
            Mutation m = Mutation.serializers.get(version).deserialize(in);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm)
        {
            return Mutation.serializers.get(version).serializedSize(cm.mutation)
                   + TypeSizes.sizeof(cm.consistency.name());
        }
    }
}
