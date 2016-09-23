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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    public static final MemtablePool MEMORY_POOL = createMemtableAllocatorPool();

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
                return new HeapPool(heapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                return new SlabPool(heapLimit, 0, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable)
                {
                    throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                }
                return new SlabPool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                return new NativePool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();

    public int compareTo(Memtable that)
    {
        return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
    }

    public static final class LastCommitLogPosition extends CommitLogPosition
    {
        public LastCommitLogPosition(CommitLogPosition copy) {
            super(copy.segmentId, copy.position);
        }
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final List<HashMap<PartitionPosition, AtomicBTreePartition>> partitions;
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();
    private final List<PartitionPosition> rangeList;
    private final boolean hasSplits;

    // The smallest timestamp for all partitions stored in this memtable
    private long minTimestamp = Long.MAX_VALUE;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final ColumnsCollector columnsCollector;
    private final StatsCollector statsCollector = new StatsCollector();

    // only to be used by init(), to setup the very first memtable for the cfs
    public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.commitLogLowerBound = commitLogLowerBound;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        this.cfs.scheduleFlush();
        this.columnsCollector = new ColumnsCollector(cfs.metadata.partitionColumns());
        this.hasSplits = !cfs.hasSpecialHandlingForTPC;
        this.rangeList = generateRangeList(cfs);
        this.partitions = generatePartitionMaps();
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(CFMetaData metadata)
    {
        this.initialComparator = metadata.comparator;
        this.cfs = null;
        this.allocator = null;
        this.columnsCollector = new ColumnsCollector(metadata.partitionColumns());
        this.hasSplits = false;
        this.rangeList = generateRangeList(null);
        this.partitions = generatePartitionMaps();
    }

    private List<PartitionPosition> generateRangeList(ColumnFamilyStore cfs)
    {
        if (!hasSplits)
            return null;

        return NettyRxScheduler.getRangeList(cfs);
    }

    private List<HashMap<PartitionPosition, AtomicBTreePartition>> generatePartitionMaps()
    {
        if (!hasSplits)
            return Collections.singletonList(new HashMap<>(256));

        int capacity = rangeList.size();
        ArrayList<HashMap<PartitionPosition, AtomicBTreePartition>> partitionMapContainer = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++)
            partitionMapContainer.add(new HashMap<>(1024));
        return partitionMapContainer;
    }

    private HashMap<PartitionPosition, AtomicBTreePartition> getPartitionMapFor(PartitionPosition key)
    {
        if (!hasSplits)
            return partitions.get(0);

        PartitionPosition rangeStart = rangeList.get(0);
        for (int i = 1; i < rangeList.size(); i++)
        {
            PartitionPosition next = rangeList.get(i);
            if (key.compareTo(rangeStart) >= 0 && key.compareTo(next) < 0)
                return partitions.get(i - 1);

            rangeStart = next;
        }

        throw new IllegalStateException(String.format("Unable to map %s to Memtable map for %s.%s", key, cfs.keyspace.getName(), cfs.getTableName()));
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    @VisibleForTesting
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are definitely destined for a future memtable
        if (!barrier.isAfter(opGroup))
            return false;
        // if we aren't durable we are directed only by the barrier
        if (commitLogPosition == null)
            return true;
        while (true)
        {
            // otherwise we check if we are in the past/future wrt the CL boundary;
            // if the boundary hasn't been finalised yet, we simply update it to the max of
            // its current value and ours; if it HAS been finalised, we simply accept its judgement
            // this permits us to coordinate a safe boundary, as the boundary choice is made
            // atomically wrt our max() maintenance, so an operation cannot sneak into the past
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastCommitLogPosition)
                return currentLast.compareTo(commitLogPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(commitLogPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, commitLogPosition))
                return true;
        }
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        return commitLogUpperBound.get();
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        for (HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange : partitions)
        {
            if (!memtableSubrange.isEmpty())
                return false;
        }
        return true;
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.params.memtableFlushPeriodInMs;
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        DecoratedKey key = update.partitionKey();
        HashMap<PartitionPosition, AtomicBTreePartition> partitionMap = getPartitionMapFor(key);
        AtomicBTreePartition previous = partitionMap.get(key);

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = allocator.clone(key, opGroup);
            AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey, allocator);
            int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
            allocator.onHeap().allocate(overhead, opGroup);
            initialSize = 8;

            // We'll add the columns later.
            partitionMap.put(cloneKey, empty);
            previous = empty;
        }

        long[] pair = previous.addAllWithSizeDelta(update, opGroup, indexer);
        minTimestamp = Math.min(minTimestamp, previous.stats().minTimestamp);
        liveDataSize.addAndGet(initialSize + pair[0]);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return pair[1];
    }

    public int partitionCount()
    {
        return partitions.size();
    }

    public List<FlushRunnable> flushRunnables(LifecycleTransaction txn)
    {
        List<Range<Token>> localRanges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));

        if (!cfs.getPartitioner().splitter().isPresent() || localRanges.isEmpty())
            return Collections.singletonList(new FlushRunnable(txn));

        return createFlushRunnables(localRanges, txn);
    }

    private List<FlushRunnable> createFlushRunnables(List<Range<Token>> localRanges, LifecycleTransaction txn)
    {
        assert cfs.getPartitioner().splitter().isPresent();

        Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations();
        List<PartitionPosition> boundaries = StorageService.getDiskBoundaries(localRanges, cfs.getPartitioner(), locations);
        List<FlushRunnable> runnables = new ArrayList<>(boundaries.size());
        PartitionPosition rangeStart = cfs.getPartitioner().getMinimumToken().minKeyBound();
        try
        {
            for (int i = 0; i < boundaries.size(); i++)
            {
                PartitionPosition t = boundaries.get(i);
                runnables.add(new FlushRunnable(rangeStart, t, locations[i], txn));
                rangeStart = t;
            }
            return runnables;
        }
        catch (Throwable e)
        {
            throw Throwables.propagate(abortRunnables(runnables, e));
        }
    }

    public Throwable abortRunnables(List<FlushRunnable> runnables, Throwable t)
    {
        if (runnables != null)
            for (FlushRunnable runnable : runnables)
                t = runnable.writer.abort(t);
        return t;
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(liveDataSize.get()), currentOperations,
                             100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange, final boolean isForThrift)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        int startComparison = includeStart ? 0 : 1;
        int endComparison = includeStop ? 0 : -1;

        // avoid iterating over the memtable if we purge all tombstones
        boolean findMinLocalDeletionTime = cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones();
        int minLocalDeletionTime = Integer.MAX_VALUE;

        // TODO combine sort and filtering into single step to handle small subrange reads efficiently?

        ArrayList<PartitionPosition> keysInRange = new ArrayList<>();
        for (int i = 1; i < partitions.size(); i++)
        {
            PartitionPosition memtableRangeStart = rangeList.get(i - 1);
            PartitionPosition memtableRangeEnd = rangeList.get(i);
            // TODO double check inclusivity of both memtable and requested range
            //  check if the requested range intersects with this memtable subrange at all
            if (keyRange.left.compareTo(memtableRangeEnd) >= 0 || keyRange.right.compareTo(memtableRangeStart) < 0)
                continue;

            // optimize for the common case where the request includes the entire memtable subrange, or at least one end of it
            boolean skipStartCheck = keyRange.left.compareTo(memtableRangeStart) <= 0;
            boolean skipEndCheck = keyRange.right.compareTo(memtableRangeEnd) >= 0;

            HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange = partitions.get(i - 1);
            for (Map.Entry<PartitionPosition, AtomicBTreePartition> entry : memtableSubrange.entrySet())
            {
                PartitionPosition key = entry.getKey();
                if ((skipStartCheck || key.compareTo(keyRange.left) >= startComparison) && (skipEndCheck || key.compareTo(keyRange.right) <= endComparison))
                {
                    keysInRange.add(key);
                    if (findMinLocalDeletionTime)
                        minLocalDeletionTime = Math.min(minLocalDeletionTime, entry.getValue().stats().minLocalDeletionTime);
                }
            }
        }
        Collections.sort(keysInRange);

        return new MemtableUnfilteredPartitionIterator(cfs, keysInRange.iterator(), isForThrift, minLocalDeletionTime, columnFilter, dataRange);
    }

    private List<PartitionPosition> getSortedKeySubrange(PartitionPosition from, PartitionPosition to)
    {
        ArrayList<PartitionPosition> keysInRange;

        if (!hasSplits)
        {
            HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange = partitions.get(0);
            keysInRange = new ArrayList<>(memtableSubrange.size());

            // TODO optimize for min/max range, where applicable?
            for (PartitionPosition key : memtableSubrange.keySet())
            {
                if (key.compareTo(from) >= 0 && key.compareTo(to) < 0)
                    keysInRange.add(key);
            }
            return keysInRange;
        }

        // if we're flushing the whole memtable, we can do something more efficient
        if (from.getToken().equals(cfs.getPartitioner().getMinimumToken()) && to.getToken().equals(cfs.getPartitioner().getMaximumToken()))
        {
            int capacity = 0;
            for (HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange : partitions)
                capacity += memtableSubrange.size();

            keysInRange = new ArrayList<>(capacity);
            for (HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange : partitions)
                keysInRange.addAll(memtableSubrange.keySet());
        }
        else
        {
            // estimate required capacity
            keysInRange = new ArrayList<>(partitions.size() * partitions.get(0).size());

            for (int i = 1; i < partitions.size(); i++)
            {
                PartitionPosition memtableRangeStart = rangeList.get(i - 1);
                PartitionPosition memtableRangeEnd = rangeList.get(i);
                // TODO double check inclusivity of both memtable and requested range
                //  check if the requested range intersects with this memtable subrange at all
                if (from.compareTo(memtableRangeEnd) >= 0 || to.compareTo(memtableRangeStart) < 0)
                    continue;

                // optimize for the common case where the request includes the entire memtable subrange, or at least one end of it
                boolean skipStartCheck = from.compareTo(memtableRangeStart) <= 0;
                boolean skipEndCheck = to.compareTo(memtableRangeEnd) >= 0;

                HashMap<PartitionPosition, AtomicBTreePartition> memtableSubrange = partitions.get(i - 1);
                if (skipStartCheck && skipEndCheck)
                {
                    keysInRange.addAll(memtableSubrange.keySet());
                }
                {
                    for (PartitionPosition key : memtableSubrange.keySet())
                    {
                        if ((skipStartCheck || key.compareTo(from) >= 0) && (skipEndCheck || key.compareTo(to) < 0))
                            keysInRange.add(key);
                    }
                }
            }
        }
        Collections.sort(keysInRange);
        return keysInRange;
    }

    private List<PartitionPosition> getSortedKeys()
    {
        int capacity = 0;
        for (HashMap<PartitionPosition, AtomicBTreePartition> partitionMap : partitions)
            capacity += partitionMap.size();

        int i = 0;
        PartitionPosition[] sortedKeys = new PartitionPosition[capacity];
        for (HashMap<PartitionPosition, AtomicBTreePartition> partitionMap : partitions)
        {
            PartitionPosition[] subrangeKeys = new PartitionPosition[partitionMap.size()];
            partitionMap.keySet().toArray(subrangeKeys);
            Arrays.sort(subrangeKeys);
            System.arraycopy(subrangeKeys, 0, sortedKeys, i, subrangeKeys.length);
            i += subrangeKeys.length;
        }
        return Arrays.asList(sortedKeys);
    }

    public Partition getPartition(DecoratedKey key)
    {
        return getPartitionMapFor(key).get(key);
    }

    public long getMinTimestamp()
    {
        return minTimestamp;
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet((long) 1024 * 1024 * 1024 * 1024 * 1024);
    }

    class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final long estimatedSize;
        private final List<PartitionPosition> keysToFlush;

        private final boolean isBatchLogTable;
        private final SSTableMultiWriter writer;

        // keeping these to be able to log what we are actually flushing
        private final PartitionPosition from;
        private final PartitionPosition to;

        FlushRunnable(PartitionPosition from, PartitionPosition to, Directories.DataDirectory flushLocation, LifecycleTransaction txn)
        {
            this(getSortedKeySubrange(from, to), flushLocation, from, to, txn);
        }

        FlushRunnable(LifecycleTransaction txn)
        {
            this(getSortedKeys(), null, null, null, txn);
        }

        FlushRunnable(List<PartitionPosition> keysToFlush, Directories.DataDirectory flushLocation, PartitionPosition from, PartitionPosition to, LifecycleTransaction txn)
        {
            this.keysToFlush = keysToFlush;
            this.from = from;
            this.to = to;
            long keySize = 0;
            for (PartitionPosition key : keysToFlush)
            {
                //  make sure we don't write nonsensical keys
                assert key instanceof DecoratedKey;
                keySize += ((DecoratedKey) key).getKey().remaining();
            }
            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + liveDataSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead

            this.isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHES) && cfs.keyspace.getName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            if (flushLocation == null)
                writer = createFlushWriter(txn, cfs.getSSTablePath(getDirectories().getWriteableLocationAsFile(estimatedSize)), columnsCollector.get(), statsCollector.get());
            else
                writer = createFlushWriter(txn, cfs.getSSTablePath(getDirectories().getLocationForDisk(flushLocation)), columnsCollector.get(), statsCollector.get());

        }

        protected Directories getDirectories()
        {
            return cfs.getDirectories();
        }

        private void writeSortedContents()
        {
            logger.debug("Writing {}, flushed range = ({}, {}]", Memtable.this.toString(), from, to);

            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (PartitionPosition key : keysToFlush)
            {
                // TODO optimize this by tracking which memtable subrange we're currently on to avoid repeated lookups
                AtomicBTreePartition partition = getPartitionMapFor(key).get(key);

                // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                // we don't need to preserve tombstones for repair. So if both operation are in this
                // memtable (which will almost always be the case if there is no ongoing failure), we can
                // just skip the entry (CASSANDRA-4667).
                if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                    continue;

                if (!partition.isEmpty())
                {
                    try (UnfilteredRowIterator iter = partition.unfilteredIterator())
                    {
                        writer.append(iter);
                    }
                }
            }

            long bytesFlushed = writer.getFilePointer();
            logger.debug(String.format("Completed flushing %s (%s) for commitlog position %s",
                                                                              writer.getFilename(),
                                                                              FBUtilities.prettyPrintMemory(bytesFlushed),
                                                                              commitLogUpperBound));
            // Update the metrics
            cfs.metric.bytesFlushed.inc(bytesFlushed);
        }

        public SSTableMultiWriter createFlushWriter(LifecycleTransaction txn,
                                                  String filename,
                                                  PartitionColumns columns,
                                                  EncodingStats stats)
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator)
                    .commitLogIntervals(new IntervalSet<>(commitLogLowerBound.get(), commitLogUpperBound.get()));

            return cfs.createSSTableMultiWriter(Descriptor.fromFilename(filename),
                                                (long)keysToFlush.size(),
                                                ActiveRepairService.UNREPAIRED_SSTABLE,
                                                sstableMetadataCollector,
                                                new SerializationHeader(true, cfs.metadata, columns, stats), txn);
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0 ; i < count ; i++)
                partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    public class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final ColumnFamilyStore cfs;
        private final Iterator<PartitionPosition> iter;
        private final boolean isForThrift;
        private final int minLocalDeletionTime;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(ColumnFamilyStore cfs, Iterator<PartitionPosition> iter, boolean isForThrift, int minLocalDeletionTime, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.cfs = cfs;
            this.iter = iter;
            this.isForThrift = isForThrift;
            this.minLocalDeletionTime = minLocalDeletionTime;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }

        public int getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public CFMetaData metadata()
        {
            return cfs.metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            PartitionPosition position = iter.next();
            // Actual stored key should be true DecoratedKey
            assert position instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)position;
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, getPartitionMapFor(position).get(position));
        }
    }

    private static class ColumnsCollector
    {
        private final HashMap<ColumnDefinition, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnDefinition> extra = new ConcurrentSkipListSet<>();
        ColumnsCollector(PartitionColumns columns)
        {
            for (ColumnDefinition def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnDefinition def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(PartitionColumns columns)
        {
            for (ColumnDefinition s : columns.statics)
                update(s);
            for (ColumnDefinition r : columns.regulars)
                update(r);
        }

        private void update(ColumnDefinition definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public PartitionColumns get()
        {
            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (Map.Entry<ColumnDefinition, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}
