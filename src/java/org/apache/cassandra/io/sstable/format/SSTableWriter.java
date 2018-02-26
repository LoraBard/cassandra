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

package org.apache.cassandra.io.sstable.format;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter extends SSTable implements Transactional
{
    protected long repairedAt;
    protected UUID pendingRepair;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final TransactionalProxy txnProxy = txnProxy();
    protected final Collection<SSTableFlushObserver> observers;

    protected abstract TransactionalProxy txnProxy();

    // due to lack of multiple inheritance, we use an inner class to proxy our Transactional implementation details
    protected abstract class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        protected SSTableReader finalReader;
        protected boolean openResult;
    }

    protected SSTableWriter(Descriptor descriptor,
                            long keyCount,
                            long repairedAt,
                            UUID pendingRepair,
                            TableMetadataRef metadata,
                            MetadataCollector metadataCollector,
                            SerializationHeader header,
                            Collection<SSTableFlushObserver> observers)
    {
        super(descriptor, components(metadata.get()), metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.metadataCollector = metadataCollector;
        this.header = header;
        this.observers = observers == null ? Collections.emptySet() : observers;
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       Long keyCount,
                                       Long repairedAt,
                                       UUID pendingRepair,
                                       TableMetadataRef metadata,
                                       MetadataCollector metadataCollector,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleTransaction txn)
    {
        Factory writerFactory = descriptor.getFormat().getWriterFactory();
        return writerFactory.open(descriptor, keyCount, repairedAt, pendingRepair, metadata, metadataCollector, header, observers(descriptor, indexes, txn.opType()), txn);
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleTransaction txn)
    {
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(descriptor);
        return create(metadata, descriptor, keyCount, repairedAt, pendingRepair, sstableLevel, header, indexes, txn);
    }

    public static SSTableWriter create(TableMetadataRef metadata,
                                       Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleTransaction txn)
    {
        MetadataCollector collector = new MetadataCollector(metadata.get().comparator).sstableLevel(sstableLevel);
        return create(descriptor, keyCount, repairedAt, pendingRepair, metadata, collector, header, indexes, txn);
    }

    @VisibleForTesting
    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleTransaction txn)
    {
        return create(descriptor, keyCount, repairedAt, pendingRepair, 0, header, indexes, txn);
    }

    private static Set<Component> components(TableMetadata metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                Component.PARTITION_INDEX,
                Component.ROW_INDEX,
                Component.STATS,
                Component.TOC,
                Component.DIGEST));

        if (metadata.params.bloomFilterFpChance < 1.0)
            components.add(Component.FILTER);

        if (metadata.params.compression.isEnabled())
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }
        return components;
    }

    private static Collection<SSTableFlushObserver> observers(Descriptor descriptor,
                                                              Collection<Index> indexes,
                                                              OperationType operationType)
    {
        if (indexes == null)
            return Collections.emptyList();

        List<SSTableFlushObserver> observers = new ArrayList<>(indexes.size());
        for (Index index : indexes)
        {
            SSTableFlushObserver observer = index.getFlushObserver(descriptor, operationType);
            if (observer != null)
            {
                observer.begin();
                observers.add(observer);
            }
        }

        return ImmutableList.copyOf(observers);
    }

    public abstract void mark();

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public abstract RowIndexEntry append(UnfilteredRowIterator iterator);

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    public abstract void resetAndTruncate();

    public SSTableWriter setRepairedAt(long repairedAt)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
        return this;
    }

    public SSTableWriter setMaxDataAge(long maxDataAge)
    {
        this.maxDataAge = maxDataAge;
        return this;
    }

    public SSTableWriter setOpenResult(boolean openResult)
    {
        txnProxy.openResult = openResult;
        return this;
    }

    /**
     * Open the resultant SSTableReader before it has been fully written.
     *
     * The passed consumer will be called when the necessary data has been flushed to disk/cache. This may never happen
     * (e.g. if the table was finished before the flushes materialized, or if this call returns false e.g. if a table
     * was already prepared but hasn't reached readiness yet).
     *
     * Uses callback instead of future because preparation and callback happen on the same thread.
     */
    public abstract boolean openEarly(Consumer<SSTableReader> callWhenReady);

    /**
     * Open the resultant SSTableReader once it has been fully written, but before the
     * _set_ of tables that are being written together as one atomic operation are all ready
     */
    public abstract SSTableReader openFinalEarly();

    public SSTableReader finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
        this.maxDataAge = maxDataAge;
        return finish(openResult);
    }

    public SSTableReader finish(boolean openResult)
    {
        setOpenResult(openResult);
        txnProxy.finish();
        observers.forEach(SSTableFlushObserver::complete);
        return finished();
    }

    /**
     * Open the resultant SSTableReader once it has been fully written, and all related state
     * is ready to be finalised including other sstables being written involved in the same operation
     */
    public SSTableReader finished()
    {
        return txnProxy.finalReader;
    }

    // finalise our state on disk, including renaming
    public final void prepareToCommit()
    {
        txnProxy.prepareToCommit();
    }

    public final Throwable commit(Throwable accumulate)
    {
        try
        {
            return txnProxy.commit(accumulate);
        }
        finally
        {
            observers.forEach(SSTableFlushObserver::complete);
        }
    }

    public final Throwable abort(Throwable accumulate)
    {
        return txnProxy.abort(accumulate);
    }

    public final void close()
    {
        txnProxy.close();
    }

    public final void abort()
    {
        txnProxy.abort();
    }

    protected Map<MetadataType, MetadataComponent> finalizeMetadata()
    {
        return metadataCollector.finalizeMetadata(getPartitioner().getClass().getCanonicalName(),
                                                  metadata().params.bloomFilterFpChance,
                                                  repairedAt,
                                                  pendingRepair,
                                                  header);
    }

    protected StatsMetadata statsMetadata()
    {
        return (StatsMetadata) finalizeMetadata().get(MetadataType.STATS);
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        List<Pair<File, File>> renames = new ArrayList<>();
        for (Component component : Sets.difference(components, ImmutableSet.of(Component.DATA)))
            renames.add(Pair.create(new File(tmpdesc.filenameFor(component)), new File(newdesc.filenameFor(component))));

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        renames.add(Pair.create(new File(tmpdesc.filenameFor(Component.DATA)), new File(newdesc.filenameFor(Component.DATA))));

        List<File> nonExisting = null;
        for (Pair<File, File> rename : renames)
        {
            if (!rename.left.exists())
            {
                if (nonExisting == null)
                    nonExisting = new ArrayList<>();
                nonExisting.add(rename.left);
            }
        }
        if (nonExisting != null)
            throw new AssertionError("One or more of the required components for the rename does not exist: " + nonExisting);

        for (Pair<File, File> rename : renames)
            FileUtils.renameWithConfirm(rename.left, rename.right);
    }


    public static abstract class Factory
    {
        public abstract SSTableWriter open(Descriptor descriptor,
                                           long keyCount,
                                           long repairedAt,
                                           UUID pendingRepair,
                                           TableMetadataRef metadata,
                                           MetadataCollector metadataCollector,
                                           SerializationHeader header,
                                           Collection<SSTableFlushObserver> observers,
                                           LifecycleTransaction txn);
    }
}
