/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sasi;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

import com.google.common.collect.Multimap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

class SASIIndexBuilder extends SecondaryIndexBuilder
{
    private final ColumnFamilyStore cfs;
    private final UUID compactionId = UUIDGen.getTimeUUID();

    private final SortedMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> sstables;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    public SASIIndexBuilder(ColumnFamilyStore cfs, SortedMap<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> sstables)
    {
        long totalIndexBytes = 0;
        for (SSTableReader sstable : sstables.keySet())
            totalIndexBytes += getDataLength(sstable);

        this.cfs = cfs;
        this.sstables = sstables;
        this.totalSizeInBytes = totalIndexBytes;
    }

    public void build()
    {
        AbstractType<?> keyValidator = cfs.metadata().partitionKeyType;
        for (Map.Entry<SSTableReader, Multimap<ColumnMetadata, ColumnIndex>> e : sstables.entrySet())
        {
            SSTableReader sstable = e.getKey();
            Multimap<ColumnMetadata, ColumnIndex> indexes = e.getValue();

            try (RandomAccessReader dataFile = sstable.openDataReader())
            {
                PerSSTableIndexWriter indexWriter = SASIIndex.newWriter(keyValidator, sstable.descriptor, indexes, OperationType.COMPACTION);

                long previousDataPosition = 0;
                try (PartitionIndexIterator keys = sstable.allKeysIterator())
                {
                    for (; keys.key() != null; keys.advance())
                    {
                        if (isStopRequested())
                            throw new CompactionInterruptedException(getCompactionInfo());

                        final DecoratedKey key = keys.key();
                        final long dataPosition = keys.dataPosition();

                        indexWriter.startPartition(key, dataPosition);

                        try
                        {
                            dataFile.seek(dataPosition);
                            ByteBufferUtil.skipShortLength(dataFile); // key

                            try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                            {
                                // if the row has statics attached, it has to be indexed separately
                                if (cfs.metadata().hasStaticColumns())
                                    indexWriter.nextUnfilteredCluster(partition.staticRow());

                                while (partition.hasNext())
                                    indexWriter.nextUnfilteredCluster(partition.next());
                            }
                        }
                        catch (IOException ex)
                        {
                            throw new FSReadError(ex, sstable.getFilename());
                        }

                        bytesProcessed += dataPosition - previousDataPosition;
                        previousDataPosition = dataPosition;
                    }

                    completeSSTable(indexWriter, sstable, indexes.values());
                } catch (IOException e1)
                {
                    throw new FSReadError(e1, sstable.getFilename());
                }
            }
        }
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(cfs.metadata(),
                                  OperationType.INDEX_BUILD,
                                  bytesProcessed,
                                  totalSizeInBytes,
                                  compactionId);
    }

    private long getDataLength(SSTable sstable)
    {
        File primaryIndex = new File(sstable.getFilename());
        return primaryIndex.exists() ? primaryIndex.length() : 0;
    }

    private void completeSSTable(PerSSTableIndexWriter indexWriter, SSTableReader sstable, Collection<ColumnIndex> indexes)
    {
        indexWriter.complete();

        for (ColumnIndex index : indexes)
        {
            File tmpIndex = new File(sstable.descriptor.filenameFor(index.getComponent()));
            if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                continue;

            index.update(Collections.<SSTableReader>emptyList(), Collections.singletonList(sstable));
        }
    }
}
