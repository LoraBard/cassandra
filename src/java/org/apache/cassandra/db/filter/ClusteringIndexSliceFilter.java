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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A filter over a single partition.
 */
public class ClusteringIndexSliceFilter extends AbstractClusteringIndexFilter
{
    static final InternalDeserializer deserializer = new SliceDeserializer();

    private final Slices slices;

    public ClusteringIndexSliceFilter(Slices slices, boolean reversed)
    {
        super(reversed);
        this.slices = slices;
    }

    public Slices requestedSlices()
    {
        return slices;
    }

    public boolean selectsAllPartition()
    {
        return slices.size() == 1 && !slices.hasLowerBound() && !slices.hasUpperBound();
    }

    public boolean selects(Clustering clustering)
    {
        return slices.selects(clustering);
    }

    public ClusteringIndexSliceFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive)
    {
        Slices newSlices = slices.forPaging(comparator, lastReturned, inclusive, reversed);
        return slices == newSlices
             ? this
             : new ClusteringIndexSliceFilter(newSlices, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        // Partition is guaranteed to cover the whole filter if it includes the filter start and finish bounds.

        // (note that since partition is the head of a partition, to have no lower bound is ok)
        if (!slices.hasUpperBound() || partition.isEmpty())
            return false;

        return partition.metadata().comparator.compare(slices.get(slices.size() - 1).end(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return !reversed && slices.size() == 1 && !slices.hasLowerBound();
    }

    private Row filterNotIndexedStaticRow(ColumnFilter columnFilter, TableMetadata metadata, Row row)
    {
        return columnFilter.fetchedColumns().statics.isEmpty() ? Rows.EMPTY_STATIC_ROW : row.filter(columnFilter, metadata);
    }

    private Unfiltered filterNotIndexedRow(ColumnFilter columnFilter, TableMetadata metadata, Slices.InOrderTester tester, Unfiltered unfiltered)
    {
        if (unfiltered instanceof Row)
        {
            Row row = (Row)unfiltered;
            return tester.includes(row.clustering()) ? row.filter(columnFilter, metadata) : null;
        }

        return unfiltered;
    }

    public FlowableUnfilteredPartition filterNotIndexed(ColumnFilter columnFilter, FlowableUnfilteredPartition partition)
    {
        final Slices.InOrderTester tester = slices.inOrderTester(reversed);
        return partition.skippingMapContent(row -> filterNotIndexedRow(columnFilter,
                                                                       partition.metadata(),
                                                                       tester,
                                                                       row),
                                            filterNotIndexedStaticRow(columnFilter,
                                                                      partition.metadata(),
                                                                      partition.staticRow()));
    }



    public Slices getSlices(TableMetadata metadata)
    {
        return slices;
    }

    public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition)
    {
        return partition.unfilteredIterator(columnFilter, slices, reversed);
    }

    public FlowableUnfilteredPartition getFlowableUnfilteredPartition(ColumnFilter columnFilter, Partition partition)
    {
        return partition.unfilteredPartition(columnFilter, slices, reversed);
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;

        if (minClusteringValues.isEmpty() || maxClusteringValues.isEmpty())
            return true;

        return slices.intersects(minClusteringValues, maxClusteringValues);
    }

    public String toString(TableMetadata metadata)
    {
        return String.format("slice(slices=%s, reversed=%b)", slices, reversed);
    }

    public String toCQLString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();

        if (!selectsAllPartition())
            sb.append(slices.toCQLString(metadata));

        appendOrderByToCQLString(metadata, sb);

        return sb.toString();
    }

    public Kind kind()
    {
        return Kind.SLICE;
    }

    protected void serializeInternal(DataOutputPlus out, ReadVersion version) throws IOException
    {
        Slices.serializer.serialize(slices, out, version.encodingVersion.clusteringVersion);
    }

    protected long serializedSizeInternal(ReadVersion version)
    {
        return Slices.serializer.serializedSize(slices, version.encodingVersion.clusteringVersion);
    }

    private static class SliceDeserializer implements InternalDeserializer
    {
        public ClusteringIndexFilter deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata, boolean reversed) throws IOException
        {
            Slices slices = Slices.serializer.deserialize(in, version.encodingVersion.clusteringVersion, metadata);
            return new ClusteringIndexSliceFilter(slices, reversed);
        }
    }
}
