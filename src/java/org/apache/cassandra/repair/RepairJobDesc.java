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
package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * RepairJobDesc is used from various repair processes to distinguish one RepairJob to another.
 *
 * @since 2.0
 */
public class RepairJobDesc
{
    public static final Versioned<RepairVersion, Serializer<RepairJobDesc>> serializers = RepairVersion.versioned(RepairJobDescSerializer::new);

    public final UUID parentSessionId;
    /** RepairSession id */
    public final UUID sessionId;
    public final String keyspace;
    public final String columnFamily;
    /** repairing range  */
    public final Collection<Range<Token>> ranges;

    public RepairJobDesc(UUID parentSessionId, UUID sessionId, String keyspace, String columnFamily, Collection<Range<Token>> ranges)
    {
        this.parentSessionId = parentSessionId;
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.ranges = ranges;
    }

    @Override
    public String toString()
    {
        return "[repair #" + sessionId + " on " + keyspace + "/" + columnFamily + ", " + ranges + "]";
    }

    public String toString(PreviewKind previewKind)
    {
        return '[' + previewKind.logPrefix() + " #" + sessionId + " on " + keyspace + "/" + columnFamily + ", " + ranges + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairJobDesc that = (RepairJobDesc) o;

        if (!columnFamily.equals(that.columnFamily)) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (ranges != null ? that.ranges == null || (ranges.size() != that.ranges.size()) || (ranges.size() == that.ranges.size() && !ranges.containsAll(that.ranges)) : that.ranges != null) return false;
        if (!sessionId.equals(that.sessionId)) return false;
        if (parentSessionId != null ? !parentSessionId.equals(that.parentSessionId) : that.parentSessionId != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sessionId, keyspace, columnFamily, ranges);
    }

    private static class RepairJobDescSerializer extends VersionDependent<RepairVersion> implements Serializer<RepairJobDesc>
    {
        private RepairJobDescSerializer(RepairVersion version)
        {
            super(version);
        }

        public void serialize(RepairJobDesc desc, DataOutputPlus out) throws IOException
        {
            out.writeBoolean(desc.parentSessionId != null);
            if (desc.parentSessionId != null)
                UUIDSerializer.serializer.serialize(desc.parentSessionId, out);

            UUIDSerializer.serializer.serialize(desc.sessionId, out);
            out.writeUTF(desc.keyspace);
            out.writeUTF(desc.columnFamily);
            MessagingService.validatePartitioner(desc.ranges);
            out.writeInt(desc.ranges.size());
            for (Range<Token> rt : desc.ranges)
                AbstractBounds.tokenSerializer.serialize(rt, out, version.boundsVersion);
        }

        public RepairJobDesc deserialize(DataInputPlus in) throws IOException
        {
            UUID parentSessionId = null;
            if (in.readBoolean())
                parentSessionId = UUIDSerializer.serializer.deserialize(in);
            UUID sessionId = UUIDSerializer.serializer.deserialize(in);
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            int nRanges = in.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>();
            Range<Token> range;

            for (int i = 0; i < nRanges; i++)
            {
                range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in,
                        MessagingService.globalPartitioner(), version.boundsVersion);
                ranges.add(range);
            }

            return new RepairJobDesc(parentSessionId, sessionId, keyspace, columnFamily, ranges);
        }

        public long serializedSize(RepairJobDesc desc)
        {
            long size = TypeSizes.sizeof(desc.parentSessionId != null);
            if (desc.parentSessionId != null)
                size += UUIDSerializer.serializer.serializedSize(desc.parentSessionId);
            size += UUIDSerializer.serializer.serializedSize(desc.sessionId);
            size += TypeSizes.sizeof(desc.keyspace);
            size += TypeSizes.sizeof(desc.columnFamily);
            size += TypeSizes.sizeof(desc.ranges.size());
            for (Range<Token> rt : desc.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(rt, version.boundsVersion);
            return size;
        }
    }
}
