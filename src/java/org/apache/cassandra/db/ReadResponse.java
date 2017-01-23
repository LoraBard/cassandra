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

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Single;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    // Serializer for single partition read response
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    private static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    protected ReadResponse()
    {
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalDataResponse(data, command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new RemoteDataResponse(RemoteDataResponse.build(data, command.columnFilter()));
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new DigestResponse(makeDigest(data, command));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest, command.digestVersion());
        return ByteBuffer.wrap(digest.digest());
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            super();
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(ReadCommand command)
        {
            // We assume that the digest is in the proper version, which bug excluded should be true since this is called with
            // ReadCommand.digestVersion() as argument and that's also what we use to produce the digest in the first place.
            // Validating it's the proper digest in this method would require sending back the digest version along with the
            // digest which would waste bandwith for little gain.
            return digest;
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    // built on the owning node responding to a query
    private static class LocalDataResponse extends ReadResponse
    {
//        private final List<Single<ImmutableBTreePartition>> partitions;
        final UnfilteredPartitionIterator iter;
        private LocalDataResponse(UnfilteredPartitionIterator iter, ReadCommand command)
        {
            this.iter = iter; // build(iter, command);
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            return iter;

/*            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

                public CFMetaData metadata()
                {
                    return command.metadata();
                }

                public boolean hasNext()
                {
                    return idx < partitions.size();
                }

                public Single<UnfilteredRowIterator> next()
                {
                    // TODO: we know rows don't require any filtering and that we return everything. We ought to be able to optimize this.
                    return partitions.get(idx++).map(p -> p.unfilteredIterator(command.columnFilter(), Slices.ALL, command.isReversed()));
                }

                public void close()
                {

                }
            };*/
        }

        private static List<Single<ImmutableBTreePartition>> build(UnfilteredPartitionIterator iterator, ReadCommand command)
        {
            if (!iterator.hasNext())
                return Collections.emptyList();

            try
            {
                if (command instanceof SinglePartitionReadCommand)
                {
                    Single<UnfilteredRowIterator> partition = iterator.next();
                    return Collections.singletonList(partition.map(p ->
                                                                   {
                                                                       ImmutableBTreePartition b = ImmutableBTreePartition.create(p);
                                                                       p.close();
                                                                       return b;
                                                                   }));
                }

                List<Single<ImmutableBTreePartition>> partitions = new ArrayList<>();
                while (iterator.hasNext())
                {
                    Single<UnfilteredRowIterator> partition = iterator.next();
                    Single<ImmutableBTreePartition> r = partition.map(p -> {

                        ImmutableBTreePartition b = ImmutableBTreePartition.create(p);
                        p.close();
                        return b;
                    });
                    partitions.add(r);
                }

                return partitions;
            }
            finally
            {
                iterator.close();
            }
        }

        public ByteBuffer digest(ReadCommand command)
        {
            return makeDigest(makeIterator(command), command);
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    // built on the coordinator node receiving a response
    private static class RemoteDataResponse extends DataResponse
    {
        protected RemoteDataResponse(ByteBuffer data)
        {
            super(data, SerializationHelper.Flag.FROM_REMOTE);
        }

        private static ByteBuffer build(UnfilteredPartitionIterator iter, ColumnFilter selection)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, selection, buffer, MessagingService.current_version);
                return buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    static abstract class DataResponse extends ReadResponse
    {
        // TODO: can the digest be calculated over the raw bytes now?
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final SerializationHelper.Flag flag;

        protected DataResponse(ByteBuffer data, SerializationHelper.Flag flag)
        {
            super();
            this.data = data;
            this.flag = flag;
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            try (DataInputBuffer in = new DataInputBuffer(data, true))
            {
                // Note that the command parameter shadows the 'command' field and this is intended because
                // the later can be null (for RemoteDataResponse as those are created in the serializers and
                // those don't have easy access to the command). This is also why we need the command as parameter here.
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                                                                                         MessagingService.current_version,
                                                                                         command.metadata(),
                                                                                         command.columnFilter(),
                                                                                         flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest(ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(command))
            {
                return makeDigest(iterator, command);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithVIntLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
            // version, we'll have to deserialize/re-serialize the data to be in the proper version.
            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    }
}
