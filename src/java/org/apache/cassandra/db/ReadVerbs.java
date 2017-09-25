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

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class ReadVerbs extends VerbGroup<ReadVerbs.ReadVersion>
{
    private static final InetAddress local = FBUtilities.getBroadcastAddress();

    public enum ReadVersion implements Version<ReadVersion>
    {
        OSS_30   (EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),
        OSS_3014 (EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),  // Contains changes to ColumnFilter from CASSANDRA-13004
        OSS_40   (EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),  // Changes the meaning of isFetchAll in ColumnFilter
        DSE_60   (EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30);  // Uses the encodingVersion ordinal when writing digest versions
                                                                                        // rather than the whole messaging version,
                                                                                        // serializes the bytes limit in DataLimits

        public final EncodingVersion encodingVersion;
        public final BoundsVersion boundsVersion;
        public final DigestVersion digestVersion;

        ReadVersion(EncodingVersion encodingVersion, BoundsVersion boundsVersion, DigestVersion digestVersion)
        {
            this.encodingVersion = encodingVersion;
            this.boundsVersion = boundsVersion;
            this.digestVersion = digestVersion;
        }

        public static <T> Versioned<ReadVersion, T> versioned(Function<ReadVersion, ? extends T> function)
        {
            return new Versioned<>(ReadVersion.class, function);
        }
    }

    public final RequestResponse<SinglePartitionReadCommand, ReadResponse> SINGLE_READ;
    public final RequestResponse<PartitionRangeReadCommand, ReadResponse> RANGE_READ;
    final RequestResponse<NodeSyncReadCommand, ReadResponse> NODESYNC;

    private static <T extends ReadCommand> VerbHandlers.MonitoredRequestResponse<T, ReadResponse> readHandler()
    {
        return (from, command, monitor) ->
        {
            final boolean isLocal = from.equals(local);

            // Note that we want to allow locally delivered reads no matter what
            if (StorageService.instance.isBootstrapMode() && !isLocal)
                throw new RuntimeException("Cannot service reads while bootstrapping!");

            // Monitoring tests want to artificially slow down their reads, but we don't want this
            // to impact the queries drivers do on system/schema tables
            if (Monitor.isTesting() && !SchemaConstants.isUserKeyspace(command.metadata().keyspace))
                monitor = null;

            CompletableFuture<ReadResponse> result = new CompletableFuture<>();
            command.createResponse(command.executeLocally(monitor), isLocal)
                   .subscribe(result::complete, result::completeExceptionally);

            return result;
        };
    }

    public ReadVerbs(Verbs.Group id)
    {
        super(id, false, ReadVersion.class);

        RegistrationHelper helper = helper();

        SINGLE_READ = helper.monitoredRequestResponse("SINGLE_READ", SinglePartitionReadCommand.class, ReadResponse.class)
                            .timeout(DatabaseDescriptor::getReadRpcTimeout)
                            .droppedGroup(DroppedMessages.Group.READ)
                            .handler(readHandler());
        RANGE_READ = helper.monitoredRequestResponse("RANGE_READ", PartitionRangeReadCommand.class, ReadResponse.class)
                     .timeout(DatabaseDescriptor::getRangeRpcTimeout)
                     .droppedGroup(DroppedMessages.Group.RANGE_SLICE)
                     .handler(readHandler());
        // Nodesync mainly use vanilla range queries so this is very similar to RANGE_READ, but we use a separate verb
        // so we can force responses to be executed on the NodeSync executor. Also allow us to count dropped messages
        // separately for increasing clarity. And it may prove useful in the future to know is for NodeSync or not, so
        // that's a good way to do so.
        // TODO: it might also make sense to have a specific timeout, not that the default one for ranges is not appropriate,
        // but rather so that it's not impacted if user sets that later timeout to very or very small value. Unclear if
        // benefits are worth the trouble at that point though.
        NODESYNC = helper.monitoredRequestResponse("NODESYNC", NodeSyncReadCommand.class, ReadResponse.class)
                         .timeout(DatabaseDescriptor::getRangeRpcTimeout)
                         .responseExecutor(NodeSyncReadCommand::nodeSyncExecutor)
                         .droppedGroup(DroppedMessages.Group.NODESYNC)
                         .handler(readHandler());
    }
}
