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

package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class FinalizeCommit extends ConsistentRepairMessage<FinalizeCommit>
{
    public static Versioned<RepairVersion, MessageSerializer<FinalizeCommit>> serializers = RepairVersion.versioned(v -> new MessageSerializer<FinalizeCommit>(v)
    {
        public void serialize(FinalizeCommit msg, DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
        }

        public FinalizeCommit deserialize(DataInputPlus in) throws IOException
        {
            return new FinalizeCommit(UUIDSerializer.serializer.deserialize(in));
        }

        public long serializedSize(FinalizeCommit msg)
        {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID);
        }
    });

    public FinalizeCommit(UUID sessionID)
    {
        super(sessionID);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FinalizeCommit that = (FinalizeCommit) o;

        return sessionID.equals(that.sessionID);
    }

    public int hashCode()
    {
        return sessionID.hashCode();
    }

    public String toString()
    {
        return "FinalizeCommit{" +
               "sessionID=" + sessionID +
               '}';
    }

    public MessageSerializer<FinalizeCommit> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Optional<Verb<FinalizeCommit, ?>> verb()
    {
        return Optional.of(Verbs.REPAIR.FINALIZE_COMMIT);
    }
}
