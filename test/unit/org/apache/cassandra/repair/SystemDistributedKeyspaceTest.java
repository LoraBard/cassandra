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

import java.util.Collections;

import org.junit.Test;

import com.datastax.bdp.db.nodesync.Segment;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static org.junit.Assert.*;

public class SystemDistributedKeyspaceTest extends CQLTester
{
    @Test
    public void testBasicNodeSyncRecording() throws Exception
    {
        // Simple table, just to get a proper TableMetadata to insert segments; we don't truly insert anything in it
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        TableMetadata table = currentTableMetadata();

        Segment seg = seg(table, 0, 100);

        assertEquals(Collections.emptyList(), SystemDistributedKeyspace.nodeSyncRecords(seg).get());

        // Add one and check it gets picked up
        SystemDistributedKeyspace.recordNodeSyncValidation(seg(table, 0, 50), fullInSync(10), true);
        assertEquals(records(table).add(0, 50, fullInSync(10)).asList(), SystemDistributedKeyspace.nodeSyncRecords(seg).get());

        // Add one that isn't part of the requested segment and check it isn't picked up
        SystemDistributedKeyspace.recordNodeSyncValidation(seg(table, 100, 150), fullInSync(15), true);
        assertEquals(records(table).add(0, 50, fullInSync(10)).asList(), SystemDistributedKeyspace.nodeSyncRecords(seg).get());

        // Add one more within the requested segment and check it's picked up. Also record both a full and an more recent validation
        SystemDistributedKeyspace.recordNodeSyncValidation(seg(table, 50, 100), fullInSync(7), true);
        SystemDistributedKeyspace.recordNodeSyncValidation(seg(table, 50, 100), partialRepaired(2, inet(127, 0, 0, 10)), true);
        assertEquals(records(table).add(0, 50, fullInSync(10))
                                   .add(50, 100, partialRepaired(2, inet(127, 0, 0, 10)), fullInSync(7))
                                   .asList(),
                     SystemDistributedKeyspace.nodeSyncRecords(seg).get());

        // Lastly, test querying with a segment that goes up to min token since that's a separate code path. Make sure
        // it fetches all our insert segments.
        assertEquals(records(table).add(0, 50, fullInSync(10))
                                   .add(50, 100, partialRepaired(2, inet(127, 0, 0, 10)), fullInSync(7))
                                   .add(100, 150, fullInSync(15))
                                   .asList(),
                     SystemDistributedKeyspace.nodeSyncRecords(seg(table, 0, min())).get());
    }
}