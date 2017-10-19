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
package com.datastax.bdp.db.nodesync;

import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.datastax.bdp.db.nodesync.NodeSyncHelpers;
import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.nodesync.UserValidationOptions;
import com.datastax.bdp.db.nodesync.UserValidationProposer;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class UserValidationProposerTest extends AbstractValidationProposerTester
{
    @After
    public void cleanupTask()
    {
        NodeSyncHelpers.resetTableSizeAndLocalRangeProviders();
    }

    @Test
    public void testSegmentGeneration() throws Exception
    {
        testSegmentGeneration(0,
                              asList(range(200, 300)),
                              range(200, 250));
        testSegmentGeneration(0,
                              asList(range(200, 300)),
                              range(200, 300));
        testSegmentGeneration(0,
                              asList(range(0, 100), range(200, 300)),
                              range(50, 100), range(200, 300));

        testSegmentGeneration(1,
                              asList(range(200, 250)),
                              range(200, 250));
        testSegmentGeneration(1,
                              asList(range(200, 250), range(250, 300)),
                              range(200, 275));
        testSegmentGeneration(1,
                              asList(range(50, 100), range(200, 250), range(250, 300)),
                              range(50, 100), range(200, 300));

    }

    @SuppressWarnings("unchecked")
    private void testSegmentGeneration(int depth,
                                       List<Range<Token>> expected,
                                       Range... requestedRanges) throws Exception
    {
        // We need the keyspace to say RF > 1 or the proposer creation will complain
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");

        // Don't set nodesync mostly to test it's ignore by user validations.
        TableMetadata table = createDummyTable(ks, false);

        List<Range<Token>> requested = asList(requestedRanges);

        // We have 3 local ranges and 10MB max seg size, so...
        NodeSyncHelpers.setTableSizeAndLocalRangeProviders(t -> depth * mb(31), TEST_RANGES);

        NodeSyncService service = new NodeSyncService(); // Not even started, just here because we need a reference below
        UserValidationOptions options = new UserValidationOptions("test", table, requested);
        UserValidationProposer proposer = UserValidationProposer.create(service, options, mb(10));

        proposer.init();

        assertEquals(segs(table).addAll(expected).asList(), proposer.segmentsToValidate());
    }

}