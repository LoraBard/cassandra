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
package org.apache.cassandra.cdc;

import java.time.Instant;

import org.apache.cassandra.db.commitlog.CommitLogPosition;

/**
 * Metadata about the source of the change event
 */
public class SourceInfo  {

    public final CommitLogPosition commitLogPosition;
    public final String keyspace;
    public final String table;
    public final Instant timestamp;

    public SourceInfo(CommitLogPosition commitLogPosition, String keyspace, String table, Instant timestamp) {
        this.commitLogPosition = commitLogPosition;
        this.keyspace = keyspace;
        this.table = table;
        this.timestamp = timestamp;
    }

}
