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

import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.commitlog.CommitLogPosition;


public interface MutationEmitter<P> {

    static class MutationFuture
    {
        Mutation mutation;
        CompletableFuture<Long> sentFuture;

        public MutationFuture(Mutation mutation,
                              CompletableFuture<Long> sentFuture)
        {
            this.mutation = mutation;
            this.sentFuture = sentFuture;
        }

        public MutationFuture retry(MutationEmitter mutationEmitter) {
            return mutationEmitter.sendMutationAsync(this.mutation);
        }
    }

    MutationFuture sendMutationAsync(final Mutation mutation);
}
