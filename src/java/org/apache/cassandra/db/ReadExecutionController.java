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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class ReadExecutionController implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ReadExecutionController.class);

    // For every reads
    private final OpOrder baseOp;
    private volatile OpOrder.Group baseOpGroup;
    private final TableMetadata baseMetadata; // kept to sanity check that we have take the op order on the right table

    // For index reads
    private final ReadExecutionController indexController;
    private final OpOrder writeOp;
    private volatile OpOrder.Group writeOpGroup;
    private boolean closed;

    private ReadExecutionController(OpOrder baseOp,
                                    TableMetadata baseMetadata,
                                    ReadExecutionController indexController,
                                    OpOrder writeOp)
    {
        // We can have baseOp == null, but only when empty() is called, in which case the controller will never really be used
        // (which validForReadOn should ensure). But if it's not null, we should have the proper metadata too.
        assert (baseOp == null) == (baseMetadata == null);
        this.baseOp = baseOp;
        this.baseMetadata = baseMetadata;
        this.indexController = indexController;
        this.writeOp = writeOp;
        this.closed = false;
    }

    public ReadExecutionController indexReadController()
    {
        return indexController;
    }

    public OpOrder.Group writeOpOrderGroup()
    {
        assert writeOpGroup != null;
        return writeOpGroup;
    }

    public boolean startIfValid(ColumnFamilyStore cfs)
    {
        if (closed || !cfs.metadata.id.equals(baseMetadata.id) || baseOp == null)
            return false;

        if (baseOpGroup == null)
        {
            baseOpGroup = baseOp.start();
            if (writeOp != null)
                writeOpGroup = writeOp.start();
        }

        return true;
    }

    public static ReadExecutionController empty()
    {
        return new ReadExecutionController(null, null, null, null);
    }

    /**
     * Creates an execution controller for the provided command.
     *
     * @param command the command for which to create a controller.
     * @return the created execution controller, which must always be closed.
     */
    @SuppressWarnings("resource") // ops closed during controller close
    public static ReadExecutionController forCommand(ReadCommand command)
    {
        ColumnFamilyStore baseCfs = Keyspace.openAndGetStore(command.metadata());
        ColumnFamilyStore indexCfs = maybeGetIndexCfs(baseCfs, command);

        if (indexCfs == null)
        {
            return new ReadExecutionController(baseCfs.readOrdering, baseCfs.metadata(), null, null);
        }
        else
        {
            OpOrder baseOp = null, writeOp = null;
            ReadExecutionController indexController = null;
            // OpOrder.start() shouldn't fail, but better safe than sorry.
            try
            {
                baseOp = baseCfs.readOrdering;
                indexController = new ReadExecutionController(indexCfs.readOrdering, indexCfs.metadata(), null, null);
                // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
                // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
                writeOp = Keyspace.writeOrder;
                return new ReadExecutionController(baseOp, baseCfs.metadata(), indexController, writeOp);
            }
            catch (RuntimeException e)
            {
                if (indexController != null)
                    indexController.close();

                throw e;
            }
        }
    }

    private static ColumnFamilyStore maybeGetIndexCfs(ColumnFamilyStore baseCfs, ReadCommand command)
    {
        Index index = command.getIndex(baseCfs);
        return index == null ? null : index.getBackingTable().orElse(null);
    }

    public TableMetadata metadata()
    {
        return baseMetadata;
    }

    public void close()
    {
        if (closed)
            return; // this should be idempotent

        closed = true;

        Throwable fail = null;
        fail = Throwables.closeNonNull(fail, baseOpGroup);
        fail = Throwables.closeNonNull(fail, indexController);
        fail = Throwables.closeNonNull(fail, writeOpGroup);
        if (fail != null)
        {
            JVMStabilityInspector.inspectThrowable(fail);
            logger.error("Failed to close ReadExecutionController: {}", fail);
        }
    }
}
