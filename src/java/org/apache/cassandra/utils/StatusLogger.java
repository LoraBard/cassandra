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
package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.TreeSet;
import javax.management.MBeanServer;

import com.google.common.collect.Multimap;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.big.BigRowIndexEntry;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tools.nodetool.stats.TpStatsPrinter;
import org.apache.cassandra.utils.memory.BufferPool;

public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);


    public static void log()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        // everything from o.a.c.concurrent
        String headerFormat = "%-" + (TpStatsPrinter.longestStrLength(Arrays.asList(TPCTaskType.values())) + 5) + "s%10s%10s%15s%10s%18s%n";
        logger.info(String.format(headerFormat, "Pool Name", "Active", "Pending", "Completed", "Blocked", "All Time Blocked"));

        Multimap<String, String> jmxThreadPools = ThreadPoolMetrics.getJmxThreadPools(server);
        for (String poolKey : jmxThreadPools.keySet())
        {
            // Sort values
            TreeSet<String> poolValues = new TreeSet<>(jmxThreadPools.get(poolKey));
            for (String poolValue : poolValues)
                logger.info(String.format(headerFormat,
                                          poolValue,
                                          ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "ActiveTasks"),
                                          ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "PendingTasks"),
                                          ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CompletedTasks"),
                                          ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CurrentlyBlockedTasks"),
                                          ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBlockedTasks")));
        }

        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", CompactionManager.instance.getActiveCompactions(), CompactionManager.instance.getPendingTasks()));
        int pendingLargeMessages = 0;
        for (int n : MessagingService.instance().getLargeMessagePendingTasks().values())
        {
            pendingLargeMessages += n;
        }
        int pendingSmallMessages = 0;
        for (int n : MessagingService.instance().getSmallMessagePendingTasks().values())
        {
            pendingSmallMessages += n;
        }
        logger.info(String.format("%-25s%10s%10s",
                                  "MessagingService", "n/a", pendingLargeMessages + "/" + pendingSmallMessages));

        //BufferPool stats
        logger.info("Global file buffer pool size: {}", FBUtilities.prettyPrintMemory(BufferPool.sizeInBytes()));

        //MemtablePool stats
        logger.info("Global memtable buffer pool size: onHeap = {}, offHeap = {}",
                    FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.onHeap.used()),
                    FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.offHeap.used()));

        // Global key/row cache information
        AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache = CacheService.instance.keyCache;
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "Cache Type", "Size", "Capacity", "KeysToSave"));
        logger.info(String.format("%-25s%10s%25s%25s",
                                  "KeyCache",
                                  keyCache.weightedSize(),
                                  keyCache.getCapacity(),
                                  keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave));

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "RowCache",
                                  rowCache.weightedSize(),
                                  rowCache.getCapacity(),
                                  rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave));

        // per-CF stats
        logger.info(String.format("%-25s%20s", "Table", "Memtable ops,data"));
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            logger.info(String.format("%-25s%20s",
                                      cfs.keyspace.getName() + "." + cfs.name,
                                      cfs.metric.memtableColumnsCount.getValue() + "," + cfs.metric.memtableLiveDataSize.getValue()));
        }
    }
}
