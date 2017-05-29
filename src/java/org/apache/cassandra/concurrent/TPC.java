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
package org.apache.cassandra.concurrent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.FastThreadLocal;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.Inline;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderSimple;
import org.apache.cassandra.utils.concurrent.OpOrderThreaded;

/**
 * Static methods at the core of the TPC architecture, allowing to get the proper scheduler for a given key, check
 * if the current thread if a TPC one, etc...
 *
 * This is initialized on startup:
 * @see CassandraDaemon#initializeTPC()
 *
 * Each loop run managed on a single thread ({@link TPCThread}) which may be pinned to a particular CPU. Apollo can
 * route tasks relative to a particular partition to a single loop thereby avoiding any multi-threaded access, removing
 * the need to concurrent datastructures and locks.
 */
public class TPC
{
    private static final Logger logger = LoggerFactory.getLogger(TPC.class);

    /**
     * Set this to true in order to log the caller's thread stack trace in case of exception when running a task on an Rx scheduler.
     */
    private static boolean LOG_CALLER_STACK_ON_EXCEPTION = System.getProperty("cassandra.log_caller_stack_on_tpc_exception", "false")
                                                                 .equalsIgnoreCase("true");

    private static final int NUM_CORES = DatabaseDescriptor.getTPCCores();
    private static final int NIO_IO_RATIO = Integer.valueOf(System.getProperty("io.netty.ratioIO", "50"));
    public static final boolean USE_EPOLL = Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true"))
                                            && Epoll.isAvailable();

    private final static FastThreadLocal<TPCScheduler> threadLocalEventLoopReferences = new FastThreadLocal<>();

    final static Map<String, List<Token>> keyspaceToRangeMapping = new HashMap<>();

    // monotonically increased in order to distribute in a round robin fashion the next core for scheduling a task
    private final static AtomicLong roundRobinIndex = new AtomicLong(0);

    // The core event loops as a Netty EventLoopGroup. The group is created to contain exactly NUM_CORES loops.
    private static final TPCEventLoopGroup eventLoopGroup;

    // Maps each core ID to its TPCScheduler (which wraps the corresponding event loop from eventLoopGroup).
    private final static TPCScheduler[] perCoreSchedulers = new TPCScheduler[NUM_CORES];

    private final static OpOrderThreaded.ThreadIdentifier threadIdentifier = new OpOrderThreaded.ThreadIdentifier()
    {
        public int idFor(Thread t)
        {
            // For TPC threads, we return their coreId. For any other thread, we want to use a "shared" ID and use the
            // first available one, namely NUM_CORES. This is what getCoreId gives us.
            return getCoreId(t);
        }

        public boolean barrierPermitted()
        {
            return !isTPCThread();
        }
    };

    // Initialization
    static
    {
        logger.debug("Initializing TPC");
        // Creates the event loops
        if (USE_EPOLL)
        {
            eventLoopGroup = new EpollTPCEventLoopGroup(NUM_CORES);
            logger.info("Created {} epoll event loops.", NUM_CORES);
        }
        else
        {
            NioTPCEventLoopGroup group = new NioTPCEventLoopGroup(NUM_CORES);
            group.setIoRatio(NIO_IO_RATIO);
            ApproximateTime.schedule(group.next());
            eventLoopGroup = group;
            logger.info("Created {} NIO event loops (with I/O ratio set to {}).", NUM_CORES, NIO_IO_RATIO);
        }

        // Then create and set the scheduler corresponding to each event loop. Note that the initialization of each
        // scheduler must be done on the thread corresponding to that scheduler/event loop because 1) we need to be able
        // to access said thread easily and 2) we set thread locals as part of the initialization.
        logger.debug(">> Starting creating schedulers");
        eventLoopGroup.eventLoops().forEach(TPC::register);

        initRx();

        logger.debug("TPC fully initialized");
    }

    private static void register(TPCEventLoop loop)
    {
        int coreId = loop.thread().coreId();
        assert coreId >= 0 && coreId < NUM_CORES;
        assert perCoreSchedulers[coreId] == null;

        TPCScheduler scheduler = new TPCScheduler(loop);
        perCoreSchedulers[coreId] = scheduler;
        threadLocalEventLoopReferences.set(loop.thread().threadLocalMap(), scheduler);
    }

    private static void initRx()
    {
        RxJavaPlugins.setComputationSchedulerHandler((s) -> TPC.bestTPCScheduler());
        RxJavaPlugins.setErrorHandler(e -> CassandraDaemon.defaultExceptionHandler.accept(Thread.currentThread(), e));

        /**
         * This handler wraps every scheduled task with a runnable that sets the thread local state to
         * the same state as the thread requesting the task to be scheduled, that means every time
         * a scheduler subscribe is called, and therefore indirectly every time observeOn or subscribeOn
         * are called. Provided that the thread local of the calling thread is not changed after scheduling
         * the task, we can be confident that the scheduler's thread will inherit the same thread state,
         * see APOLLO-488 for more details.
         */
        RxJavaPlugins.setScheduleHandler((runnable) -> {
            Runnable ret = runnable instanceof ExecutorLocals.WrappedRunnable
                           ? runnable
                           : new ExecutorLocals.WrappedRunnable(runnable);

            return LOG_CALLER_STACK_ON_EXCEPTION ? new RunnableWithCallerThreadInfo(ret) : ret;
        });

        //RxSubscriptionDebugger.enable();
    }

    public static void ensureInitialized()
    {
        // The only goal of this method is to make sure TPC _is_ initialized by the time this method returns, but
        // as initialization is static, simply having it called will ensure that.
    }

    /**
     * The {@link EventLoopGroup} holding our internal Thread-Per-Core event loops. That group is re-used to handle
     * I/O tasks through Netty.
     */
    public static EventLoopGroup eventLoopGroup()
    {
        return eventLoopGroup;
    }

    /**
     * Returns the TPC scheduler corresponding to the current thread <b>assuming</b> the current thread is a TPC thread.
     * <p>
     * It is a programming error to call this on a non-TPC thread and the method will throw. This is to be used when
     * you know you are supposed to be on a TPC thread and this is important for performance so you want the code to
     * complain loudly if that assertion is violated.
     * <p>
     * If you are not sure to be on a TPC scheduler and don't particular care which scheduler/core to use, then you
     * should prefer the {@link #bestTPCScheduler()} method.
     */
    public static TPCScheduler currentThreadTPCScheduler()
    {
        assert isTPCThread() : "This method should not be called from a non-TPC thread.";
        return threadLocalEventLoopReferences.get();
    }

    /**
     * Returns the "best" TPC scheduler if no particular core is preferred (typically because the task is not based on
     * a particular token). In practice, this return the current thread if we are already on a TPC thread, or this
     * return a "random" TPC scheduler (random as far as the caller is concerned, but in practice we round-robin the
     * returned scheduler through {@link #getNextCore()}).
     * <p>
     * If you don't want to favour the current thread if it's a TPC thread, simply use {@code getForCore(getNextCore())}.
     */
    public static TPCScheduler bestTPCScheduler()
    {
        TPCScheduler scheduler = threadLocalEventLoopReferences.get();
        return scheduler == null ? getForCore(getNextCore()) : scheduler;
    }

    /**
     * Creates a new {@link OpOrder} suitable for synchronizing operations that mostly executes on TPC threads.
     * <p>
     * More precisely, the returned {@link OpOrder} reduces contentions between operations calling
     * {@link OpOrder#start()} if those operations are on a TPC thread by internally using per-TPC-thread "groups". It
     * is still valid to call {@link OpOrder#start()} from a non TPC thread, but all such calls will contend with one
     * another (in other words, if all calls to {@link OpOrder#start()} are done from non-TPC thread, the returned
     * {@code OpOrder} won't provide any benefit over a simple {@link OpOrderSimple}, but it will help if most are from
     * TPC threads).
     * <p>
     * Note however that the {@link OpOrder#newBarrier()} method on the returned object <b>must</b> only be called from
     * a <b>non</b>-TPC thread (an assertion error will be thrown if that's not the case) as it is blocking by nature.
     *
     * @param creator the object for which the {@code OpOrder} is created. Mainly use for debugging purposes.
     * @return the newly created {@code OpOrder}.
     */
    public static OpOrder newOpOrder(Object creator)
    {
        // As mention above, we avoid contention for operations on TPC thread by using a separate "id" (in the
        // OpOrderThreaded parlance) for each such thread (we simply use the core ID). Any other thread ends up using
        // a shared "id", which is why 1) we use NUM_CORES+1 as "idLimit" and why 2) operations on non-TPC thread do
        // still contend. Also see the definition of threadIdentifier for how we identify the "id" to any thread.
        return new OpOrderThreaded(creator, threadIdentifier, NUM_CORES + 1);
    }

    /**
     * @return the core id for netty threads, otherwise the number of cores. Callers can verify if the returned
     * core is valid via {@link TPC#isValidCoreId(int)}, or alternatively can allocate an
     * array with length num_cores + 1, and use thread safe operations only on the last element.
     */
    public static int getCoreId()
    {
        return getCoreId(Thread.currentThread());
    }

    /**
     * Whether the current thread is the TPC thread corresponding to the provided core ID.
     *
     * @param coreId the coreId to check.
     * @return {@code true} if we're on the TPC thread of core ID {@code coreId}, {@code false} otherwise (including
     * when we're not on a TPC thread at all).
     */
    public static boolean isOnCore(int coreId)
    {
        return getCoreId() == coreId;
    }

    /**
     * @return the core id for netty threads, otherwise the number of cores. Callers can verify if the returned
     * core is valid via {@link TPC#isValidCoreId(int)}, or alternatively can allocate an
     * array with length num_cores + 1, and use thread safe operations only on the last element.
     */
    private static int getCoreId(Thread t)
    {
        return t instanceof TPCThread ? ((TPCThread)t).coreId() : NUM_CORES;
    }

    public static boolean isTPCThread(Thread thread)
    {
        return thread instanceof TPCThread;
    }

    public static boolean isTPCThread()
    {
        return isTPCThread(Thread.currentThread());
    }

    public static int getNumCores()
    {
        return NUM_CORES;
    }

    /**
     * Return a valid core number for scheduling one or more tasks always on the same core.
     * To balance the execution of tasks, we select the next available core in a round-robin fashion.
     *
     * This method should normally be called during initialization, it should not be called
     * by methods in the critical execution patch, since the modulo operator is not optimized.
     *
     * @return a valid core id, distributed in a round-robin way
     */
    public static int getNextCore()
    {
        return (int)(roundRobinIndex.getAndIncrement() % getNumCores());
    }

    /**
     * Return a scheduler for a specific core.
     *
     * @param core - the core number for which we want a scheduler of
     *
     * @return - the scheduler of the core specified, or the scheduler of core zero if not yet assigned
     */
    public static TPCScheduler getForCore(int core)
    {
        return perCoreSchedulers[core];
    }

    public static boolean isValidCoreId(int coreId)
    {
        return coreId >= 0 && coreId < getNumCores();
    }

    /**
     * Return the id of the core that is assigned to run operations on the specified keyspace
     * and partition key, see {@link TPC#perCoreSchedulers}.
     * <p>
     * Core zero is returned if {@link StorageService} is not yet initialized,
     * since in this case we cannot assign any partition key to any core.
     *
     * @param keyspaceName - the keyspace name
     * @param key - the partition key
     *
     * @return the core id for this partition
     */

    @Inline
    public static int getCoreForKey(String keyspaceName, DecoratedKey key)
    {
        // nothing we can do until we have the local ranges
        if (!StorageService.instance.isInitialized())
            return 0;

        Token keyToken;

        if (key.getPartitioner() == DatabaseDescriptor.getPartitioner())
        {
            keyToken = key.getToken();
        }
        else
        {
            // Convert OP partitions to top level partitioner for secondary indexes; always route
            // system table mutations through core 0
            if (SchemaConstants.isSystemKeyspace(keyspaceName))
                return 0;

            keyToken = DatabaseDescriptor.getPartitioner().getToken(key.getKey());
        }

        List<Token> keyspaceRanges = getRangeList(keyspaceName, true);


        // Note that keyspaceRanges starts with the min token and end with the max one (see SS.getCpuBoundaries()) so
        // we know we'll find the token, and we can skip comparing the first and last token.

        for (int i = 1; i < keyspaceRanges.size() - 1; i++)
        {
            if (keyToken.compareTo(keyspaceRanges.get(i)) < 0)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Read moving to {} from {}", i-1, getCoreId());
                return i - 1;
            }
        }

        // As mentioned above, we skipped the last token because it's the max token. If we get here, we know the token
        // belong to the last cpu.
        return perCoreSchedulers.length - 1;
    }

    /**
     * Return the Netty rx scheduler of the core that is assigned to run operations on the specified keyspace
     * and partition key, see {@link TPC#perCoreSchedulers}.
     * <p>
     * The scheduler for core zero is returned if {@link StorageService} is not yet initialized,
     * since in this case we cannot assign any partition key to any core.
     *
     * @param keyspaceName - the keyspace name
     * @param key - the partition key
     *
     * @return the Netty RX scheduler
     */
    @Inline
    public static TPCScheduler getForKey(String keyspaceName, DecoratedKey key)
    {
        return getForCore(getCoreForKey(keyspaceName, key));
    }

    public static List<Token> getRangeList(String keyspaceName, boolean persist)
    {
        return getRangeList(Keyspace.open(keyspaceName), persist);
    }

    public static List<Token> getRangeList(Keyspace keyspace, boolean persist)
    {
        List<Token> ranges = keyspaceToRangeMapping.get(keyspace.getName());

        if (ranges != null)
            return ranges;

        List<Range<Token>> localRanges = StorageService.getStartupTokenRanges(keyspace);
        List<Token> splits = StorageService.getCpuBoundaries(localRanges, DatabaseDescriptor.getPartitioner(), NUM_CORES)
                                           .stream()
                                           .map(RingPosition::getToken)
                                           .collect(Collectors.toList());

        if (persist)
        {
            if (isTPCThread() && currentThreadTPCScheduler().thread().coreId() == 0)
            {
                keyspaceToRangeMapping.put(keyspace.getName(), splits);
            }
            else
            {
                CountDownLatch ready = new CountDownLatch(1);

                getForCore(0).scheduleDirect(() -> {
                    keyspaceToRangeMapping.put(keyspace.getName(), splits);
                    ready.countDown();
                });

                Uninterruptibles.awaitUninterruptibly(ready);
            }
        }

        return splits;
    }

    /**
     * Log the caller thread stack trace in case of exception when running a task.
     */
    private static final class RunnableWithCallerThreadInfo implements Runnable
    {
        private final Runnable runnable;
        private final FBUtilities.Debug.ThreadInfo threadInfo;

        RunnableWithCallerThreadInfo(Runnable runnable)
        {
            this.runnable = runnable;
            this.threadInfo = new FBUtilities.Debug.ThreadInfo();
        }

        public void run()
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                logger.error("Got exception {} with message <{}> when running Rx task. Caller's thread stack:\n{}",
                             t.getClass().getName(), t.getMessage(),
                             FBUtilities.Debug.getStackTrace(threadInfo));
                throw t;
            }
        }
    }

}
