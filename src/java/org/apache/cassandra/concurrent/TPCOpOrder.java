/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Single;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers (writing threads) are modifying a structure that the consumer
 * (flush executor) only batch syncs, but needs to know what 'position' the work is at
 * for co-ordination with other processes,
 *
 * <p>The typical usage is something like:
 * <pre>
 * {@code
public final class ExampleShared
{
final OpOrder order = new OpOrder();
volatile SharedState state;

static class SharedState
{
volatile Barrier barrier;

// ...
}

public void consume()
{
SharedState state = this.state;
state.setReplacement(new State())
state.doSomethingToPrepareForBarrier();

state.barrier = order.newBarrier();
// seal() MUST be called after newBarrier() else barrier.isAfter()
// will always return true, and barrier.await() will fail
state.barrier.issue();

// wait for all producer work started prior to the barrier to complete
state.barrier.await();

// change the shared state to its replacement, as the current state will no longer be used by producers
this.state = state.getReplacement();

state.doSomethingWithExclusiveAccess();
}

public void produce()
{
try (Group opGroup = order.start())
{
SharedState s = state;
while (s.barrier != null && !s.barrier.isAfter(opGroup))
s = s.getReplacement();
s.doProduceWork();
}
}
}
 * }
 * </pre>
 */
public class TPCOpOrder
{
    private static final boolean DEBUG_OP_ORDER_CALLERS = false;

    private static final class DebugThreadInfo
    {
        private final String name;
        private final int callingCoreId;
        private final StackTraceElement[] stackTraceElements;

        DebugThreadInfo(Thread t, int callingCoreId)
        {
            this.name = t.getName();
            this.callingCoreId = callingCoreId;
            this.stackTraceElements = t.getStackTrace();
        }

        @Override
        public String toString()
        {
            StringBuilder ret = new StringBuilder();
            ret.append(name).append(" - calling core id: ").append(callingCoreId).append('\n');
            for (StackTraceElement t : stackTraceElements)
                ret.append("\t at ").append(t.toString()).append('\n');
            ret.append('\n');

            return ret.toString();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TPCOpOrder.class);

    /**
     * Constant that when an Ordered.running is equal to, indicates the Ordered is complete
     */
    private static final int FINISHED = -1;

    /**
     * A linked list starting with the most recent Ordered object, i.e. the one we should start new operations from,
     * with (prev) links to any incomplete Ordered instances, and (next) links to any potential future Ordered instances.
     * Once all operations started against an Ordered instance and its ancestors have been finished the next instance
     * will unlink this one
     */
    private volatile Group current;
    private final int coreId;

    public TPCOpOrder(int coreId, OpOrder parent)
    {
        this.coreId = coreId;
        this.current = new Group(coreId, parent);
    }

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public Group start(int callingCore)
    {
        final DebugThreadInfo debugThreadInfo = DEBUG_OP_ORDER_CALLERS ? new DebugThreadInfo(Thread.currentThread(), callingCore) : null;
        Callable<Group> c = () ->
        {
            boolean ret = current.register(debugThreadInfo);
            if (!ret)
            {
                current.logThreadInfo("Failed to register new operation to local op order group");
                throw new IllegalStateException("Failed to register new operation to local op order group");
            }
            return current;
        };

        if (callingCore != coreId)
        {
            Single<Group> g = Single.fromCallable(c);
            //logger.debug("Calling core {} not oporder owner will route open() to core {}", callingCore, coreId);
            g = g.subscribeOn(NettyRxScheduler.getForCore(coreId));
            return g.blockingGet();
        }

        try
        {
            return c.call();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Group getCurrent()
    {
        return current;
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it,
     * after which all new operations will start against a new Group that will not be accepted
     * by barrier.isAfter(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     *
     * @return
     */
    public Barrier newBarrier()
    {
        return new Barrier();
    }


    /**
     * Represents a group of identically ordered operations, i.e. all operations started in the interval between
     * two barrier issuances. For each register() call this is returned, close() must be called exactly once.
     * It should be treated like taking a lock().
     */
    public static final class Group implements Comparable<Group>, AutoCloseable
    {
        /**
         * In general this class goes through the following stages:
         * 1) LIVE:      many calls to register() and close()
         * 2) FINISHING: a call to expire() (after a barrier issue), means calls to register() will now fail,
         *               and we are now 'in the past' (new operations will be started against a new Ordered)
         * 3) FINISHED:  once the last close() is called, this Ordered is done. We call unlink().
         * 4) ZOMBIE:    all our operations are finished, but some operations against an earlier Ordered are still
         *               running, or tidying up, so unlink() fails to remove us
         * 5) COMPLETE:  all operations started on or before us are FINISHED (and COMPLETE), so we are unlinked
         * <p/>
         * Another parallel states is ISBLOCKING:
         * <p/>
         * isBlocking => a barrier that is waiting on us (either directly, or via a future Ordered) is blocking general
         * progress. This state is entered by calling Barrier.markBlocking(). If the running operations are blocked
         * on a Signal that is also registered with the isBlockingSignal (probably through isSafeBlockingSignal)
         * then they will be notified that they are blocking forward progress, and may take action to avoid that.
         */

        private volatile Group prev, next;
        public final int coreId;
        private final long id; // monotonically increasing id for compareTo()
        private volatile int running = 0; // number of operations currently running.  < 0 means we're expired, and the count of tasks still running is -(running + 1)
        private volatile boolean isBlocking; // indicates running operations are blocking future barriers
        private final WaitQueue isBlockingSignal = new WaitQueue(); // signal to wait on to indicate isBlocking is true
        private final WaitQueue waiting = new WaitQueue(); // signal to wait on for completion
        private final OpOrder parent;

        private final List<DebugThreadInfo> openingThreads = DEBUG_OP_ORDER_CALLERS ? new ArrayList<>(16) : null;
        private final List<DebugThreadInfo> closingThreads = DEBUG_OP_ORDER_CALLERS ? new ArrayList<>(16) : null;

        // constructs first instance only
        private Group(int coreId, OpOrder parent)
        {
            this.coreId = coreId;
            this.id = 0;
            this.parent = parent;
        }

        private Group(Group prev)
        {
            this.coreId = prev.coreId;
            this.id = prev.id + 1;
            this.prev = prev;
            this.parent = prev.parent;
        }

        // prevents any further operations starting against this Ordered instance
        // if there are no running operations, calls unlink; otherwise, we let the last op to close call it.
        // this means issue() won't have to block for ops to finish.
        private void expire()
        {
            if (running < 0)
                throw new IllegalStateException("A local op order was expired more then once.");

            int current = running;

            running = -1 - running;

            // if we're already finished (no running ops), unlink ourselves
            if (current == 0)
                unlink();
        }

        // attempts to start an operation against this Ordered instance, and returns true if successful.
        private boolean register(DebugThreadInfo debugThreadInfo)
        {
            if (running < 0)
                return false;

            if (DEBUG_OP_ORDER_CALLERS)
                openingThreads.add(debugThreadInfo);

            ++running;
            return true;
        }

        /**
         * To be called exactly once for each register() call this object is returned for, indicating the operation
         * is complete
         */
        public void close()
        {
            final NettyRxScheduler scheduler = NettyRxScheduler.instance();
            final DebugThreadInfo debugThreadInfo = DEBUG_OP_ORDER_CALLERS ? new DebugThreadInfo(Thread.currentThread(), scheduler.cpuId) : null;

            Runnable c = () ->
            {
                if (DEBUG_OP_ORDER_CALLERS)
                    closingThreads.add(debugThreadInfo);

                if (running < 0)
                {
                    ++running;

                    if (running == FINISHED)
                    {
                        // if we're now finished, unlink ourselves
                        unlink();
                    }
                }
                else
                {
                    --running;
                    if (running < 0)
                    {
                        logThreadInfo("A local op oprder was closed more than once");
                        throw new IllegalStateException("A local op order was closed more than once.");
                    }
                }
            };

            if (scheduler.cpuId != coreId)
            {
                //logger.debug("Calling core {} not oporder owner will route close() to core {}", scheduler.cpuId, coreId);
                NettyRxScheduler.getForCore(coreId).scheduleDirect(c);
                return;
            }

            c.run();
        }

        private void logThreadInfo(String message)
        {
            if (DEBUG_OP_ORDER_CALLERS)
            {
                logger.debug("{} - parent: {}", message, parent.toString());
                logger.debug("Opening threads: \n{}", openingThreads.stream().map(DebugThreadInfo::toString).collect(Collectors.joining("\n")));
                logger.debug("Closing threads: \n{}", closingThreads.stream().map(DebugThreadInfo::toString).collect(Collectors.joining("\n")));
            }
        }
        /**
         * called once we know all operations started against this Ordered have completed,
         * however we do not know if operations against its ancestors have completed, or
         * if its descendants have completed ahead of it, so we attempt to create the longest
         * chain from the oldest still linked Ordered. If we can't reach the oldest through
         * an unbroken chain of completed Ordered, we abort, and leave the still completing
         * ancestor to tidy up.
         */
        private void unlink()
        {
            // walk back in time to find the start of the list
            Group start = this;
            while (true)
            {
                Group prev = start.prev;
                if (prev == null)
                    break;
                // if we haven't finished this Ordered yet abort and let it clean up when it's done
                if (prev.running > FINISHED)
                    return;
                start = prev;
            }

            // now walk forwards in time, in case we finished up late
            Group end = this.next;
            while (end.running <= FINISHED)
                end = end.next;

            // now walk from first to last, unlinking the prev pointer and waking up any blocking threads
            while (start != end)
            {
                Group next = start.next;
                next.prev = null;
                start.waiting.signalAll();
                start = next;
            }
        }

        /**
         * @return true if a barrier we are behind is, or may be, blocking general progress,
         * so we should try more aggressively to progress
         */
        public boolean isBlocking()
        {
            return isBlocking;
        }

        /**
         * register to be signalled when a barrier waiting on us is, or maybe, blocking general progress,
         * so we should try more aggressively to progress
         */
        private WaitQueue.Signal isBlockingSignal()
        {
            return isBlockingSignal.register(Thread.currentThread());
        }

        /**
         * wrap the provided signal to also be signalled if the operation gets marked blocking
         */
        public WaitQueue.Signal isBlockingSignal(WaitQueue.Signal signal)
        {
            return WaitQueue.any(signal, isBlockingSignal());
        }

        public int compareTo(Group that)
        {
            // we deliberately use subtraction, as opposed to Long.compareTo() as we care about ordering
            // not which is the smaller value, so this permits wrapping in the unlikely event we exhaust the long space
            long c = this.id - that.id;
            if (c > 0)
                return 1;
            else if (c < 0)
                return -1;
            else
                return 0;
        }
    }

    /**
     * This class represents a synchronisation point providing ordering guarantees on operations started
     * against the enclosing OpOrder.  When issue() is called upon it (may only happen once per Barrier), the
     * Barrier atomically partitions new operations from those already running (by expiring the current Group),
     * and activates its isAfter() method
     * which indicates if an operation was started before or after this partition. It offers methods to
     * determine, or block until, all prior operations have finished, and a means to indicate to those operations
     * that they are blocking forward progress. See {@link TPCOpOrder} for idiomatic usage.
     */
    public final class Barrier
    {
        // this Barrier was issued after all Group operations started against orderOnOrBefore
        private volatile Group orderOnOrBefore;

        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior you are probably
         * Doing It Wrong.)
         */
        public boolean isAfter(Group group)
        {
            if (orderOnOrBefore == null)
                return true;
            // we subtract to permit wrapping round the full range of Long - so we only need to ensure
            // there are never Long.MAX_VALUE * 2 total Group objects in existence at any one timem which will
            // take care of itself
            return orderOnOrBefore.id - group.id >= 0;
        }

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue()
        {
            if (orderOnOrBefore != null)
                throw new IllegalStateException("Can only call issue() once on each Barrier");

            final Group oldCurrent = current;
            orderOnOrBefore = oldCurrent;
            current = oldCurrent.next = new Group(current);
            oldCurrent.expire();
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            Group current = orderOnOrBefore;
            while (current != null)
            {
                current.isBlocking = true;
                current.isBlockingSignal.signalAll();
                current = current.prev;
            }
        }

        /**
         * Register to be signalled once allPriorOpsAreFinished() or allPriorOpsAreFinishedOrSafe() may return true
         */
        public WaitQueue.Signal register(Thread caller)
        {
            return orderOnOrBefore.waiting.register(caller);
        }

        /**
         * @return true if all operations started prior to barrier.issue() have completed
         */
        public boolean allPriorOpsAreFinished()
        {
            Group current = orderOnOrBefore;
            if (current == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
            if (current.next.prev == null)
                return true;

            return false;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public Optional<WaitQueue.Signal> await(Thread caller)
        {
            while (!allPriorOpsAreFinished())
            {
                WaitQueue.Signal signal = register(caller);
                if (allPriorOpsAreFinished())
                {
                    signal.cancel();
                    return Optional.empty();
                }

                return Optional.of(signal);
            }

            return Optional.empty();
        }

        /**
         * returns the Group we are waiting on - any Group with {@code .compareTo(getSyncPoint()) <= 0}
         * must complete before await() returns
         */
        public Group getSyncPoint()
        {
            return orderOnOrBefore;
        }
    }
}

