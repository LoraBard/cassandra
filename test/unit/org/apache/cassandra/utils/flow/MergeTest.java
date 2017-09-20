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
package org.apache.cassandra.utils.flow;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class MergeTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        LineNumberInference.init();
    }

    private static class CountingComparator<T> implements Comparator<T>
    {
        final Comparator<T> wrapped;
        int count = 0;

        protected CountingComparator(Comparator<T> wrapped)
        {
            this.wrapped = wrapped;
        }

        public int compare(T o1, T o2)
        {
            count++;
            return wrapped.compare(o1, o2);
        }
    }

    static int ITERATOR_COUNT = 15;
    static int LIST_LENGTH = 4500;
    static int DELAY_CHANCE = 9;            // 1/this of the inputs will be async delayed randomly, 0 for all
    static int SCHEDULE_CHANCE = 4;
    Random rand = new Random();

    @BeforeClass
    public static void setup()
    {
        // This initializes metrics, which depend on the number of cores, which depends on the Yaml.
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRandomInts()
    {
        System.out.println("testRandomInts");
        final Random r = new Random();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = new NaturalListGenerator<Integer>(ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public Integer next()
            {
                return r.nextInt(5 * LIST_LENGTH);
            }
        }.result;
        testMergeFlow(reducer, lists);
    }
    
    @Test
    public void testNonOverlapInts()
    {
        System.out.println("testNonOverlapInts");
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = new NaturalListGenerator<Integer>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 1;
            @Override
            public Integer next()
            {
                return next++;
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testCombinationInts()
    {
        System.out.println("testCombinationInts");
        final Random r = new Random();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = new NaturalListGenerator<Integer>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 1;
            @Override
            public Integer next()
            {
                return r.nextBoolean() ? r.nextInt(5 * LIST_LENGTH) : next++;
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testLCSTotalOverlap()
    {
        testLCS(2, LIST_LENGTH / 100, 1f);
        testLCS(3, LIST_LENGTH / 100, 1f);
        testLCS(3, LIST_LENGTH / 100, 1f, 10, LIST_LENGTH);
        testLCS(4, LIST_LENGTH / 100, 1f);
        testLCS(4, LIST_LENGTH / 100, 1f, 10, LIST_LENGTH);
    }

    @Test
    public void testLCSPartialOverlap()
    {
        testLCS(2, LIST_LENGTH / 100, 0.5f);
        testLCS(3, LIST_LENGTH / 100, 0.5f);
        testLCS(3, LIST_LENGTH / 100, 0.5f, 10, LIST_LENGTH);
        testLCS(4, LIST_LENGTH / 100, 0.5f);
        testLCS(4, LIST_LENGTH / 100, 0.5f, 10, LIST_LENGTH);
    }

    @Test
    public void testLCSNoOverlap()
    {
        testLCS(2, LIST_LENGTH / 100, 0f);
        testLCS(3, LIST_LENGTH / 100, 0f);
        testLCS(3, LIST_LENGTH / 100, 0f, 10, LIST_LENGTH);
        testLCS(4, LIST_LENGTH / 100, 0f);
        testLCS(4, LIST_LENGTH / 100, 0f, 10, LIST_LENGTH);
    }

    public void testLCS(int levelCount, int levelMultiplier, float levelOverlap)
    {
        testLCS(levelCount, levelMultiplier, levelOverlap, 0, 0);
    }
    public void testLCS(int levelCount, int levelMultiplier, float levelOverlap, int countOfL0, int sizeOfL0)
    {
        System.out.printf("testLCS(lc=%d,lm=%d,o=%.2f,L0=%d*%d)\n", levelCount, levelMultiplier, levelOverlap, countOfL0, countOfL0 == 0 ? 0 : sizeOfL0 / countOfL0);
        final Random r = new Random();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();
        List<List<Integer>> lists = new LCSGenerator<Integer>(Ordering.<Integer>natural(), levelCount, levelMultiplier, levelOverlap) {
            @Override
            public Integer newItem()
            {
                return r.nextInt();
            }
        }.result;
        if (sizeOfL0 > 0 && countOfL0 > 0)
            lists.addAll(new NaturalListGenerator<Integer>(countOfL0, sizeOfL0 / countOfL0)
            {
                Integer next()
                {
                    return r.nextInt();
                }
            }.result);
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testRandomStrings()
    {
        System.out.println("testRandomStrings");
        final Random r = new Random();
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = new NaturalListGenerator<String>(ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public String next()
            {
                return "longish_prefix_" + r.nextInt(5 * LIST_LENGTH);
            }
        }.result;
        testMergeFlow(reducer, lists);
    }
    
    @Test
    public void testNonOverlapStrings()
    {
        System.out.println("testNonOverlapStrings");
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = new NaturalListGenerator<String>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 1;
            @Override
            public String next()
            {
                return "longish_prefix_" + next++;
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testCombinationStrings()
    {
        System.out.println("testCombinationStrings");
        final Random r = new Random();
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = new NaturalListGenerator<String>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 1;
            public String next()
            {
                return "longish_prefix_" + (r.nextBoolean() ? r.nextInt(5 * LIST_LENGTH) : next++);
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testTimeUuids()
    {
        System.out.println("testTimeUuids");
        Reducer<UUID, Counted<UUID>> reducer = new Counter<UUID>();

        List<List<UUID>> lists = new NaturalListGenerator<UUID>(ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public UUID next()
            {
                return UUIDGen.getTimeUUID();
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testRandomUuids()
    {
        System.out.println("testRandomUuids");
        Reducer<UUID, Counted<UUID>> reducer = new Counter<UUID>();

        List<List<UUID>> lists = new NaturalListGenerator<UUID>(ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public UUID next()
            {
                return UUID.randomUUID();
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testTimeUuidType()
    {
        System.out.println("testTimeUuidType");
        final AbstractType<UUID> type = TimeUUIDType.instance;
        Reducer<ByteBuffer, Counted<ByteBuffer>> reducer = new Counter<ByteBuffer>();

        List<List<ByteBuffer>> lists = new SimpleListGenerator<ByteBuffer>(type, ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public ByteBuffer next()
            {
                return type.decompose(UUIDGen.getTimeUUID());
            }
        }.result;
        testMergeFlow(reducer, lists, type);
    }

    @Test
    public void testUuidType()
    {
        System.out.println("testUuidType");
        final AbstractType<UUID> type = UUIDType.instance;
        Reducer<ByteBuffer, Counted<ByteBuffer>> reducer = new Counter<ByteBuffer>();

        List<List<ByteBuffer>> lists = new SimpleListGenerator<ByteBuffer>(type, ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public ByteBuffer next()
            {
                return type.decompose(UUIDGen.getTimeUUID());
            }
        }.result;
        testMergeFlow(reducer, lists, type);
    }

    
    @Test
    public void testSets()
    {
        System.out.println("testSets");
        final Random r = new Random();

        Reducer<KeyedSet<Integer, UUID>, KeyedSet<Integer, UUID>> reducer = new Union<Integer, UUID>();

        List<List<KeyedSet<Integer, UUID>>> lists = new NaturalListGenerator<KeyedSet<Integer, UUID>>(ITERATOR_COUNT, LIST_LENGTH) {
            @Override
            public KeyedSet<Integer, UUID> next()
            {
                return new KeyedSet<>(r.nextInt(5 * LIST_LENGTH), UUIDGen.getTimeUUID());
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testLimitedOverlapStrings2()
    {
        System.out.println("testLimitedOverlapStrings2");
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = new NaturalListGenerator<String>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 0;
            @Override
            public String next()
            {
                ++next;
                int list = next / LIST_LENGTH;
                int id = next % LIST_LENGTH;
                return "longish_prefix_" + (id + list * LIST_LENGTH / 2);
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    @Test
    public void testLimitedOverlapStrings3()
    {
        System.out.println("testLimitedOverlapStrings3");
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = new NaturalListGenerator<String>(ITERATOR_COUNT, LIST_LENGTH) {
            int next = 0;
            @Override
            public String next()
            {
                ++next;
                int list = next / LIST_LENGTH;
                int id = next % LIST_LENGTH;
                return "longish_prefix_" + (id + list * LIST_LENGTH / 3);
            }
        }.result;
        testMergeFlow(reducer, lists);
    }

    private static abstract class ListGenerator<T>
    {
        abstract boolean hasMoreItems();
        abstract boolean hasMoreLists();
        abstract T next();

        final Comparator<T> comparator;
        final List<List<T>> result = Lists.newArrayList();

        protected ListGenerator(Comparator<T> comparator)
        {
            this.comparator = comparator;
        }

        void build()
        {
            while (hasMoreLists())
            {
                List<T> l = Lists.newArrayList();
                while (hasMoreItems())
                    l.add(next());
                Collections.sort(l, comparator);
                result.add(l);
            }
        }
    }

    private static abstract class NaturalListGenerator<T extends Comparable<T>> extends SimpleListGenerator<T>
    {
        private NaturalListGenerator(int listCount, int perListCount)
        {
            super(Ordering.natural(), listCount, perListCount);
        }
    }
    private static abstract class SimpleListGenerator<T> extends ListGenerator<T>
    {
        final int listCount;
        final int perListCount;

        int listIdx = 0, itemIdx = 0;

        private SimpleListGenerator(Comparator<T> comparator, int listCount, int perListCount)
        {
            super(comparator);
            this.listCount = listCount;
            this.perListCount = perListCount;
            build();
        }

        public boolean hasMoreItems()
        {
            return itemIdx++ < perListCount;
        }

        public boolean hasMoreLists()
        {
            itemIdx = 0;
            return listIdx++ < listCount;
        }
    }

    private static abstract class LCSGenerator<T> extends ListGenerator<T>
    {
        final int levelCount;
        final int itemMultiplier;
        final float levelOverlap;

        int levelIdx, itemIdx;
        int levelItems, overlapItems, runningTotalItems;
        final Random random = new Random();

        public LCSGenerator(Comparator<T> comparator, int levelCount, int l1Items, float levelOverlap)
        {
            super(comparator);
            this.levelCount = levelCount;
            this.itemMultiplier = l1Items;
            this.levelOverlap = levelOverlap;
            build();
        }

        public boolean hasMoreItems()
        {
            return itemIdx++ < levelItems;
        }

        public boolean hasMoreLists()
        {
            if (result.size() > 0)
                runningTotalItems += result.get(result.size() - 1).size();
            itemIdx = 0;
            levelItems = itemMultiplier * (int)Math.pow(10, levelCount - levelIdx);
            overlapItems = levelIdx == 0 ? 0 : (int) (levelItems * levelOverlap);
            return levelIdx++ < levelCount;
        }

        abstract T newItem();

        T next()
        {
            if (itemIdx < overlapItems)
            {
                int item = random.nextInt(runningTotalItems);
                for (List<T> list : result)
                {
                    if (item < list.size()) return list.get(item);
                    else item -= list.size();
                }
            }
            return newItem();
        }
    }

    public <T extends Comparable<T>, AutoCloseable> void testMergeFlow(Reducer<T, ?> reducer, List<List<T>> lists)
    {
        testMergeFlow(reducer, lists, Ordering.natural());
    }

    public <T, O> void testMergeFlow(Reducer<T, O> reducer, List<List<T>> lists, Comparator<T> comparator)
    {
        {
            Flow<O> tested = Merge.get(flowables(lists),
                                       comparator,
                                       reducer);
            MergeIteratorPQ<T,O> baseIter = new MergeIteratorPQ<>(closeableIterators(lists), comparator, reducer);
            List<O> base = new ArrayList<>();
            Iterators.addAll(base, baseIter);
            List<O> result = null;
            try
            {
                result = tested.reduceBlocking(new ArrayList<O>(),
                                                       (list, item) ->
                                                       {
                                                           list.add(item);
                                                           return list;
                                                       });
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }

            Assert.assertEquals(base, result);
        }
    }

    <T> List<Flow<T>> flowables(List<List<T>> lists)
    {
        return lists.stream().map(list -> maybeDelayed(Flow.fromIterable(list))).collect(Collectors.toList());
    }
    
    <T> Flow<T> maybeDelayed(Flow<T> flow)
    {
        if (rand.nextInt(DELAY_CHANCE) == 0)
            return flow.delayOnNext(rand.nextInt(15), TimeUnit.MICROSECONDS, TPCTaskType.TIMED_UNKNOWN);
        else if (rand.nextInt(SCHEDULE_CHANCE) == 0)
            return flow.lift(Threads.requestOnIo(TPCTaskType.READ));
        else
            return flow;
    }
    
    public <T> List<CLI<T>> closeableIterators(List<List<T>> iterators)
    {
        return Lists.transform(iterators, new Function<List<T>, CLI<T>>() {

            @Override
            public CLI<T> apply(List<T> arg)
            {
                return new CLI<T>(arg.iterator());
            }
        });
    }

    static class Counted<T> {
        T item;
        int count;
        
        Counted(T item) {
            this.item = item;
            count = 0;
        }

        public boolean equals(Object obj)
        {
            if (obj == null || !(obj instanceof Counted))
                return false;
            Counted<?> c = (Counted<?>) obj;
            return Objects.equal(item, c.item) && count == c.count;
        }

        @Override
        public String toString()
        {
            return item.toString() + "x" + count;
        }
    }
    
    static class Counter<T> extends Reducer<T, Counted<T>> {
        Counted<T> current = null;
        boolean read = true;

        @Override
        public void reduce(int idx, T next)
        {
            if (current == null)
                current = new Counted<>(next);
            assert current.item.equals(next);
            ++current.count;
        }

        @Override
        public void onKeyChange()
        {
            assert read;
            current = null;
            read = false;
        }

        @Override
        public Counted<T> getReduced()
        {
            assert current != null;
            read = true;
            return current;
        }
    }
    
    static class KeyedSet<K extends Comparable<? super K>, V> extends Pair<K, Set<V>> implements Comparable<KeyedSet<K, V>>
    {
        protected KeyedSet(K left, V right)
        {
            super(left, ImmutableSet.of(right));
        }
        
        protected KeyedSet(K left, Collection<V> right)
        {
            super(left, Sets.newHashSet(right));
        }

        @Override
        public int compareTo(KeyedSet<K, V> o)
        {
            return left.compareTo(o.left);
        }
    }
    
    static class Union<K extends Comparable<K>, V> extends Reducer<KeyedSet<K, V>, KeyedSet<K, V>> {
        KeyedSet<K, V> current = null;
        boolean read = true;

        @Override
        public void reduce(int idx, KeyedSet<K, V> next)
        {
            if (current == null)
                current = new KeyedSet<>(next.left, next.right);
            else {
                assert current.left.equals(next.left);
                current.right.addAll(next.right);
            }
        }

        @Override
        public void onKeyChange()
        {
            assert read;
            current = null;
            read = false;
        }

        @Override
        public KeyedSet<K, V> getReduced()
        {
            assert current != null;
            read = true;
            return current;
        }
    }
    
    // closeable list iterator
    public static class CLI<E> extends AbstractIterator<E> implements CloseableIterator<E>
    {
        Iterator<E> iter;
        boolean closed = false;
        public CLI(Iterator<E> items)
        {
            this.iter = items;
        }

        protected E computeNext()
        {
            if (!iter.hasNext()) return endOfData();
            return iter.next();
        }

        public void close()
        {
            assert !this.closed;
            this.closed = true;
        }
    }

    // Old MergeIterator implementation for comparison.
    public class MergeIteratorPQ<In,Out> extends AbstractIterator<Out>
    {
        protected final Reducer<In, Out> reducer;
        // a queue for return: all candidates must be open and have at least one item
        protected final PriorityQueue<CandidatePQ<In>> queue;
        // a stack of the last consumed candidates, so that we can lazily call 'advance()'
        // TODO: if we had our own PriorityQueue implementation we could stash items
        // at the end of its array, so we wouldn't need this storage
        protected final ArrayDeque<CandidatePQ<In>> candidates;
        public MergeIteratorPQ(List<? extends Iterator<In>> iters, Comparator<In> comp, Reducer<In, Out> reducer)
        {
            this.reducer = reducer;
            this.queue = new PriorityQueue<>(Math.max(1, iters.size()));
            for (int i = 0; i < iters.size(); i++)
            {
                CandidatePQ<In> candidate = new CandidatePQ<>(i, iters.get(i), comp);
                if (!candidate.advance())
                    // was empty
                    continue;
                this.queue.add(candidate);
            }
            this.candidates = new ArrayDeque<>(queue.size());
        }

        protected final Out computeNext()
        {
            advance();
            return consume();
        }

        /** Consume values by sending them to the reducer while they are equal. */
        protected final Out consume()
        {
            CandidatePQ<In> candidate = queue.peek();
            if (candidate == null)
                return endOfData();
            reducer.onKeyChange();
            do
            {
                candidate = queue.poll();
                candidates.push(candidate);
                reducer.reduce(candidate.idx, candidate.item);
            }
            while (queue.peek() != null && queue.peek().compareTo(candidate) == 0);
            return reducer.getReduced();
        }

        /** Advance and re-enqueue all items we consumed in the last iteration. */
        protected final void advance()
        {
            CandidatePQ<In> candidate;
            while ((candidate = candidates.pollFirst()) != null)
                if (candidate.advance())
                    queue.add(candidate);
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class CandidatePQ<In> implements Comparable<CandidatePQ<In>>
    {
        private final Iterator<? extends In> iter;
        private final Comparator<? super In> comp;
        private final int idx;
        private In item;
        boolean equalParent;

        public CandidatePQ(int idx, Iterator<? extends In> iter, Comparator<? super In> comp)
        {
            this.iter = iter;
            this.comp = comp;
            this.idx = idx;
        }

        /** @return true if our iterator had an item, and it is now available */
        protected boolean advance()
        {
            if (!iter.hasNext())
                return false;
            item = iter.next();
            return true;
        }

        public int compareTo(CandidatePQ<In> that)
        {
            return comp.compare(this.item, that.item);
        }
    }

    @Test
    public void testNulls()
    {
        boolean failed = false;
        try
        {
            System.out.println(Flow.merge(ImmutableList.of(Flow.empty(), Flow.just("one"), Flow.just(null)), Ordering.natural(), new Counter<>()).countBlocking());
            failed = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            assertTrue(t instanceof AssertionError);
        }
        assertFalse(failed);
    }

    @Test
    public void testDoubleNext()
    {
        boolean failed = false;
        try
        {
            Flow<String> bad = new BadFlow("a", new Object[]{ "b", "c"}, null);

            System.out.println(Flow.merge(ImmutableList.of(Flow.empty(), Flow.just("a"), bad), Ordering.natural(), new Counter<>()).countBlocking());
            failed = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            assertTrue(t instanceof AssertionError);
        }
        assertFalse(failed);
    }

    @Test
    public void testNextComplete()
    {
        boolean failed = false;
        try
        {
            Flow<String> bad = new BadFlow("a", new Object[]{ "b", null});

            System.out.println(Flow.merge(ImmutableList.of(Flow.empty(), Flow.just("a"), bad), Ordering.natural(), new Counter<>()).countBlocking());
            failed = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            assertTrue(t instanceof AssertionError);
        }
        assertFalse(failed);
    }

    @Test
    public void testErrorNext()
    {
        boolean failed = false;
        try
        {
            Flow<String> bad = new BadFlow("a", new Object[]{ new AssertionError(), "b"}, null);

            System.out.println(Flow.merge(ImmutableList.of(Flow.empty(), Flow.just("a"), bad), Ordering.natural(), new Counter<>()).countBlocking());
            failed = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            assertTrue(t instanceof AssertionError);
        }
        assertFalse(failed);
    }

    static class MyException extends Exception
    {
    }

    @Test
    public void testError()
    {
        boolean failed = false;
        try
        {
            Flow<String> bad = new BadFlow("a", new MyException(), null);

            System.out.println(Flow.merge(ImmutableList.of(Flow.empty(), Flow.just("a"), bad), Ordering.natural(), new Counter<>()).countBlocking());
            failed = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            assertTrue(t instanceof RuntimeException);
            assertTrue(Throwables.getRootCause(t) instanceof MyException);
        }
        assertFalse(failed);
    }

    class BadFlow extends FlowSource<String>
    {
        Object[] inputs;
        int pos = 0;

        public BadFlow(Object... inputs)
        {
            this.inputs = inputs;
        }

        public void requestNext()
        {
            if (pos >= inputs.length)
                return;

            Object o = inputs[pos++];
            process(o);
        }

        public void close() throws Exception
        {
        }

        public String toString()
        {
            return Flow.formatTrace("BadFlow " + Arrays.toString(inputs));
        }

        private void process(Object o)
        {
            if (o instanceof String)
                subscriber.onNext((String) o);
            else if (o instanceof Throwable)
                subscriber.onError((Throwable) o);
            else if (o == null)
                subscriber.onComplete();
            else
                for (Object oo : (Object[]) o)
                    process(oo);
        }
    }

    @Test
    public void testRejectingReducerDoesntOverflowStack() throws Exception
    {
        Flow<Integer> f1 = Flow.fromIterable(() -> IntStream.range(0, 100000).iterator());
        Flow<Integer> f2 = Flow.fromIterable(() -> IntStream.range(0, 10000).iterator());
        Flow<Integer> merge = Merge.get(ImmutableList.of(f1, f2), Ordering.natural(), new Reducer<Integer, Integer>()
        {
            public void reduce(int idx, Integer current)
            {
            }

            public Integer getReduced()
            {
                return null;
            }
        });

        Assert.assertEquals(0, merge.countBlocking());
    }
}
