/*
 *
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
 *
 */
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.*;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.memory.BufferPool;

public class ChunkCache
        implements AsyncCacheLoader<ChunkCache.Key, ChunkCache.Buffer>, RemovalListener<ChunkCache.Key, ChunkCache.Buffer>, CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCache.class);
    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    public static final long cacheSize = 1024L * 1024L * Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - RESERVED_POOL_SPACE_IN_MB);

    private static final boolean roundUp = DatabaseDescriptor.getFileCacheRoundUp();
    private static final DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();

    public static final ChunkCache instance = cacheSize > 0 ? new ChunkCache() : null;
    private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;

    private final AsyncLoadingCache<Key, Buffer> cache;
    public final CacheMissMetrics metrics;

    public static int roundForCaching(int bufferSize)
    {
        return diskOptimizationStrategy.roundForCaching(bufferSize, roundUp);
    }

    static class Key
    {
        final ChunkReader file;
        final long position;

        public Key(ChunkReader file, long position)
        {
            super();
            this.file = file;
            this.position = position;
        }

        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + path().hashCode();
            result = prime * result + file.getClass().hashCode();
            result = prime * result + Long.hashCode(position);
            return result;
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                    && file.getClass() == other.file.getClass()
                    && path().equals(other.path());
        }

        public String path()
        {
            return file.channel().filePath();
        }

        public String toString()
        {
            return path() + '@' + position;
        }
    }

    public static class Buffer implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;
        private final Key key;

        private volatile int references = 1; // Start referenced

        private static final AtomicIntegerFieldUpdater<Buffer> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(Buffer.class, "references");

        public Buffer(Key key, ByteBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }

        Buffer reference()
        {
            int refCount;
            do
            {
                refCount = references;

                if (refCount == 0)
                {
                    // Buffer was released before we managed to reference it.
                    return null;
                }

            } while (!referencesUpdater.compareAndSet(this, refCount, refCount + 1));

            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert references > 0;
            return buffer.duplicate();
        }

        @Override
        public long offset()
        {
            return key.position;
        }

        @Override
        public void release()
        {
            //The read from disk read may be in flight
            //We need to keep this buffer till the async callback has fired
            if (referencesUpdater.decrementAndGet(this) == 0)
                BufferPool.put(buffer);
        }

        @Override
        public String toString()
        {
            return "ChunkCache$Buffer(" + key + ")";
        }
    }

    public ChunkCache()
    {
        cache = Caffeine.newBuilder()
                .maximumWeight(cacheSize)
                .executor(TPC.getWrappedExecutor())
                .weigher((key, buffer) -> ((Buffer) buffer).buffer.capacity())
                .removalListener(this)
                .buildAsync(this);
        metrics = new CacheMissMetrics("ChunkCache", this);
    }

    @Override
    @SuppressWarnings("resource")
    public CompletableFuture<Buffer> asyncLoad(Key key, Executor executor)
    {
        ChunkReader rebufferer = key.file;
        metrics.misses.mark();

        Timer.Context ctx = metrics.missLatency.timer();
        try
        {
            ByteBuffer buffer = BufferPool.get(key.file.chunkSize(), key.file.preferredBufferType());
            assert buffer != null;
            assert !buffer.isDirect() || (UnsafeByteBufferAccess.getAddress(buffer) & (512 - 1)) == 0 : "Buffer from pool is not properly aligned!";

            return rebufferer.readChunk(key.position, buffer)
                             .thenApply(b -> new Buffer(key, b))
                             .whenComplete((b, t) -> ctx.close());
        }
        catch (Throwable t)
        {
           ctx.close();
           throw t;
        }
    }

    @Override
    public void onRemoval(Key key, Buffer buffer, RemovalCause cause)
    {
        buffer.release();
    }

    public void close()
    {
        cache.synchronous().invalidateAll();
    }

    public RebuffererFactory wrap(ChunkReader file)
    {
        return new CachingRebufferer(file);
    }

    public RebuffererFactory maybeWrap(ChunkReader file)
    {
        return wrapper.apply(file);
    }

    public void invalidatePosition(FileHandle dfile, long position)
    {
        if (!(dfile.rebuffererFactory() instanceof CachingRebufferer))
            return;

        ((CachingRebufferer) dfile.rebuffererFactory()).invalidate(position);
    }

    /**
     * Remove any buffers from this file from the chunk cache. Note, this is a very expensive operation, O(N) on
     * the cache size, and should only be used for testing purposes.
     *
     * @param fileName - the name of the file whose buffers we want to remove from the cache
     */
    @VisibleForTesting
    public void invalidateFile(String fileName)
    {
        cache.synchronous().invalidateAll(Iterables.filter(cache.synchronous().asMap().keySet(), x -> x.path().equals(fileName)));
    }

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        wrapper = enabled ? this::wrap : x -> x;
        cache.synchronous().invalidateAll();
        metrics.reset();
    }

    @VisibleForTesting
    public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor)
    {
        final Function<ChunkReader, RebuffererFactory> prevWrapper = wrapper;
        wrapper = rdr -> interceptor.apply(prevWrapper.apply(rdr));
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified ChunkReader.
     * Thread-safe. One instance per SegmentedFile, created by ChunkCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ChunkReader source;
        final long alignmentMask;

        public CachingRebufferer(ChunkReader file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1 : String.format("%d must be a power of two", chunkSize);
            alignmentMask = -chunkSize;
        }

        @Override
        public Buffer rebuffer(long position)
        {
            try
            {
                metrics.requests.mark();
                long pageAlignedPos = position & alignmentMask;
                Buffer buf = null;
                Key pageKey = new Key(source, pageAlignedPos);

                Buffer page = null;

                int spin = 0;
                //There is a small window when a released buffer/invalidated chunk
                //is still in the cache. In this case it will return null
                //so we spin loop while waiting for the cache to re-populate
                while(page == null || ((buf = page.reference()) == null))
                {
                    page = cache.get(pageKey).join();

                    if (page != null && buf == null && ++spin == 1024)
                        logger.error("Spinning for {}", pageKey);
                }

                return buf;
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }

        @Override
        public Buffer rebuffer(long position, ReaderConstraint rc)
        {
            if (rc != ReaderConstraint.ASYNC)
                return rebuffer(position);

            metrics.requests.mark();
            long pageAlignedPos = position & alignmentMask;
            Key key = new Key(source, pageAlignedPos);

            CompletableFuture<Buffer> asyncBuffer = cache.get(key);

            if (asyncBuffer.isDone())
            {
                Buffer buf = asyncBuffer.join();

                if (buf != null && (buf = buf.reference()) != null)
                    return buf;

                asyncBuffer = cache.get(key);
            }

            /**
             * Notify the caller this page isn't ready
             * but don't give them the buffer because it has not been referenced.
             */
            throw new NotInCacheException(asyncBuffer.thenAccept(buffer -> {}), key.path(), key.position);
        }

        @Override
        public CompletableFuture<BufferHolder> rebufferAsync(long position)
        {
            metrics.requests.mark();
            CompletableFuture<BufferHolder> ret = new CompletableFuture<>();
            getPage(position, ret, 0);
            return ret;
        }

        private void getPage(long position, CompletableFuture<BufferHolder> ret, int numAttempts)
        {
            long pageAlignedPos = position & alignmentMask;
            cache.get(new Key(source, pageAlignedPos))
                 .whenComplete((page, error) -> {
                     if (error != null)
                     {
                         ret.completeExceptionally(error);
                         return;
                     }

                     BufferHolder buffer = page.reference();
                     if (buffer != null)
                     {
                         ret.complete(buffer);
                     }
                     else if (numAttempts < 1024)
                     {
                         TPC.bestTPCScheduler().scheduleDirect(() -> getPage(position, ret, numAttempts + 1));
                     }
                     else
                     {
                         ret.completeExceptionally(new IllegalStateException("Failed to acquire buffer from cache after 1024 attempts"));
                     }
                 });
        }

        @Override
        public int rebufferSize()
        {
            return source.chunkSize();
        }

        public void invalidate(long position)
        {
            long pageAlignedPos = position & alignmentMask;
            cache.synchronous().invalidate(new Key(source, pageAlignedPos));
        }

        @Override
        @SuppressWarnings("resource") // channel closed by the PrefetchingRebufferer
        public Rebufferer instantiateRebufferer(FileAccessType accessType)
        {
            if (accessType == FileAccessType.RANDOM || source.isMmap())
                return this;

            AsynchronousChannelProxy channel = this.source.channel().maybeBatched(PrefetchingRebufferer.READ_AHEAD_VECTORED);
            return new PrefetchingRebufferer(new CachingRebufferer(source.withChannel(channel)), channel);
        }

        @Override
        public void close()
        {
            source.close();
        }

        @Override
        public void closeReader()
        {
            // Instance is shared among readers. Nothing to release.
        }

        @Override
        public AsynchronousChannelProxy channel()
        {
            return source.channel();
        }

        @Override
        public long fileLength()
        {
            return source.fileLength();
        }

        @Override
        public double getCrcCheckChance()
        {
            return source.getCrcCheckChance();
        }

        @Override
        public String toString()
        {
            return "CachingRebufferer:" + source;
        }
    }

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Chunk cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.synchronous().asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.synchronous().policy().eviction()
                .map(policy -> policy.weightedSize().orElseGet(cache.synchronous()::estimatedSize))
                .orElseGet(cache.synchronous()::estimatedSize);
    }
}
