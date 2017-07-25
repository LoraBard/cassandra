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
package org.apache.cassandra.io.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.mos.MemoryLockedBuffer;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * {@link FileHandle} provides access to a file for reading, including the ones written by various {@link SequentialWriter}
 * instances, and it is typically used by {@link org.apache.cassandra.io.sstable.format.SSTableReader}.
 *
 * Use {@link FileHandle.Builder} to create an instance, and call {@link #createReader()} (and its variants) to
 * access the readers for the underlying file.
 *
 * You can use {@link Builder#complete()} several times during its lifecycle with different {@code overrideLength}(i.e. early opening file).
 * For that reason, the builder keeps a reference to the file channel and makes a copy for each {@link Builder#complete()} call.
 * Therefore, it is important to close the {@link Builder} when it is no longer needed, as well as any {@link FileHandle}
 * instances.
 */
public class FileHandle extends SharedCloseableImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FileHandle.class);

    public final AsynchronousChannelProxy channel;

    public final long onDiskLength;

    /**
     * Rebufferer factory to use when constructing RandomAccessReaders
     */
    private final RebuffererFactory rebuffererFactory;

    /**
     * Optional CompressionMetadata when dealing with compressed file
     */
    private final Optional<CompressionMetadata> compressionMetadata;

    /**
     A private copy of the memory mapped regions for files that are memory mapped,
     null otherwise.

     This is needed for MemoryOnlyStrategy, see APOLLO-342. It could be removed by storing
     a property in the regions to determine if the memory should be locked, rather than driving
     the decision to lock the memory from the compaction strategy.
     */
    @Nullable
    private MmappedRegions regions;

    private FileHandle(Cleanup cleanup,
                       AsynchronousChannelProxy channel,
                       RebuffererFactory rebuffererFactory,
                       CompressionMetadata compressionMetadata,
                       long onDiskLength,
                       MmappedRegions regions)
    {
        super(cleanup);
        this.rebuffererFactory = rebuffererFactory;
        this.channel = channel;
        this.compressionMetadata = Optional.ofNullable(compressionMetadata);
        this.onDiskLength = onDiskLength;
        this.regions = regions;
    }

    private FileHandle(FileHandle copy)
    {
        super(copy);
        channel = copy.channel;
        rebuffererFactory = copy.rebuffererFactory;
        compressionMetadata = copy.compressionMetadata;
        onDiskLength = copy.onDiskLength;
        regions = copy.regions;
    }

    /**
     * @return Path to the file this factory is referencing
     */
    public String path()
    {
        return channel.filePath();
    }

    public boolean mmapped()
    {
        return regions != null;
    }

    public long dataLength()
    {
        return compressionMetadata.map(c -> c.dataLength).orElseGet(rebuffererFactory::fileLength);
    }

    public RebuffererFactory rebuffererFactory()
    {
        return rebuffererFactory;
    }

    public Optional<CompressionMetadata> compressionMetadata()
    {
        return compressionMetadata;
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        compressionMetadata.ifPresent(metadata -> metadata.addTo(identities));
    }

    @Override
    public FileHandle sharedCopy()
    {
        return new FileHandle(this);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     *
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader()
    {
        return createReader(Rebufferer.ReaderConstraint.NONE);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     *
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader(Rebufferer.ReaderConstraint constraint)
    {
        return new RandomAccessReader(instantiateRebufferer(null), constraint);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     * Reading from file will be rate limited by given {@link RateLimiter}.
     *
     * @param limiter RateLimiter to use for rate limiting read
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new RandomAccessReader(instantiateRebufferer(limiter), Rebufferer.ReaderConstraint.NONE);
    }

    public FileDataInput createReader(long position, Rebufferer.ReaderConstraint rc)
    {
        RandomAccessReader reader = createReader(rc);
        reader.seek(position);
        return reader;
    }

    /**
     * Drop page cache from start to given {@code before}.
     *
     * @param before uncompressed position from start of the file to be dropped from cache. if 0, to end of file.
     */
    public void dropPageCache(long before)
    {
        long position = compressionMetadata.map(metadata -> {
            if (before >= metadata.dataLength)
                return 0L;
            else
                return metadata.chunkFor(before).offset;
        }).orElse(before);
        NativeLibrary.trySkipCache(channel.getFileDescriptor(), 0, position, path());
    }

    private Rebufferer instantiateRebufferer(RateLimiter limiter)
    {
        Rebufferer rebufferer = rebuffererFactory.instantiateRebufferer();

        if (limiter != null)
            rebufferer = new LimitingRebufferer(rebufferer, limiter, DiskOptimizationStrategy.MAX_BUFFER_SIZE);
        return rebufferer;
    }

    public void lock(MemoryOnlyStatus instance)
    {
        if (regions != null)
            regions.lock(instance);
        else
            instance.reportFailedAttemptedLocking(onDiskLength);
    }

    public void unlock(MemoryOnlyStatus instance)
    {
        if (regions != null)
            regions.unlock(instance);
        else
            instance.clearFailedAttemptedLocking(onDiskLength);
    }

    public List<MemoryLockedBuffer> getLockedMemory()
    {
        if (regions != null)
            return regions.getLockedMemory();

        // the existing behavior ported from DSE is that if memory locked is enabled withough memory mapping,
        // then we should report to the user that we could not lock the entire length on disk
        return Collections.singletonList(MemoryLockedBuffer.failed(0, onDiskLength));
    }

    /**
     * Perform clean up of all resources held by {@link FileHandle}.
     */
    private static class Cleanup implements RefCounted.Tidy
    {
        final AsynchronousChannelProxy channel;
        final RebuffererFactory rebufferer;
        final CompressionMetadata compressionMetadata;
        final Optional<ChunkCache> chunkCache;
        final MmappedRegions regions;

        private Cleanup(AsynchronousChannelProxy channel,
                        RebuffererFactory rebufferer,
                        CompressionMetadata compressionMetadata,
                        ChunkCache chunkCache,
                        MmappedRegions regions)
        {
            this.channel = channel;
            this.rebufferer = rebufferer;
            this.compressionMetadata = compressionMetadata;
            this.chunkCache = Optional.ofNullable(chunkCache);
            this.regions = regions;
        }

        public String name()
        {
            return channel.filePath();
        }

        public void tidy()
        {
            Throwables.perform(()-> chunkCache.ifPresent(cache -> cache.invalidateFile(name())),
                               () -> { if (compressionMetadata != null) compressionMetadata.close();},
                               () -> channel.close(),
                               () -> rebufferer.close(),
                               () -> { if (regions != null) regions.close();});
        }
    }

    /**
     * Configures how the file will be read (compressed, mmapped, use cache etc.)
     */
    public static class Builder implements AutoCloseable
    {
        private final String path;

        private AsynchronousChannelProxy channel;
        private CompressionMetadata compressionMetadata;
        private MmappedRegions regions;
        private ChunkCache chunkCache;
        private int bufferSize = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        private BufferType bufferType = BufferType.OFF_HEAP;

        private boolean mmapped = false;
        private boolean compressed = false;

        public Builder(String path)
        {
            this.path = path;
        }

        public Builder(AsynchronousChannelProxy channel)
        {
            this.channel = channel;
            this.path = channel.filePath();
        }

        public Builder compressed(boolean compressed)
        {
            this.compressed = compressed;
            return this;
        }

        /**
         * Set {@link ChunkCache} to use.
         *
         * @param chunkCache ChunkCache object to use for caching
         * @return this object
         */
        public Builder withChunkCache(ChunkCache chunkCache)
        {
            this.chunkCache = chunkCache;
            return this;
        }

        /**
         * Provide {@link CompressionMetadata} to use when reading compressed file.
         *
         * @param metadata CompressionMetadata to use
         * @return this object
         */
        public Builder withCompressionMetadata(CompressionMetadata metadata)
        {
            this.compressed = Objects.nonNull(metadata);
            this.compressionMetadata = metadata;
            return this;
        }

        /**
         * Set whether to use mmap for reading
         *
         * @param mmapped true if using mmap
         * @return this instance
         */
        public Builder mmapped(boolean mmapped)
        {
            this.mmapped = mmapped;
            return this;
        }

        /**
         * Set the buffer size to use (if appropriate).
         *
         * @param bufferSize Buffer size in bytes
         * @return this instance
         */
        public Builder bufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * Set the buffer type (on heap or off heap) to use (if appropriate).
         *
         * @param bufferType Buffer type to use
         * @return this instance
         */
        public Builder bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        /**
         * Complete building {@link FileHandle} without overriding file length.
         *
         * @see #complete(long)
         */
        public FileHandle complete()
        {
            return complete(-1L);
        }

        /**
         * Complete building {@link FileHandle} with the given length, which overrides the file length.
         *
         * @param overrideLength Override file length (in bytes) so that read cannot go further than this value.
         *                       If the value is less than or equal to 0, then the value is ignored.
         * @return Built file
         */
        @SuppressWarnings("resource")
        public FileHandle complete(long overrideLength)
        {
            if (channel == null)
            {
                channel = new AsynchronousChannelProxy(path);
            }

            AsynchronousChannelProxy channelCopy = channel.sharedCopy();
            try
            {
                if (compressed && compressionMetadata == null)
                    compressionMetadata = CompressionMetadata.create(channelCopy.filePath());

                long length = overrideLength > 0 ? overrideLength : compressed ? compressionMetadata.compressedFileLength : channelCopy.size();

                RebuffererFactory rebuffererFactory;
                if (length == 0)
                {
                    rebuffererFactory = new EmptyRebufferer(channelCopy);
                }
                else if (mmapped)
                {
                    try (ChannelProxy blockingChannel = channelCopy.getBlockingChannel())
                    {
                        if (compressed)
                        {
                            regions = MmappedRegions.map(blockingChannel, compressionMetadata);
                            rebuffererFactory = maybeCached(new CompressedChunkReader.Mmap(channelCopy, compressionMetadata,
                                                                                           regions));
                        }
                        else
                        {
                            updateRegions(blockingChannel, length);
                            rebuffererFactory = new MmapRebufferer(channelCopy, length, regions.sharedCopy());
                        }
                    }
                }
                else
                {
                    regions = null;
                    if (compressed)
                    {
                        rebuffererFactory = maybeCached(new CompressedChunkReader.Standard(channelCopy, compressionMetadata));
                    }
                    else
                    {
                        int chunkSize = ChunkCache.bufferToChunkSize(bufferSize);
                        rebuffererFactory = maybeCached(new SimpleChunkReader(channelCopy, length, bufferType, chunkSize));
                    }
                }

                Cleanup cleanup = new Cleanup(channelCopy, rebuffererFactory, compressionMetadata, chunkCache, regions == null ? null : regions.sharedCopy());
                return new FileHandle(cleanup, channelCopy, rebuffererFactory, compressionMetadata, length, cleanup.regions);
            }
            catch (Throwable t)
            {
                channelCopy.close();
                throw t;
            }
        }

        public Throwable close(Throwable accumulate)
        {
            if (!compressed && regions != null)
                accumulate = regions.close(accumulate);
            if (channel != null)
                return channel.close(accumulate);

            return accumulate;
        }

        public void close()
        {
            maybeFail(close(null));
        }

        private RebuffererFactory maybeCached(ChunkReader reader)
        {
            if (chunkCache != null)
                return chunkCache.maybeWrap(reader);

            return reader;
        }

        private void updateRegions(ChannelProxy channel, long length)
        {
            if (regions != null && !regions.isValid(channel))
            {
                Throwable err = regions.close(null);
                if (err != null)
                    logger.error("Failed to close mapped regions", err);

                regions = null;
            }

            if (regions == null)
                regions = MmappedRegions.map(channel, length, bufferSize);
            else
                regions.extend(length, bufferSize);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(path='" + path() + '\'' +
               ", length=" + rebuffererFactory.fileLength() +
               ')';
    }
}
