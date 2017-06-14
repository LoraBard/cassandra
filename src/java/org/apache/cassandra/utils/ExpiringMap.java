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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.TPC;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
    private volatile boolean shutdown;

    public static class CacheableObject<T>
    {
        private final T value;
        private final long createdAtNanos;
        private final long timeout;

        private CacheableObject(T value, long timeout)
        {
            assert value != null;
            this.value = value;
            this.timeout = timeout;
            this.createdAtNanos = Clock.instance.nanoTime();
        }

        private boolean isReadyToDieAt(long atNano)
        {
            return atNano - createdAtNanos > TimeUnit.MILLISECONDS.toNanos(timeout);
        }

        /**
         * The value stored.
         */
        public T get()
        {
            return value;
        }

        /**
         * How long that object has been in the map.
         *
         * @param unit the time unit to return the lifetime in.
         * @return how long the object has beed in the map.
         */
        public long lifetime(TimeUnit unit)
        {
            long lifetimeNanos = Clock.instance.nanoTime() - createdAtNanos;
            return unit.convert(lifetimeNanos, TimeUnit.NANOSECONDS);
        }

        public long timeoutMillis()
        {
            return timeout;
        }
    }

    // if we use more ExpiringMaps we may want to add multiple threads to this executor
    private static final ScheduledExecutorService service = new DebuggableScheduledThreadPoolExecutor("EXPIRING-MAP-REAPER");

    private final ConcurrentMap<K, CacheableObject<V>> cache = new ConcurrentHashMap<K, CacheableObject<V>>();
    private final long defaultExpiration;

    /**
     *
     * @param defaultExpiration the TTL for objects in the cache in milliseconds
     */
    public ExpiringMap(long defaultExpiration, final Consumer<Pair<K,CacheableObject<V>>> postExpireHook)
    {
        this.defaultExpiration = defaultExpiration;

        if (defaultExpiration <= 0)
        {
            throw new IllegalArgumentException("Argument specified must be a positive number");
        }

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                long start = Clock.instance.nanoTime();
                int n = 0;
                for (Map.Entry<K, CacheableObject<V>> entry : cache.entrySet())
                {
                    if (entry.getValue().isReadyToDieAt(start))
                    {
                        if (cache.remove(entry.getKey()) != null)
                        {
                            n++;
                            if (postExpireHook != null)
                                postExpireHook.accept(Pair.create(entry.getKey(), entry.getValue()));
                        }
                    }
                }
                logger.trace("Expired {} entries", n);
            }
        };
        service.scheduleWithFixedDelay(runnable, defaultExpiration / 2, defaultExpiration / 2, TimeUnit.MILLISECONDS);
    }

    public boolean shutdownBlocking()
    {
        service.shutdown();
        try
        {
            return service.awaitTermination(defaultExpiration * 2, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public void reset()
    {
        shutdown = false;
        cache.clear();
    }

    public V put(K key, V value)
    {
        return put(key, value, this.defaultExpiration);
    }

    public V put(K key, V value, long timeout)
    {
        if (shutdown)
        {
            // StorageProxy isn't equipped to deal with "I'm nominally alive, but I can't send any messages out."
            // So we'll just sit on this thread until the rest of the server shutdown completes.
            //
            // See comments in CustomTThreadPoolServer.serve, CASSANDRA-3335, and CASSANDRA-3727.
            // TPC: If we are on a TPC thread we cannot wait and so we just ignore the request
            if (!TPC.isTPCThread())
            {
                Uninterruptibles.sleepUninterruptibly(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
            else
            {
                logger.debug("Received request after messaging service shutdown, ignoring it");
                return null;
            }
        }
        CacheableObject<V> previous = cache.put(key, new CacheableObject<V>(value, timeout));
        return (previous == null) ? null : previous.value;
    }

    public CacheableObject<V> get(K key)
    {
        return cache.get(key);
    }

    public CacheableObject<V> remove(K key)
    {
        return cache.remove(key);
    }

    public int size()
    {
        return cache.size();
    }

    public boolean containsKey(K key)
    {
        return cache.containsKey(key);
    }

    public boolean isEmpty()
    {
        return cache.isEmpty();
    }

    public Set<K> keySet()
    {
        return cache.keySet();
    }
}
