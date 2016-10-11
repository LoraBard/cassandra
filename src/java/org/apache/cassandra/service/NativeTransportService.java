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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import net.openhft.affinity.AffinitySupport;
import org.apache.cassandra.concurrent.MonitoredEpollEventLoopGroup;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private Collection<Server> servers = Collections.emptyList();

    private static Integer pIO = Integer.valueOf(System.getProperty("io.netty.ratioIO", "50"));
    private static Boolean affinity = Boolean.valueOf(System.getProperty("io.netty.affinity","false"));

    public static final int NUM_NETTY_THREADS = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", String.valueOf(FBUtilities.getAvailableProcessors() * 2)));

    private boolean initialized = false;
    private boolean tpcInitialized = false;
    private EventLoopGroup workerGroup;

    private final InetAddress nativeAddr;
    private final int nativePort;

    @VisibleForTesting
    public NativeTransportService(InetAddress nativeAddr, int nativePort)
    {
        this.nativeAddr = nativeAddr;
        this.nativePort = nativePort;
    }

    public NativeTransportService()
    {
        this.nativeAddr = DatabaseDescriptor.getRpcAddress();
        this.nativePort = DatabaseDescriptor.getNativeTransportPort();
    }

    /**
     * Creates netty thread pools and event loops.
     */
    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;


        if (useEpoll())
        {
            workerGroup = new MonitoredEpollEventLoopGroup(NUM_NETTY_THREADS);
            logger.info("Netty using native Epoll event loop");
        }
        else
        {
            workerGroup = new NioEventLoopGroup();
            logger.info("Netty using Java NIO event loop");
        }

        int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();

        org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                                                                .withEventLoopGroup(workerGroup)
                                                                .withHost(nativeAddr);

        if (!DatabaseDescriptor.getClientEncryptionOptions().enabled)
        {
            servers = Collections.singleton(builder.withSSL(false).withPort(nativePort).build());
        }
        else
        {
            if (nativePort != nativePortSSL)
            {
                // user asked for dedicated ssl port for supporting both non-ssl and ssl connections
                servers = Collections.unmodifiableList(
                Arrays.asList(
                builder.withSSL(false).withPort(nativePort).build(),
                builder.withSSL(true).withPort(nativePortSSL).build()
                )
                );
            }
            else
            {
                // ssl only mode using configured native port
                servers = Collections.singleton(builder.withSSL(true).withPort(nativePort).build());
            }
        }


        // register metrics
        ClientMetrics.instance.addCounter("connectedNativeClients", () ->
        {
            int ret = 0;
            for (Server server : servers)
                ret += server.getConnectedClients();
            return ret;
        });

        AuthMetrics.init();

        initialized = true;
    }

    private void initializeTPC()
    {
        if (tpcInitialized)
            return;

        CountDownLatch ready = new CountDownLatch(NUM_NETTY_THREADS);

        for (int i = 0; i < NUM_NETTY_THREADS; i++)
        {
            final int cpuId = i;
            EventLoop loop = workerGroup.next();

            loop.schedule(() -> {
                NettyRxScheduler.instance(loop, cpuId);

                if (affinity)
                {
                    logger.info("Locking {} netty thread to {}", cpuId, Thread.currentThread().getName());
                    AffinitySupport.setAffinity(1L << cpuId);
                }
                {
                    logger.info("Allocated netty thread to {}", Thread.currentThread().getName());
                }

                ready.countDown();
            }, 0, TimeUnit.SECONDS);
        }

        Uninterruptibles.awaitUninterruptibly(ready);

        logger.info("Netting ioWork ration to {}", pIO);
        if (useEpoll())
        {
            //((EpollEventLoopGroup)workerGroup).setIoRatio(pIO);
        }
        else
        {
            ((NioEventLoopGroup)workerGroup).setIoRatio(pIO);
        }

        tpcInitialized = true;
    }


    /**
     * Starts native transport servers.
     */
    public void start()
    {
        initialize();
        initializeTPC();

        servers.forEach(Server::start);
    }

    /**
     * Stops currently running native transport servers.
     */
    public void stop()
    {
        servers.forEach(Server::stop);
    }

    /**
     * Ultimately stops servers and closes all resources.
     */
    public void destroy()
    {
        stop();
        servers = Collections.emptyList();

        // shutdown executors used by netty for native transport server
        workerGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS).awaitUninterruptibly();
    }

    /**
     * @return intend to use epoll bassed event looping
     */
    public static boolean useEpoll()
    {
        final boolean enableEpoll = Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true"));
        return enableEpoll && Epoll.isAvailable();
    }

    /**
     * @return true in case native transport server is running
     */
    public boolean isRunning()
    {
        for (Server server : servers)
            if (server.isRunning()) return true;
        return false;
    }

    @VisibleForTesting
    EventLoopGroup getWorkerGroup()
    {
        return workerGroup;
    }

    @VisibleForTesting
    Collection<Server> getServers()
    {
        return servers;
    }
}
