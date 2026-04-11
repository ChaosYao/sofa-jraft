/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.benchmark.client;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.benchmark.Yaml;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Timer;

/**
 * Benchmark client that sends a fixed number of put operations then exits.
 *
 * Usage: BenchmarkClient [configPath] [threads] [valueSize] [throttleSleepMs](optional) [putCount](optional, default 300)
 *
 * putCount can also be set via system property: -DputCount=300
 *
 * @author jiachun.fjc
 */
public class BenchmarkClient {

    private static final Logger LOG               = LoggerFactory.getLogger(BenchmarkClient.class);

    private static final int    DEFAULT_PUT_COUNT = 300;

    private static final Timer  putTimer          = KVMetrics.timer("put_benchmark_timer");

    public static void main(final String[] args) {
        if (args.length < 3) {
            LOG.error(
                "Args: [configPath], [threads], [valueSize], [throttleSleepMs](optional), [putCount](optional, default {})"
                        + " are needed. putCount can also be set via -DputCount=N.", DEFAULT_PUT_COUNT);
            System.exit(-1);
        }
        final String configPath = args[1];
        final int threads = Integer.parseInt(args[2]);
        final int valueSize = Integer.parseInt(args[3]);
        final int throttleSleepMs = args.length >= 5 ? Integer.parseInt(args[4]) : 0;
        final int putCount = resolvePutCount(args);

        final RheaKVStoreOptions opts = Yaml.readConfig(configPath);
        final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        if (!rheaKVStore.init(opts)) {
            LOG.error("Fail to init [RheaKVStore]");
            System.exit(-1);
        }

        final List<RegionRouteTableOptions> regionRouteTableOptionsList = opts.getPlacementDriverOptions()
            .getRegionRouteTableOptionsList();
        rebalance(rheaKVStore, opts.getInitialServerList(), regionRouteTableOptionsList);

        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
            .build() //
            .start(30, TimeUnit.SECONDS);

        LOG.info("Starting benchmark: threads={}, valueSize={}, throttleSleepMs={}, putCount={}", threads, valueSize,
            throttleSleepMs, putCount);

        final long startMs = System.currentTimeMillis();
        runPuts(rheaKVStore, threads, valueSize, throttleSleepMs, regionRouteTableOptionsList, putCount);
        final long elapsed = System.currentTimeMillis() - startMs;

        LOG.info("Benchmark finished: {} puts completed in {} ms ({} puts/sec)", putCount, elapsed,
            elapsed > 0 ? putCount * 1000L / elapsed : 0);

        rheaKVStore.shutdown();
    }

    /**
     * Resolves putCount from CLI args (position 5) or system property -DputCount, defaulting to 300.
     */
    private static int resolvePutCount(final String[] args) {
        if (args.length >= 6) {
            return Integer.parseInt(args[5]);
        }
        final String sysProp = System.getProperty("putCount");
        if (sysProp != null) {
            return Integer.parseInt(sysProp);
        }
        return DEFAULT_PUT_COUNT;
    }

    /**
     * Runs exactly {@code putCount} puts distributed across {@code threads} worker threads,
     * then returns once all puts have completed.
     */
    public static void runPuts(final RheaKVStore rheaKVStore, final int threads, final int valueSize,
                               final int throttleSleepMs,
                               final List<RegionRouteTableOptions> regionRouteTableOptionsList, final int putCount) {
        final AtomicInteger toIssue = new AtomicInteger(putCount);
        final CountDownLatch done = new CountDownLatch(putCount);

        final Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(
                () -> putWorker(rheaKVStore, valueSize, throttleSleepMs, regionRouteTableOptionsList, toIssue, done));
            t.setDaemon(true);
            workers[i] = t;
            t.start();
        }

        try {
            done.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Benchmark interrupted before completion.");
        }
    }

    private static void putWorker(final RheaKVStore rheaKVStore, final int valueSize, final int throttleSleepMs,
                                  final List<RegionRouteTableOptions> regionRouteTableOptionsList,
                                  final AtomicInteger toIssue, final CountDownLatch done) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int regionSize = regionRouteTableOptionsList.size();
        final byte[] valueBytes = new byte[valueSize];
        random.nextBytes(valueBytes);

        while (toIssue.getAndDecrement() > 0) {
            if (throttleSleepMs > 0) {
                try {
                    Thread.sleep(throttleSleepMs);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    done.countDown();
                    return;
                }
            }

            byte[] keyBytes = regionRouteTableOptionsList.get(random.nextInt(regionSize)).getStartKeyBytes();
            if (keyBytes == null) {
                keyBytes = BytesUtil.writeUtf8(String.valueOf(random.nextLong()));
            }

            final Timer.Context ctx = putTimer.time();
            try {
                final CompletableFuture<Boolean> f = rheaKVStore.put(keyBytes, valueBytes);
                f.whenComplete((ignored, throwable) -> {
                    ctx.stop();
                    if (throwable != null) {
                        LOG.warn("Put failed: {}", throwable.getMessage());
                    }
                    done.countDown();
                });
            } catch (final Throwable t) {
                ctx.stop();
                LOG.error("Error issuing put: {}", StackTraceUtil.stackTrace(t));
                done.countDown();
            }
        }
    }

    // Because we use fake PD, so we need manual rebalance
    public static void rebalance(final RheaKVStore rheaKVStore, final String initialServerList,
                                 final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        final PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        final Configuration configuration = new Configuration();
        configuration.parse(initialServerList);
        final int serverSize = configuration.size();
        final int regionSize = regionRouteTableOptionsList.size();
        final int regionSizePerServer = regionSize / serverSize;
        final Queue<Long> regions = new ArrayDeque<>();
        for (final RegionRouteTableOptions r : regionRouteTableOptionsList) {
            regions.add(r.getRegionId());
        }
        final Map<PeerId, Integer> peerMap = Maps.newHashMap();
        for (;;) {
            final Long regionId = regions.poll();
            if (regionId == null) {
                break;
            }
            PeerId peerId;
            try {
                final Endpoint endpoint = pdClient.getLeader(regionId, true, 10000);
                if (endpoint == null) {
                    continue;
                }
                peerId = new PeerId(endpoint, 0);
                LOG.info("Region {} leader is {}", regionId, peerId);
            } catch (final Exception e) {
                regions.add(regionId);
                continue;
            }
            final Integer size = peerMap.get(peerId);
            if (size == null) {
                peerMap.put(peerId, 1);
                continue;
            }
            if (size < regionSizePerServer) {
                peerMap.put(peerId, size + 1);
                continue;
            }
            for (final PeerId p : configuration.listPeers()) {
                final Integer pSize = peerMap.get(p);
                if (pSize != null && pSize >= regionSizePerServer) {
                    continue;
                }
                try {
                    pdClient.transferLeader(regionId, JRaftHelper.toPeer(p), true);
                    LOG.info("Region {} transfer leader to {}", regionId, p);
                    regions.add(regionId);
                    break;
                } catch (final Exception e) {
                    LOG.error("Fail to transfer leader to {}", p);
                }
            }
        }

        for (final RegionRouteTableOptions r : regionRouteTableOptionsList) {
            final Long regionId = r.getRegionId();
            try {
                final Endpoint endpoint = pdClient.getLeader(regionId, true, 10000);
                LOG.info("Finally, the region: {} leader is: {}", regionId, endpoint);
            } catch (final Exception e) {
                LOG.error("Fail to get leader: {}", StackTraceUtil.stackTrace(e));
            }
        }
    }
}
