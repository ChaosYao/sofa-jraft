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
import java.util.concurrent.Semaphore;
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
import com.alipay.sofa.jraft.util.Endpoint;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Timer;

/**
 * @author jiachun.fjc
 */
public class BenchmarkClient {

    private static final Logger LOG      = LoggerFactory.getLogger(BenchmarkClient.class);

    private static final byte[] BYTES    = new byte[] { 0, 1 };
    private static final Timer  putTimer = KVMetrics.timer("put_benchmark_timer");
    private static final Timer  timer    = KVMetrics.timer("benchmark_timer");

    private static final int DEFAULT_TOTAL_REQUESTS = 3000;

    public static void main(final String[] args) {
        if (args.length < 4) {
            LOG.error("Args: [configPath], [threads], [valueSize], [totalPuts](optional, default=3000) are needed.");
            System.exit(-1);
        }
        final String configPath = args[1];
        final int threads = Integer.parseInt(args[2]);
        final int valueSize = Integer.parseInt(args[3]);
        final int totalPuts = args.length >= 5 ? Integer.parseInt(args[4]) : DEFAULT_TOTAL_REQUESTS;

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

        int round = 0;
        while (true) {
            round++;
            LOG.info("========== Round {} starting: totalPuts={}, threads={}, valueSize={} ==========",
                round, totalPuts, threads, valueSize);

            final AtomicInteger sentCount = new AtomicInteger(0);
            final CountDownLatch completionLatch = new CountDownLatch(totalPuts);

            final long startTime = System.currentTimeMillis();
            startBenchmark(rheaKVStore, threads, valueSize, regionRouteTableOptionsList, totalPuts, sentCount,
                completionLatch);

            try {
                completionLatch.await();
            } catch (final InterruptedException e) {
                LOG.warn("Benchmark interrupted while waiting for completion.");
                Thread.currentThread().interrupt();
                break;
            }

            final long elapsedMs = System.currentTimeMillis() - startTime;
            final double elapsedSec = elapsedMs / 1000.0;
            final double throughput = totalPuts / elapsedSec;
            LOG.info("========== Round {} completed: totalPuts={}, elapsedTime={}ms ({} s), throughput={} puts/s ==========",
                round, totalPuts, elapsedMs, String.format("%.2f", elapsedSec), String.format("%.2f", throughput));

            stopBenchmark();

            LOG.info("Waiting 5 minutes before next round...");
            try {
                Thread.sleep(5 * 60 * 1000);
            } catch (final InterruptedException e) {
                LOG.warn("Benchmark interrupted during rest, exiting...");
                Thread.currentThread().interrupt();
                break;
            }
        }

        rheaKVStore.shutdown();
    }

    private static volatile boolean shouldStop       = false;
    private static Thread[]         benchmarkThreads = null;

    public static void startBenchmark(final RheaKVStore rheaKVStore, final int threads, final int valueSize,
                                      final List<RegionRouteTableOptions> regionRouteTableOptionsList,
                                      final int totalPuts, final AtomicInteger sentCount,
                                      final CountDownLatch completionLatch) {
        shouldStop = false;
        benchmarkThreads = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(
                () -> doRequest(rheaKVStore, valueSize, regionRouteTableOptionsList, totalPuts, sentCount,
                    completionLatch));
            t.setDaemon(false);
            benchmarkThreads[i] = t;
            t.start();
        }
    }

    public static void stopBenchmark() {
        shouldStop = true;
        if (benchmarkThreads != null) {
            // Wait for threads to finish current operations
            for (final Thread t : benchmarkThreads) {
                if (t != null && t.isAlive()) {
                    try {
                        t.join(1000); // Wait up to 1 second for thread to finish
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            benchmarkThreads = null;
        }
    }

    public static void doRequest(final RheaKVStore rheaKVStore, final int valueSize,
                                 final List<RegionRouteTableOptions> regionRouteTableOptionsList,
                                 final int totalPuts, final AtomicInteger sentCount,
                                 final CountDownLatch completionLatch) {
        final int regionSize = regionRouteTableOptionsList.size();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Semaphore slidingWindow = new Semaphore(128);
        final byte[] valueBytes = new byte[valueSize];
        random.nextBytes(valueBytes);
        while (!shouldStop) {
            final int seq = sentCount.getAndIncrement();
            if (seq >= totalPuts) {
                shouldStop = true;
                break;
            }
            try {
                try {
                    slidingWindow.acquire();
                } catch (final Exception e) {
                    LOG.error("Wrong slidingWindow: {}, {}", slidingWindow.toString(), StackTraceUtil.stackTrace(e));
                }
                final int randomRegionIndex = random.nextInt(regionSize);
                byte[] keyBytes = regionRouteTableOptionsList.get(randomRegionIndex).getStartKeyBytes();
                if (keyBytes == null) {
                    keyBytes = BYTES;
                }
                final Timer.Context ctx = timer.time();
                final Timer.Context putCtx = putTimer.time();
                final CompletableFuture<Boolean> f = put(rheaKVStore, keyBytes, valueBytes);
                f.whenComplete((ignored, throwable) -> {
                    slidingWindow.release();
                    ctx.stop();
                    putCtx.stop();
                    completionLatch.countDown();
                });
            } catch (final Throwable t) {
                LOG.error("Error in doRequest: {}", StackTraceUtil.stackTrace(t));
                completionLatch.countDown();
            }
        }
    }

    public static CompletableFuture<Boolean> put(final RheaKVStore rheaKVStore, final byte[] key, final byte[] value) {
        return rheaKVStore.put(key, value);
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
