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
package com.alipay.sofa.jraft.rhea.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.util.Requires;

/**
 *
 * @author jiachun.fjc
 */
public class DefaultRheaKVCliService implements RheaKVCliService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRheaKVCliService.class);

    private RpcClient           rpcClient;
    private CliService          cliService;
    private CliOptions          opts;

    private boolean             started;

    @Override
    public boolean init(final CliOptions opts) {
        if (this.started) {
            LOG.info("[DefaultRheaKVRpcService] already started.");
            return true;
        }
        initCli(opts);
        LOG.info("[DefaultRheaKVCliService] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public void shutdown() {
        if (this.cliService != null) {
            this.cliService.shutdown();
        }
        this.started = false;
        LOG.info("[DefaultRheaKVCliService] shutdown successfully.");
    }

    @Override
    public Status rangeSplit(final long regionId, final long newRegionId, final String groupId, final Configuration conf) {
        // RangeSplit is not supported
        return new Status(-1, "RangeSplit is not supported");
    }

    private void initCli(CliOptions cliOpts) {
        if (cliOpts == null) {
            cliOpts = new CliOptions();
            cliOpts.setTimeoutMs(5000);
            cliOpts.setMaxRetry(3);
        }
        this.opts = cliOpts;
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOpts);
        final CliClientService cliClientService = ((CliServiceImpl) this.cliService).getCliClientService();
        Requires.requireNonNull(cliClientService, "cliClientService");
        this.rpcClient = ((AbstractClientService) cliClientService).getRpcClient();
    }
}
