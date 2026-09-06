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
package com.alipay.sofa.jraft.rhea;

import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.GetRequest;
import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.GetResponse;

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.rhea.cmd.store.NoRegionFoundResponse;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Rhea KV store GET request processing service.
 *
 * @author jiachun.fjc
 */
public class GetCommandProcessor implements RpcProcessor<GetRequest> {

    private final StoreEngine storeEngine;

    public GetCommandProcessor(StoreEngine storeEngine) {
        this.storeEngine = Requires.requireNonNull(storeEngine, "storeEngine");
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final GetRequest request) {
        Requires.requireNonNull(request, "request");
        final long regionId = request.getRegionId();
        final RegionKVService regionKVService = this.storeEngine.getRegionKVService(regionId);
        if (regionKVService == null) {
            final RequestProcessClosure<Object, Object> closure = new RequestProcessClosure<>(request, rpcCtx);
            final NoRegionFoundResponse noRegion = new NoRegionFoundResponse();
            noRegion.setRegionId(regionId);
            noRegion.setError(Errors.NO_REGION_FOUND);
            noRegion.setValue(false);
            closure.sendResponse(noRegion);
            return;
        }
        final RequestProcessClosure<Object, GetResponse> closure = new RequestProcessClosure<>(request, rpcCtx);
        regionKVService.handleGetRequest(request, closure);
    }

    @Override
    public String interest() {
        return GetRequest.class.getName();
    }

    @Override
    public Executor executor() {
        return this.storeEngine.getKvRpcExecutor();
    }
}
