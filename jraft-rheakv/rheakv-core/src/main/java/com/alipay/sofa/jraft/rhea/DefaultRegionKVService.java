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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.GetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.PutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutResponse;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.util.KVParameterRequires;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;

/**
 * Rhea KV region RPC request processing service.
 *
 * @author jiachun.fjc
 */
public class DefaultRegionKVService implements RegionKVService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRegionKVService.class);

    private final RegionEngine  regionEngine;
    private final RawKVStore    rawKVStore;

    public DefaultRegionKVService(RegionEngine regionEngine) {
        this.regionEngine = regionEngine;
        this.rawKVStore = regionEngine.getMetricsRawKVStore();
    }

    @Override
    public long getRegionId() {
        return this.regionEngine.getRegion().getId();
    }

    @Override
    public RegionEpoch getRegionEpoch() {
        return this.regionEngine.getRegion().getRegionEpoch();
    }

    @Override
    public void handlePutRequest(final PutRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final PutResponse response = new PutResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "put.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "put.value");
            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }


    @Override
    public void handleGetRequest(final GetRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final GetResponse response = new GetResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "get.key");
            this.rawKVStore.get(key, request.isReadOnlySafe(), new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((byte[]) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }


    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }
}
