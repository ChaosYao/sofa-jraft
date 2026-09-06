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

import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.GetRequest;
import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.GetResponse;
import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.PutRequest;
import static com.alipay.sofa.jraft.rhea.cmd.store.RheaKVStoreProto.PutResponse;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.store.ProtoConverter;
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
    public void handlePutRequest(final PutRequest request, final RequestProcessClosure<Object, PutResponse> closure) {
        final PutResponse.Builder responseBuilder = PutResponse.newBuilder();
        responseBuilder.setRegionId(getRegionId());
        responseBuilder.setRegionEpoch(ProtoConverter.toProto(getRegionEpoch()));
        try {
            final RegionEpoch requestEpoch = ProtoConverter.fromProto(request.getRegionEpoch());
            KVParameterRequires.requireSameEpoch(requestEpoch, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "put.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue().toByteArray(), "put.value");
            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        responseBuilder.setErrorCode(Errors.NONE.code());
                        responseBuilder.setValue((Boolean) getData());
                    } else {
                        setFailure(request, responseBuilder, status, getError());
                    }
                    closure.sendResponse(responseBuilder.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            responseBuilder.setErrorCode(Errors.forException(t).code());
            closure.sendResponse(responseBuilder.build());
        }
    }

    @Override
    public void handleGetRequest(final GetRequest request, final RequestProcessClosure<Object, GetResponse> closure) {
        final GetResponse.Builder responseBuilder = GetResponse.newBuilder();
        responseBuilder.setRegionId(getRegionId());
        responseBuilder.setRegionEpoch(ProtoConverter.toProto(getRegionEpoch()));
        try {
            final RegionEpoch requestEpoch = ProtoConverter.fromProto(request.getRegionEpoch());
            KVParameterRequires.requireSameEpoch(requestEpoch, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "get.key");
            this.rawKVStore.get(key, request.getReadOnlySafe(), new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        responseBuilder.setErrorCode(Errors.NONE.code());
                        final byte[] value = (byte[]) getData();
                        if (value != null) {
                            responseBuilder.setValue(com.google.protobuf.ByteString.copyFrom(value));
                        }
                    } else {
                        setFailure(request, responseBuilder, status, getError());
                    }
                    closure.sendResponse(responseBuilder.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            responseBuilder.setErrorCode(Errors.forException(t).code());
            closure.sendResponse(responseBuilder.build());
        }
    }

    private static void setFailure(final Object request, final Object responseBuilder, final Status status,
                                   final Errors error) {
        final Errors finalError = error == null ? Errors.STORAGE_ERROR : error;
        if (responseBuilder instanceof PutResponse.Builder) {
            ((PutResponse.Builder) responseBuilder).setErrorCode(finalError.code());
        } else if (responseBuilder instanceof GetResponse.Builder) {
            ((GetResponse.Builder) responseBuilder).setErrorCode(finalError.code());
        }
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, finalError);
    }
}
