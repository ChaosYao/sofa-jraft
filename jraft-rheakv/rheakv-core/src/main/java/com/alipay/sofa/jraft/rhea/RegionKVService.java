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

import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.GetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutRequest;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * Request processing service on the KV server side.
 * <p>
 * A {@link StoreEngine} contains many {@link RegionKVService}s,
 * each {@link RegionKVService} corresponds to a region, and it
 * only processes request keys within its own region.
 *
 * @author jiachun.fjc
 */
public interface RegionKVService {

    long getRegionId();

    RegionEpoch getRegionEpoch();

    /**
     * {@link BaseRequest#PUT}
     */
    void handlePutRequest(final PutRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#GET}
     */
    void handleGetRequest(final GetRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);
}
