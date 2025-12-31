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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.PullLogEntryRequest;
import com.google.protobuf.Message;

/**
 * Handle PullLogEntry requests.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 6:11:09 PM
 */
public class PullLogEntryRequestProcessor extends NodeRequestProcessor<PullLogEntryRequest> {

    public PullLogEntryRequestProcessor(Executor executor) {
        super(executor, RpcRequests.PullLogEntryResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final PullLogEntryRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final PullLogEntryRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final PullLogEntryRequest request,
                                   final RpcRequestClosure done) {
        return service.handlePullLogEntryRequest(request, done);
    }

    @Override
    public String interest() {
        return PullLogEntryRequest.class.getName();
    }
}
