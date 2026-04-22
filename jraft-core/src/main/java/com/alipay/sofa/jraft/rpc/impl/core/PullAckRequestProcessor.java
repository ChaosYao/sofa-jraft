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
import com.alipay.sofa.jraft.rpc.RpcRequests.PullAckRequest;
import com.google.protobuf.Message;

/**
 * Handle PullAck requests from followers.
 * After a follower stably appends log entries pulled from the leader,
 * it sends a PullAckRequest so the leader can drive commit via BallotBox.
 */
public class PullAckRequestProcessor extends NodeRequestProcessor<PullAckRequest> {

    public PullAckRequestProcessor(final Executor executor) {
        super(executor, RpcRequests.PullAckResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final PullAckRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final PullAckRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final PullAckRequest request,
                                   final RpcRequestClosure done) {
        LOG.debug("[PULL-ACK] Received PullAckRequest groupId={} from {} term={} firstLogIndex={} lastLogIndex={}",
            request.getGroupId(), request.getServerId(), request.getTerm(), request.getFirstLogIndex(),
            request.getLastLogIndex());
        return service.handlePullAckRequest(request, done);
    }

    @Override
    public String interest() {
        return PullAckRequest.class.getName();
    }
}
