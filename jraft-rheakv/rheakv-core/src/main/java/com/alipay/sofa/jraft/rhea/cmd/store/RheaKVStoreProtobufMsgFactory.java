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
package com.alipay.sofa.jraft.rhea.cmd.store;

import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

/**
 * Factory for registering RheaKV store protobuf messages with gRPC.
 *
 * @author jiachun.fjc
 */
public final class RheaKVStoreProtobufMsgFactory {

    static {
        try {
            final RaftRpcFactory rpcFactory = RpcFactoryHelper.rpcFactory();

            // Register request messages
            final RheaKVStoreProto.GetRequest getRequestDefault = RheaKVStoreProto.GetRequest.getDefaultInstance();
            rpcFactory.registerProtobufSerializer(RheaKVStoreProto.GetRequest.class.getName(), getRequestDefault);

            final RheaKVStoreProto.PutRequest putRequestDefault = RheaKVStoreProto.PutRequest.getDefaultInstance();
            rpcFactory.registerProtobufSerializer(RheaKVStoreProto.PutRequest.class.getName(), putRequestDefault);

            // Register response messages using reflection to avoid direct dependency on jraft-extension
            registerResponseMessage(RheaKVStoreProto.GetRequest.class.getName(),
                RheaKVStoreProto.GetResponse.getDefaultInstance());
            registerResponseMessage(RheaKVStoreProto.PutRequest.class.getName(),
                RheaKVStoreProto.PutResponse.getDefaultInstance());
        } catch (final Throwable t) {
            throw new IllegalStateException("Failed to register RheaKV store protobuf messages", t);
        }
    }

    /**
     * Register response message using reflection to avoid direct dependency on jraft-extension.
     */
    private static void registerResponseMessage(final String requestClassName,
                                                final com.google.protobuf.Message responseInstance) {
        try {
            final Class<?> marshallerHelperClass = Class.forName("com.alipay.sofa.jraft.rpc.impl.MarshallerHelper");
            final java.lang.reflect.Method registerMethod = marshallerHelperClass.getMethod("registerRespInstance",
                String.class, com.google.protobuf.Message.class);
            registerMethod.invoke(null, requestClassName, responseInstance);
        } catch (final Exception e) {
            // If MarshallerHelper is not available (e.g., using Bolt), this is not an error
            // Only log if gRPC is being used
            if (RpcFactoryHelper.rpcFactory().getClass().getName().contains("Grpc")) {
                throw new IllegalStateException("Failed to register response message for " + requestClassName, e);
            }
        }
    }

    /**
     * Load the factory to ensure protobuf messages are registered.
     */
    public static void load() {
        // Static initializer already did the work
    }

    private RheaKVStoreProtobufMsgFactory() {
    }
}
