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

import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * Utility class for converting between protobuf messages and Java objects.
 *
 * @author jiachun.fjc
 */
public final class ProtoConverter {

    /**
     * Convert RegionEpoch to RheaKVStoreProto.RegionEpoch
     */
    public static RheaKVStoreProto.RegionEpoch toProto(final RegionEpoch epoch) {
        if (epoch == null) {
            return null;
        }
        return RheaKVStoreProto.RegionEpoch.newBuilder()
            .setConfVer(epoch.getConfVer())
            .setVersion(epoch.getVersion())
            .build();
    }

    /**
     * Convert RheaKVStoreProto.RegionEpoch to RegionEpoch
     */
    public static RegionEpoch fromProto(final RheaKVStoreProto.RegionEpoch proto) {
        if (proto == null || !proto.isInitialized()) {
            return null;
        }
        return new RegionEpoch(proto.getConfVer(), proto.getVersion());
    }

    private ProtoConverter() {
    }
}

