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

import java.io.Serializable;

import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * RPC request header
 *
 * @author jiachun.fjc
 */
public abstract class BaseRequest implements Serializable {

    private static final long serialVersionUID = -6576381361684687237L;

    public static final byte  PUT              = 0x01;
    public static final byte  GET              = 0x02;

    private long              regionId;
    private RegionEpoch       regionEpoch;

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public RegionEpoch getRegionEpoch() {
        return regionEpoch;
    }

    public void setRegionEpoch(RegionEpoch regionEpoch) {
        this.regionEpoch = regionEpoch;
    }

    public abstract byte magic();

    @Override
    public String toString() {
        return "BaseRequest{" + "regionId=" + regionId + ", regionEpoch=" + regionEpoch + '}';
    }
}
