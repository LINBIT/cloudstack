// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// 
// Automatically generated by addcopyright.py at 01/29/2013
package com.cloud.baremetal.networkservice;

import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.EntityReference;

import com.cloud.baremetal.database.BaremetalPxeVO;
import com.cloud.serializer.Param;
import com.google.gson.annotations.SerializedName;
@EntityReference(value=BaremetalPxeVO.class)
public class BaremetalPxePingResponse extends BaremetalPxeResponse {
    @SerializedName(ApiConstants.PING_STORAGE_SERVER_IP) @Param(description="PING storage server ip")
    private String pingStorageServerIp;
    
    @SerializedName(ApiConstants.PING_DIR) @Param(description="Root directory on PING storage server")
    private String pingDir;
    
    @SerializedName(ApiConstants.TFTP_DIR) @Param(description="Tftp root directory of PXE server")
    private String tftpDir;

    public String getPingStorageServerIp() {
        return pingStorageServerIp;
    }

    public void setPingStorageServerIp(String pingStorageServerIp) {
        this.pingStorageServerIp = pingStorageServerIp;
    }

    public String getPingDir() {
        return pingDir;
    }

    public void setPingDir(String pingDir) {
        this.pingDir = pingDir;
    }

    public String getTftpDir() {
        return tftpDir;
    }

    public void setTftpDir(String tftpDir) {
        this.tftpDir = tftpDir;
    }
}
