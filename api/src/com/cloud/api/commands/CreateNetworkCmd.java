/**
 *  Copyright (C) 2010 Cloud.com, Inc.  All rights reserved.
 * 
 * This software is licensed under the GNU General Public License v3 or later.
 * 
 * It is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package com.cloud.api.commands;

import org.apache.log4j.Logger;

import com.cloud.api.ApiConstants;
import com.cloud.api.BaseCmd;
import com.cloud.api.Implementation;
import com.cloud.api.Parameter;
import com.cloud.api.ServerApiException;
import com.cloud.api.BaseCmd.CommandType;
import com.cloud.api.response.NetworkResponse;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.network.Network;

@Implementation(description="Creates a network", responseObject=NetworkResponse.class)
public class CreateNetworkCmd extends BaseCmd {
    public static final Logger s_logger = Logger.getLogger(CreateNetworkCmd.class.getName());

    private static final String s_name = "createnetworkresponse";

    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////
    
    @Parameter(name=ApiConstants.NAME, type=CommandType.STRING, required=true, description="the name of the network")   
    private String name;
    
    @Parameter(name=ApiConstants.DISPLAY_TEXT, type=CommandType.STRING, required=true, description="the display text of the network")   
    private String displayText;
    
    @Parameter(name=ApiConstants.NETWORK_OFFERING_ID, type=CommandType.LONG, required=true, description="the network offering id")
    private Long networkOfferingId;
    
    @Parameter(name=ApiConstants.ZONE_ID, type=CommandType.LONG, required=true, description="the Zone ID for the Vlan ip range")
    private Long zoneId;

    @Parameter(name=ApiConstants.GATEWAY, type=CommandType.STRING, description="the gateway of the VLAN IP range")
    private String gateway;
    
    @Parameter(name=ApiConstants.NETMASK, type=CommandType.STRING, description="the netmask of the VLAN IP range")
    private String netmask;
    
    @Parameter(name=ApiConstants.START_IP, type=CommandType.STRING, description="the beginning IP address in the VLAN IP range")
    private String startIp;
    
    @Parameter(name=ApiConstants.END_IP, type=CommandType.STRING, description="the ending IP address in the VLAN IP range")
    private String endIp;

    @Parameter(name=ApiConstants.VLAN, type=CommandType.STRING, description="the ID or VID of the VLAN. Default is an \"untagged\" VLAN.")
    private String vlan;

    @Parameter(name=ApiConstants.ACCOUNT, type=CommandType.STRING, description="account who will own the network")
    private String accountName;

    @Parameter(name=ApiConstants.DOMAIN_ID, type=CommandType.LONG, description="domain ID of the account owning a network")
    private Long domainId;
    
    @Parameter(name=ApiConstants.IS_SHARED, type=CommandType.BOOLEAN, description="true is network offering supports vlans")
    private Boolean isShared; 
    
    @Parameter(name=ApiConstants.IS_DEFAULT, type=CommandType.BOOLEAN, description="true if network is default, false otherwise")
    private Boolean isDefault;
    
    @Parameter(name=ApiConstants.NETWORK_DOMAIN, type=CommandType.STRING, description="network domain")
    private String networkDomain;
    
    @Parameter(name=ApiConstants.SECURITY_GROUP_EANBLED, type=CommandType.BOOLEAN, description="true if network is security group enabled, false otherwise")
    private Boolean is_security_group_enabled;

    /////////////////////////////////////////////////////
    /////////////////// Accessors ///////////////////////
    /////////////////////////////////////////////////////
    public Long getNetworkOfferingId() {
        return networkOfferingId;
    }

    public Long getZoneId() {
        return zoneId;
    }

    public String getGateway() {
        return gateway;
    }

    public String getVlan() {
        return vlan;
    }

    public String getAccountName() {
        return accountName;
    }

    public Long getDomainId() {
        return domainId;
    }
    
    public String getNetmask() {
        return netmask;
    }

    public String getStartIp() {
        return startIp;
    }

    public String getEndIp() {
        return endIp;
    }
    
    public String getNetworkName() {
        return name;
    }
    
    public String getDisplayText() {
        return displayText;
    }
    
    public boolean getIsShared() {
        return isShared == null ? false : isShared;
    }
    
    public Boolean isDefault() {
        return isDefault;
    }

    public String getNetworkDomain() {
        return networkDomain;
    }
    
    public boolean isSecurityGroupEnabled() {
        return is_security_group_enabled == null ? false : true;
    }
    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////

    @Override
    public String getCommandName() {
        return s_name;
    }
    
    @Override
    public void execute() throws InsufficientCapacityException, ConcurrentOperationException{
        Network result = _networkService.createNetwork(this);
        if (result != null) {
            NetworkResponse response = _responseGenerator.createNetworkResponse(result);
            response.setResponseName(getCommandName());
            this.setResponseObject(response);
        }else {
            throw new ServerApiException(BaseCmd.INTERNAL_ERROR, "Failed to create network");
        }
    }
}
