/*
 * Copyright 2016 Andrew Selden.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.bunnysoft.presto;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.validation.constraints.Min;
import java.util.List;

/**
 * Configuration for elasticsearch connector.
 */
public class ElasticsearchConfig
{
    private String       clusterName;
    private String       securityUser;
    private String       securityPassword;
    private int          fetchSize;
    private boolean      securityEnabled;
    private boolean      forceArrayTypes;
    private List<String> hostAddresses = ImmutableList.of();

    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    public ElasticsearchConfig() { }

    public ElasticsearchConfig(final ElasticsearchConfig config)
    {
        this.clusterName      = config.getClusterName();
        this.hostAddresses    = config.getClusterHostAddresses();
        this.fetchSize        = config.getFetchSize();
        this.forceArrayTypes  = config.getForceArrayTypes();
        this.securityEnabled  = config.getSecurityEnabled();
        this.securityUser     = config.getSecurityUser();
        this.securityPassword = config.getSecurityPassword();
    }

    @Config("elasticsearch.cluster-name")
    public ElasticsearchConfig setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    @NotNull
    public String getClusterName()
    {
        return clusterName;
    }

    @Config("elasticsearch.cluster-host-addresses")
    public ElasticsearchConfig setClusterHostAddresses(String commaSeparatedList)
    {
        this.hostAddresses = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    @NotNull
    @Size(min = 1)
    public List<String> getClusterHostAddresses()
    {
        return hostAddresses;
    }

    @Config("elasticsearch.fetch-size")
    public ElasticsearchConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("elasticsearch.force-array-types")
    public ElasticsearchConfig setForceArrayTypes(boolean forceArrayTypes)
    {
        this.forceArrayTypes = forceArrayTypes;
        return this;
    }

    public boolean getForceArrayTypes()
    {
        return forceArrayTypes;
    }

    @Config("elasticsearch.security-enabled")
    public ElasticsearchConfig setSecurityEnabled(boolean securityEnabled)
    {
        this.securityEnabled = securityEnabled;
        return this;
    }

    public boolean getSecurityEnabled()
    {
        return securityEnabled;
    }

    @Config("elasticsearch.security-user")
    public ElasticsearchConfig setSecurityUser(String securityUser)
    {
        this.securityUser = securityUser;
        return this;
    }

    public String getSecurityUser()
    {
        return securityUser;
    }

    @Config("elasticsearch.security-password")
    public ElasticsearchConfig setSecurityPassword(String securityPassword)
    {
        this.securityPassword = securityPassword;
        return this;
    }

    public String getSecurityPassword()
    {
        return securityPassword;
    }
}
