/*
 * Copyright 2017 Bunnysoft. Stay on the happy path.
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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import io.bunnysoft.presto.metadata.ShardPolicy;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import javax.validation.constraints.Min;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Elasticsearch config.
 */
public class ElasticsearchConfig
{
    private String       clusterName;
    private int          fetchSize       = 1000;
    private boolean      forceArrayTypes = false;
    private String       shardPolicy     = ShardPolicy.defaultPolicy().name().toLowerCase();
    private Duration     scrollTimeout   = new Duration(1000.00, TimeUnit.MILLISECONDS);
    private List<String> hostAddresses   = ImmutableList.of();

    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    public ElasticsearchConfig() { }

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

    @NotNull
    @MinDuration("1000ms")
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("elasticsearch.scroll-timeout")
    public ElasticsearchConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
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

    @Config("elasticsearch.shard-policy")
    public ElasticsearchConfig setShardPolicy(String shardPolicy)
    {
        this.shardPolicy = shardPolicy;
        return this;
    }

    @NotNull
    @Pattern(regexp = "any|replica|primary")
    public String getShardPolicy()
    {
        return shardPolicy;
    }
}
