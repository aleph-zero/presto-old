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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch split.
 */
public class ElasticsearchSplit implements ConnectorSplit
{
    private final String            connectorId;
    private final String            schema;
    private final String            table;
    private final int               shardId;
    private final int               shardCount;
    private final int               fetchSize;
    private final long              timeout;
    private final List<HostAddress> addresses;

    private final TupleDomain<ElasticsearchColumnHandle> predicate;

    @JsonCreator
    public ElasticsearchSplit(@JsonProperty("connectorId") String connectorId,
                              @JsonProperty("schema") String schema,
                              @JsonProperty("table") String table,
                              @JsonProperty("shardId") int shardId,
                              @JsonProperty("shardCount") int shardCount,
                              @JsonProperty("fetchSize") int fetchSize,
                              @JsonProperty("scrollTimeout") long timeout,
                              @JsonProperty("predicate") TupleDomain<ElasticsearchColumnHandle> predicate,
                              @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schema      = requireNonNull(schema, "schema is null");
        this.table       = requireNonNull(table, "table is null");
        this.predicate   = requireNonNull(predicate, "predicate is null");
        this.addresses   = requireNonNull(addresses, "addresses is null");
        this.shardId     = shardId;
        this.shardCount  = shardCount;
        this.fetchSize   = fetchSize;
        this.timeout     = timeout;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public int getShardId()
    {
        return shardId;
    }

    @JsonProperty
    public int getShardCount()
    {
        return shardCount;
    }

    @JsonProperty
    public int getFetchSize()
    {
        return fetchSize;
    }

    @JsonProperty
    public long getTimeout()
    {
        return timeout;
    }

    @JsonProperty
    public TupleDomain<ElasticsearchColumnHandle> getPredicate()
    {
        return predicate;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
