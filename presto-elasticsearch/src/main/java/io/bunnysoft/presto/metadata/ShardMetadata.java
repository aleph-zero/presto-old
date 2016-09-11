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
package io.bunnysoft.presto.metadata;

import com.facebook.presto.spi.HostAddress;

import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.common.transport.TransportAddress;

/**
 * Metadata for elasticsearch shard.
 */
public class ShardMetadata
{
    private final int shardId;
    private final IndicesShardStoresResponse.StoreStatus storeStatus;

    public ShardMetadata(final int shardId, final IndicesShardStoresResponse.StoreStatus storeStatus)
    {
        this.shardId     = shardId;
        this.storeStatus = storeStatus;
    }

    public int getShardId()
    {
        return shardId;
    }

    public boolean isPrimary()
    {
        return storeStatus.getAllocationStatus().equals(IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY);
    }

    public boolean isReplica()
    {
        return storeStatus.getAllocationStatus().equals(IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA);
    }

    public HostAddress getHostAddress()
    {
        TransportAddress address = storeStatus.getNode().getAddress();
        return HostAddress.fromParts(address.getHost(), address.getPort());
    }
}
