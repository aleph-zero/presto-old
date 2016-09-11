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

import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Shard metadata grouped by id.
 */
public class ShardMetadatas implements Iterable<Integer>
{
    private final String                            index;
    private final Map<Integer, List<ShardMetadata>> shards;
    private final Iterator<Integer>                 iterator;

    public ShardMetadatas(final String index, final Map<Integer, List<ShardMetadata>> shards)
    {
        this.index    = requireNonNull(index, "index is null");
        this.shards   = requireNonNull(shards, "shards is null");
        this.iterator = shards.keySet().iterator();
    }

    public String getIndex()
    {
        return index;
    }

    public int getNumShards()
    {
        return shards.size();
    }

    public ShardMetadata getPrimaryShardMetadata(final int shardId)
    {
        List<ShardMetadata> metadatas = shards.get(shardId);
        if (metadatas == null || metadatas.size() == 0) {
            return null;
        }

        for (ShardMetadata metadata : metadatas) {
            if (metadata.isPrimary()) {
                return metadata;
            }
        }

        return null;
    }

    public List<ShardMetadata> getReplicaShardMetadata(final int shardId)
    {
        ImmutableList.Builder<ShardMetadata> builder = ImmutableList.builder();
        List<ShardMetadata> metadatas = shards.get(shardId);

        if (metadatas != null && metadatas.size() > 0) {
            for (ShardMetadata metadata : metadatas) {
                if (metadata.isReplica()) {
                    builder.add(metadata);
                }
            }
        }

        return builder.build();
    }

    @Override
    public Iterator<Integer> iterator()
    {
        return iterator;
    }
}
