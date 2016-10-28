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
package io.bunnysoft.presto.metadata;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch shard metadata grouped by id.
 */
public class ShardMetadatas implements Iterable<Integer>
{
    private final String                            index;
    private final Map<Integer, List<ShardMetadata>> shards;
    private final Iterator<Integer>                 iterator;

    private final static Random random = new Random();

    public ShardMetadatas(final String index, final Map<Integer, List<ShardMetadata>> shards)
    {
        this.index    = requireNonNull(index, "index is null");
        this.shards   = requireNonNull(shards, "shards is null");
        this.iterator = shards.keySet().iterator();
    }

    public int getNumShards()
    {
        return shards.size();
    }

    private ShardMetadata getPrimaryShardMetadata(final int shardId)
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

    private List<ShardMetadata> getReplicaShardMetadata(final int shardId)
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

    public ShardMetadata getShardMetadata(final int shardId, final ShardPolicy policy)
    {
        List<ShardMetadata> metadatas = shards.get(shardId);
        if (metadatas == null || metadatas.size() == 0) {
            return null;
        }

        ShardMetadata replica = null;
        List<ShardMetadata> replicas = getReplicaShardMetadata(shardId);
        if (replicas.size() > 0) {
            Collections.shuffle(replicas);
            replica = replicas.get(0);
        }

        ShardMetadata primary = getPrimaryShardMetadata(shardId);

        switch (policy)
        {
            case PRIMARY:
                return primary;
            case REPLICA:
                return replica != null ? replica : primary;
            case ANY:
            default:
                return (random.nextBoolean() && replica != null) ? replica : primary;
        }
    }

    @Override
    public Iterator<Integer> iterator()
    {
        return iterator;
    }

}
