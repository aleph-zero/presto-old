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

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import io.airlift.log.Logger;

import io.bunnysoft.presto.metadata.ShardMetadata;
import io.bunnysoft.presto.metadata.ShardMetadatas;
import io.bunnysoft.presto.metadata.ShardPolicy;

import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_SHARD_ERROR;
import static io.bunnysoft.presto.util.Validators.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Elasticsearch split manager.
 *
 * The strategy employed is to create one split per shard. The physical shards
 * that are chosen (primary|replica) can be influenced by the 'shard-policy'
 * setting in the connector's .properties file.
 */
public class ElasticsearchSplitManager implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(ElasticsearchSplitManager.class);

    private final String                connectorId;
    private final ElasticsearchMetadata metadata;

    @Inject
    public ElasticsearchSplitManager(final ElasticsearchConnectorId connectorId,
                                     final ElasticsearchMetadata metadata)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadata    = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorSplitSource getSplits(final ConnectorTransactionHandle transactionHandle,
                                          final ConnectorSession session,
                                          final ConnectorTableLayoutHandle layout)
    {
        ElasticsearchTableLayoutHandle handle = checkType(layout, ElasticsearchTableLayoutHandle.class, "layout");
        ElasticsearchTableHandle table = handle.getTable();

        return new ElasticsearchSplitSource(
                table.getSchemaName(),
                table.getTableName(),
                toElasticsearchTupleDomain(handle.getConstraint()),
                metadata.getShardLocations(table.getSchemaName()),
                ShardPolicy.valueOf(ElasticsearchSessionProperties.getShardPolicy(session).toUpperCase(Locale.ROOT)),
                ElasticsearchSessionProperties.getFetchSize(session),
                ElasticsearchSessionProperties.getScrollTimeout(session));

    }

    private static TupleDomain<ElasticsearchColumnHandle> toElasticsearchTupleDomain(final TupleDomain<ColumnHandle> tupleDomain)
    {
        return tupleDomain.transform(handle -> checkType(handle, ElasticsearchColumnHandle.class, "columnHandle"));
    }

    private class ElasticsearchSplitSource implements ConnectorSplitSource
    {
        private final String         schema;
        private final String         table;
        private final ShardMetadatas shards;
        private final int            fetchSize;
        private final long           timeout;
        private final ShardPolicy    policy;

        private final TupleDomain<ElasticsearchColumnHandle> predicate;

        public ElasticsearchSplitSource(final String schema,
                                        final String table,
                                        final TupleDomain<ElasticsearchColumnHandle> predicate,
                                        final ShardMetadatas shards,
                                        final ShardPolicy policy,
                                        final int fetchSize,
                                        final long timeout)
        {
            this.schema    = requireNonNull(schema, "schema is null");
            this.table     = requireNonNull(table, "table is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.shards    = requireNonNull(shards, "shards is null");
            this.policy    = requireNonNull(policy, "policy is null");
            this.fetchSize = fetchSize;
            this.timeout   = timeout;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(final int maxSize)
        {
            return supplyAsync(batch(maxSize));
        }

        private Supplier<List<ConnectorSplit>> batch(final int maxSize)
        {
            return () -> {
                ImmutableList.Builder<ConnectorSplit> list = ImmutableList.builder();

                for (int i = 0; i < maxSize; i++) {

                    if (!shards.iterator().hasNext()) {
                        break;
                    }

                    ShardMetadata shard = shards.getShardMetadata(shards.iterator().next(), policy);
                    if (shard == null) {
                        throw new PrestoException(ELASTICSEARCH_SHARD_ERROR, "Unable to assign shards to split for: ["
                                + schema + "." + table + "] using policy: " + policy);
                    }

                    list.add(split(shard));
                }

                return list.build();
            };
        }

        private ConnectorSplit split(final ShardMetadata shard)
        {
            logger.debug("Split created on: [" + schema + "." + table + "] for shard [" + shard.getShardId() + "/" +
                    shards.getNumShards() + "] on hosts: " + shard.getHostAddress());

            return new ElasticsearchSplit(
                    connectorId,
                    schema,
                    table,
                    shard.getShardId(),
                    shards.getNumShards(),
                    fetchSize,
                    timeout,
                    predicate,
                    ImmutableList.of(shard.getHostAddress()));
        }

        @Override
        public void close()
        {
            // nothing to close.
        }

        @Override
        public boolean isFinished()
        {
            return !shards.iterator().hasNext();
        }
    }

}
