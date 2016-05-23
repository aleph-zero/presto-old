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
package org.primer.bunny;

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import org.primer.bunny.metadata.ShardMetadata;
import org.primer.bunny.metadata.ShardMetadatas;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.primer.bunny.util.Types.checkType;
import static org.primer.bunny.ElasticsearchErrorCode.ELASTICSEARCH_SHARD_ERROR;
import static org.primer.bunny.ElasticsearchSessionProperties.getFetchSize;

/**
 * Elasticsearch split manager.
 */
public class ElasticsearchSplitManager implements ConnectorSplitManager
{
    private final String                connectorId;
    private final ElasticsearchMetadata metadata;

    @Inject
    public ElasticsearchSplitManager(final ElasticsearchConnectorId connectorId, final ElasticsearchMetadata metadata)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metadata    = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorSplitSource getSplits(final ConnectorTransactionHandle transaction,
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
                getFetchSize(session));
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

        private final TupleDomain<ElasticsearchColumnHandle> predicate;

        public ElasticsearchSplitSource(final String schema,
                                        final String table,
                                        final TupleDomain<ElasticsearchColumnHandle> predicate,
                                        final ShardMetadatas shards,
                                        final int fetchSize)
        {
            this.schema    = requireNonNull(schema, "schema is null");
            this.table     = requireNonNull(table, "table is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.shards    = requireNonNull(shards, "shards is null");
            this.fetchSize = fetchSize;
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

                    ShardMetadata primary = shards.getPrimaryShardMetadata(shards.iterator().next());
                    if (primary == null) {
                        throw new PrestoException(ELASTICSEARCH_SHARD_ERROR, "Missing primary shard: " + schema + "." + table);
                    }

                    list.add(split(primary));
                }

                return list.build();
            };
        }

        private ConnectorSplit split(final ShardMetadata shard)
        {
            ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
            builder.add(shard.getHostAddress());
            return new ElasticsearchSplit(connectorId, schema, table,
                    shard.getShardId(), shards.getNumShards(), fetchSize, predicate, builder.build());
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public boolean isFinished()
        {
            return !shards.iterator().hasNext();
        }
    }
}
