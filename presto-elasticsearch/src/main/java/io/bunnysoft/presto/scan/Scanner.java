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
package io.bunnysoft.presto.scan;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;

import io.airlift.log.Logger;

import io.bunnysoft.presto.ElasticsearchColumnHandle;
import io.bunnysoft.presto.ElasticsearchSplit;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_SEARCH_ERROR;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * A scanner, darkly.
 */
public class Scanner implements Closeable
{
    private static final Logger logger = Logger.get(Scanner.class);

    private final Client client;
    private final String schema;
    private final String table;
    private final int    shard;
    private final int    totalShards;
    private final int    fetchSize;
    private final long   timeout;

    private final List<String> names;
    private final TupleDomain<ElasticsearchColumnHandle> predicate;

    private long totalReceivedHits = 0;
    private ListenableActionFuture<SearchResponse> future;

    public Scanner(final Client client, final ElasticsearchSplit split, final List<ElasticsearchColumnHandle> columns)
    {
        this.client      = client;
        this.schema      = split.getSchema();
        this.table       = split.getTable();
        this.shard       = split.getShardId();
        this.totalShards = split.getShardCount();
        this.fetchSize   = split.getFetchSize();
        this.timeout     = split.getTimeout();
        this.predicate   = split.getPredicate();
        this.names       = columns.stream().map(ElasticsearchColumnHandle::getColumnName).collect(Collectors.toList());

        checkState(fetchSize > 0, "Invalid fetch size");
        logger.debug("Initialized scan: [%s.%s][%d] %s", schema, table, shard, names);
    }

    public void scan()
    {
        SearchRequestBuilder request = client.prepareSearch(schema)
                .setTypes(table)
                .setScroll(new TimeValue(timeout))
                .setSize(fetchSize)
                .slice(new SliceBuilder(shard, totalShards))
                .setQuery(query());

        names.forEach(request::addDocValueField);
        logger.debug("Executing search: \n%s", request);
        future = request.execute();
    }

    public void scan(final String scrollId)
    {
        future = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(timeout)).execute();
    }

    public SearchResponse response()
    {
        try {
            SearchResponse response = future.actionGet();
            totalReceivedHits += response.getHits().hits().length;

            if (response.getHits().getHits().length == 0) {
                return null;
            }

            logger.debug("Received [%d/%d] hits for scan: [%s.%s][%d]",
                    totalReceivedHits, response.getHits().getTotalHits(), schema, table, shard);

            return response;
        }
        catch (Throwable t) {
            throw new PrestoException(ELASTICSEARCH_SEARCH_ERROR, "Failed to read response", t);
        }
    }

    private QueryBuilder query()
    {
        return matchAllQuery();
    }

    @Override
    public void close() throws IOException
    {
        // XXX Finish
    }
}
