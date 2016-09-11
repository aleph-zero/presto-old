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
package io.bunnysoft.presto.scan;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;

import io.airlift.log.Logger;
import io.bunnysoft.presto.ElasticsearchColumnHandle;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_SEARCH_ERROR;
import static com.google.common.base.Preconditions.checkState;

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
    private final int    numShards;
    private final int    fetchSize;

    private final List<String> names;
    private final TupleDomain<ElasticsearchColumnHandle> predicate;

    private ListenableActionFuture<SearchResponse> future = null;

    private String scroll;
    private long   totalReceivedHits = 0;

    private static final TimeValue DEFAULT_SCROLL_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    public Scanner(final Client client, final String schema, final String table, final int shard,
                   final int numShards, final int fetchSize, final TupleDomain<ElasticsearchColumnHandle> predicate,
                   final List<ElasticsearchColumnHandle> columns)
    {
        this.client    = client;
        this.schema    = schema;
        this.table     = table;
        this.shard     = shard;
        this.numShards = numShards;
        this.fetchSize = fetchSize;
        this.predicate = predicate;
        this.names     = columns.stream().map(ElasticsearchColumnHandle::getColumnName).collect(Collectors.toList());

        checkState(numShards > 0, "Invalid number of shards");
        checkState(shard >= 0 && shard <= numShards, "Invalid shard id");
        checkState(fetchSize > 0, "Invalid fetch size");

        logger.info("Initialized scan: [%s.%s][%d] %s", schema, table, shard, names);
    }

    public void scan()
    {
        SearchRequestBuilder request = request();
        logger.debug("Search request:\n%s", request);
        future = request.execute();
    }

    public void scan(final String scroll)
    {
        future = client.prepareSearchScroll(scroll).setScroll(DEFAULT_SCROLL_TIMEOUT).execute();
    }

    public SearchResponse response()
    {
        try {
            SearchResponse response = future.actionGet();
            totalReceivedHits += response.getHits().hits().length;
            scroll = response.getScrollId();

            if (response.getHits().getHits().length == 0) {
                return null;
            }

            logger.debug("Received [%d/%d] hits for scan: [%s.%s][%d]",
                    totalReceivedHits, response.getHits().getTotalHits(), schema, table, shard);

            return response;
        }
        catch (Throwable t) {
            throw new PrestoException(ELASTICSEARCH_SEARCH_ERROR, "Failed to read search response", t);
        }
    }

    @Override
    public void close() throws IOException
    {
        try {
            if (future != null) {
                future.cancel(true);
            }

            if (scroll != null) {
                ClearScrollResponse response = client.prepareClearScroll().addScrollId(scroll).execute().actionGet();
                if (!response.isSucceeded()) {
                    logger.error("Failed to clear scroll");
                }
            }
        }
        finally {
            future = null;
            scroll = null;
        }
    }

    private SliceBuilder slice(final int id, final int max)
    {
        return new SliceBuilder(id, max);
    }

    private QueryBuilder query()
    {
        QueryBuilder qb;

        if (predicate.isAll()) {    // isAll() indicates there are no effective predicates to filter on.
            qb = QueryBuilders.matchAllQuery();
        }
        else {
            throw new UnsupportedOperationException("Predicates are not implemented");
        }

        return QueryBuilders.constantScoreQuery(qb);
    }

    private SearchRequestBuilder request()
    {
        return client.prepareSearch(schema)
                .setTypes(table)
                .setScroll(DEFAULT_SCROLL_TIMEOUT)
                .setSize(fetchSize)
                .slice(slice(shard, numShards))
                .setFetchSource(names.toArray(new String[names.size()]), null)
                .setQuery(query());
    }
}
