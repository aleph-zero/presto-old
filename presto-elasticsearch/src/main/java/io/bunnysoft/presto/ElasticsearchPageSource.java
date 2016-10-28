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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import io.airlift.log.Logger;

import io.bunnysoft.presto.scan.Scanner;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Elasticsearch page source.
 */
public class ElasticsearchPageSource implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ElasticsearchPageSource.class);

    private final Scanner      scanner;
    private final List<Type>   types;
    private final List<String> columns;

    private boolean finished = false;

    public ElasticsearchPageSource(final ElasticsearchClient client,
                                   final ElasticsearchSplit split,
                                   final List<ElasticsearchColumnHandle> columns)
    {
        this.types   = columns.stream().map(ElasticsearchColumnHandle::getColumnType).collect(Collectors.toList());
        this.columns = columns.stream().map(ElasticsearchColumnHandle::getColumnName).collect(Collectors.toList());

        this.scanner = new Scanner(client.client(), split, columns);
        scanner.scan(); // initiate scan asynchronously
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        SearchResponse response = scanner.response();
        if (response == null) {
            finished = true;
            return null;
        }

        scanner.scan(response.getScrollId());
        return page(response);
    }

    private Page page(final SearchResponse response)
    {
        PageBuilder page = new PageBuilder(types);

        for (SearchHit hit : response.getHits()) {

            //page.declarePosition();
            Map<String, SearchHitField> fields = hit.fields();

            for (int i = 0; i < types.size(); i++) {
                ;
            }
        }

        return page.build();
    }

    private void append(final Type type, final BlockBuilder builder, final Object value)
    {
        logger.debug("type: " + type + " value: " + value.toString());
    }

    @Override
    public void close() throws IOException
    {
        scanner.close();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

}
