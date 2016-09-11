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
package io.bunnysoft.presto;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Elasticsearch page sink
 */
public class ElasticsearchPageSink implements ConnectorPageSink
{
    private final List<ElasticsearchColumnHandle> columns;

    public ElasticsearchPageSink(final List<ElasticsearchColumnHandle> columns)
    {
        this.columns = columns;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {

        for (int position = 0; position < page.getPositionCount(); position++) {

            for (int channel = 0; channel < page.getChannelCount(); channel++) {

            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public Collection<Slice> finish()
    {
        return null;
    }

    @Override
    public void abort()
    {

    }
}
