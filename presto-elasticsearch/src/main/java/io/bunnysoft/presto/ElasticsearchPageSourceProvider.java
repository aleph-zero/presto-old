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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import javax.inject.Inject;

import static io.bunnysoft.presto.util.Validators.checkType;
import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch page source provider.
 */
public class ElasticsearchPageSourceProvider implements ConnectorPageSourceProvider
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchPageSourceProvider(final ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorPageSource createPageSource(final ConnectorTransactionHandle transactionHandle,
                                                final ConnectorSession session,
                                                final ConnectorSplit connectorSplit,
                                                final List<ColumnHandle> columns)
    {
        ElasticsearchSplit split = checkType(connectorSplit, ElasticsearchSplit.class, "split");

        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            handles.add(checkType(column, ElasticsearchColumnHandle.class, "columnHandle"));
        }

        return new ElasticsearchPageSource(client, split, handles.build());
    }
}
