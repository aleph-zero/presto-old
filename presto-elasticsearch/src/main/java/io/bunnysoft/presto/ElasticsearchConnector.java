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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static io.bunnysoft.presto.ElasticsearchTransactionHandle.INSTANCE;

/**
 * Elasticsearch connector.
 */
public class ElasticsearchConnector implements Connector
{
    private final ElasticsearchSplitManager       splitManager;
    private final ElasticsearchPageSourceProvider pageSourceProvider;
    private final ElasticsearchMetadata           metadata;
    private final List<PropertyMetadata<?>>       sessionProperties;
    private final List<PropertyMetadata<?>>       tableProperties;
    private final ElasticsearchPageSinkProvider   pageSinkProvider;

    @Inject
    public ElasticsearchConnector(final ElasticsearchSplitManager splitManager,
                                  final ElasticsearchPageSourceProvider pageSourceProvider,
                                  final ElasticsearchPageSinkProvider pageSinkProvider,
                                  final ElasticsearchMetadata metadata,
                                  final ElasticsearchSessionProperties sessionProperties,
                                  final ElasticsearchTableProperties tableProperties)
    {
        this.splitManager       = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider   = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.metadata           = requireNonNull(metadata, "metadata is null");
        this.sessionProperties  = requireNonNull(sessionProperties, "sessionProperties is null").getSessionProperties();
        this.tableProperties    = requireNonNull(tableProperties, "tableProperties is null").getTableProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
