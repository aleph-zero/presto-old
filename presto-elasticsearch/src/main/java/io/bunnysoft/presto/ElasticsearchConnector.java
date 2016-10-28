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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch connector.
 */
public class ElasticsearchConnector implements Connector
{
    private final ElasticsearchMetadata           metadata;
    private final ElasticsearchPageSourceProvider pageSourceProvider;
    private final ElasticsearchSplitManager       splitManager;
    private final List<PropertyMetadata<?>>       sessionProperties;

    @Inject
    public ElasticsearchConnector(final ElasticsearchPageSourceProvider pageSourceProvider,
                                  final ElasticsearchMetadata metadata,
                                  final ElasticsearchSplitManager splitManager,
                                  final ElasticsearchSessionProperties sessionProperties)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.metadata           = requireNonNull(metadata, "metadata is null");
        this.splitManager       = requireNonNull(splitManager, "splitManager is null");
        this.sessionProperties  = requireNonNull(sessionProperties, "sessionProperties is null").getSessionProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return new ElasticsearchTransactionHandle();
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
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
