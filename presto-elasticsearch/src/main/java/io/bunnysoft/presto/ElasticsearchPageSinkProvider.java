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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import static io.bunnysoft.presto.util.Types.checkType;

/**
 * Page sink provider for Elasticsearch
 */
public class ElasticsearchPageSinkProvider implements ConnectorPageSinkProvider
{
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction,
                                            ConnectorSession session,
                                            ConnectorOutputTableHandle table)
    {
        ElasticsearchOutputTableHandle handle = checkType(table, ElasticsearchOutputTableHandle.class, "table");

        return new ElasticsearchPageSink(handle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction,
                                            ConnectorSession session,
                                            ConnectorInsertTableHandle insert)
    {
        return null;
    }
}
