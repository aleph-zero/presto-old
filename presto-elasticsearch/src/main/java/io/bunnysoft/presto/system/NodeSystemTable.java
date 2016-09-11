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
package io.bunnysoft.presto.system;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import io.bunnysoft.presto.ElasticsearchClient;

import javax.inject.Inject;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;

/**
 * System table for elasticsearch nodes.
 */
public class NodeSystemTable implements SystemTable
{
    private final ElasticsearchClient client;

    public static final SchemaTableName NODE_TABLE_NAME = new SchemaTableName("system", "nodes");

    public static final ConnectorTableMetadata NODE_TABLE = tableMetadataBuilder(NODE_TABLE_NAME)
            .column("id", createUnboundedVarcharType())
            .column("name", createUnboundedVarcharType())
            .build();

    @Inject
    public NodeSystemTable(ElasticsearchClient client)
    {
        this.client = client;
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return NODE_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(NODE_TABLE);


        return table.build().cursor();
    }
}
