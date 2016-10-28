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
package io.bunnysoft.presto.system;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;

/**
 * System table for Elasticsearch nodes.
 */
public class ElasticsearchNodeSystemTable implements SystemTable
{
    private static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("runtime", "xyz");

    private static final ConnectorTableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())
            .column("node_name", createUnboundedVarcharType())
            .build();

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return NODES_TABLE;
    }

    @Override
    public RecordCursor cursor(final ConnectorTransactionHandle transactionHandle,
                               final ConnectorSession session,
                               final TupleDomain<Integer> constraint)
    {
        Builder builder = InMemoryRecordSet.builder(NODES_TABLE);

        return builder.build().cursor();
    }
}
