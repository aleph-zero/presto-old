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

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.testing.TestingConnectorSession;

import io.airlift.log.Logger;

import io.bunnysoft.presto.util.TestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.elasticsearch.client.Client;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static io.bunnysoft.presto.util.TestTable.TABLE_A_SCALAR;

/**
 * Tests for table creation.
 */
public class TestCreateTable
{
    private static final Logger logger = Logger.get(TestCreateTable.class);

    private static Client                  client;
    private static ElasticsearchMetadata   metadata;
    private static TestingConnectorSession session;

    @BeforeClass
    public static void before() throws Exception
    {
        ElasticsearchConnectorId id = new ElasticsearchConnectorId("test");
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(id, TestUtil.config());
        client = elasticsearchClient.client();
        metadata = new ElasticsearchMetadata(id, elasticsearchClient);
        session = new TestingConnectorSession(ImmutableList.of());
    }

    @AfterClass
    public static void after()
    {
        try {
            TABLE_A_SCALAR.delete(client);
        }
        finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    public void testCreateTable() throws Exception
    {
        ConnectorTableMetadata newTableMetadata =
                new ConnectorTableMetadata(TABLE_A_SCALAR.schemaTableName(), TABLE_A_SCALAR.columns());

        metadata.createTable(session, newTableMetadata);

        ConnectorTableMetadata fetchedTableMetadata = metadata.getTableMetadata(session,
                new ElasticsearchTableHandle("test", TABLE_A_SCALAR.schema(), TABLE_A_SCALAR.table()));

        assertThat(fetchedTableMetadata.getTable().getSchemaName(), equalTo(newTableMetadata.getTable().getSchemaName()));
        assertThat(fetchedTableMetadata.getTable().getTableName(), equalTo(newTableMetadata.getTable().getTableName()));

        List<ColumnMetadata> fetchedColumns = fetchedTableMetadata.getColumns();
        List<ColumnMetadata> newColumns = newTableMetadata.getColumns();
        assertThat(fetchedColumns.size(), equalTo(newColumns.size()));
    }
}
