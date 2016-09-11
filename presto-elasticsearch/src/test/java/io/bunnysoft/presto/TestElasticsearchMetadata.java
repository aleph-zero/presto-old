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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.testing.TestingConnectorSession;

import io.airlift.log.Logger;

import io.bunnysoft.presto.util.TestTable;
import io.bunnysoft.presto.util.TestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import org.elasticsearch.client.Client;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test metadata operations.
 */
public class TestElasticsearchMetadata
{
    private static final Logger logger = Logger.get(TestElasticsearchMetadata.class);

    private static Client client;
    private static ElasticsearchMetadata metadata;
    private static TestingConnectorSession SESSION, ARRAY_SESSION, SCALAR_SESSION;

    private static final String ID = "test-id";

    @BeforeClass
    public static void before() throws Exception
    {
        ElasticsearchConfig config       = TestUtil.config();
        ElasticsearchConfig scalarConfig = new ElasticsearchConfig(config);
        ElasticsearchConfig arrayConfig  = new ElasticsearchConfig(config);

        scalarConfig.setForceArrayTypes(false);
        arrayConfig.setForceArrayTypes(true);

        ElasticsearchSessionProperties scalarProperties = new ElasticsearchSessionProperties(scalarConfig);
        ElasticsearchSessionProperties arrayProperties  = new ElasticsearchSessionProperties(arrayConfig);

        SESSION        = new TestingConnectorSession(ImmutableList.of());
        SCALAR_SESSION = new TestingConnectorSession(scalarProperties.getSessionProperties());
        ARRAY_SESSION  = new TestingConnectorSession(arrayProperties.getSessionProperties());

        ElasticsearchClient ec = new ElasticsearchClient(new ElasticsearchConnectorId("test"), config);
        metadata = new ElasticsearchMetadata(new ElasticsearchConnectorId("test"), ec);
        client = ec.client();

        TestTable.TABLE_A_ARRAY.create(client);
        TestTable.TABLE_B_ARRAY.create(client);
        TestTable.TABLE_A_SCALAR.create(client);
        TestTable.TABLE_B_SCALAR.create(client);
        TestUtil.green(client);
    }

    @AfterClass
    public void after()
    {
        try {
            TestTable.TABLE_A_SCALAR.delete(client);
            TestTable.TABLE_B_SCALAR.delete(client);
            TestTable.TABLE_A_ARRAY.delete(client);
            TestTable.TABLE_B_ARRAY.delete(client);
        }
        finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @DataProvider(name = "provider")
    public Object[][] provider()
    {
        return new Object[][] { { TestTable.TABLE_A_ARRAY, ARRAY_SESSION }, { TestTable.TABLE_A_SCALAR, SCALAR_SESSION } };
    }

    @DataProvider(name = "provider2")
    public Object[][] provider2()
    {
        return new Object[][] {
                { TestTable.TABLE_A_ARRAY, TestTable.TABLE_B_ARRAY, TestTable.TABLE_A_SCALAR, TestTable.TABLE_B_SCALAR, SESSION } };
    }

    @Test(dataProvider = "provider", expectedExceptions = TableNotFoundException.class)
    public void testGetTableMetadataNoSuchTable(TestTable unused, TestingConnectorSession session) throws Exception
    {
        metadata.getTableMetadata(session, new ElasticsearchTableHandle(ID, "nonexistent", "x"));
    }

    @Test(dataProvider = "provider")
    public void testGetTableMetadata(TestTable table, TestingConnectorSession session) throws Exception
    {
        ConnectorTableMetadata metadata =
                TestElasticsearchMetadata.metadata.getTableMetadata(
                        session, new ElasticsearchTableHandle(ID, table.schema(), table.table()));

        assertThat(metadata.getTable().getSchemaName(), equalTo(table.schema()));
        assertThat(metadata.getTable().getTableName(), equalTo(table.table()));

        logger.info("metadata: " + metadata);
    }

    @Test(dataProvider = "provider")
    public void testGetTableHandle(TestTable table, TestingConnectorSession session) throws Exception
    {
        ElasticsearchTableHandle handle =
                (ElasticsearchTableHandle)
                        metadata.getTableHandle(session, new SchemaTableName(table.schema(), table.table()));

        assertThat(handle.getSchemaName(), equalTo(table.schema()));
        assertThat(handle.getTableName(), equalTo(table.table()));
    }

    @Test(dataProvider = "provider")
    public void testGetColumnMetadata(TestTable table, TestingConnectorSession session) throws Exception
    {
        for (ColumnMetadata column : table.columns()) {

            ColumnMetadata meta = metadata.getColumnMetadata(
                    session,
                    new ElasticsearchTableHandle(ID, table.schema(), table.table()),
                    new ElasticsearchColumnHandle(ID, column.getName(), column.getType()));

            assertThat(meta.getName(), equalTo(column.getName()));
            assertThat(meta.getType(), equalTo(column.getType()));
        }
    }

    @Test(dataProvider = "provider")
    public void testGetColumnHandles(TestTable table, TestingConnectorSession session) throws Exception
    {
        Map<String, ColumnHandle> handles =
                metadata.getColumnHandles(session, new ElasticsearchTableHandle(ID, table.schema(), table.table()));

        assertThat(handles.size(), equalTo(table.columns().size()));

        for (ColumnMetadata column : table.columns()) {

            assertThat(handles.containsKey(column.getName()), equalTo(true));
            ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) handles.get(column.getName());

            long count = table.columns().stream().filter(
                    c -> c.getType().equals(handle.getColumnType())
            ).count();

            assertThat("Expecting column: " + column, count, equalTo(1L));
        }
    }

    @Test(dataProvider = "provider")
    public void testListTableColumns(TestTable table, TestingConnectorSession session) throws Exception
    {
        Map<SchemaTableName, List<ColumnMetadata>> columns =
                metadata.listTableColumns(session, new SchemaTablePrefix(table.schema(), table.table()));
        assertThat(columns, notNullValue());

        for (Map.Entry<SchemaTableName, List<ColumnMetadata>> entry : columns.entrySet()) {
            logger.info("table: " + entry.getKey());
            assertThat(entry.getKey(), equalTo(table.schemaTableName()));

            for (ColumnMetadata column : entry.getValue()) {
                logger.info("\t" + column);
                assertThat(column, isIn(table.columns()));
            }
        }
    }

    @Test(dataProvider = "provider2")
    public void testListSchemaNames(TestTable table1, TestTable table2, TestTable table3, TestTable table4,
                                    TestingConnectorSession session) throws Exception
    {
        List<String> schemas = metadata.listSchemaNames(session);
        assertThat(schemas.size(), equalTo(4));

        for (String schema : schemas) {
            assertThat(schema, isOneOf(table1.schema(), table2.schema(), table3.schema(), table4.schema()));
        }
    }

    @Test(dataProvider = "provider2")
    public void testListTables(TestTable table1, TestTable table2, TestTable table3, TestTable table4,
                               TestingConnectorSession session) throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(session, null);
        assertThat(tables.size(), equalTo(4));

        for (SchemaTableName _table : tables) {
            assertThat(_table, isOneOf(table1.schemaTableName(), table2.schemaTableName(),
                    table3.schemaTableName(), table4.schemaTableName()));
        }
    }

    @Test(dataProvider = "provider")
    public void testListTablesForSchema(TestTable table, TestingConnectorSession session) throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(session, table.schema());
        assertThat(tables.size(), equalTo(1));

        SchemaTableName schemaTableName = tables.get(0);
        assertThat(schemaTableName.getSchemaName(), equalTo(table.schema()));
        assertThat(schemaTableName.getTableName(), equalTo(table.table()));
    }
}
