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

import com.google.common.collect.ImmutableMap;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.testing.TestingConnectorSession;

import io.airlift.log.Logger;

import io.bunnysoft.presto.util.TestUtilities;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.elasticsearch.client.Client;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.bunnysoft.presto.util.TestUtilities.plugin;
import static io.bunnysoft.presto.util.TestUtilities.client;
import static io.bunnysoft.presto.util.TestUtilities.getTestingSessionProperties;
import static io.bunnysoft.presto.util.TestTable.TABLE_A_SCALAR;
import static io.bunnysoft.presto.util.TestTable.TABLE_B_SCALAR;

public class TestElasticsearchMetadata
{
    private static final Logger logger = Logger.get(TestElasticsearchMetadata.class);

    private static Client client;
    private static ConnectorMetadata metadata;

    private static final ConnectorSession SESSION = new TestingConnectorSession(getTestingSessionProperties());

    @BeforeClass
    public void before() throws Exception
    {
        metadata = initialize();
        client = client(TestUtilities.CLUSTER_NAME, TestUtilities.CLUSTER_HOST);
        TABLE_A_SCALAR.create(client);
        TABLE_B_SCALAR.create(client);
    }

    @AfterClass
    public void after() throws Exception
    {
        TABLE_A_SCALAR.delete(client);
        TABLE_B_SCALAR.delete(client);
    }

    @Test
    public void testSchemaExists()
    {
        assertThat(metadata.schemaExists(SESSION, TABLE_A_SCALAR.schema()), is(true));
    }

    @Test
    public void testSchemaDoesNotExist()
    {
        assertThat(metadata.schemaExists(SESSION, "xxx"), is(false));
    }

    @Test
    public void testListSchemaNames()
    {
        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas.size(), equalTo(2));
        assertThat(schemas, hasItems(TABLE_A_SCALAR.schema(), TABLE_B_SCALAR.schema()));
    }

    @Test
    public void testListTables()
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, TABLE_A_SCALAR.schema());

        assertThat(tables.size(), equalTo(1));
        assertThat(tables.get(0).getSchemaName(), equalTo(TABLE_A_SCALAR.schema()));
        assertThat(tables.get(0).getTableName(), equalTo(TABLE_A_SCALAR.table()));
    }

    @Test
    public void testListTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> columns =
                metadata.listTableColumns(SESSION, new SchemaTablePrefix(TABLE_A_SCALAR.schema(), TABLE_A_SCALAR.table()));
        assertThat(columns, notNullValue());

        for (Map.Entry<SchemaTableName, List<ColumnMetadata>> entry : columns.entrySet()) {
            logger.info("table: " + entry.getKey());
            assertThat(entry.getKey(), equalTo(TABLE_A_SCALAR.schemaTableName()));

            for (ColumnMetadata column : entry.getValue()) {
                logger.info("\t" + column);
                assertThat((ElasticsearchColumnMetadata) column, isIn(TABLE_A_SCALAR.columns()));
            }
        }
    }

    @Test
    public void testGetColumnHandles()
    {
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                SESSION, new ElasticsearchTableHandle("test-id", TABLE_A_SCALAR.schema(), TABLE_A_SCALAR.table()));

        assertThat(handles.size(), equalTo(TABLE_A_SCALAR.columns().size()));

        for (ColumnMetadata column : TABLE_A_SCALAR.columns()) {

            assertThat(handles.containsKey(column.getName()), equalTo(true));
            ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) handles.get(column.getName());

            long count = TABLE_A_SCALAR.columns().stream().filter(
                    c -> c.getType().equals(handle.getColumnType())
            ).count();

            assertThat("Expecting column: " + column, count, equalTo(1L));
        }
    }

    @Test
    public void testGetColumnMetadata()
    {
        for (ColumnMetadata column : TABLE_A_SCALAR.columns()) {

            ColumnMetadata metadata = TestElasticsearchMetadata.metadata.getColumnMetadata(
                    SESSION,
                    new ElasticsearchTableHandle("test-id", TABLE_A_SCALAR.schema(), TABLE_A_SCALAR.table()),
                    new ElasticsearchColumnHandle("test-id", column.getName(), column.getType(), true, false));

            assertThat(metadata, instanceOf(ElasticsearchColumnMetadata.class));
            ElasticsearchColumnMetadata _metadata = (ElasticsearchColumnMetadata) metadata;

            assertThat(_metadata.getName(), equalTo(column.getName()));
            assertThat(_metadata.getType(), equalTo(column.getType()));
            assertThat(_metadata.docValues(), equalTo(true));
            assertThat(_metadata.stored(), equalTo(false));
        }
    }

    private ConnectorMetadata initialize()
    {
        ElasticsearchPlugin plugin = plugin(ElasticsearchPlugin.class);

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ElasticsearchConnectorFactory.class);

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("elasticsearch.cluster-name", TestUtilities.CLUSTER_NAME)
                .put("elasticsearch.cluster-host-addresses", TestUtilities.CLUSTER_HOST)
                .put("elasticsearch.fetch-size", TestUtilities.FETCH_SIZE)
                .put("elasticsearch.scroll-timeout", TestUtilities.SCROLL_TIMEOUT)
                .build();

        Connector connector = factory.create("test", config, new TestingConnectorContext());

        return connector.getMetadata(new ConnectorTransactionHandle() { });
    }
}
