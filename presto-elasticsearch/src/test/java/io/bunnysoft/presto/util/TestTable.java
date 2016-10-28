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
package io.bunnysoft.presto.util;

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.type.ArrayType;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.bunnysoft.presto.ElasticsearchColumnMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.bunnysoft.presto.type.GeoPointType.GEO_POINT;
import static io.bunnysoft.presto.type.TypeUtils.toElasticsearchTypeName;
import static io.bunnysoft.presto.util.TestUtilities.SETTINGS;

/**
 * Archetypical table definitions for testing.
 */
public class TestTable
{
    private final SchemaTableName name;
    private final List<ElasticsearchColumnMetadata> columns;

    public static final ImmutableList<ElasticsearchColumnMetadata> TEST_COLUMNS_SCALAR = ImmutableList.<ElasticsearchColumnMetadata>builder()
            .add(new ElasticsearchColumnMetadata("a", INTEGER, true, false))
            .add(new ElasticsearchColumnMetadata("b", BIGINT, true, false))
            .add(new ElasticsearchColumnMetadata("c", DOUBLE, true, false))
            .add(new ElasticsearchColumnMetadata("d", BOOLEAN, true, false))
            .add(new ElasticsearchColumnMetadata("e", VARCHAR, true, false))
            .add(new ElasticsearchColumnMetadata("f", VARBINARY, true, false))
            .add(new ElasticsearchColumnMetadata("g", GEO_POINT, true, false))
            .build();

    public static final ImmutableList<ElasticsearchColumnMetadata> TEST_COLUMNS_ARRAY = ImmutableList.<ElasticsearchColumnMetadata>builder()
            .add(new ElasticsearchColumnMetadata("a", new ArrayType(INTEGER), true, false))
            .add(new ElasticsearchColumnMetadata("b", new ArrayType(BIGINT), true, false))
            .add(new ElasticsearchColumnMetadata("c", new ArrayType(DOUBLE), true, false))
            .add(new ElasticsearchColumnMetadata("d", new ArrayType(BOOLEAN), true, false))
            .add(new ElasticsearchColumnMetadata("e", new ArrayType(VARCHAR), true, false))
            .add(new ElasticsearchColumnMetadata("f", new ArrayType(VARBINARY), true, false))
            .add(new ElasticsearchColumnMetadata("g", new ArrayType(GEO_POINT), true, false))
            .build();

    public static final TestTable TABLE_A_ARRAY  = new TestTable("aaa_array", "hecka", TEST_COLUMNS_ARRAY);
    public static final TestTable TABLE_B_ARRAY  = new TestTable("bbb_array", "boo", TEST_COLUMNS_ARRAY);
    public static final TestTable TABLE_A_SCALAR = new TestTable("aaa_scalar", "hecka", TEST_COLUMNS_SCALAR);
    public static final TestTable TABLE_B_SCALAR = new TestTable("bbb_scalar", "boo", TEST_COLUMNS_SCALAR);

    public TestTable(final String schema, final String table, final List<ElasticsearchColumnMetadata> columns)
    {
        this.name    = new SchemaTableName(schema, table);
        this.columns = columns;
    }

    public String schema()
    {
        return name.getSchemaName();
    }

    public String table()
    {
        return name.getTableName();
    }

    public SchemaTableName schemaTableName()
    {
        return name;
    }

    public List<ElasticsearchColumnMetadata> columns()
    {
        return columns;
    }

    public void create(Client client) throws IOException
    {
        create(client, new HashMap<>());
    }

    public void create(Client client, Map<String, String> properties) throws IOException
    {
        IndicesExistsResponse exists = client.admin().indices().prepareExists(name.getSchemaName()).execute().actionGet();
        if (!exists.isExists()) {
            client.admin().indices().prepareCreate(name.getSchemaName()).setSettings(SETTINGS).execute().actionGet();
        }

        XContentBuilder json = XContentFactory.jsonBuilder();
        json.startObject();
        json.startObject(name.getTableName());
        json.startObject("properties");

        for (ElasticsearchColumnMetadata column : columns) {
            json.startObject(column.getName());
            json.field("type", toElasticsearchTypeName(column.getType()));

            if (properties != null && properties.size() > 0) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    json.field(entry.getKey(), entry.getValue());
                }
            }
            json.endObject();
        }

        json.endObject();
        json.endObject();
        json.endObject();

        client.admin().indices().preparePutMapping(name.getSchemaName())
                .setType(name.getTableName())
                .setSource(json)
                .execute()
                .actionGet();
    }

    public void delete(Client client)
    {
        DeleteIndexResponse response = client.admin().indices().prepareDelete(schema()).execute().actionGet();
        if (!response.isAcknowledged()) {
            throw new RuntimeException("Failed receive acknowledgement for index deletion: [" + schema() + "]");
        }
    }
}
