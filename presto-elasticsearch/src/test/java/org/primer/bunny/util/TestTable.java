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
package org.primer.bunny.util;

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.type.ArrayType;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static org.primer.bunny.types.GeoPointType.GEO_POINT;
import static org.primer.bunny.types.TypeUtils.toElasticsearchTypeName;

/**
 * Archetypical table definitions for testing.
 */
public class TestTable
{
    private final SchemaTableName name;
    private final List<ColumnMetadata> columns;

    private static final int NUMBER_OF_SHARDS   = 1;
    private static final int NUMBER_OF_REPLICAS = 0;

    private static final Settings SETTINGS = Settings.builder()
            .put("index.number_of_shards", NUMBER_OF_SHARDS)
            .put("index.number_of_replicas", NUMBER_OF_REPLICAS)
            .build();

    public static final ImmutableList<ColumnMetadata> TEST_COLUMNS_SCALAR = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("a", INTEGER))
            .add(new ColumnMetadata("b", BIGINT))
            .add(new ColumnMetadata("c", DOUBLE))
            .add(new ColumnMetadata("d", BOOLEAN))
            .add(new ColumnMetadata("e", VARCHAR))
            .add(new ColumnMetadata("f", GEO_POINT))
            .build();

    public static final ImmutableList<ColumnMetadata> TEST_COLUMNS_ARRAY = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("a", new ArrayType(INTEGER)))
            .add(new ColumnMetadata("b", new ArrayType(BIGINT)))
            .add(new ColumnMetadata("c", new ArrayType(DOUBLE)))
            .add(new ColumnMetadata("d", new ArrayType(BOOLEAN)))
            .add(new ColumnMetadata("e", new ArrayType(VARCHAR)))
            .add(new ColumnMetadata("f", new ArrayType(GEO_POINT)))
            .build();

    public static final TestTable TABLE_A_ARRAY  = new TestTable("aaa_array", "hecka", TEST_COLUMNS_ARRAY);
    public static final TestTable TABLE_B_ARRAY  = new TestTable("bbb_array", "boo", TEST_COLUMNS_ARRAY);
    public static final TestTable TABLE_A_SCALAR = new TestTable("aaa_scalar", "hecka", TEST_COLUMNS_SCALAR);
    public static final TestTable TABLE_B_SCALAR = new TestTable("bbb_scalar", "boo", TEST_COLUMNS_SCALAR);

    public TestTable(final String schema, final String table, final List<ColumnMetadata> columns)
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

    public List<ColumnMetadata> columns()
    {
        return columns;
    }

    public void create(Client client) throws IOException
    {
        IndicesExistsResponse exists = client.admin().indices().prepareExists(name.getSchemaName()).execute().actionGet();
        if (!exists.isExists()) {
            client.admin().indices().prepareCreate(name.getSchemaName()).setSettings(SETTINGS).execute().actionGet();
        }

        XContentBuilder json = XContentFactory.jsonBuilder();
        json.startObject();
        json.startObject(name.getTableName());
        json.startObject("properties");

        for (ColumnMetadata column : columns) {
            json.startObject(column.getName());
            json.field("type", toElasticsearchTypeName(column.getType()));
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

    public void index(Client client, int count)
    {
        BulkRequestBuilder bulk = client.prepareBulk();

        for (int i = 0; i < count; i++) {
            IndexRequestBuilder irb = client.prepareIndex(name.getSchemaName(), name.getTableName());

            // XXX Add data here to index.

            bulk.add(irb);
        }

        BulkResponse response = bulk.execute().actionGet();
        if (response.hasFailures()) {
            throw new RuntimeException("Failed to index sample documents: " + response.buildFailureMessage());
        }
    }
}
