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
package org.primer.bunny.metadata;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;

import org.primer.bunny.ElasticsearchColumnHandle;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;

import static org.primer.bunny.types.TypeUtils.toElasticsearchTypeName;


/**
 * Metadata utilities
 */
public class MetadataUtils
{

    public static boolean schemaExists(final Client client, final String schema)
    {
        IndicesExistsResponse response = client.admin().indices().prepareExists(schema).execute().actionGet();
        return response.isExists();
    }

    public static boolean tableExists(final Client client, final String schema, final String table)
    {
        try {
            TypesExistsResponse response =
                    client.admin().indices().prepareTypesExists(schema).setTypes(table).execute().actionGet();
            return response.isExists();
        }
        catch (IndexNotFoundException e) {
            return false;
        }
    }

    public static XContentBuilder createMapping(final String type,
                                                final List<ElasticsearchColumnHandle> columns) throws IOException
    {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        builder.startObject(type);
        builder.startObject("properties");

        for (ElasticsearchColumnHandle column : columns) {
            builder.startObject(column.getColumnName()).field("type", toElasticsearchTypeName(column.getColumnType()));
            builder.endObject();
        }

        builder.endObject();
        builder.endObject();
        builder.endObject();

        return builder;
    }

    public static Settings createIndexSettings(final OptionalInt shards, final OptionalInt replicas)
    {
        if (!shards.isPresent() && !replicas.isPresent()) {
            return Settings.EMPTY;
        }

        Settings.Builder builder = Settings.builder();

        if (shards.isPresent()) {
            builder.put("index.number_of_shards", shards.getAsInt());
        }

        if (replicas.isPresent()) {
            builder.put("index.number_of_replicas", replicas.getAsInt());
        }

        return builder.build();
    }
}
