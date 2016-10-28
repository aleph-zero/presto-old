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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.airlift.log.Logger;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;

import io.bunnysoft.presto.metadata.ShardMetadata;
import io.bunnysoft.presto.metadata.ShardMetadatas;
import io.bunnysoft.presto.type.TypeUtils;

import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_METADATA_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static io.bunnysoft.presto.type.DateTypeUtil.determineDateType;
import static io.bunnysoft.presto.util.Validators.checkType;

/**
 * Elasticsearch metadata.
 *
 * This class handles the translation between the Elasticsearch concepts of index/_type to
 * the relational concepts of schema/table.
 *
 */
public class ElasticsearchMetadata implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(ElasticsearchMetadata.class);

    private final Client client;
    private final String connectorId;

    @Inject
    public ElasticsearchMetadata(final ElasticsearchConnectorId connectorId, final ElasticsearchClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client      = requireNonNull(client, "client is null").client();
    }

    @Override
    public boolean schemaExists(final ConnectorSession session, final String schemaName)
    {
        IndicesExistsResponse response = client.admin().indices().prepareExists(schemaName).execute().actionGet();
        return response.isExists();
    }

    @Override
    public List<String> listSchemaNames(final ConnectorSession session)
    {
        IndicesStatsResponse response = client.admin().indices().prepareStats().execute().actionGet();
        return response.getIndices().keySet().stream().collect(toList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(final ConnectorSession session, final SchemaTableName table)
    {
        requireNonNull(table, "table is null");
        return new ElasticsearchTableHandle(connectorId, table.getSchemaName(), table.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = checkType(tableHandle, ElasticsearchTableHandle.class, "tableHandle");
        SchemaTableName schema = new SchemaTableName(handle.getSchemaName(), handle.getTableName());

        return getTableMetadata(session, schema);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ElasticsearchTableLayoutHandle layout = checkType(handle, ElasticsearchTableLayoutHandle.class, "handle");
        return new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(layout.getTable(), layout.getConstraint()));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(final ConnectorSession session,
                                                            final ConnectorTableHandle table,
                                                            final Constraint<ColumnHandle> constraint,
                                                            final Optional<Set<ColumnHandle>> desiredColumns)
    {
        ElasticsearchTableHandle handle = checkType(table, ElasticsearchTableHandle.class, "table");
        ConnectorTableLayout layout =
                getTableLayout(session, new ElasticsearchTableLayoutHandle(handle, constraint.getSummary()));

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public List<SchemaTableName> listTables(final ConnectorSession session, final String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        IndicesStatsRequestBuilder request = client.admin().indices().prepareStats();
        if (schemaNameOrNull != null ) {
            request.setIndices(schemaNameOrNull);
        }

        IndicesStatsResponse response = request.execute().actionGet();
        String[] indices = response.getIndices().keySet().toArray(new String[response.getIndices().keySet().size()]);
        GetMappingsResponse mappings = client.admin().indices().prepareGetMappings(indices).execute().actionGet();

        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> cursor : mappings.mappings()) {
            for (ObjectCursor<String> key : cursor.value.keys()) {
                builder.add(new SchemaTableName(cursor.key, key.value));
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = checkType(tableHandle, ElasticsearchTableHandle.class, "tableHandle");

        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();

        Map<SchemaTableName, List<ColumnMetadata>> columns =
                listTableColumns(session, new SchemaTablePrefix(handle.getSchemaName(), handle.getTableName()));

        List<ColumnMetadata> metadatas = columns.get(handle.getSchemaTableName());
        if (metadatas == null) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Missing column metadata for: " + handle);
        }

        for (ColumnMetadata metadata : metadatas) {

            ElasticsearchColumnMetadata _metadata =
                    checkType(metadata, ElasticsearchColumnMetadata.class, "Wrong metadata type");

            builder.put(_metadata.getName(),
                    new ElasticsearchColumnHandle(connectorId,
                            _metadata.getName(), _metadata.getType(), _metadata.docValues(), _metadata.stored()));
        }

        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, ElasticsearchTableHandle.class, "tableHandle");
        return checkType(columnHandle, ElasticsearchColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        for (SchemaTableName table : listTables(session, prefix)) {
            columns.put(table, getTableMetadata(session, table).getColumns());
        }

        return columns.build();
    }

    /** ***** utility methods ***** **/

    private ConnectorTableMetadata getTableMetadata(final ConnectorSession session, final SchemaTableName table)
    {
        GetMappingsResponse response;

        try {
            response = client.admin().indices().prepareGetMappings(
                    table.getSchemaName()).addTypes(table.getTableName()).execute().actionGet();
        }
        catch (IndexNotFoundException e) {
            throw new TableNotFoundException(table, e);
        }

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.mappings();
        if (mappings.size() == 0) {
            throw new TableNotFoundException(table);
        }

        if (mappings.size() != 1) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Multiple mappings found for: " + table.toString());
        }

        for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings.get(table.getSchemaName())) {
            try {
                List<ColumnMetadata> columns = mappings(cursor.value.getSourceAsMap(), ElasticsearchSessionProperties.getForceArrayTypes(session));
                return new ConnectorTableMetadata(table, columns, getTableProperties(table.getSchemaName()));
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Unable to read mappings", e);
            }
        }

        throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "No mappings found for: " + table.toString());
    }

    private Map<String, Object> getTableProperties(final String table)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        GetSettingsResponse response = client.admin().indices().prepareGetSettings(table).execute().actionGet();
        Settings settings = response.getIndexToSettings().get(table);

        properties.putAll(settings.getAsMap());
        return properties.build();
    }

    @SuppressWarnings("unchecked")
    private List<ColumnMetadata> mappings(Map<String, Object> mappings, boolean array)
    {
        if (!mappings.containsKey("properties")) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Unable to read mappings");
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        for (Map.Entry<String, Object> entry : ((Map<String, Object>) mappings.get("properties")).entrySet()) {

            Map<String, Object> properties = (Map<String, Object>) entry.getValue();
            String type = (String) properties.get("type");

            if (type == null || type.isEmpty()) {
                throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Unable to read mappings");
            }
            else if (type.equals("nested") || type.equals("object")) {

                List<ColumnMetadata> nested = mappings(properties, array);

                builder.add(new ElasticsearchColumnMetadata(
                        entry.getKey(),
                        array ? new ArrayType(row(nested)) : row(nested),
                        false,
                        false));
            }
            else {
                builder.add(columnMetadata(entry.getKey(), type, array, properties));
            }
        }

        return builder.build();
    }

    private ColumnMetadata columnMetadata(final String name, final String type, final boolean array,
                                          final Map<String, Object> properties)
    {
        final Type _type;

        if (type.equalsIgnoreCase("date")) {
            _type = determineDateType((String) properties.get("format"));
        }
        else {
            _type = array ? new ArrayType(TypeUtils.type(type)) : TypeUtils.type(type);
        }

        return new ElasticsearchColumnMetadata(
                name,
                _type,
                TypeUtils.docValues(type, properties),
                TypeUtils.isStored(properties));
    }

    private RowType row(final List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        for (ColumnMetadata column : columns) {
            types.add(column.getType());
            names.add(column.getName());
        }

        return new RowType(types.build(), Optional.of(names.build()));
    }

    private List<SchemaTableName> listTables(final ConnectorSession session, final SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    public ShardMetadatas getShardLocations(final String table)
    {
        IndicesShardStoresResponse response =
                client.admin().indices().prepareShardStores(table).setShardStatuses("green").execute().actionGet();

        if (response.getFailures().size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (IndicesShardStoresResponse.Failure failure : response.getFailures()) {
                sb.append(failure.toString());
            }
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Error reading shard metadata: " + sb.toString());
        }

        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> cursor = response.getStoreStatuses().get(table);
        if (cursor == null) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "No shard metadata for: " + table);
        }

        ImmutableMap.Builder<Integer, List<ShardMetadata>> builder = ImmutableMap.builder();

        for (IntObjectCursor<List<IndicesShardStoresResponse.StoreStatus>> shardStoresCursor : cursor) {
            ImmutableList.Builder<ShardMetadata> metadatas = ImmutableList.builder();
            for (IndicesShardStoresResponse.StoreStatus storeStatus : shardStoresCursor.value) {
                metadatas.add(new ShardMetadata(shardStoresCursor.key, storeStatus));
            }
            builder.put(shardStoresCursor.key, metadatas.build());
        }

        return new ShardMetadatas(table, builder.build());
    }

}
