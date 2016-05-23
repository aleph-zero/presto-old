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
package org.primer.bunny;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;

import org.primer.bunny.metadata.ShardMetadata;
import org.primer.bunny.metadata.ShardMetadatas;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static org.primer.bunny.util.Types.checkType;
import static org.primer.bunny.ElasticsearchErrorCode.ELASTICSEARCH_METADATA_ERROR;
import static org.primer.bunny.types.TypeUtils.type;
import static org.primer.bunny.ElasticsearchSessionProperties.getForceArrayTypes;
import static org.primer.bunny.util.DateUtil.determineDateType;
import static org.primer.bunny.ElasticsearchTableProperties.getNumberOfShards;
import static org.primer.bunny.ElasticsearchTableProperties.getNumberOfReplicas;
import static org.primer.bunny.metadata.MetadataUtils.schemaExists;
import static org.primer.bunny.metadata.MetadataUtils.tableExists;
import static org.primer.bunny.metadata.MetadataUtils.createMapping;
import static org.primer.bunny.metadata.MetadataUtils.createIndexSettings;

/**
 * Elasticsearch metadata
 */
public class ElasticsearchMetadata implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(ElasticsearchMetadata.class);

    private final String connectorId;
    private final Client client;

    public interface TableProperties
    {
        String NUMBER_OF_SHARDS   = "number_of_shards";
        String NUMBER_OF_REPLICAS = "number_of_replicas";
        String UUID               = "uuid";
    }

    @Inject
    public ElasticsearchMetadata(ElasticsearchConnectorId connectorId, ElasticsearchClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null").client();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout)
    {
        createTable(session, tableMetadata);

        List<ElasticsearchColumnHandle> columns = buildColumnHandles(tableMetadata);

        return new ElasticsearchOutputTableHandle(
                connectorId,
                tableMetadata.getTable(),
                columns.stream().collect(Collectors.toList()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schema = tableMetadata.getTable();

        if (tableExists(client, schema.getSchemaName(), schema.getTableName())) {
            throw new PrestoException(ALREADY_EXISTS, format("Table [%s.%s] already exists", schema.getSchemaName(), schema.getTableName()));
        }

        OptionalInt shards   = getNumberOfShards(tableMetadata.getProperties());
        OptionalInt replicas = getNumberOfReplicas(tableMetadata.getProperties());

        if (schemaExists(session, schema.getSchemaName())) {
            if (shards.isPresent()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot set 'shards' property on existing schema");
            }
            if (replicas.isPresent()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot set 'replicas' property on existing schema");
            }
        }

        List<ElasticsearchColumnHandle> columns = buildColumnHandles(tableMetadata);

        XContentBuilder mapping;
        try {
            mapping = createMapping(schema.getTableName(), columns);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR,
                    format("Failed to create mapping for: [%s.%s]", schema.getSchemaName(), schema.getTableName()), e);
        }

        String _mapping = null;
        try {
            _mapping = mapping.string();
            logger.info("Creating table: [%s.%s] [%s]", schema.getSchemaName(), schema.getTableName(), _mapping);

            client.admin().indices().prepareCreate(schema.getSchemaName())
                    .addMapping(schema.getTableName(), mapping)
                    .setSettings(createIndexSettings(shards, replicas))
                    .execute().get();
        }
        catch (InterruptedException | ExecutionException | IOException e) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR,
                    format("Failed to create table: [%s.%s] [%s]", schema.getSchemaName(), schema.getTableName(),
                            _mapping == null ? "<invalid mapping>" : _mapping), e);
        }
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {

    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        IndicesStatsResponse response = client.admin().indices().prepareStats().execute().actionGet();
        return response.getIndices().keySet().stream().collect(toList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        requireNonNull(table, "table is null");

        try {
            ConnectorTableMetadata metadata = getTableMetadata(session, table);

            return new ElasticsearchTableHandle(connectorId,
                    table.getSchemaName(),
                    table.getTableName(),
                    (Integer) metadata.getProperties().get(TableProperties.NUMBER_OF_SHARDS),
                    (Integer) metadata.getProperties().get(TableProperties.NUMBER_OF_REPLICAS),
                    (String) metadata.getProperties().get(TableProperties.UUID));
        }
        catch (TableNotFoundException e) {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ElasticsearchTableHandle handle = checkType(table, ElasticsearchTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ElasticsearchTableLayoutHandle layout = checkType(handle, ElasticsearchTableLayoutHandle.class, "handle");
        return new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(layout.getTable(), layout.getConstraint()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = checkType(table, ElasticsearchTableHandle.class, "table");
        return getTableMetadata(session, handle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
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
            builder.put(metadata.getName(),
                    new ElasticsearchColumnHandle(connectorId, metadata.getName(), metadata.getType()));
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

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
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
                List<ColumnMetadata> columns = mappings(cursor.value.getSourceAsMap(), getForceArrayTypes(session));
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

        try {
            properties.put(TableProperties.NUMBER_OF_SHARDS,
                    Integer.valueOf(settings.get("index." + TableProperties.NUMBER_OF_SHARDS)));
        }
        catch (NumberFormatException e) {
            logger.error(e, "Unable to read " + TableProperties.NUMBER_OF_SHARDS + " for: " + table);
            properties.put(TableProperties.NUMBER_OF_SHARDS, 1);
        }

        try {
            properties.put(TableProperties.NUMBER_OF_REPLICAS,
                    Integer.valueOf(settings.get("index." + TableProperties.NUMBER_OF_REPLICAS)));
        }
        catch (NumberFormatException e) {
            logger.error(e, "Unable to read " + TableProperties.NUMBER_OF_REPLICAS + " for: " + table);
            properties.put(TableProperties.NUMBER_OF_REPLICAS, 0);
        }

        properties.put(TableProperties.UUID, settings.get("index." + TableProperties.UUID));

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
                builder.add(new ColumnMetadata(entry.getKey(), array ? new ArrayType(row(nested)) : row(nested)));
            }
            else {
                if (type.equals("date")) {
                    Type t = determineDateType((String) properties.get("format"));
                    builder.add(new ColumnMetadata(entry.getKey(), array ? new ArrayType(t) : t));
                }
                else {
                    builder.add(new ColumnMetadata(entry.getKey(), array ? new ArrayType(type(type)) : type(type)));
                }
            }
        }

        return builder.build();
    }

    private RowType row(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        for (ColumnMetadata column : columns) {
            types.add(column.getType());
            names.add(column.getName());
        }

        return new RowType(types.build(), Optional.of(names.build()));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    public ShardMetadatas getShardLocations(String table)
    {
        IndicesShardStoresResponse response =
                client.admin().indices().prepareShardStores(table).setShardStatuses("green").execute().actionGet();

        if (response.getFailures().size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (IndicesShardStoresResponse.Failure failure : response.getFailures()) {
                sb.append(failure.toString());
            }
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "Error reading shard store information: " + sb.toString());
        }

        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> cursor = response.getStoreStatuses().get(table);
        if (cursor == null) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, "No shard store information for: " + table);
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

    private List<ElasticsearchColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new ElasticsearchColumnHandle(connectorId, m.getName(), m.getType()))
                .collect(Collectors.toList());
    }
}
