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

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch output table handle.
 */
public class ElasticsearchOutputTableHandle implements ConnectorOutputTableHandle
{
    private final String                          connectorId;
    private final SchemaTableName                 schemaTableName;
    private final List<ElasticsearchColumnHandle> columns;

    @JsonCreator
    public ElasticsearchOutputTableHandle(@JsonProperty("connectorId") String connectorId,
                                          @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                                          @JsonProperty("columns") List<ElasticsearchColumnHandle> columns)
    {
        this.connectorId     = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns         = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<ElasticsearchColumnHandle> getColumns()
    {
        return columns;
    }
}
