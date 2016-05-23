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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch table handle.
 */
public class ElasticsearchTableHandle implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String tableUUID;
    private final int    numberOfShards;
    private final int    numberOfReplicas;

    @JsonCreator
    public ElasticsearchTableHandle(@JsonProperty("connectorId") String connectorId,
                                    @JsonProperty("schemaName") String schemaName,
                                    @JsonProperty("tableName") String tableName,
                                    @JsonProperty("numberOfShards") int numberOfShards,
                                    @JsonProperty("numberOfReplicas") int numberOfReplicas,
                                    @JsonProperty("tableUUID") String tableUUID)
    {
        this.connectorId      = requireNonNull(connectorId, "connectorId is null");
        this.schemaName       = requireNonNull(schemaName, "schemaName is null");
        this.tableName        = requireNonNull(tableName, "tableName is null");
        this.tableUUID        = requireNonNull(tableUUID, "tableUUID is null");
        this.numberOfShards   = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
    }

    ElasticsearchTableHandle(String connectorId, String schemaName, String tableName)
    {
        this.connectorId      = requireNonNull(connectorId, "connectorId is null");
        this.schemaName       = requireNonNull(schemaName, "schemaName is null");
        this.tableName        = requireNonNull(tableName, "tableName is null");
        this.tableUUID        = "<unknown>";
        this.numberOfShards   = 1;
        this.numberOfReplicas = 0;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public int getNumberOfShards()
    {
        return numberOfShards;
    }

    @JsonProperty
    public int getNumberOfReplicas()
    {
        return numberOfReplicas;
    }

    @JsonProperty
    public String getTableUUID()
    {
        return tableUUID;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ElasticsearchTableHandle that = (ElasticsearchTableHandle) o;

        if (!connectorId.equals(that.connectorId)) {
            return false;
        }
        if (!schemaName.equals(that.schemaName)) {
            return false;
        }
        return tableName.equals(that.tableName);

    }

    @Override
    public int hashCode()
    {
        int result = connectorId.hashCode();
        result = 31 * result + schemaName.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + ":" + tableName;
    }
}
