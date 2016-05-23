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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch column handle.
 */
public class ElasticsearchColumnHandle implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final Type   columnType;

    @JsonCreator
    public ElasticsearchColumnHandle(@JsonProperty("connectorId") String connectorId,
                                     @JsonProperty("columnName") String columnName,
                                     @JsonProperty("columnType") Type columnType)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName  = requireNonNull(columnName, "columnName is null");
        this.columnType  = requireNonNull(columnType, "columnType is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + columnName + ":" + columnType;
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
        ElasticsearchColumnHandle that = (ElasticsearchColumnHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName, columnType);
    }
}
