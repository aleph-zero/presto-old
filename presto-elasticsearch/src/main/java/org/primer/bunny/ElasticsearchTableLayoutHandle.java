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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch table layout handle.
 */
public class ElasticsearchTableLayoutHandle implements ConnectorTableLayoutHandle
{
    private final ElasticsearchTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public ElasticsearchTableLayoutHandle(@JsonProperty("table") ElasticsearchTableHandle table,
                                          @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.table      = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "tuple is null");
    }

    @JsonProperty
    public ElasticsearchTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
