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

import com.facebook.presto.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;

/**
 * Elasticsearch table properties
 */
public class ElasticsearchTableProperties
{
    private static final String NUMBER_OF_SHARDS   = "shards";
    private static final String NUMBER_OF_REPLICAS = "replicas";

    private final List<PropertyMetadata<?>> tableProperties;

    public ElasticsearchTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerSessionProperty(
                        NUMBER_OF_SHARDS,
                        "Number of shards",
                        null,
                        false))
                .add(integerSessionProperty(
                        NUMBER_OF_REPLICAS,
                        "Number of replicas",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static OptionalInt getNumberOfShards(Map<String, Object> properties)
    {
        Integer value = (Integer) properties.get(NUMBER_OF_SHARDS);
        return (value != null) ? OptionalInt.of(value) : OptionalInt.empty();
    }

    public static OptionalInt getNumberOfReplicas(Map<String, Object> properties)
    {
        Integer value = (Integer) properties.get(NUMBER_OF_REPLICAS);
        return (value != null) ? OptionalInt.of(value) : OptionalInt.empty();
    }
}
