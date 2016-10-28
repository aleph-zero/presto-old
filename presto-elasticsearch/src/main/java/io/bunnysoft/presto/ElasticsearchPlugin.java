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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;

import com.google.common.collect.ImmutableList;

import io.bunnysoft.presto.types.GeoPointType;

/**
 * Elasticsearch plugin.
 */
public class ElasticsearchPlugin implements Plugin
{
    private final String name;

    public ElasticsearchPlugin()
    {
        this("elasticsearch");
    }

    public ElasticsearchPlugin(final String name)
    {
        this.name = name;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ElasticsearchConnectorFactory(name));
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return ImmutableList.of(GeoPointType.GEO_POINT);
    }

    @Override
    public Iterable<ParametricType> getParametricTypes()
    {
        return null;
    }
}
