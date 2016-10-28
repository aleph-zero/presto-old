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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch module.
 */
public class ElasticsearchModule implements Module
{
    private final String connectorId;

    public ElasticsearchModule(final String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ElasticsearchConnectorId.class).toInstance(new ElasticsearchConnectorId(connectorId));
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ElasticsearchConfig.class);
    }

    @Singleton
    @Provides
    public static ElasticsearchClient createElasticsearchClient(final ElasticsearchConnectorId connectorId,
                                                                final ElasticsearchConfig config)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        return new ElasticsearchClient(connectorId, config);
    }
}

