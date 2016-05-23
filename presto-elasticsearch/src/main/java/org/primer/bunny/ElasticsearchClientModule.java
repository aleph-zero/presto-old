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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.type.TypeManager;

import org.primer.bunny.system.NodeSystemTable;

import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Elasticsearch client module.
 */
public class ElasticsearchClientModule implements Module
{
    private final String      connectorId;
    private final TypeManager typeManager;

    public ElasticsearchClientModule(final String connectorId, final TypeManager typeManager)
    {
        this.connectorId = connectorId;
        this.typeManager = typeManager;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ElasticsearchConnectorId.class).toInstance(new ElasticsearchConnectorId(connectorId));
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).toInstance(typeManager);

        Multibinder<SystemTable> tableBinder = newSetBinder(binder, SystemTable.class);
        tableBinder.addBinding().to(NodeSystemTable.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ElasticsearchConfig.class);
    }

    @Singleton
    @Provides
    public static ElasticsearchClient createElasticsearchClient(ElasticsearchConnectorId connectorId,
                                                                ElasticsearchConfig config)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        return new ElasticsearchClient(connectorId, config);
    }
}
