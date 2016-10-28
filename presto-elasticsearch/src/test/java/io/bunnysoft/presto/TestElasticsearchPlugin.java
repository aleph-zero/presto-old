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

import com.google.common.collect.ImmutableMap;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import io.bunnysoft.presto.util.TestUtil;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;

/**
 * Elasticsearch plugin tests.
 */
public class TestElasticsearchPlugin
{
    @Test
    public void testPlugin() throws Exception
    {
        ElasticsearchPlugin plugin = loadPlugin(ElasticsearchPlugin.class);

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ElasticsearchConnectorFactory.class);

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("elasticsearch.cluster-name", TestUtil.CLUSTER_NAME)
                .put("elasticsearch.cluster-host-addresses", TestUtil.CLUSTER_HOST)
                .put("elasticsearch.fetch-size", "1000")
                .build();

        factory.create("elasticsearch-test", config, new ConnectorContext() {});
    }

    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}
