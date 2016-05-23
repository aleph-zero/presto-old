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

import com.google.common.collect.ImmutableMap;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;

import io.airlift.tpch.TpchTable;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.primer.bunny.util.TestUtil.CLUSTER_NAME;
import static org.primer.bunny.util.TestUtil.CLUSTER_HOST;
import static org.primer.bunny.util.TestUtil.config;

/**
 * Distributed query runner for testing.
 */
public class ElasticsearchQueryRunner
{
    private ElasticsearchQueryRunner() { }

    public static DistributedQueryRunner createElasticsearchQueryRunner(Map<String, String> extraProperties, boolean loadTpch)
            throws Exception
    {
        DistributedQueryRunner runner = new DistributedQueryRunner(createSession("tpch"), 2, extraProperties);

        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch");

        ElasticsearchPlugin plugin = new ElasticsearchPlugin("elasticsearch");
        runner.installPlugin(plugin);

        runner.createCatalog("elasticsearch", "elasticsearch", ImmutableMap.<String, String>builder()
                .put("elasticsearch.cluster-name", CLUSTER_NAME)
                .put("elasticsearch.cluster-host-addresses", CLUSTER_HOST)
                .put("elasticsearch.fetch-size", "1000")
                .build());

        if (loadTpch) {
            copyTables(runner, "tpch", createSession());
        }

        return runner;
    }

    private static void copyTables(QueryRunner queryRunner, String catalog, Session session)
            throws Exception
    {
        String schema = TINY_SCHEMA_NAME;
        copyTpchTables(queryRunner, catalog, schema, session, TpchTable.getTables());
    }

    public static Session createSession()
    {
        return createSession("tpch");
    }

    private static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();

        ElasticsearchConfig config = config();

        ElasticsearchSessionProperties sessionProperties = new ElasticsearchSessionProperties(config);

        sessionPropertyManager.addConnectorSessionProperties(
                "elasticsearch",
                sessionProperties.getSessionProperties());

        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("elasticsearch")
                .setSchema(schema)
                .setSystemProperties(ImmutableMap.of())
                .build();
    }
}
