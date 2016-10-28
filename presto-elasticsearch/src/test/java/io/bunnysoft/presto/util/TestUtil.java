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
package io.bunnysoft.presto.util;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

import io.bunnysoft.presto.ElasticsearchConfig;

/**
 * Test utilities.
 */
public class TestUtil
{
    public static final String CLUSTER_NAME = "test-cluster";
    public static final String CLUSTER_HOST = "127.0.0.1:9300";

    public static ElasticsearchConfig config()
    {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setClusterName(CLUSTER_NAME);
        config.setClusterHostAddresses(CLUSTER_HOST);
        return config;
    }

    public static void green(Client client)
    {
        ClusterHealthResponse response = client.admin().cluster().health(
                Requests.clusterHealthRequest()
                        .timeout(TimeValue.timeValueSeconds(15))
                        .waitForGreenStatus()
                        .waitForEvents(Priority.LANGUID)).actionGet();

        if (response.isTimedOut()) {
            throw new RuntimeException("Timed out waiting for cluster to go green");
        }

        if (!response.getStatus().equals(ClusterHealthStatus.GREEN)) {
            throw new RuntimeException("Cluster status not green: [" + response.getStatus() + "]");
        }
    }
}
