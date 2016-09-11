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

import com.google.common.net.HostAndPort;

import com.facebook.presto.spi.PrestoException;

import io.airlift.log.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;

/**
 * Elasticsearch client.
 */
public class ElasticsearchClient implements Closeable
{
    private static final Logger logger = Logger.get(ElasticsearchClient.class);

    private final Client client;

    public ElasticsearchClient(ElasticsearchConnectorId connectorId, ElasticsearchConfig config)
    {
        Settings settings = Settings.builder().put("cluster.name", config.getClusterName()).build();

        // XXX - Security currently broken pending resolution of changing x-pack security architecture
        /*
        if (config.getSecurityEnabled()) {
            settings = Settings.builder()
                    .put(settings)
                    .put("shield.user", config.getSecurityUser() + ":" + config.getSecurityPassword())
                    .build();
        }
        */

        logger.info("Establishing connection: [%s]", settings.toDelimitedString(' '));

        client = TransportClient.builder().settings(settings).build();

        for (String hostAddress : config.getClusterHostAddresses()) {
            HostAndPort hostAndPort = HostAndPort.fromString(hostAddress).withDefaultPort(9300);
            try {
                ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName(hostAndPort.getHostText()), hostAndPort.getPort()));
            }
            catch (UnknownHostException e) {
                throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, "Invalid host:port pair: " + hostAddress, e);
            }
        }
    }

    public Client client()
    {
        return client;
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }
}
