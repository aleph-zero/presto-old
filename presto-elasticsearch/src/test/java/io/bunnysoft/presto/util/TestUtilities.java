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
package io.bunnysoft.presto.util;

import com.google.common.net.HostAndPort;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.session.PropertyMetadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import io.bunnysoft.presto.ElasticsearchConfig;
import io.bunnysoft.presto.ElasticsearchSessionProperties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;


public class TestUtilities
{
    public static final String CLUSTER_NAME   = "elasticsearch-test-cluster";
    public static final String CLUSTER_HOST   = "127.0.0.1:9300";
    public static final String FETCH_SIZE     = "1000";
    public static final String SCROLL_TIMEOUT = "1000ms";

    public static final Settings SETTINGS = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

    public static List<PropertyMetadata<?>> getTestingSessionProperties()
    {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setClusterHostAddresses(CLUSTER_HOST);
        config.setClusterName(CLUSTER_NAME);
        ElasticsearchSessionProperties sessionProperties = new ElasticsearchSessionProperties(config);
        return sessionProperties.getSessionProperties();
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin> T plugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }

    public static Client client(final String cluster, final String host) throws UnknownHostException
    {
        Settings settings = Settings.builder().put("cluster.name", cluster).build();

        Client client = new PreBuiltTransportClient(settings);

        HostAndPort hostAndPort = HostAndPort.fromString(host).withDefaultPort(9300);
        ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(
            InetAddress.getByName(hostAndPort.getHostText()), hostAndPort.getPort()));

        return client;
    }

    public static class TestDataLoader
    {
        private static final String INDEX_NAME     = "movies";
        private static final String TYPE_NAME      = "movies";
        private static final String DATA_FILE      = "/data/movies-data.json";
        private static final String MAPPING_FILE   = "/data/movies-mapping.json";
        private static final String MAPPING_FILE_2 = "/data/movies-mapping-no-date.json";

        private static final boolean SKIP_DATE_TYPES = true;

        public static void main(final String[] args) throws Exception
        {
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch-test-cluster")
                    .build();

            Client client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

            int n = load(client);
            System.out.println("Loaded " + n + " docs into " + INDEX_NAME + "." + TYPE_NAME);
        }

        private static int load(Client client) throws Exception
        {
            Settings indexSettings = Settings.builder()
                    .put("number_of_shards", 3)
                    .put("number_of_replicas", 0)
                    .build();

            if (client.admin().indices().prepareExists(INDEX_NAME).execute().actionGet().isExists()) {
                client.admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
            }

            client.admin().indices()
                    .prepareCreate(INDEX_NAME)
                    .setSettings(indexSettings)
                    .execute()
                    .actionGet();

            createMappingFromFile(client, INDEX_NAME, TYPE_NAME, SKIP_DATE_TYPES ? MAPPING_FILE_2 : MAPPING_FILE);

            URL url = TestDataLoader.class.getResource(DATA_FILE);
            JsonParser parser = new JsonFactory().createParser(url);

            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();

            ObjectNode currentNode;
            BulkRequestBuilder bulk = client.prepareBulk();
            int count = 0;

            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();

                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText();

                try {
                    IndexRequestBuilder irb = client.prepareIndex().setIndex(INDEX_NAME).setType(TYPE_NAME);
                    XContentBuilder content = XContentFactory.jsonBuilder();

                    content.startObject();
                    content.field("year", year);
                    content.field("title", title);
                    content.startObject("info");

                    JsonNode info = currentNode.path("info");
                    content.field("rating", info.path("rating").asDouble());
                    if (!SKIP_DATE_TYPES) {
                        content.field("release_date", info.path("release_date").textValue());
                    }

                    content.startArray("directors");
                    JsonNode directors = info.path("directors");
                    for (JsonNode director : directors) {
                        content.value(director.textValue());
                    }
                    content.endArray();

                    content.startArray("genres");
                    JsonNode genres = info.path("genres");
                    for (JsonNode genre : genres) {
                        content.value(genre.textValue());
                    }
                    content.endArray();

                    content.startArray("actors");
                    JsonNode actors = info.path("actors");
                    for (JsonNode actor : actors) {
                        content.value(actor.textValue());
                    }
                    content.endArray();

                    content.field("image_url", info.path("image_url").textValue());
                    content.field("plot", info.path("plot").textValue());
                    content.field("running_time_secs", info.path("running_time_secs").intValue());

                    content.endObject();
                    content.endObject();

                    irb.setSource(content);
                    bulk.add(irb);
                    count++;

                    if ((count % 1000) == 0) {
                        BulkResponse response = bulk.execute().actionGet();
                        if (response.hasFailures()) {
                            throw new RuntimeException("Failed to load all movies: " + response.buildFailureMessage());
                        }
                        System.out.println("Loaded " + count + " movies...");
                        bulk = client.prepareBulk();
                    }
                }
                catch (Exception e) {
                    System.err.println("Unable to add movie: " + year + " " + title);
                    System.err.println(e.getMessage());
                    break;
                }
            }
            parser.close();

            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                throw new RuntimeException("Failed to load all movies: " + response.buildFailureMessage());
            }
            return count;
        }

        private static void createMappingFromFile(Client client, String schema, String table, String file) throws IOException
        {
            StringBuilder sb = new StringBuilder();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(TestDataLoader.class.getResourceAsStream(file)))) {

                for (String line; (line = reader.readLine()) != null; ) {
                    sb.append(line);
                }
            }

            String _mapping = sb.toString().replaceAll("<TABLE_NAME>", table);
            PutMappingRequestBuilder put = client.admin().indices().preparePutMapping(schema);
            put.setType(table);
            put.setSource(_mapping);
            put.execute().actionGet();
        }
    }
}
