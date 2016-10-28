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

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;

import io.airlift.units.Duration;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.BigintType.BIGINT;

/**
 * Configurable session properties.
 */
public class ElasticsearchSessionProperties
{
    private static final String FORCE_ARRAY_TYPES = "force_array_types";
    private static final String FETCH_SIZE        = "fetch_size";
    private static final String SHARD_POLICY      = "shard_policy";
    private static final String SCROLL_TIMEOUT    = "scroll_timeout";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ElasticsearchSessionProperties(final ElasticsearchConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        FORCE_ARRAY_TYPES,
                        "Force all fields to be handled as arrays",
                        config.getForceArrayTypes(),
                        false),
                integerSessionProperty(
                        FETCH_SIZE,
                        "Number of records to fetch on each request",
                        config.getFetchSize(),
                        false
                ),
                stringSessionProperty(
                        SHARD_POLICY,
                        "Type of shard on which queries are allowed to execute",
                        config.getShardPolicy(),
                        false
                ),
                durationSessionProperty(
                        SCROLL_TIMEOUT,
                        "Length of time to keep scroll context alive",
                        config.getScrollTimeout(),
                        false
                )
        );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean getForceArrayTypes(ConnectorSession session)
    {
        return session.getProperty(FORCE_ARRAY_TYPES, Boolean.class);
    }

    public static int getFetchSize(ConnectorSession session)
    {
        return session.getProperty(FETCH_SIZE, Integer.class);
    }

    public static String getShardPolicy(ConnectorSession session)
    {
        return session.getProperty(SHARD_POLICY, String.class);
    }

    public static long getScrollTimeout(ConnectorSession session)
    {
        Duration duration = session.getProperty(SCROLL_TIMEOUT, Duration.class);
        return duration.toMillis();
    }

    private static PropertyMetadata<Duration> durationSessionProperty(String name,
                                                                      String description,
                                                                      Duration defaultValue,
                                                                      boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                BIGINT,
                Duration.class,
                defaultValue,
                hidden,
                value -> Duration.valueOf((String) value),
                Duration::toMillis);
    }
}
