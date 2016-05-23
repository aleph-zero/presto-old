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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;

import java.util.List;
import javax.inject.Inject;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;

/**
 * Elasticsearch session properties
 */
public class ElasticsearchSessionProperties
{
    private static final String FORCE_ARRAY_TYPES = "force_array_types";
    private static final String FETCH_SIZE        = "fetch_size";

    public static final boolean DEFAULT_FORCE_ARRAY_TYPES = false;
    public static final int     DEFAULT_FETCH_SIZE        = 1000;

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
                )
        );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean getForceArrayTypes(ConnectorSession session)
    {
        try {
            return session.getProperty(FORCE_ARRAY_TYPES, Boolean.class);
        }
        catch (PrestoException e) {
            return DEFAULT_FORCE_ARRAY_TYPES;
        }
    }

    public static int getFetchSize(ConnectorSession session)
    {
        try {
            return session.getProperty(FETCH_SIZE, Integer.class);
        }
        catch (PrestoException e) {
            return DEFAULT_FETCH_SIZE;
        }
    }
}
