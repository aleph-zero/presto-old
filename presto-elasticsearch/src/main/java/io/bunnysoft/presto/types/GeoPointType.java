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
package io.bunnysoft.presto.types;

import com.google.common.collect.ImmutableList;

import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.RowType;

import java.util.Optional;

/**
 * Elasticsearch geo_point type.
 */
public class GeoPointType extends RowType implements CustomTypeInfo
{
    public static final GeoPointType GEO_POINT = new GeoPointType();

    public GeoPointType()
    {
        super(ImmutableList.<Type>builder().add(DoubleType.DOUBLE).add(DoubleType.DOUBLE).build(),
                Optional.of(ImmutableList.<String>builder().add("lat").add("lon").build()));
    }

    @Override
    public String getDisplayName()
    {
        return "geo_point(lat " + DoubleType.DOUBLE.getDisplayName() + ", lon " + DoubleType.DOUBLE.getDisplayName() + ")";
    }

    @Override
    public String getElasticsearchTypeName()
    {
        return "geo_point";
    }
}
