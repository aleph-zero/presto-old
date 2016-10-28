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
package io.bunnysoft.presto.type;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;

import java.util.Map;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.bunnysoft.presto.type.GeoPointType.GEO_POINT;
import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_UNSUPPORTED_TYPE;

/**
 * Type utilities for conversion between Elasticsearch and Presto types.
 */
public final class TypeUtils
{

    /**
     * Given the name of an Elasticsearch type, returns the Presto type.
     */
    public static Type type(final String type)
    {
        switch (type) {
            case "byte":
            case "short":
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "float":
            case "double":
                return DOUBLE;
            case "string":
            case "keyword":
            case "text":
                return VARCHAR;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            case "geo_point":
                return GEO_POINT;
            case "geo_shape":
            case "ip":
            case "murmur3":
            case "attachment":
            case "token_count":
            case "completion":
            default:
                throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported data type: " + type);
        }
    }

    /**
     * Given a Presto type, returns the name of the Elasticsearch type.
     * If the type is an array, this method will return the name of the element type.
     */
    public static String toElasticsearchTypeName(final Type type)
    {
        Type t = type;
        if (type instanceof ArrayType) {
            t = ((ArrayType) type).getElementType();
        }

        if (t instanceof CustomTypeInfo) {
            return ((CustomTypeInfo) t).getElasticsearchTypeName();
        }
        else {
            return name(t);
        }
    }

    /**
     * Given a Presto type, returns the name of the Elasticsearch type.
     */
    private static String name(final Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case "bigint":
                return "long";
            case "byte":
            case "integer":
                return "integer";
            case "boolean":
                return "boolean";
            case "double":
                return "double";
            case "varchar":
                return "keyword";
            case "geo_point":
                return "geo_point";
            case "varbinary":
                return "binary";
            default:
                throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported data type: " + type);
        }
    }

    /**
     * Given a column's metadata properties, determine whether the 'doc_values'
     * attribute is present and enabled.
     */
    public static boolean docValues(final String type, final Map<String, Object> properties)
    {
        if (type.equalsIgnoreCase("text") || type.equalsIgnoreCase("string")) {
            return false;
        }

        Object value = properties.get("doc_values");
        if (value == null) {
            return true;
        }

        String s = value.toString();
        return s.isEmpty() || s.equalsIgnoreCase("true");
    }

    /**
     * Given a column's metadata properties, determine whether the 'store'
     * attribute is present and enabled.
     */
    public static boolean isStored(final Map<String, Object> properties)
    {
        String stored = (String) properties.get("store");
        if (stored == null || stored.isEmpty()) {
            return false;
        }

        return stored.equalsIgnoreCase("true");
    }
}
