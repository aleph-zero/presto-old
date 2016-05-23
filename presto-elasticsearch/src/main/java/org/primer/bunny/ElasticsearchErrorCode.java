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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

/**
 * Elasticsearch error codes.
 */
public enum ElasticsearchErrorCode implements ErrorCodeSupplier
{
    ELASTICSEARCH_ERROR(0, EXTERNAL),                 // Generic error
    ELASTICSEARCH_CONNECTION_ERROR(1, EXTERNAL),      // Connection error
    ELASTICSEARCH_METADATA_ERROR(2, EXTERNAL),        // Error locating or reading metadata
    ELASTICSEARCH_UNSUPPORTED_TYPE(3, EXTERNAL),      // Unsupported data type
    ELASTICSEARCH_SHARD_ERROR(4, EXTERNAL),           // Error locating or reading shard
    ELASTICSEARCH_SEARCH_ERROR(5, EXTERNAL),          // Error while searching
    ELASTICSEARCH_DATE_PARSE_ERROR(6, EXTERNAL);      // Unable to parse date format string

    private final ErrorCode errorCode;

    ElasticsearchErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0900_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
