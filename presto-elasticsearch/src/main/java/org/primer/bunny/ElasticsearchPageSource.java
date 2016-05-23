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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;

import io.airlift.log.Logger;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import org.primer.bunny.scan.Scanner;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static com.google.common.base.Preconditions.checkState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static org.primer.bunny.ElasticsearchErrorCode.ELASTICSEARCH_UNSUPPORTED_TYPE;


/**
 * Elasticsearch page source.
 */
public class ElasticsearchPageSource implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ElasticsearchPageSource.class);

    private final Scanner      scanner;
    private final List<Type>   types;
    private final List<String> fields;

    private boolean finished = false;

    public ElasticsearchPageSource(final ElasticsearchClient client,
                                   final ElasticsearchSplit split,
                                   final List<ElasticsearchColumnHandle> columns)
    {
        this.types  = columns.stream().map(ElasticsearchColumnHandle::getColumnType).collect(Collectors.toList());
        this.fields = columns.stream().map(ElasticsearchColumnHandle::getColumnName).collect(Collectors.toList());

        this.scanner = new Scanner(client.client(),
                split.getSchema(),
                split.getTable(),
                split.getShardId(),
                split.getNumShards(),
                split.getFetchSize(),
                split.getPredicate(),
                columns);

        scanner.scan(); // initiate scan asynchronously
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        SearchResponse response = scanner.response();
        if (response == null) {
            finished = true;
            return null;
        }

        scanner.scan(response.getScrollId());
        return page(response);
    }

    private Page page(final SearchResponse response)
    {
        PageBuilder page = new PageBuilder(types);

        for (SearchHit hit : response.getHits().getHits()) {

            page.declarePosition();
            Map<String, Object> source = hit.sourceAsMap();

            for (int i = 0; i < types.size(); i++) {
                append(types.get(i), page.getBlockBuilder(i), source.get(fields.get(i)));
            }
        }

        return page.build();
    }

    private void append(final Type type, final BlockBuilder block, final Object value)
    {
        if (value == null) {
            block.appendNull();
        }
        else if (type.equals(BIGINT)) {
            type.writeLong(block, ((Number) value).longValue());
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(block, ((Number) value).intValue());
        }
        else if (type.equals(VARCHAR)) {
            type.writeSlice(block, utf8Slice(value.toString()));
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(block, ((Number) value).doubleValue());
        }
        else if (type.equals(BOOLEAN)) {
            type.writeBoolean(block, (Boolean) value);
        }
        else if (type.equals(DATE)) {
            LocalDate ld = DateTimeFormatter.BASIC_ISO_DATE.parse((String) value, LocalDate::from);
            type.writeLong(block, ld.toEpochDay());
        }
        else if (type.equals(TIMESTAMP)) {
            type.writeLong(block, ((Number) value).longValue());
        }
        else if (isArrayType(type)) {
            writeArray(type, block, value);
        }
        else if (isRowType(type)) {
            writeRow(type, block, value);
        }
        else {
            throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: [" + type + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private void writeArray(final Type type, final BlockBuilder block, final Object value)
    {
        List<Object> values;

        if (!(value instanceof List)) {
            values = new ArrayList<>(1);    // convert scalar value into array
            values.add(value);
        }
        else {
            values = (List<Object>) value;
        }

        List<Type> parameters = type.getTypeParameters();
        BlockBuilder builder = parameters.get(0).createBlockBuilder(new BlockBuilderStatus(), values.size());
        values.forEach(element -> append(type.getTypeParameters().get(0), builder, element));
        type.writeObject(block, builder.build());
    }

    private void writeRow(final Type type, final BlockBuilder block, final Object value)
    {
        if (!(value instanceof Map)) {
            throw new PrestoException(ELASTICSEARCH_UNSUPPORTED_TYPE, "Unsupported type: [" + type + "]");
        }

        Map<?, ?> map = (Map<?, ?>) value;
        List<Type> parameters = type.getTypeParameters();

        BlockBuilder interleaved = new InterleavedBlockBuilder(
                parameters, new BlockBuilderStatus(), parameters.size() * map.size());

        List<String> fields = type.getTypeSignature().getParameters()
                .stream()
                .map(TypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getName)
                .collect(Collectors.toList());

        checkState(parameters.size() == fields.size(), "Field names do not match parameter types: %s", type);

        for (int i = 0; i < parameters.size(); i++) {
            append(parameters.get(i), interleaved, map.get(fields.get(i)));
        }

        type.writeObject(block, interleaved.build());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        scanner.close();
    }

    private static boolean isRowType(final Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static boolean isArrayType(final Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

}
