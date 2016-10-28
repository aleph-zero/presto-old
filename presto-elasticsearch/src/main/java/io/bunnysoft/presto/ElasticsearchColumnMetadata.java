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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

/**
 * Elasticsearch column metadata.
 */
public class ElasticsearchColumnMetadata extends ColumnMetadata
{
    private boolean docValues;
    private boolean stored;

    public ElasticsearchColumnMetadata(final String name, final Type type, final boolean docValues, final boolean stored)
    {
        super(name, type);

        this.docValues = docValues;
        this.stored    = stored;
    }

    public boolean docValues()
    {
        return docValues;
    }

    public boolean stored()
    {
        return stored;
    }
}
