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

import com.google.common.collect.ImmutableMap;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;

import static org.primer.bunny.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static org.primer.bunny.ElasticsearchQueryRunner.createSession;

/**
 * Standard distributed query tests for Elasticsearch.
 */
public class TestElasticsearchDistributedQueries extends AbstractTestDistributedQueries
{
    @SuppressWarnings("unused")
    public TestElasticsearchDistributedQueries() throws Exception
    {
        this(createElasticsearchQueryRunner(ImmutableMap.of(), true));
    }

    protected TestElasticsearchDistributedQueries(QueryRunner runner)
    {
        super(runner, createSession());
    }
}
