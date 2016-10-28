#!/bin/bash

curl -XDELETE   'localhost:9200/test-index-a'
curl -XDELETE   'localhost:9200/test-index-b'
curl -XPUT      'localhost:9200/test-index-a' -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 3, 
            "number_of_replicas" : 0 
        }
    }
}'
curl -XPUT      'localhost:9200/test-index-b' -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 3, 
            "number_of_replicas" : 0 
        }
    }
}'
curl -XPOST     'localhost:9200/test-index-a/_mapping/user?pretty'  -d @test-index-a-mapping.json
curl -XPOST     'localhost:9200/test-index-b/_mapping/user?pretty'  -d @test-index-b-mapping.json
curl -XPOST     'localhost:9200/test-index-a/user/1?pretty'         -d @test-index-a-data-1.json
curl -XPOST     'localhost:9200/test-index-a/user/2?pretty'         -d @test-index-a-data-2.json
curl -XPOST     'localhost:9200/test-index-b/user/1?pretty'         -d @test-index-b-data-1.json
