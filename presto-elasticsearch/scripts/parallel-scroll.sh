#!/bin/bash

echo "XXXXXXXXXXX SLICE 0"

curl -XGET  'localhost:9200/test-index-a/user/_search?pretty&scroll=1m' -d '{
    "slice": {
        "id" : 0,
        "max": 3
    },
    "query": {
        "match_all": {}
    }
}'

echo "XXXXXXXXXXX SLICE 1"

curl -XGET  'localhost:9200/test-index-a/user/_search?pretty&scroll=1m' -d '{
    "slice": {
        "id" : 1,
        "max": 3
    },
    "query": {
        "match_all": {}
    }
}'

echo "XXXXXXXXXXX SLICE 2"

curl -XGET  'localhost:9200/test-index-a/user/_search?pretty&scroll=1m' -d '{
    "slice": {
        "id" : 2,
        "max": 3
    },
    "query": {
        "match_all": {}
    }
}'
