#!/bin/bash

curl -XGET  'localhost:9200/test-index-a/user/_search?pretty&scroll=1m' -d '{
    "slice": {
        "id" : 0,
        "max": 3
    },
    "query": {
        "match_all": {}
    }
}'
