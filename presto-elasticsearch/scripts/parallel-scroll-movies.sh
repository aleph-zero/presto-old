#!/bin/bash

echo "XXXXXXXXXXX SLICE 0"

curl -XGET  'localhost:9200/movies/movies/_search?pretty&scroll=1m' -d '{
    "size" : 2,
    "slice": {
        "id" : 0,
        "max": 3
    },
    "query": {
        "match": {
            "year" : "2011"
        }
    }
}'

echo "XXXXXXXXXXX SLICE 1"

curl -XGET  'localhost:9200/movies/movies/_search?pretty&scroll=1m' -d '{
    "size" : 2,
    "slice": {
        "id" : 1,
        "max": 3
    },
    "query": {
        "match": {
            "year" : "2011"
        }
    }
}'

echo "XXXXXXXXXXX SLICE 2"

curl -XGET  'localhost:9200/movies/movies/_search?pretty&scroll=1m' -d '{
    "size" : 2,
    "slice": {
        "id" : 2,
        "max": 3
    },
    "query": {
        "match": {
            "year" : "2011"
        }
    }
}
'
