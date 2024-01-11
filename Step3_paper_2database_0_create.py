from elasticsearch import Elasticsearch
import pandas as pd
from pprint import pprint
ES_URL = "https://elastic:mg8RfGAgIIJ80ts5YtLO@58.37.114.191:7360"
es_client = Elasticsearch(ES_URL, verify_certs=False)



## add the paprameters
request_body_mpnet ={
    "settings": { "number_of_shards": 4, "number_of_replicas": 1 },
    "mappings": { "properties": {
        "text": {"type": "text"},
        "vector": {"type": "dense_vector", "dims": 768, 'index': True, 'similarity': 'cosine'}, # dimensionality of m3e-base 是768
        }
    }
}
es_client.indices.create(index = "paperm3e_test" , body = request_body_mpnet)