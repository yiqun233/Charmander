from elasticsearch import Elasticsearch
import pandas as pd
from pprint import pprint

#es_client = Elasticsearch("https://elastic:mg8RfGAgIIJ80ts5YtLO@10.2.72.208:9200",ca_certs="./ES_certs/http_ca.crt")
es_client = Elasticsearch("https://elastic:b5XSbvqdjCGwUylz9c6c@localhost:9200",ca_certs="/Users/toddzzr/Documents/Database/ElasticSearch/elasticsearch-8.11.3/config/certs/http_ca.crt")



# Get all indices
indices = es_client.indices.get_alias()

# Print the list of indices
print("Indices:")
for index in indices:
    print(index)

es_client.close()