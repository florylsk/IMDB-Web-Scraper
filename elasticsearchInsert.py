from pprint import pprint
import requests
import urllib
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.serializer import JSONSerializer
import concurrent.futures
import tqdm

MAX_THREADS=32
es = Elasticsearch()

#open the json for reading
with open("movies_final.json", 'r', encoding='utf-8') as f:
    data=json.loads(f.read())

#function to insert the data in the elastic search index
def insert_data(segmented_data):
    es.index(index='movies', doc_type='doc', body=segmented_data)

#maps each thread to the function insert data
def concurrent_insert(total_data):
    #choose the number of threads
    threads = min(MAX_THREADS, len(total_data))
    #map the threads to the main get data function
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(insert_data, total_data)

#calls the concurrent insert function
concurrent_insert(data)
