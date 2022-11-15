#!/usr/bin/env python
# coding: utf-8

# In[58]:

# In[32]:


import json
import urllib.parse
import boto3
import logging
import os
import pprint
import time
import boto3
import requests
import inflect
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    message = event['q']
    # message = 'bridge and red'
    userid = '123' #hard code user id
    
    #get slots from lex
    slots = get_slots(userid, message)
#     print(slots)
    
    if len(slots) == 0:
        return {
            'statusCode': '500',
            'messages': {"code":0,
                         "message": "No User Input."}
            }
    
    #construct query string
    query_string = construct_querystring(slots)
    
    #query elastic search
    hits = query_es(query_string)
#     print(hits)
    
    #construct reponse
    return {
            'statusCode': '200',
            'messages': construct_response(hits)
            }


def get_slots(userid, message):
    
    client = boto3.client("lex-runtime")
    response = client.post_text(
        botName = 'SearchBot',
        botAlias ='dev',
        userId = userid, # to be changed
        # sessionAttributes={
        #     'string': 'string'
        # },
        # requestAttributes={
        #     'string': 'string'
        # },
        inputText = message,
        # activeContexts=[
        #     {
        #         'name': 'string',
        #         'timeToLive': {
        #             'timeToLiveInSeconds': 123,
        #             'turnsToLive': 123
        #         },
        #         'parameters': {
        #             'string': 'string'
        #         }
        #     },
        # ]
    )

    return response['slots']

def construct_querystring(slots):
    p = inflect.engine()
    
    query_string = ''
    for i in slots:
#         print(slots[i])
        if slots[i] is not None:
            if i == 'tag_two':
                query_string = query_string + ' OR '
            if p.singular_noun(slots[i]) == False:
                query_string = query_string + slots[i]
            else:
                query_string = query_string + p.singular_noun(slots[i])
    
    return query_string


def query_es(query_string):
    
    region = 'us-east-1'
    service = 'es'
    host = 'https://search-photos-i6xlvas6hikk7cammtg5zr625e.us-east-1.es.amazonaws.com' 
    index = 'photos'
    typ = '_search'
    url = host + '/' + index + '/' + typ
    
    # Put the user query into the query DSL for more accurate search results.
    # Note that certain fields are boosted (^).
    query = {
        "size": 1000,
        "query": {
            "query_string": {
                "query": query_string,
                "fields": ["labels"]
            }
        }
    }

    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request. Hide user info on purpose!!!
    response = requests.get(url, auth=('mx608', '!Xmcc891001'), data=json.dumps(query), headers=headers)
    
    return json.loads(response.text)['hits']['hits']

def construct_response(hits):        
    results=[]
    
    if len(hits) == 0:
        return {"results" : []}
    
    for item in hits:
        bucket = item['_source']['bucket']
        key = item['_source']['objectKey']
        labels = item['_source']['labels']
        url = "https://%s.s3.amazonaws.com/%s" % (bucket, key)
        res = {"url": url,
               "labels": labels}
        results.append(res)
    
    return {"results" : results}
