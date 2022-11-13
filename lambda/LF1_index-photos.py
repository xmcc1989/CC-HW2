#!/usr/bin/env python
# coding: utf-8

# In[58]:


#aws user
#bot
#access key: AKIAYNMA4JYZZAWISICO
#secrete access key: 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

import json
import urllib.parse
import boto3
import logging
import os
import pprint
import time
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# In[60]:


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     bucket = 'b2photostore'
#     key = '4.PNG'
    try:
        metadata = s3.head_object(Bucket=bucket, Key=key)
#         logger.debug('{}'.format(metadata))
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    
    #get label from Rekognito
    label_list = detect_labels(bucket, key)
#     logger.debug('{}'.format(label_list))
    
    #create json file
    label_json = create_json(bucket, key, metadata, label_list)
    pprint.pprint(label_json, indent=2)
    
    #upload to elastic search
    response = upload_ES(label_json)
    pprint.pprint(response, indent=2)
        
    return


def detect_labels(bucket, photo):

    client=boto3.client('rekognition')
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}}, 
                                    MaxLabels=10, 
                                    MinConfidence=80)

    return response['Labels']


def create_json(bucket, key, metadata, labels):
    
    #create timestamp
    timestamp_raw = metadata['ResponseMetadata']['HTTPHeaders']['date']
    full_mo_date = timestamp_raw[:len(timestamp_raw)-3]
    full_mo_date_format = "%a, %d %b %Y %H:%M:%S "
    timestamp = datetime.strptime(full_mo_date, full_mo_date_format).isoformat()
    
    #create label list from custom label
    label_list =[]
    if 'x-amz-meta-customlabels' in metadata['ResponseMetadata']['HTTPHeaders']:
        customlabels = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-customlabels'].split(',')
        for label in customlabels:
            label_list.append(label)
    
    #append labels from rekognito
    for label in labels:
        if label['Name'] not in label_list:
            label_list.append(label['Name'])
        
    #form json file    
    label_array = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': timestamp,
        'labels': label_list
    }
    
    return json.dumps(label_array)

def upload_ES(label_json):
    
    region = 'us-east-1'
    service = 'es'
    host = 'https://search-photos-i6xlvas6hikk7cammtg5zr625e.us-east-1.es.amazonaws.com' 
    index = 'photos'
    typ = 'photo'
    url = host + '/' + index + '/' + typ
    
    headers = { "Content-Type": "application/json" }
    
#     client = boto3.client('es')
    payload = json.loads(label_json)
    r = requests.post(url, auth=('mx608', '!Xmcc891001'), json=payload)
    print(r.text)
    
    return r


# In[57]:


lambda_handler('','')


# In[29]:


response=detect_labels(BUCKET, KEY)
response


# In[ ]:




