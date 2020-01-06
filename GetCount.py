import requests
import base64
import json
import sys

#Base URL for interacting with REST server
baseurl = "https://fax-nlp.glaceemr.com:5000/consumers/ocr_processing_new"

#Create the Consumer instance
print ("Creating consumer instance")
payload ={
    "format": "binary"
    }
headers = {
"Content-Type" : "application/vnd.kafka.v1+json"
    }
#cert = ('/var/ssl/kafka.cer', '/var/ssl/glaceprivate.key')
r = requests.post(baseurl, data=json.dumps(payload), headers=headers)

if r.status_code !=200:
    print ("Status Code: " + str(r.status_code))
    print (r.text)
    sys.exit("Error thrown while creating consumer")

# Base URI is used to identify the consumer instance
base_uri = r.json()["base_uri"]

#Get the messages from the consumer
headers = {
    "Accept" : "application/vnd.kafka.binary.v1 + json"
    }

# Request messages for the instance on the Topic
r = requests.get(base_uri + "/topics/image_process", headers = headers, timeout =20,cert=cert)

if r.status_code != 200:
    print( "Status Code: " + str(r.status_code))
    print (r.text)
    sys.exit("Error thrown while getting message")

#Output all messages
for message in r.json():
    if message["key"] is not None:
        print ("Message Key:" + base64.b64decode(message["key"]))
    print ("Message Value:" + base64.b64decode(message["value"]))

# When we're done, delete the consumer
headers = {
    "Accept" : "application/vnd.kafka.v1+json"
    }

r = requests.delete(base_uri, headers=headers)

if r.status_code != 204:
    print ("Status Code: " + str(r.status_code))
    print (r.text)