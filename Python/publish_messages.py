# -*- coding: utf-8 -*-

import time#,os,json
import argparse
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from configparser import ConfigParser


def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
            futures.pop(data)
        except:  # noqa
            print("Please handle {} for {}.".format(f.exception(), data))

    return callback

def publish(filepath):
    
    with open(filepath,encoding="utf8") as openfileobject:
        for line in openfileobject:
            '''dict1 = json.loads(line)
            #dict2 = {}
            #dict2["business_id"] = dict1['business_id']
            #dict2["highlights"]=dict1['highlights']
            dict2 = {x.replace(' ', '_'): v 
                     for x, v in dict1.items()}
            data = str(dict2)
            futures.update({data: None})'''
            data = line
            futures.update({data: None})
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic=topic_path, data=data.encode("utf-8"))
            futures[data] = future
            # Publish failures shall be handled in the callback function.
            future.add_done_callback(get_callback(future, data))
    
    # Wait for all the publish futures to resolve before exiting.
    while futures:
        time.sleep(5)
    
    print("Published messages with error handler to "+topic_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_path', required=True,
        help='Config path from where config will be read.')
    args = parser.parse_args()
    #print(args.config_path)
    config = ConfigParser()
    config.read(args.config_path)
    #print (config.get('gcp','credentials_path'))
    
    credentials = service_account.Credentials.from_service_account_file(
        config.get('gcp','credentials_path'))
    
    # TODO(developer)
    project_id = config.get('gcp','project_id')
    topic_id = config.get('gcp','topic_id')
    
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(project_id, topic_id)
    futures = dict()
    publish(config.get('gcp','file_path'))