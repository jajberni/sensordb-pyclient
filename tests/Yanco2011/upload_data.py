# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 16:22:54 2012

@author: jim006
"""

from pandas import *
import sys
import time
import requests

sys.path.append('../..')

from sensordb_api import *

nodeFile = 'YAN_node_index.csv'
dataFile = 'YAN_crop_data_sub.csv'

nodes = read_csv(nodeFile,index_col=2)
data = read_csv(dataFile,index_col=[1,0],parse_dates=True)

host = "http://phenonet.com:9001"
username = ""
password = ""

test_data = data['temp_obj']['0013A20040773518']

test_db = SensorDB(host, username, password)


tok = test_db.user.get_tokens()
token = tok[0]['token']

payload_data = {}
i=0
for ts in test_data.index:
    tt=int(time.mktime(ts.timetuple()))
    value = test_data[ts]
    
    payload_data[str(tt)] = value
    
    if i>=20:
        break
    i+=1

payload_tk = {}

payload_tk[token] = payload_data

payload = { 'data':payload_tk}

r = requests.post(host + '/data', payload, cookies = test_db._cookie)
print(payload)
print(r.text)

