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



host = "http://phenonet.com:9001"
username = ""
password = ""


test_db = SensorDB(host, username, password)


test=test_db.experiments[0].nodes[0].streams[0].get_data('01-01-2010','01-01-2013')

print(test)
