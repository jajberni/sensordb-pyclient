# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 15:37:32 2012

!!! Use with care, it will delete all the experiments for this user!!!!

@author: jim006
"""

from pandas import *
import sys
import requests

sys.path.append('../..')

from sensordb_api import *


host = "http://phenonet.com:9001"
username = ""
password = ""



test_db = SensorDB(host, username, password)

experiments = test_db.experiments

for experiment in experiments:
    r = experiment.delete()
    print(r)

