'''
Created on 10/10/2012

@author: hol40p

A script to upload voltage data from Smartcrop base stations.
'''

from pandas import read_csv
import sys
import time
import datetime
import pytz
import requests
from os.path import exists

sys.path.append('../..')

from sensordb_api import *


host = "http://phenonet.com:9001"
username = "testUser"
password = "password"

base_list = {1625:"\\\\win2008-bz1-vc\\D\\Weather\\SmartField\\1625_voltages.csv", 1511:"\\\\win2008-bz1-vc\\D\\Weather\\SmartField\\1511_voltages.csv"}

experiment_name = "2012 Gatton Gilbert G3 East"
timezone_name = "Australia/Queensland"
node_start = "Smartcrop Base"

sys_stream_name = "System Voltage"
batt_stream_name = "Battery Voltage"

if __name__ == '__main__':
    ''' Connect to SensorDB '''
    print "Connecting to Database"
    sensor_db = SensorDB(host, username, password)
    
    print "Getting Measurement IDs"
    for measurement in sensor_db.get_measurements():
        if measurement['name'] == 'Celsius':
            volt_id = measurement['_id']
            break
    
    
    print "Checking Experiments"
    main_exp = None
    for experiment in sensor_db.experiments:
        if experiment.name == experiment_name:
            main_exp = experiment
            break
    
    if main_exp is None:
        main_exp = sensor_db.user.create_experiment(experiment_name, timezone_name)
    
    for serial, filename in base_list.iteritems():
        node_name = node_start + " " + str(serial) 
        
        base_node = None
        for node in main_exp.nodes:
            if node.name == node_name:
                base_node = node
                break
            
        if base_node is None:
            base_node = main_exp.create_node(node_name)
        
        sys_stream = None
        batt_stream = None
        
        for stream in base_node.streams:
           if stream.name == sys_stream_name:
               sys_stream = stream
           elif stream.name == batt_stream_name:
               batt_stream = stream
           
           #if (sys_stream is not None) and (batt_stream is not None): break
       
        print "Reading Data File"
        #Read in voltage data
        voltage_data = read_csv(filename, parse_dates = ["datetime"], dayfirst = True)
        last_entry = voltage_data[len(voltage_data)-1:]
        
        
        if sys_stream is None:
            # If creating the stream upload all of the data
            print "Uploading All Data"  
            sys_stream = base_node.create_stream(sys_stream_name, volt_id)
            sys_stream.post_dataframe(voltage_data, "datetime", "sys_voltage")
        else:
            print "Uploading Most Recent Datum"
            sys_stream.post_dataframe(last_entry, "datetime", "sys_voltage")
        
        
        if batt_stream is None:
            print "Uploading All Data"
            # If creating the stream upload all of the data  
            batt_stream = base_node.create_stream(batt_stream_name, volt_id)
            batt_stream.post_dataframe(voltage_data, "datetime", "batt_voltage")
        else:
            print "Uploading Most Recent Datum"
            batt_stream.post_dataframe(last_entry, "datetime", "batt_voltage")
    