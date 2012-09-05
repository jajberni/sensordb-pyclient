'''
Created on 03/09/2012

@author: hol40p
'''

from pandas import read_csv
import sys
import time
import requests
from os.path import exists
from os import chdir

sys.path.append('../..')

from sensordb_api import *

#exp_name = "2012 Wthr Gttn Gilbert G3 East"
exp_name = "2012 Gatton Gilbert G3 East"

# Gets the node for a particular sensor. Create it if it does not exist.
def get_sensor_node(experiment, sensor):
    for node in experiment.nodes:
        if node.name == sensor["description"]:
            return node
        
    new_node = experiment.create_node(sensor["description"])
    
    if new_node is None:
        raise Exception("Error creating Node")
    
    return new_node

# Gets the sensor nodes. Creates them if they do not exist.
def get_sensor_nodes(experiment):
    sensor_list = read_csv("sensors.csv", parse_dates="datetime")
    
    sensor_ids = dict()
    
    for sensor in sensor_list:
        node = None #get_sensor_node(experiment, sensor)
        sensor_ids[sensor["sensorid"]] = node
    
    return sensor_ids

def upload_old_data(experiment, sensor_ids):
    for sensor in sensor_ids.keys():
        sensor_filename = ".\\data\\" + str(sensor) + ".csv" 
        if(exists(sensor_filename)):
            sensor_data = read_csv(sensor_file)
            
            #TODO - upload to DB
            pass

        else:
            print "Warning - No old data found for sensor: " + str(sensor)

def upload_new_data(experiment, sensor_ids):
    pass


if __name__ == '__main__':

    host = "http://phenonet.com:900"
    username = "testUser"
    password = "password"

    working_dir = "\\\\win2008-bz1-vc\\D\\Weather\\Gatton"

    old_data = False

    chdir(working_dir)

    ''' Connect to SensorDB '''
    print "Connecting to Database"
    sensor_db = SensorDB(host, username, password)
    
    ''' Create experiment '''
    # Read in the sensor list
    print "Finding Experiment"
    weather_exp = None
    for experiment in sensor_db.experiments:
        if experiment.name == exp_name:
            weather_exp = experiment
            break
    
    if weather_exp is None:
        old_data = True
        #weather_exp = sensor_db.user.create_experiment(exp_name, "Australia/Queensland")
    
        if weather_exp is None:
            pass
            #raise Exception("Error creating experiment")
        
        #weather_exp.metadata_add("Site","Gatton Gilbert")
        #weather_exp.metadata_add("Field","G3")
    else:
        old_data = False
    
    # 
    sensor_ids = get_sensor_nodes(weather_exp)
    
    #TODO - More thorough check if old data is required
    if old_data:
        upload_old_data(weather_exp, sensor_ids)
    
    
    
    
    
    