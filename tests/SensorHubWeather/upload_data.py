'''
Created on 03/09/2012

@author: hol40p
'''

from pandas import read_csv
from pandas.io.date_converters import parse_all_fields
import sys
import time
import datetime
import pytz
import requests
from os.path import exists
from os import chdir

sys.path.append('../..')

from sensordb_api import *

#exp_name = "2012 Wthr Gttn Gilbert G3 East"
exp_name = "2012 Gatton Gilbert G3 East"
node_name = "SensorHub Node 1"
timezone_name = "Australia/Queensland"

sensor_units = {"Air Humidity":"Relative Humidity",
                "Air Pressure":"Kilopascals (KPa)",
                "Air Temperature":"Celsius",
                "Body Temperature":"Celsius",
                "Light":"Watts per Squarse Metre (W/m^2)", # Typo in DB
                "Light - PAR":"Photosynthetic Photon Flux (umol/m^2)",
                "Rain Accumulation":"Millimeters (mm)",
                "Rain Duration":"Seconds (sec)",                # Not in DB
                "Rain Intensity":"Millimeters per Hour (mm/h)", # Not in DB
                "Target Temperature":"Celsius",
                "Thermocouple":"Celsius",
                "Wind Direction":"Cardinal Direction (Degrees)", # Not in DB
                "Wind Speed":"Metres per Second (m/s)",
                "Hub Temperature":"Celsius"}


# Gets the node for a particular sensor. Create it if it does not exist.
def get_sensor_stream(node, sensor_name):
    for stream in node.streams:
        if stream.name == sensor_name:
            return stream
    
    stream_mid = None
    units = node._parent_db.get_measurements()
    for unit in units:
        if unit['name'] == sensor_units[sensor_name]:
            stream_mid = unit['_id']
            break
    
    new_stream = None
    if stream_mid is not None:
        new_stream = node.create_stream(sensor_name, stream_mid)
    
    #if new_stream is None:
    #    raise Exception("Error creating Node")
    
    return new_stream


# Gets the sensor nodes. Creates them if they do not exist.
def get_sensor_streams(node):
    sensor_list = read_csv("sensors.csv", index_col = "sensorid")
    
    sensor_ids = dict()
    
    for sensor in sensor_list.index:
        # Get sensor description
        sensor_name = sensor_list.xs(sensor)["description"]
        # Remove anything after the '(' and strip single quotation marks and whitespace
        sensor_name = sensor_name[:sensor_name.index("(")].strip(" '")
        
        stream = get_sensor_stream(node, sensor_name)
        
        if stream is not None:
            sensor_ids[sensor] = stream
    
    return sensor_ids


def upload_old_data(sensor_ids):
    timezone = pytz.timezone(timezone_name)
    
    for sensor in sensor_ids.keys():
        sensor_filename = ".\\data\\" + str(sensor) + ".csv" 
        
        if(exists(sensor_filename)):
            stream = sensor_ids[sensor]

            if stream is not None:
                sensor_data = read_csv(sensor_filename, parse_dates = ["datetime"])

                if type(sensor_data["datetime"][1]) == type(str()):
                    pass

                stream.post_dataframe(sensor_data, "datetime", "value", timezone)
            else:
                pass


def upload_new_data(sensor_ids):
    sensor_data = read_csv("temp.csv", header = None, parse_dates = {"datetime":["year", "month", "day", "hours", "minutes", "seconds"]}, date_parser = parse_all_fields, names = ["sensor", "year", "month", "day", "hours", "minutes", "seconds", "milliseconds", "value"])
    
    timezone = pytz.timezone(timezone_name)
    
    for sensor in sensor_ids.keys():
        stream = sensor_ids[sensor]
        
        stream.post_dataframe(sensor_data[sensor_data["sensor"] == sensor], "datetime", "value", timezone)


if __name__ == '__main__':

    host = "http://phenonet.com:9001"
    username = "testUser"
    password = "password"

    working_dir = "\\\\win2008-bz1-vc\\D\\Weather\\Gatton"

    old_data = False

    chdir(working_dir)

    ''' Connect to SensorDB '''
    print "Connecting to Database"
    sensor_db = SensorDB(host, username, password)
    
    
    print "Clearing Experiments"
    for experiment in sensor_db.experiments:
        # DEBUG - delete experiment
        if experiment.name == exp_name:
            experiment.delete()
            pass
    
    
    
    ''' Create experiment '''
    # Read in the sensor list
    print "Finding Experiment"
    weather_exp = None
    for experiment in sensor_db.experiments:
        if experiment.name == exp_name:
            weather_exp = experiment
            break
    
    if weather_exp is None:
        print "Creating Experiment"
        old_data = True
        weather_exp = sensor_db.user.create_experiment(exp_name, timezone_name)
    
        if weather_exp is None:
            raise Exception("Error creating experiment")
        
        weather_exp.metadata_add("Site","Gatton Gilbert")
        weather_exp.metadata_add("Field","G3")
    else:
        old_data = False
    
    
    sensorhub_node = None
    
    print "Getting Node"
    for node in weather_exp.nodes:
        if node.name == node_name:
            sensorhub_node = node
            break
    
    if sensorhub_node is None:
        print "Creating Node" 
        sensorhub_node = weather_exp.create_node(node_name)
    
    print "Geting Streams"
    sensor_ids = get_sensor_streams(sensorhub_node)
    
    #TODO - More thorough check if old data is required
    if old_data:
        print "Uploading Old Data"
        upload_old_data(sensor_ids)
    
    print "Uploading New Data"
    upload_new_data(sensor_ids)
    
    sys.exit() # Success
    
    