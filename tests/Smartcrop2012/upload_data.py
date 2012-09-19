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
username = "eholland"
password = "john1098"

sensor_list_file = "\\\\win2008-bz1-vc\\D\\Weather\\SmartField\\SmartField SensorList.csv"
sensor_data_location = "\\\\win2008-bz1-vc\\D\\Weather\\SmartField\\2012\\Sensors\\"

timezone_name = "Australia/Queensland"

''' Connect to SensorDB '''
print "Connecting to Database"
sensor_db = SensorDB(host, username, password)

''' Create experiments '''
# Read in the sensor list
print "Loading Sensor List"
sensor_list = read_csv(sensor_list_file, parse_dates = ["StartDate", "EndDate"], dayfirst = True)
# Remove items from the list that have no sensor
sensor_list = sensor_list[sensor_list["Serial Number"] > 0]

'''
print "Clearing Experiments"
for experiment in sensor_db.experiments:
    # DEBUG - delete experiment
    experiment.delete()
'''

# Create a list of unique experiment names
experiment_names = set()
print "Checking Experiments"
for experiment in sensor_db.experiments:
    experiment_names.add(experiment.name)

''' Note - This code is written assuming that a single trial is only ever on a single site'''

print "Creating Trials"

# For each unique trial
for trial in sensor_list["TrialCode"].unique():
    # --- Generate an experiment ---

    # Get the site
    sites = sensor_list.ix[sensor_list["TrialCode"] == trial, "Site"].unique()
    
    print trial +" "+ sites
    
    if sites.size > 1:
        print "\"" + trial + "\" has multiple sites. This is not currently handled. Only the first site will be used"
    
    
    # Skip this trial if it already exists as an experiment
    if trial in experiment_names:
        print "Trial already exists"
        continue
    
    # TODO - determine the timezone from the site. This would require a dictionary of sites.
    

    experiment = sensor_db.user.create_experiment(trial, timezone_name)
    
    # Add the site/s as metadata
    experiment.metadata_add("site", sites[0])
    #experiment.metadata_add("Sites", str(sites))

    print "Trial Created"
    
''' Add nodes to experiments '''

sensor_db.get_session()

existing_nodes = dict()

experiments = dict()

print "Creating Experiments"
for experiment in sensor_db.experiments:
    existing_nodes[experiment.name] = dict()
    
    # Create a dict to reference the experiments by name
    experiments[experiment.name] = experiment
    
    for node in experiment.nodes:
        existing_nodes[experiment.name][node.name] = node


print "Getting Measurement IDs"
for measurement in sensor_db.get_measurements():
    if measurement['name'] == 'Celsius':
        temp_id = measurement['_id']
        break


print "Creating Nodes"
for node_index in sensor_list.index:

    # If the sensor has no data, skip it
    file_name = sensor_data_location + str(int(sensor_list["Serial Number"][node_index])) + ".csv"
    if not exists(file_name):
        continue

    # Name should be Genotype, Range, Plot. e.g. ABC-R01P01
    node_name = str(sensor_list["Genotype"][node_index])
    
    '''
    # Convert +/- to 'plus'/'minus'
    
    
    if node_name.endswith('+'):
        node_name = node_name[:len(node_name)-1]
        node_name += 'plus'
    
    elif node_name.endswith('-'):
        node_name = node_name[:len(node_name)-1]
        node_name += 'minus'
    '''
    
    node_name += "-R{:02}".format(sensor_list["Range"][node_index]) + "P{:02}".format(sensor_list["Plot"][node_index])
    trial_name = sensor_list["TrialCode"][node_index]
    
    # add the node if it doesn't exist
    if node_name not in existing_nodes[trial_name]:
    
        node = experiments[trial_name].create_node(node_name)
        
        if node is None:
            print "Error creating node: " + node_name
            continue

        existing_nodes[trial_name][node_name] = node
    
        node.metadata_add("Genotype", sensor_list["Genotype"][node_index])    
        node.metadata_add("Site", sensor_list["Site"][node_index])
        node.metadata_add("Range", sensor_list["Range"][node_index])
        node.metadata_add("Plot", sensor_list["Plot"][node_index])
        node.metadata_add("Replicate", sensor_list["Replicate"][node_index])
        node.metadata_add("Treatment", sensor_list["Treatment"][node_index])
    
    else:
        node = existing_nodes[trial_name][node_name]
        
    position = str(sensor_list['Pos'][node_index]).upper()
    serial = str(sensor_list['Serial Number'][node_index])
    
    canopy_stream = node.create_stream("CanopyTemp" + position + "-" + serial, temp_id)
    #ambient_stream = node.create_stream("AmbientTemp" + position + "-" + serial, temp_id)
    # Add other streams here
    # Humidity?
    
    if canopy_stream is None:# or ambient_stream is None:
        # Streams are already created?
        continue
    
    start_time = time.mktime(sensor_list["StartDate"][node_index].timetuple())
    end_time = time.mktime(sensor_list["EndDate"][node_index].timetuple())
    
    canopy_stream.metadata_add("Sensor", serial, start_ts=int(start_time), end_ts=int(end_time))
    canopy_stream.metadata_add("Position", position)
    
    #ambient_stream.metadata_add("Sensor", serial, start_ts=int(start_time), end_ts=int(end_time))
    #ambient_stream.metadata_add("Position", position)
    
    #Read in Sensor Data
    sensor_data = read_csv(file_name, parse_dates = ["datetimeread"], dayfirst = True)
    
    timezone = pytz.timezone(timezone_name)
    
    canopy_stream.post_dataframe(sensor_data, "datetimeread", "irtemp", timezone)
    #ambient_stream.post_dataframe(sensor_data, "datetimeread", "ambienttemp", timezone)
    
    print "Posted data for " + node_name
    
    """
    data = dict()
    
    canopy_token = str(canopy_stream.token)
    #ambient_token = str(ambient_stream.token)
    
    data[canopy_token] = dict()
    #data[ambient_token] = dict()
    
    count = 0
    
    for line_index in sensor_data.index:
        
        # A single post cannot be larger than 200000 characters.
        # If there is enough data in the file then it can go over this limit.
        # Therefore the data should be separated into multiple posts   
        if (len(str(data)) > 100000):
            r = sensor_db.post_data(data)
            
            try:
                count += r["length"]
            except:
                print "Error posting data for node " + node_name
                #continue
            
            data = dict()
            
            data[canopy_token] = dict()
            #data[ambient_token] = dict()
        
        # Get the data timestamp in seconds since epoch and store it as a string
        timestamp = "{:d}".format(int(time.mktime(sensor_data["datetimeread"][line_index].timetuple())))
        
        
        data[canopy_token][timestamp] = "{:.2f}".format(sensor_data["irtemp"][line_index])
        #data[ambient_token][timestamp] = "{:.2f}".format(sensor_data["ambienttemp"][line_index])
    
    r = sensor_db.post_data(data)
    
    try:
        count += r["length"]
    except:
        print "Error posting data for node " + node_name
        #continue
    
    
    if count != (sensor_data.index.size * 1):
        print "Data Error  - Response was: " + str(count) + " Expected: " + str(sensor_data.index.size * 1)
    else:
        print "Posted data for " + node_name
    """
    
    