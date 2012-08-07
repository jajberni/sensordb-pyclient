from pandas import read_csv
import sys
import time
import requests

sys.path.append('../..')

from sensordb_api import *

host = "http://phenonet.com:9001"
username = ""
password = ""

sensor_list_file = "R:/MCWheat/Equipment/SmartCrop/data2011/SmartField/SmartField SensorList.csv"


''' Connect to SensorDB '''
sensor_db = SensorDB(host, username, password)

''' Create experiments ''' 
# Read in the sensor list
sensor_list = read_csv(sensor_list_file, parse_dates = [4, 5], dayfirst = True)

# Create a list of unique experiment names
experiment_names = set()
for experiment in sensor_db.experiments:
    # DEBUG - delete experiment
    experiment.delete()
    #experiment_names.add(experiment.name)

''' Note - This code is written assuming that a single trial is only ever on a single site'''

# For each unique trial
for trial in sensor_list["TrialCode"].unique():
    # --- Generate an experiment ---

    # Get the site
    sites = sensor_list.ix[sensor_list["TrialCode"] == trial, "Site"].unique()
    
    print trial
    print sites
    
    if sites.size > 1:
        print "\"" + trial + "\" has multiple sites. This is not currently handled. Only the first site will be used"
    
    
    # Skip this trial if it already exists as an experiment
    if trial in experiment_names:
        continue
    
    # TODO - determine the timezone from the site. This would require a dictionary of sites.
    

    experiment = sensor_db.user.create_experiment(trial, "Australia/Brisbane")
    
    # Add the site/s as metadata
    experiment.metadata_add("site", sites[0])
    #experiment.metadata_add("Sites", str(sites))

    
''' Add nodes to experiments '''

sensor_db.get_session()

existing_nodes = dict()

experiments = dict()

for experiment in sensor_db.experiments:
    existing_nodes[experiment.name] = dict()
    
    # Create a dict to reference the experiments by name
    experiments[experiment.name] = experiment
    
    for node in experiment.nodes:
        existing_nodes[experiment.name][node.name] = node


for measurement in sensor_db.get_measurements():
    if measurement['name'] == 'Celsius':
        temp_id = measurement['_id']
        break


for node_index in sensor_list.index:
    # Name should be Genotype, Range, Plot. e.g. ABC-R01P01
    node_name = str(sensor_list["Genotype"][node_index])
    if node_name.endswith('+'):
        node_name = node_name[:len(node_name)-1]
        node_name += 'plus'
    
    elif node_name.endswith('-'):
        node_name = node_name[:len(node_name)-1]
        node_name += 'minus'
    
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
    ambient_stream = node.create_stream("AmbientTemp" + position + "-" + serial, temp_id)
    # Add other streams here
    #Humiditiy
    
    if canopy_stream is None or ambient_stream is None:
        # Streams are already created?
        continue
    
    start_time = time.mktime(sensor_list["StartDate"])
    end_time = time.mktime(sensor_list[""])
    
    canopy_stream.metadata_add()
    
    
for sensor_index in sensor_list.index:
    