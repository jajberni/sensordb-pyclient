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
    #experiment.delete()
    experiment_names.add(experiment.name)

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

for experiment in sensor_db.experiments:
    existing_nodes[experiment.name] = set()
    
    for node in experiment.nodes:
        existing_nodes[experiment.name].add(node.name)

for node_index in sensor_list.index:
    # Name should be Genotype, Range, Plot. e.g. ABC-R01P01
    node_name = sensor_list["Genotype"][node_index] + "-R{:02}".format(sensor_list["Range"][node_index]) + "P{:02}".format(sensor_list["Plot"][node_index])
    
    # skip existing added nodes
    if node_name in existing_nodes:
        continue
    
    
    
    existing_nodes.add(node_name)
    
    
    