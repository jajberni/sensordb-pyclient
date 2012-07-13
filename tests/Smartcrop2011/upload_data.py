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
sensor_list = read_csv(sensor_list_file)

# Create a list of unique experiment names
experiment_names = set()
for experiment in sensor_db.experiments:
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
        print "\"" + trial + "\" has multiple sites. This is not yet handled. Only the first site will be used"
    
    
    # Skip this trial if it already exists as an experiment
    if trial in experiment_names:
        continue
    
    # TODO - determine the timezone from the site. This would require a dictionary of sites.
    
    print sensor_db.user.create_experiment(trial, "Australia/Brisbane")
    
    # TODO - Find the experiment and add the site as metadata
    
    pass
    