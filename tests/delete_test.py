import sys
sys.path.append('../')

from sensordb_api import *

host = "http://phenonet.com:9001"
username = "testUser"
password = "password"

''' Connect to SensorDB '''
print "Connecting to Database"
sensor_db = SensorDB(host, username, password)

print "Clearing Experiments"
for experiment in sensor_db.experiments:
    # DEBUG - delete experiment
    experiment.delete()


experiment = sensor_db.user.create_experiment("Experiment Name", "Australia/Queensland")