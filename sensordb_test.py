from sensordb_api import *
import getpass

default_host = "http://phenonet.com:9001"

host = raw_input("Please enter the host you wish to connect to (Enter for default). [" + default_host + "]:")
if host == "":
    host = default_host

username = raw_input("Username: ")
password = getpass.getpass("Password: ")


test_db = SensorDB(host, username, password)

print vars(test_db.user).keys()

print test_db.user.description

for experiment in test_db.experiments:
    print "Experiment Variables: " + repr(vars(experiment).keys())
    print "Experiment ID: " + experiment._id
    print "Experiment Name: " + experiment.name
    print "Experiment Nodes: " + experiment.nodes






