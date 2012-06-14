import sys
sys.path.append("../")
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

#test_db.user.experiments[0].metadata_add("test_metadata", "Testing the metadata", description="This is test metadata")

#measurements = test_db.get_measurements()

#test_db.experiments[1].nodes[0].create_stream("TestStream", measurements[0]["_id"], description="A Test stream")

for experiment in test_db.experiments:
    print "Experiment Variables: " + repr(vars(experiment).keys())
    print "Experiment ID: " + experiment._id
    print "Experiment Name: " + experiment.name
    #print "Experiment Nodes: " + repr(experiment.nodes)
    print "Metadata: " + repr(experiment.metadata)
    for node in experiment.nodes:
        print "Node Name: " + node.name + " ID: " + node._id
        for stream in node.streams:
            print "Stream Name: " + stream.name






