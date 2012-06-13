from sensordb_api import *
import getpass

default_host = "http://phenonet.com:9001"

#host = raw_input("Please enter the host you wish to connect to (Enter for default). [" + default_host + "]:")
if host == "":
    host = default_host

username = ""
password = ""



test_db = SensorDB(host, username, password)

   
    

print vars(test_db.user).keys()

print test_db.user.description

test_db.user.experiments[0].metadata_add("test_metadata_500", "Metadata TEst ! 123420", description="This is test metadata")

for experiment in test_db.experiments:
    print "Experiment Variables: " + repr(vars(experiment).keys())
    print "Experiment ID: " + experiment._id
    print "Experiment Name: " + experiment.name
    print "Experiment Nodes: "
    for node in experiment.nodes:
        print "Node Name: " + node.name + " ID: " + node._id
        for stream in node.streams:
            print "Stream Name: " + stream.name
    #print "Metadata: " + repr(experiment.metadata)
    






