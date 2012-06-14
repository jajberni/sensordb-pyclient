from pandas import *
import sys
sys.path.append('../..')

from sensordb_api import *

nodeFile = 'YAN_node_index.csv'
nodes = read_csv(nodeFile,index_col=2)

host = "http://phenonet.com:9001"
username = ""
password = ""



experimentName = "Yanco2011h"


def checkExperiment():
    for name in test_db.experiments:
        if name==experimentName:
            print("The experiment exists")
            return
    r = test_db.user.create_experiment(experimentName,"Australia/Sydney")
    print("Experiment created")



test_db = SensorDB(host, username, password)
checkExperiment()

#test_db.experiments[1].nodes[2].delete()


for node in nodes['Name']:
    filtered_node = "sn"+str(node).replace(".","")
    new_node = test_db.experiments[8].create_node(filtered_node)
    if new_node != None:
        print("Creating: " + filtered_node)
        new_stream = new_node.create_stream("CanopyTemp","4fbefb0312ddf207cbe9a365")
        if new_stream != None:
            print("Creating Stream: CanopyTemp")
        else:
            print("Error creating Stream")
    else:
        print("Error creating node")
    
    




