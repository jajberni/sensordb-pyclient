from pandas import *
import sys
sys.path.append('../..')

from sensordb_api import *

nodeFile = 'YAN_node_index.csv'
nodes = read_csv(nodeFile,index_col=2)

default_host = "http://phenonet.com:9001"
username = "jajberni"
password = "bernido"



experimentName = "Yanco2011"


def checkExperiment():
    for name in test_db.experiments:
        if name==experimentName:
            print("The experiment exists")
            return
    r = test_db.user.create_experiment(experimentName,"Australia/Sydney")
    print("Experiment created")



test_db = SensorDB(host, username, password)
checkExperiment()

for node in nodes['Name']:
    test_db.user.experiments[1].create_node(node)
    print("Creating: " + node)
    
    




