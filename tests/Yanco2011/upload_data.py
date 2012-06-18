from pandas import *
import sys
import time
import requests

sys.path.append('../..')

from sensordb_api import *

nodeFile = 'YAN_node_index.csv'
dataFile = 'YAN_crop_data_sub.csv'

nodes = read_csv(nodeFile,index_col=2)
data = read_csv(dataFile,index_col=[1,0],parse_dates=True)

host = "http://phenonet.com:9001"
username = ""
password = ""



experimentName = "Yanco2011"


def checkExperiment():
    for experiment in test_db.experiments:
        if experiment.name==experimentName:
            print("The experiment exists")
            return
    r = test_db.user.create_experiment(experimentName,"Australia/Sydney", description="Sample data from MEF Yanco 2011")
    print("Experiment created")

def postData(stream):
    token =  str(stream.token)
    test_data = data['temp_obj'][stream.metadata['MAC']['value']]
    payload_data = {}
    i=0
    for ts in test_data.index:
        #if i==20: break
        tt=int(time.mktime(ts.timetuple()))
        value = test_data[ts]
        i+=1
        payload_data[str(tt)] = value
        payload_tk = {}

    payload_tk[token] = payload_data
    
    payload = { 'data': str(payload_tk).replace("'","\"")}
    print(payload)
    r = requests.post(host + '/data', payload, cookies = test_db._cookie)
    print(r.text)
    

test_db = SensorDB(host, username, password)
checkExperiment()

#test_db.experiments[1].nodes[2].delete()


for node in nodes.index:
    #filtered_node = "sn"+str(nodes['Name'][node]).replace(".","")
    new_node = test_db.experiments[0].create_node(nodes['Name'][node],lat=nodes['Lat'][node],lon=nodes['Lon'][node])
    if new_node != None:
        new_node.metadata_add('Genotype',nodes['Treatment1'][node])
        new_node.metadata_add('Treatment',nodes['Treatment2'][node])
        new_node.metadata_add('NodeID',nodes['nodeID'][node])
        print("Creating: " + nodes['Name'][node])
        new_stream = new_node.create_stream("CanopyTemp","4fbefb0312ddf207cbe9a365")
        if new_stream != None:
            print("Creating Stream: CanopyTemp")
            new_stream.metadata_add('MAC',node)
            postData(new_stream)
        else:
            print("Error creating Stream")
    else:
        print("Error creating node")
    




