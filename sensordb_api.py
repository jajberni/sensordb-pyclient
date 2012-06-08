"""@package sensordb_api
Low Level SensorDB functions 
"""

import requests
import json

#TODO - Perform initialisation checks in the User, Experiment, Node and Stream objects to make sure the minimum required variables exist. 

class User(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
    
    def get_session(self):
        return self.__parent_db.get_session(username = self.username)
    
    

class Experiment(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
        
class Node(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
            
    def delete(self):
        return requests.delete(self.__parent_db._host + "/nodes", {"nid" : self.nid})  

class Stream(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]     

    def get_measurments(self):
        """Gets measurement data associated with the stream."""
        return requests.get(self.__parent_db._host + "/measurements", {"mid": self.mid})




class SensorDB(object):
    """The SensorDB class handles the interface to a sensorDB server 
    """
    
    # Initialise with host address  
    def __init__(self, host, username = None, password = None):
        """The constructor for SensorDB.
        A host address must be specified. Optionally a username and password may be specified to log in immediately.
        """
        self._host = host
        self.cookie = None
        self.user = None
        self.experiments = []
        self.nodes = []
        self.streams = []
        
        if(username and password):
            self.login(username, password)
            
    
    

    def __convert_session(self, json_text):
        #TODO - Check IDs against old data so new objects aren't created every time.
        #TODO - Streams should be stored in their parent nodes, nodes should be 
        #    stored in experiments, experiments should be stored in the user object.
        
        if json_text == "":
            return None
        
        value_store = json.loads(json_text)
            
        if 'user' in value_store:
            print value_store['user']
            self.user = User(self, **value_store['user'])
            
        if 'experiments' in value_store:
            self.experiments = []
            for experiment_data in value_store['experiments']:
                self.experiments.append(Experiment(self, **experiment_data))
            
        if 'nodes' in value_store:
            self.nodes = []
            for node_data in value_store['nodes']:
                self.nodes.append(Node(self, **node_data))
            
        if 'streams' in value_store:
            self.streams = []
            for stream_data in value_store['streams']:
                self.experiments.append(Stream(self, **stream_data))
        
        return
    
    # --- User Management API ---
    
    # logs in to the server and stores the login cookie
    def login(self, username, password):
        """Log in to the SensorDB server.
        A username and password must be specified.
        Returns the session data if successful.
        """
        payload_login = {'name' : username, 'password':password}
        r = requests.post(self._host + '/login', data=payload_login)
        self.cookie = r.cookies
        return self.__convert_session(r.text)
    
    #logs out and deletes the cookie
    def logout(self):
        """Logs out the current user"""
        requests.post(self._host + '/logout');
        self.cookie = None
        return
    
    #Registers a new user
    def register(self, name, password, email, description = None, picture = None, website = None):
        """Registers a new user.
        Name, password and email are required. Description, picture and websites are all optional.
        """
        payload = {'name' : name, 'password' : password, email : "email"}
        
        if description != None:
            payload['description'] = description
        
        if picture != None:
            payload['picture'] = picture
         
        if website != None:
            payload['website'] = website
             
        r = requests.post(self._host + '/login', data = payload)
        return r.text
    
    def remove(self, username, password):
        """Removes a regitered user.
        Username and Password are required.
        """
        r = requests.post(self._host + '/remove', data={'name' : username, 'password':password})
        return r.text
    
    # --- User Access API ---
    
    # Returns session data
    def get_session(self, username = None):
        """Gets session data.
        Gets data for a particular user or the current user if none is specified
        """
        if username == None:
            r = requests.get(self._host + '/session', cookies = self.cookie)
        else:
            r = requests.get(self._host + '/session', cookies = self.cookie, data = {"username" : username})
        return self.__convert_session(r.text)
    
    def get_users(self):
        """Gets the user list."""
        r = requests.get(self._host + "/users")
        return r.text
    
    # Put this in the user object?
    def create_experiment(self, name, timezone, description = None, website = None, picture = None, public_access = None):
        payload = {"name": name, "timezone": timezone}
        if description != None:
            payload["description"] = description
    
        if website != None:
            payload["website"] = website
        
        if picture != None:
            payload["picture"] = picture
            
        if public_access != None:
            payload["public_access"] = public_access
        
        r = requests.post(self._host + '/experiments', cookies = self.cookie, data = payload)
        
        #TODO - examine r to determine success
        
        #Refresh object data - There should now be a new experiment 
        self.get_session()
        
        return r.text
    
    
if (__name__ == '__main__'):
    sensor_test = SensorDB("http://127.0.0.1:2000")
    
    #print sensor_test.register('username', 'password', 'username@example.com')
    
    print sensor_test.login('username', 'password')
    
    print sensor_test.get_session()
    
    