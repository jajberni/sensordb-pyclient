"""@package sensordb_api
Low Level SensorDB functions 
"""

import requests
import json

#TODO - Perform initialisation checks in the User, Experiment, Node and Stream objects to make sure the minimum required variables exist. 

# Check that an object was initialised with all of the required variables
def _field_check(check_object, required_fields):
    for field in required_fields:
        if not field in vars(check_object):
            # Should this create a variable containing 'None' instead of raising an error?
            raise Exception("Missing input field: " + field)

class User(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
            
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at"]
        _field_check(self, required_fields)

    def create_experiment(self, name, timezone, **kwargs):
        payload = {"name": name, "timezone": timezone}
        optional_fields = ["description", "website", "picture", "public_access"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        r = requests.post(self.__parent_db._host + '/experiments', cookies = self.cookie, data = payload)
        
        #TODO - examine r to determine success
        
        #Refresh object data - There should now be a new experiment 
        self.__parent_db.get_session()
        
        return r.text
    
    def get_session(self):
        return self.__parent_db.get_session(username = self.username)
    
    def get_tokens(self):
        r = requests.get(self.__parent_db._host + "/tokens")
        return json.loads(r.text)


class Experiment(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        self.metadata = {} # Create blank metadata variable in case it is not in the json file
        self.nodes = []
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
        
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "metadata", "uid", "timezone"]
        _field_check(self, required_fields)
        
          
    def update(self, **kwargs):
        valid_fields = ["name", "website", "description", "picture", "access_restriction"]
        
        for key in kwargs:
            if key in valid_fields:
                requests.put(self.__parent_db._host + "/experiments", {key: kwargs[key]});
                # TODO - Check the return value for an error
        return 
    
    def create_node(self, name, **kwargs):
        payload = {"name": name, "eid":self._id}
        optional_fields = ["description", "website", "picture", "lat", "lon", "alt"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        requests.post(self.__parent_db._host + "/nodes", payload);
        
        self.__parent_db.get_session()
        
        return

        
class Node(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        self.streams = []
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]

        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "metadata", "uid", "eid", "alt", "lat", "lon"]
        _field_check(self, required_fields)
    
    
    def create_stream(self, name, **kwargs):
        payload = {"name": name, "nid":self._id}
        optional_fields = ["description", "website", "picture", "mid"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        requests.post(self.__parent_db._host + "/nodes", payload);
    
    def delete(self):
        r = requests.delete(self.__parent_db._host + "/nodes", {"nid" : self.nid})
        self.__parent_db.get_session()
        return r.text
      

class Stream(object):
    def __init__(self, sensor_db, **kwargs):
        self.__parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
            
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "mid", "uid", "nid"]
        _field_check(self, required_fields)


    def get_measurements(self):
        """Gets measurement data associated with the stream."""
        return requests.get(self.__parent_db._host + "/measurements", {"mid": self.mid})

    def get_data(self, start_date, end_date, level = None):
        payload = {"sd": start_date, "ed": end_date, "sid":self._id}
        
        if level != None:
            payload["level"] = level
        
        r = requests.get(self.__parent_db._host + "/data", data = payload)

        return json.loads(r.text)

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
        self._nodes = []
        self._streams = []
        
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
                new_node = Node(self, **node_data)
                
                self._nodes.append(new_node)
                
                for experiment in self.experiments:
                    if new_node.eid == experiment._id:
                        experiment.nodes.append(new_node)
                        break
                
            
        if 'streams' in value_store:
            self.streams = []
            for stream_data in value_store['streams']:
                new_stream = Stream(self, **stream_data)
                
                self._streams.append(new_stream)
                
                for node in self._nodes:
                    if new_stream.nid == node._id:
                        node.streams.append(new_stream)
                
        
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
    

    
if (__name__ == '__main__'):
    sensor_test = SensorDB("http://127.0.0.1:2000")
    
    #print sensor_test.register('username', 'password', 'username@example.com')
    
    print sensor_test.login('username', 'password')
    
    print sensor_test.get_session()
    
    