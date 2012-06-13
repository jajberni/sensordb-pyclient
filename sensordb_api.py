"""@package sensordb_api
Low Level SensorDB functions 
"""

import requests
import json


class metaBase(object):
    """metaBase acts as a base class for other objects that contain sensorDB data.
    It contains functions that interface with the metadata API.
    """
    
    def __init__(self, sensor_db, required_fields, **kwargs):
        """Performs initialisation common to all metaBase objects.
        Requires a SensorDB instance and a list of required fields as arguments. Takes a dictionary of values."""
        self.metadata = {}
        self._parent_db = sensor_db
        for key in kwargs:
            # Copy each variable in the dictionary as a variable
            vars(self)[key] = kwargs[key]
            
        # Check that the object was initialised with all of the required variables
        for field in required_fields:
            if not field in vars(self):
                # Should this create a variable containing 'None' instead of raising an error?
                raise Exception("Missing input field: " + field)
    
    def update(self, page, id_field, valid_fields, **kwargs):
        """This is a common method to update data.
        Derivative classes should call this method to update data in the database.
        'page' is the url of the page not including the hostname.
        'id_field' is the name of the id field that the server is expecting (e.g. 'sid' for streams).
        'valid_fields' is a list of field names that are valid for this object.
        Keyword arguments should be of the form "field='value'". Multiple fields can be updated with one call to the function.
        """

        for key in kwargs:
            if key in valid_fields:
                requests.put(self._parent_db._host + page, {id_field: self._id, key: kwargs[key]}, cookies = self._parent_db._cookie);
                # TODO - Check the return value for an error
            else:
                print "Warning - the field \"" + key + "\" is not recognised. It will be ignored."

        return 
    
    def metadata_add(self, name, value, **kwargs):
        """Adds a metadata entry to the current item.
        Name and value are required.
        Optional arguments are 'description', 'start-ts' and 'end-ts'.
        """
        payload = {"name": name, "value": value}
        optional_fields = ["description", "start-ts", "end-ts"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        r = requests.get(self._parent_db._host + '/metadata/add', payload, cookies = self._parent_db._cookie)
        return r.text
    
    def metadata_remove(self):
        """Removes the metadata for the current object."""
        payload = {"id": self._id, "name": self.name}
        r = requests.get(self._parent_db._host + '/metadata/remove', payload, cookies = self._parent_db._cookie)
        if r.text == "":
            self._parent_db.get_session();
            return None
        return r.text
    
    def metadata_retrieve(self):
        """Retrieves and updates the metadata for the current object."""
        r = requests.get(self._parent_db._host + '/metadata/' + self._id, cookies = self._parent_db._cookie)
        self.metadata = json.loads(r.text)
        return


class User(metaBase):
    """The User class.
    This contains information about the user including a list of their experiments."""
    def __init__(self, sensor_db, **kwargs):
        """Initialises the Stream object.
        Requires a SensorDB instance as an argument. Takes a dictionary of values."""
        self.experiments = []
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at"]
        super(User, self).__init__(sensor_db, required_fields, **kwargs);

    def create_experiment(self, name, timezone, **kwargs):
        payload = {"name": name, "timezone": timezone}
        optional_fields = ["description", "website", "picture", "public_access"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        r = requests.post(self._parent_db._host + '/experiments', cookies = self._parent_db._cookie, data = payload)
        
        #TODO - examine r to determine success
        
        #Refresh object data - There should now be a new experiment 
        self._parent_db.get_session()
        
        return r.text
    
    def get_session(self):
        """Retrieves session data from the server and updates the local copy."""
        return self._parent_db.get_session(username = self.username)
    
    def get_tokens(self):
        """Retrieves a list of tokens from the server."""
        r = requests.get(self._parent_db._host + "/tokens", cookies = self._parent_db._cookie)
        return json.loads(r.text)


class Experiment(metaBase):
    """Experiment object class.
    This class contains methods and data pertaining to experiments within SensorDB. 
    """
    def __init__(self, sensor_db, **kwargs):
        """Initialises the Experiment object.
        Requires a SensorDB instance as an argument. Takes a dictionary of values."""
        self.nodes = []
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "uid", "timezone"]
        super(Experiment, self).__init__(sensor_db, required_fields, **kwargs);
        
          
    def update(self, **kwargs):
        """Updates experiment data.
        Arguments should be of the form "field='value'".
        Multiple fields can be updated using a single call.
        e.g. update(description="A new description", website="example.com/new")
        """
        valid_fields = ["name", "website", "description", "picture", "access_restriction"]
        return super(Experiment, self).update("/experiments", "eid", valid_fields, **kwargs)
    
    def create_node(self, name, **kwargs):
        """Creates a node within the current experiment"""
        payload = {"name": name, "eid":self._id}
        optional_fields = ["description", "website", "picture", "lat", "lon", "alt"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        requests.post(self._parent_db._host + "/nodes", payload, cookies = self._parent_db._cookie);
        
        self._parent_db.get_session()
        
        return

        
class Node(metaBase):
    def __init__(self, sensor_db, **kwargs):
        """Initialises the Node object.
        Requires a SensorDB instance as an argument. Takes a dictionary of values."""
        self.streams = []
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "uid", "eid", "alt", "lat", "lon"]
        super(Node, self).__init__(sensor_db, required_fields, **kwargs);
    
    
    def create_stream(self, name, mid, **kwargs):
        """Creates a new stream in the current node.
        Requires a name. Optional arguments are:
        "description", "website", "picture" and "mid"."""
        payload = {"name": name, "mid": mid, "nid":self._id}
        optional_fields = ["description", "website", "picture"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        requests.post(self._parent_db._host + "/nodes", payload, cookies = self._parent_db._cookie);
        self._parent_db.get_session();
    
    def update(self, **kwargs):
        """Updates node data.
        Arguments should be of the form "field='value'".
        Multiple fields can be updated using a single call.
        e.g. update(description="A new description", website="example.com/new")
        """
        valid_fields = ["website", "description", "picture", "lat", "lon", "alt", "eid"]
        return super(Node, self).update("/nodes", "nid", valid_fields, **kwargs)
    
    def delete(self):
        """Deletes the current node."""
        r = requests.delete(self._parent_db._host + "/nodes", {"nid" : self.nid}, cookies = self._parent_db._cookie)
        self._parent_db.get_session();
        return r.text
      

class Stream(metaBase):
    def __init__(self, sensor_db, **kwargs):
        """Initialises the Stream object.
        Requires a SensorDB instance as an argument. Takes a dictionary of values."""
        required_fields = ["_id", "name", "picture", "website", "description", "created_at", "updated_at", "mid", "uid", "nid"]
        super(Stream, self).__init__(sensor_db, required_fields, **kwargs);
        return

    def update(self, **kwargs):
        """Updates stream data.
        Arguments should be of the form "field='value'".
        Multiple fields can be updated using a single call.
        e.g. update(description="A new description", website="example.com/new")
        """
        valid_fields = ["name", "website", "description", "picture", "mid", "nid"]
        return super(Stream, self).update("/streams", "sid", valid_fields, **kwargs)

    def get_measurements(self):
        """Gets measurement data associated with the stream."""
        r = requests.get(self._parent_db._host + "/measurements", {"mid": self.mid}, cookies = self._parent_db._cookie)
        return json.loads(r.text)

    def get_data(self, start_date, end_date, level = None):
        """Returns data for this stream between the start and end dates.
        Optionally the aggregation level may be specified as one of the following:
        raw, 1-minute, 5-minute, 15-minute, 1-hour, 3-hour, 6-hour, 1-day, 1-month, 1-year""" 
        payload = {"sd": start_date, "ed": end_date, "sid":self._id}
        
        if level != None:
            payload["level"] = level
        
        r = requests.get(self._parent_db._host + "/data", payload, cookies = self._parent_db._cookie)

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
        self._cookie = None
        self.user = None
        self.experiments = []
        self._nodes = []
        self._streams = []
        
        if(username and password):
            self.login(username, password)

    def __convert_session(self, json_text):
        """This function is used internally by the SensorDB class to construct session data object instances from JSON text."""
        
        if json_text == "":
            return False
        
        value_store = json.loads(json_text)
            
        if 'user' in value_store:
            print value_store['user']
            self.user = User(self, **value_store['user'])
            #self.user.metadata_retrieve();
            
        if 'experiments' in value_store:
            self.experiments = []
            for experiment_data in value_store['experiments']:
                new_experiment = Experiment(self, **experiment_data)
                #new_experiment.metadata_retrieve();
                self.experiments.append(new_experiment)
                
                if(new_experiment.uid == self.user._id):
                    self.user.experiments.append(new_experiment)
            
        if 'nodes' in value_store:
            self.nodes = []
            for node_data in value_store['nodes']:
                new_node = Node(self, **node_data)
                #new_node.metadata_retrieve();
                
                self._nodes.append(new_node)
                
                for experiment in self.experiments:
                    if new_node.eid == experiment._id:
                        experiment.nodes.append(new_node)
                        break
                
            
        if 'streams' in value_store:
            self.streams = []
            for stream_data in value_store['streams']:
                new_stream = Stream(self, **stream_data)
                #new_stream.metadata_retrieve();
                
                self._streams.append(new_stream)
                
                for node in self._nodes:
                    if new_stream.nid == node._id:
                        node.streams.append(new_stream)
                        
        return True
    
    # --- User Management API ---
    
    # logs in to the server and stores the login _cookie
    def login(self, username, password):
        """Log in to the SensorDB server.
        A username and password must be specified.
        Returns the session data if successful.
        """
        payload_login = {'name' : username, 'password':password}
        r = requests.post(self._host + '/login', data=payload_login)
        self._cookie = r.cookies
        return self.__convert_session(r.text)
    
    #logs out and deletes the _cookie
    def logout(self):
        """Logs out the current user"""
        requests.post(self._host + '/logout');
        self._cookie = None
        return
    
    #Registers a new user
    def register(self, name, password, email, **kwargs):
        """Registers a new user.
        Name, password and email are required. Description, picture and websites are all optional.
        """
        payload = {'name' : name, 'password' : password, "email" : email}
        
        optional_fields = ["description", "website", "picture"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
             
        r = requests.post(self._host + '/login', data = payload)
        return r.text
    
    def remove(self, username, password):
        """Removes a registered user.
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
            r = requests.get(self._host + '/session', cookies = self._cookie)
        else:
            r = requests.get(self._host + '/session', {"username" : username}, cookies = self._cookie)
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
    
    