"""@package sensordb_api
Low Level SensorDB functions
"""

import requests
import json
import time
import datetime
from calendar import timegm
#import pytz

class metaBase(object):
    """metaBase acts as a base class for other objects that contain sensorDB data.
    It contains functions that interface with the metadata API.
    """
    
    def __init__(self, sensor_db, required_fields, **kwargs):
        """Performs initialisation common to all metaBase objects.
        Requires a SensorDB instance and a list of required fields as arguments. Takes a dictionary of values."""
        self.metadata = {}
        self._parent_db = sensor_db
        for key in kwargs.iterkeys():
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
        'id_field' is the name of the id field that the server is expecting (e.g. 'sid' for streams, 'eid' for experiments).
        'valid_fields' is a list of field names that are valid for the object being updated.
        The keyword arguments are the key/value pairs to be updated. Multiple fields can be updated with one call to the function.
        """

        for key in kwargs.iterkeys():
            if key in valid_fields:
                self._parent_db._session.put(self._parent_db._host + page, {id_field: self._id, key: kwargs[key]});
                # TODO - Check the return value for an error
            else:
                print "Warning - the field \"" + key + "\" is not recognised. It will be ignored."

        return 
    
    def metadata_add(self, name, value, **kwargs):
        """Adds a metadata entry to the current item.
        Name and value are required.
        Optional arguments are 'description', 'start_ts' and 'end_ts'.
        """
        payload = {"id": self._id, "name": name, "value": value}
        optional_fields = ["description", "start_ts", "end_ts"] 
        for key in kwargs.iterkeys():
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        
        # Because Python does not allow hyphens in variable names, underscore is accepted and replaced with a hyphen
        if "start_ts" in payload:
            payload["start-ts"] = payload.pop("start_ts")
        
        if "end_ts" in payload:
            payload["end-ts"] = payload.pop("end_ts")
        
        r = self._parent_db._session.get(self._parent_db._host + '/metadata/add', params=payload)
        if r.text == "":
            #self._parent_db.get_session();
            self.metadata_retrieve()
            return None        
        return r.text
    
    def metadata_remove(self, name):
        """Removes the metadata for the current object with the name provided."""
        payload = {"id": self._id, "name": name}
        r = self._parent_db._session.get(self._parent_db._host + '/metadata/remove', params=payload)
        if r.text == "":
            #self._parent_db.get_session();
            self.metadata_retrieve()
            return None
        return r.text
    
    def metadata_retrieve(self):
        """Retrieves and updates the metadata for the current object."""
        r = self._parent_db._session.get(self._parent_db._host + '/metadata/retrieve/' + self._id)        
        #self.metadata = json.loads(r.text)
        #self.metadata = r.json
        self.metadata = dict()
        
        """
        Create a dictionary of metadata values.
        This allows the metadata to be retrieved by name.
        To access the metadata as a list use metadata.values()
        """
        for metavalue in r.json:
            self.metadata[metavalue['name']] = metavalue
        
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
        """Creates a new experiment as the current user.
        Name and timezone are required.
        Optional arguments are: "description", "website", "picture" and "public_access"."""
        payload = {"name": name, "timezone": timezone}
        optional_fields = ["description", "website", "picture", "public_access"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        r = self._parent_db._session.post(self._parent_db._host + '/experiments', data=payload)
        
        returned_json = r.json

        if returned_json.has_key("errors"):
            raise Exception(returned_json["errors"])
        
        if returned_json is not None:
            # Create experiment object from returned data
            new_experiment = Experiment(self._parent_db, **returned_json)
            
            # Retrieving metadata is not strictly necessary for a new experiment. It should have no metadata. 
            new_experiment.metadata_retrieve();
            
            # Add the new experiment to parent objects
            self.experiments.append(new_experiment)
            self._parent_db.experiments.append(new_experiment)
            
            return new_experiment

        # The experiment was not successfully created
        return None
    
    def get_session(self):
        """Retrieves session data from the server and updates the local copy."""
        return self._parent_db.get_session(username=self.username)
    
    def get_tokens(self):
        """Retrieves a list of tokens from the server."""
        r = self._parent_db._session.get(self._parent_db._host + "/tokens")
        #return json.loads(r.text)
        return r.json


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
        valid_fields = ["name", "website", "description", "picture", "public_access"]
        return super(Experiment, self).update("/experiments", "eid", valid_fields, **kwargs)
    
    def create_node(self, name, **kwargs):
        """Creates a node within the current experiment.
        A unique name is required. """
        payload = {"name": name, "eid":self._id}
        optional_fields = ["description", "website", "picture", "lat", "lon", "alt"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        
        r = self._parent_db._session.post(self._parent_db._host + "/nodes", payload);
        #self._parent_db.get_session()
        if r.status_code == 200:
            #value_store = json.loads(r.text)
            value_store = r.json
            
            #print("Node creation: " + r.text)
            new_node = Node(self, **value_store)
            new_node._parent_db = self._parent_db
            
            self.nodes.append(new_node)
            self._parent_db._nodes.append(new_node)
            
            return new_node

        return None
        
        
    def delete(self):
        """Deletes the current experiment."""
        r = self._parent_db._session.delete(self._parent_db._host + "/experiments", params={"eid" : str(self._id)})
        self._parent_db.get_session();
        return r.text

        
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
        payload = {"name": name, "nid":self._id, "mid": mid}
        optional_fields = ["description", "website", "picture"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."        
        r = self._parent_db._session.post(self._parent_db._host + "/streams", payload);
        #self._parent_db.get_session();
        #print ("Creating node: " + r.text)
        if r.status_code == 200:
            #value_store = json.loads(r.text)
            value_store = r.json
            #print("Node creation: " + r.text)
            new_stream = Stream(self, **value_store)
            new_stream._parent_db = self._parent_db
            self.streams.append(new_stream)
            return new_stream
        else:
            return None
        
    
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
        r = self._parent_db._session.delete(self._parent_db._host + "/nodes", params={"nid" : str(self._id)})
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
        
    def delete(self):
        """Deletes the current stream."""
        r = self._parent_db._session.delete(self._parent_db._host + "/streams", params={'sid' : str(self._id)})
        self._parent_db.get_session();
        return r.text

    def get_data(self, start_date=None, end_date=None, level=None):
        """Returns data for this stream.
        Start date may be specified. If it is not the earliest date is used.
        End date may be specified. If it is not the latest date is used.
        Optionally the aggregation level may be specified as one of the following:
        raw, 1-minute, 5-minute, 15-minute, 1-hour, 3-hour, 6-hour, 1-day, 1-month, 1-year""" 
        payload = {"sid":self._id}
        
        '''
        optional_fields = ["sd", "ed", "level"] 
        for key in kwargs:
            if key in optional_fields:
                payload[key] = kwargs[key]
            else:
                print "Warning - The field \"" + key + "\" is not recognised. It will be ignored."
        '''
        
        if start_date is not None:
            payload["sd"] = start_date
        
        if end_date is not None:
            payload["ed"] = end_date
        
        if level is not None:
            payload["level"] = level
        
        r = self._parent_db._session.get(self._parent_db._host + "/data", params=payload)
        return r #The data output is not json compatible, so I had to change this. 

        #return json.loads(r.text)


    def post_dataframe(self, data_frame, time_col, value_col, tz=None):
        """Posts data that is stored in a dataFrame created by the PANDAS read_csv function.
        Requires timestamp and value column names and a stream token.
        tz - Optional timezone that is assigned to all timestamps.
        (Note: If timestamps do not already have timezone info and no timezone is given then local timezone is assumed.) 
        """
        
        # Stream token is enclosed in double quote-marks.
        stream_token = '"' + str(self.token) + '"'
        
        data = {stream_token:dict()}
        count = 0
        uncounted = 0
        
        for line_index in data_frame.index:

            # A single post cannot be larger than 200000 characters.
            # If there is enough data in the data frame then it can go over this limit.
            # Therefore the data should be separated into multiple posts
            # 5000 data points should be approximately 100000 characters.   
            if (len(data[stream_token]) >= 5000):

                r = self._parent_db.post_data(data)
                
                try:
                    count += r["length"]
                except:
                    raise Exception("Error posting data.")
                    #continue
                
                # Clear data for next post
                data = {stream_token:dict()}
            

            if tz is not None:
                # Adjust for timezone
                timestamp = data_frame[time_col][line_index].replace(tzinfo=tz)
            else:
                timestamp = data_frame[time_col][line_index]
            
            # Get the data timestamp in seconds since epoch and store it as a string
            if timestamp.tzinfo is not None:
                # If there is timezone information convert it to UTC first
                timestamp = timestamp.utctimetuple()
                timestamp = timegm(timestamp)
            else:
                # Otherwise assume the timestamp is in local time. 
                timestamp = timestamp.timetuple()
                timestamp = time.mktime(timestamp)
            
            # Note - Timestamps are enclosed in double quote-marks.  
            timestamp = "\"{:d}\"".format(int(timestamp))
            
            # Duplicate timestamps will not be uploaded more than once.
            # The final timestamp will be uploaded.            
            if timestamp in data[stream_token]:
                uncounted += 1
            
            
            # Store value in the data dict using the timestamp as a key
            data[stream_token][timestamp] = "{:.2f}".format(data_frame[value_col][line_index])
            # The database recognises 'null' instead of 'nan'
            if data[stream_token][timestamp] == 'nan':
                data[stream_token][timestamp] = 'null'
        
        r = self._parent_db.post_data(data)
        
        try:
            count += r["length"]
        except:
            raise Exception("Error posting data.")
            #continue
        
        
        if count != (data_frame.index.size - uncounted):
            print "Data Error  - Count was: " + str(count) + " Expected: " + str(data_frame.index.size - uncounted)

class SensorDB(object):
    """The SensorDB class handles the interface to a sensorDB server 
    """
    
    # Initialise with host address  
    def __init__(self, host, username=None, password=None):
        """The constructor for SensorDB.
        A host address must be specified. Optionally a username and password may be specified to log in immediately.
        """
        self._host = host
        self._session = requests.session() # Create new requests session to store cookies
        self.user = None
        self.experiments = []
        self._nodes = []
        self._streams = []
        
        if(username and password):
            self.login(username, password)

    def __convert_session(self, json_data):
        """This function is used internally by the SensorDB class to construct session data object instances from JSON text."""
        
        if json_data is None:
            return False
        
        #value_store = json.loads(json_text)
        value_store = json_data
            
        if 'user' in value_store:
            #print value_store['user']
            self.user = User(self, **value_store['user'])
            self.user.metadata_retrieve();
            
        if 'experiments' in value_store:
            self.experiments = []
            for experiment_data in value_store['experiments']:
                new_experiment = Experiment(self, **experiment_data)
                new_experiment.metadata_retrieve();
                self.experiments.append(new_experiment)
                
                if(new_experiment.uid == self.user._id):
                    self.user.experiments.append(new_experiment)
            
        if 'nodes' in value_store:
            self.nodes = []
            for node_data in value_store['nodes']:
                new_node = Node(self, **node_data)
                new_node.metadata_retrieve();
                
                self._nodes.append(new_node)
                
                for experiment in self.experiments:
                    if new_node.eid == experiment._id:
                        experiment.nodes.append(new_node)
                        break
                
            
        if 'streams' in value_store:
            self.streams = []
            for stream_data in value_store['streams']:
                new_stream = Stream(self, **stream_data)
                new_stream.metadata_retrieve();
                
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
        r = self._session.post(self._host + '/login', data=payload_login)
      
        if not (r.status_code in range (200, 300)):
            raise Exception("Login failed!")
        #self._cookie = r.cookies
        return self.__convert_session(r.json)
    
    #logs out and deletes the _cookie
    def logout(self):
        """Logs out the current user"""
        self._session.post(self._host + '/logout');
        #self._session.cookies.clear_session_cookies()
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
             
        r = self._session.post(self._host + '/register', data=payload)
        return r.text
    
    def remove(self, username, password):
        """Removes a registered user.
        Username and Password are required.
        """
        r = self._session.post(self._host + '/remove', data={'name' : username, 'password':password})
        return r.text
    
    # --- User Access API ---
    
    # Returns session data
    def get_session(self, username=None):
        """Gets session data.
        Gets data for a particular user or the current user if none is specified
        """
        if username is None:
            r = self._session.get(self._host + '/session2')
        else:
            r = self._session.get(self._host + '/session2', {"username" : username})
        return self.__convert_session(r.json)
    
    def get_users(self):
        """Gets the user list."""
        r = self._session.get(self._host + "/users")
        return r.text
    

    def get_measurements(self):
        """Gets all measurements.
        Measurements are returned as an array of dictionaries. Each dictionary contains information on a single measurement."""
        r = self._session.get(self._host + "/measurements")
        return json.loads(r.text)
    
    def post_data(self, data):
        """Posts data to the Database.
        Data should be a dictionary that contains stream data."""
        
        '''
        Note - Items within 'data' are usually stored as strings.
        The single-quotes around each item are removed after the dict is converted to a string.
        This is so the data is interpreted correctly.
        The main issue is that "'null'" (with quotes) is not considered the same as "null". 
        '''
        payload = {"data":str(data).replace("'", "")}
        
        r = self._session.post(self._host + '/data', payload)
        return r.json


    
if (__name__ == '__main__'):
    print "Testing SensorDB API"
    
    host = "http://phenonet.com:9002"
    
    print "Connecting to host: {0}".format(host)
    sensor_test = SensorDB(host)
    
    print "Registering new user"
    print sensor_test.register('testUser', 'password', 'testUser@example.com')
    
    print "Logging in with new user"
    print sensor_test.login('testUser', 'password')
    
    print "Retrieving session"
    print sensor_test.get_session()
    
    print "Logging out"
    sensor_test.logout()
    
    print "Removing user"
    print sensor_test.remove('testUser', 'password')
