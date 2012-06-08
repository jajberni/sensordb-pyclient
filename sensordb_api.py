"""@package sensordb_api
Low Level SensorDB functions 
"""

import requests

class SensorDB:
    """The SensorDB class handles the interface to a sensorDB server 
    """
    
    # --- User Management API ---
    
    # Initialise with host address  
    def __init__(self, host, username = None, password = None):
        """The constructor for SensorDB.
        A host address must be specified. Optionally a username and password may be specified to log in immediately.
        """
        self.host = host
        self.cookie = None
        if(username and password):
            self.login(username, password)
    
    # logs in to the server and stores the login cookie
    def login(self, username, password):
        """Log in to the SensorDB server.
        A username and password must be specified.
        Returns the session data if successful.
        """
        payload_login = {'name' : username, 'password':password}
        r = requests.post(self.host + '/login', data=payload_login)
        self.cookie = r.cookies
        return r.text
    
    #logs out and deletes the cookie
    def logout(self):
        """Logs out the current user"""
        requests.post(self.host + '/logout');
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
             
        r = requests.post(self.host + '/login', data = payload)
        return r.text 
    
    def remove(self, username, password):
        """Removes a regitered user.
        Username and Password are required.
        """
        r = requests.post(self.host + '/remove', data={'name' : username, 'password':password})
        return r.text
    
    # --- User Access API ---
    
    # Returns session data
    def get_session(self, username = None):
        """Gets session data.
        Gets data for a particular user or the current user if none is specified
        """
        if username == None:
            r = requests.get(self.host + '/session', cookies = self.cookie)
        else:
            r = requests.get(self.host + '/session', cookies = self.cookie, data = {"username" : username})
        return r.text
    
    def get_users(self):
        """Gets the user list."""
        r = requests.get(self.host + "/users")
        return r.text
    
    
    
if (__name__ == '__main__'):
    sensor_test = SensorDB("http://127.0.0.1:2000")
    
    #print sensor_test.register('username', 'password', 'username@example.com')
    
    print sensor_test.login('username', 'password')
    
    print sensor_test.get_session()
    
    