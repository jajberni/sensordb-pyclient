import requests

class SensorDB:
    # Initialise with host address  
    def __init__(self, host, username = None, password = None):
        self.host = host
        self.cookie = None
        if(username and password):
            self.login(username, password)
    
    # logs in to the server and stores the login cookie
    def login(self, username, password):
        payload_login = {'name' : username, 'password':password}
        r = requests.post(self.host + '/login', data=payload_login)
        self.cookie = r.cookies
        return r.text
    
    #logs out and deletes the cookie
    def logout(self):
        requests.post(self.host + '/logout');
        self.cookie = None
        return
    
    #Registers a new user
    def register(self, name, password, email, description = None, picture = None, website = None):
        payload = {'name' : name, 'password' : password, email : "email"}
        
        if description != None:
            payload['description'] = description
        
        if picture != None:
            payload['picture'] = picture
         
        if website != None:
            payload['website'] = website
             
        r = requests.post(self.host + '/login', data = payload)
        return r.text 
    
    
    # Returns session data
    def get_session(self, username = None):
        if username == None:
            r = requests.get(self.host + '/session', cookies = self.cookie)
        else:
            r = requests.get(self.host + '/session', cookies = self.cookie, data = {"username" : username})
        return r.text
    
if (__name__ == '__main__'):
    sensor_test = SensorDB("http://127.0.0.1:2000")
    
    #print sensor_test.register('username', 'password', 'username@example.com')
    
    print sensor_test.login('username', 'password')
    
    print sensor_test.get_session()
    
    