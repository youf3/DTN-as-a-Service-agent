from flask import Flask
from flask_testing import TestCase
import os
import app
import unittest
import tempfile

def create_temp_file(tmpdir):
    with open(os.path.join(tmpdir, 'hello_world'), 'w') as fp:
        fp.write('Hello world!')
        fp.close()

class AgentTest(TestCase):

    def create_app(self):
        app.app.config['TESTING'] = True
        self.tmpdirname = tempfile.TemporaryDirectory()
        app.app.config['FILE_LOC'] = self.tmpdirname.name
        return app.app

    def setUp(self):
        create_temp_file(self.tmpdirname.name)

    def tearDown(self):
        self.tmpdirname.cleanup()

    def test_running(self):
        response = self.client.get('/')
        result = response.data
        assert result is not None
        assert result == b'The agent is running'

    def test_listfile(self):        
        response = self.client.get('/files/')
        result = response.get_json()
        assert result is not None
        assert len(result) == 1
        assert result[0]['name'] == 'hello_world'
        assert result[0]['size'] == 12

    def test_get_tools(self):
        response = self.client.get('/tools')
        result = response.get_json()
        assert 'nuttcp' in result

    def test_sendfile_nuttcp(self):
        data = {            
            'file' : 'hello_world',            
            'direct' : False
        }        
        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True

        result['file'] = 'hello_world2'
        result['address'] = '127.0.0.1'
        result['direct'] = False

        response = self.client.post('/receiver/nuttcp', json=result)
        result = response.get_json()
        assert result.pop('result') == True

        cport = result.pop('cport')

        data = {
            'node' : 'receiver',
            'port' : cport
        }

        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == {'return code' : 0}

        data['node'] = 'sender'
        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == {'return code' : 0}

        data['node'] = 'something'
        response = self.client.get('/nuttcp/poll', json=data)
        assert response.status_code == 400
        result = response.get_json()        
        assert result == {'message' : 'Exception: Node has to be either sender or receiver'}

        with open(os.path.join(self.tmpdirname.name, 'hello_world2'), 'r') as fp:
            contents = fp.readlines()
        assert contents == ['Hello world!']

if __name__ == '__main__':
    unittest.main(verbosity=2)