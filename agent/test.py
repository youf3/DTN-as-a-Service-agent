from flask import Flask
from flask_testing import TestCase
import os
import app
import unittest
import tempfile
import time

def create_temp_file(tmpdir):
    with open(os.path.join(tmpdir, 'hello_world'), 'w') as fp:
        fp.write('Hello world!')
        fp.close()

def get_prom_metric(metric_name, in_text, index = 0):
    return [x.split(' ')[1] for x in in_text.splitlines() if metric_name in x][index]

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
        data = {
            'test2/hello_world' : {                
                'size' : '1M'
            }
        }  
        self.client.post('/create_file/', json=data)


        response = self.client.get('/files/')
        result = response.get_json()
        assert result is not None
        assert len(result) == 3        

    def test_create_file(self):
        data = {
            'hello_world' : {                
                'size' : '100M'
            },
            'hello_world2' : {
                'size' : '100M'
            }
        }  
        response = self.client.post('/create_file/', json=data)
        result = response.get_json()
        assert result == 0 

    def test_delete_file(self):
        response = self.client.delete('file/hello_world')        
        assert response.status_code == 200

    def test_create_dir(self):
        data = ['hello_dir', 'hello_dir2']  
        response = self.client.post('/create_dir/', json=data)        
        assert response.status_code == 200

        data = {'hello_dir/testfile' : {'size' : '1M'}}
        response = self.client.post('/create_file/',json=data)
        assert response.status_code == 200

        response = self.client.delete('file/hello_dir')        
        assert response.status_code == 200

        response = self.client.delete('file/hello_dir2')
        assert response.status_code == 200

    def test_create_many_files(self):
        num_files = 10        
        data = {}
        
        for i in range(num_files):
            data['file{}'.format(i)] = {'size' : '1M'}            
        response = self.client.post('/create_file/',json=data)
        result = response.get_json()

        response = self.client.delete('/file/*')
        result = response.get_json()

    def test_get_tools(self):
        response = self.client.get('/tools')
        result = response.get_json()
        assert 'nuttcp' in result

    def test_sendfile_nuttcp(self):
        data = {            
            'file' : 'hello_world',            
            'direct' : False,
            'blocksize' : 1
        }        
        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True

        # check prom metric for sender
        response = self.client.get('/metrics')
        data = response.data.decode()
        sender_counter = get_prom_metric('daas_agent_sender_total{status="200"}', data)
        assert sender_counter == '5.0'

        result['file'] = 'hello_world2'
        result['address'] = '127.0.0.1'
        result['direct'] = False
        result['blocksize'] = 1

        response = self.client.post('/receiver/nuttcp', json=result)
        result = response.get_json()
        assert result.pop('result') == True

        # check prom metric for receiver
        response = self.client.get('/metrics')
        data = response.data.decode()
        receiver_counter = get_prom_metric('daas_agent_receiver_total{status="200"}', data)
        assert receiver_counter == '6.0'

        cport = result.pop('cport')

        data = {
            'node' : 'receiver',
            'cport' : cport,
            'dstfile' : 'hello_world2'
        }

        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == [0, 12]

        data['node'] = 'sender'
        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == 0

        data['node'] = 'something'
        response = self.client.get('/nuttcp/poll', json=data)
        assert response.status_code == 400
        result = response.get_json()        
        assert result == {'message' : 'Exception: Node has to be either sender or receiver'}

        with open(os.path.join(self.tmpdirname.name, 'hello_world2'), 'r') as fp:
            contents = fp.readlines()
        assert contents == ['Hello world!']

        # check prom metric for transfer
        response = self.client.get('/metrics')
        data = response.data.decode()
        transfer_counter = get_prom_metric('daas_agent_num_transfers 0.0', data)
        assert transfer_counter == '0.0'

    def test_nuttcp_timeout(self):
        data = {
            'hello_world' : {                
                'size' : '10M'
            }
        }  
        response = self.client.post('/create_file/', json=data)
        result = response.get_json()
        assert result == 0 

        data = {            
            'file' : 'hello_world',            
            'direct' : False,
            'blocksize' : 1
        }        
        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True

        result['file'] = 'hello_world2'
        result['address'] = '127.0.0.1'
        result['direct'] = False
        result['blocksize'] = 1        

        response = self.client.post('/receiver/nuttcp', json=result)
        result = response.get_json()
        assert result.pop('result') == True        

        cport = result.pop('cport')

        data = {
            'node' : 'receiver',
            'cport' : cport,
            'dstfile' : 'hello_world2',
            'timeout' : 0
        }

        response = self.client.get('/nuttcp/poll', json=data)
        #result = response.get_json()        
        assert response.status_code == 400

        data['node'] = 'sender'
        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()
        assert result['file'] == 'hello_world'

    def test_free_port(self):       
        data = {            
            'file' : 'hello_world',            
            'direct' : False,
            'blocksize' : 1
        }        
        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True        
        
        cport = result['cport']
        response = self.client.get('/free_port/nuttcp/{}'.format(cport))
        result = response.get_json()        

    def test_msrsync_cleanup(self):

        self.test_create_file()

        data = {            
            'address' : os.path.join(self.tmpdirname.name, ''),
            'file' : 'msrsync'
        }

        response = self.client.post('/receiver/msrsync', json=data)
        result = response.get_json()
        assert result.pop('result') == True        

        time.sleep(1)

        response = self.client.get('/cleanup/msrsync')
        assert response.status_code == 200        

        response = self.client.get('/msrsync/poll', json={})
        
        assert response.status_code == 200

    def test_msrsync(self):       
        data = {            
            'address' : os.path.join(self.tmpdirname.name, ''),
            'file' : 'msrsync'
        }

        response = self.client.post('/receiver/msrsync', json=data)
        result = response.get_json()
        assert result.pop('result') == True
        
        # check prom metric for receiver
        response = self.client.get('/metrics')
        data = response.data.decode()
        receiver_counter = get_prom_metric('daas_agent_receiver_total{status="200"}', data)
        assert receiver_counter == '1.0'

        response = self.client.get('/msrsync/poll', json={})
        
        assert response.status_code == 200

        with open(os.path.join(self.tmpdirname.name, 'msrsync/hello_world'), 'r') as fp:
            contents = fp.readlines()
        assert contents == ['Hello world!']

    def test_msrsync_and_nuttcp(self):

        self.test_create_file()

        data = {            
            'address' : os.path.join(self.tmpdirname.name, ''),
            'file' : 'msrsync'
        }

        response = self.client.post('/receiver/msrsync', json=data)
        result = response.get_json()
        assert result.pop('result') == True
        
        data = {
            'file' : 'hello_world',            
            'direct' : False,
            'blocksize' : 1
        }        
        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True

        result['file'] = 'hello_world3'
        result['address'] = '127.0.0.1'
        result['direct'] = False
        result['blocksize'] = 1

        response = self.client.post('/receiver/nuttcp', json=result)
        result = response.get_json()
        assert result.pop('result') == True

        cport = result.pop('cport')

        data = {
            'node' : 'receiver',
            'cport' : cport,
            'dstfile' : 'hello_world2'
        }

        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == [0, 104857600]

        data['node'] = 'sender'
        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == 0

        response = self.client.get('/msrsync/poll', json={})
        
        assert response.status_code == 200

    def test_sendfile_nuttcp_numa(self):
        data = {            
            'file' : 'hello_world',            
            'direct' : False,
            'numa_scheme' : 2,
            'numa_node' : 0
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
            'cport' : cport,
            'numa_scheme' : 3,
            'numa_node' : 1,
            'dstfile' : 'hello_world2'
        }

        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == [0,12]

        data['node'] = 'sender'
        response = self.client.get('/nuttcp/poll', json=data)
        result = response.get_json()        
        assert result == 0

    def test_ping(self):
        response = self.client.get('/ping/127.0.0.1')
        result = float(response.get_json()['latency'])
        assert result is not None

    def test_trim(self):
        response = self.client.get('/trim')
        result = response.get_json()       
        assert result['returncode'] == 0

    def test_cleanup(self):
        data = {            
            'file' : 'hello_world',            
            'direct' : False,
            'numa_scheme' : 2,
            'numa_node' : 0,            
        }        

        response = self.client.post('/sender/nuttcp', json=data)
        result = response.get_json()
        assert result.pop('result') == True  

        response = self.client.get('/cleanup/nuttcp')
        assert response.status_code == 200

    def test_stressio(self):
        data = {
            'sequence' : {
                0: '100M',                
                10 : '0',
                20 : '10M'
                
            },
            'file':'disk0/fiotest',
            'size' : '1G',
            'address' : '',
            'iomode' : 'read'
        }
        response = self.client.post('/receiver/stress', json=data)
        result = response.get_json()
        assert result.pop('result') == True

        # response = self.client.get('/stress/poll', json={})
        # result = response.get_json()
        # assert response.status_code == 200

        response = self.client.get('/cleanup/stress')        
        assert response.status_code == 200

        response = self.client.get('/stress/poll', json={})
        assert response.status_code == 400

if __name__ == '__main__':
    unittest.main(verbosity=2)