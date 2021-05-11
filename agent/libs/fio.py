from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os, glob

class fio(TransferTools):   

    running_threads = {}
    proc_index = 0

    def __init__(self, **optional_args) -> None:
        return 

    def run_sender(self, srcfile, **optional_args):
        # raise NotImplementedError

        iomode = 'write'
        if 'iomode' in optional_args:
            if optional_args['iomode'].lower() != 'read' and optional_args['iomode'].lower() != 'write':
                raise Exception('io mode has to be read or write')
            iomode = optional_args['iomode']
        
        return {'result': True, 'size' : os.path.getsize(srcfile), 'iomode': iomode}

    def run_receiver(self, address, dstfile, **optional_args):               
        logging.debug('Running fio test')
        logging.debug('args {}'.format(optional_args))        

        if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
            blocksize = optional_args['blocksize']
        else:
            blocksize = 8192        
        
        iomode = 'write'
        if 'iomode' in optional_args:
            if optional_args['iomode'].lower() != 'read' and optional_args['iomode'].lower() != 'write':
                raise Exception('io mode has to be read or write')
            iomode = optional_args['iomode']

        os.makedirs(os.path.dirname(dstfile), exist_ok=True)        
        
        proc = subprocess.Popen(['fio', '--thread', '--direct=1', '--rw=%s'%iomode,  '--ioengine=sync', '--bs=%sk'%blocksize, '--iodepth=32', 
        '--name=index_%s'% fio.proc_index, '--filename=%s'%dstfile ], stdout = sys.stdout, stderr = sys.stdout)
        fio.running_threads[fio.proc_index] = proc
        fio.proc_index += 1
        return {'result': True, 'cport' : fio.proc_index-1}

    @classmethod
    def free_port(cls, port, **optional_args):
        threads = fio.running_threads
        err_thread = threads.pop(port)
        err_thread.kill()
        err_thread.kill()
        err_thread.communicate()
        pass

    @classmethod
    def poll_progress(cls, **optional_args):
        
        if 'cport' not in optional_args:
            logging.error('cport (index) is required')
            raise Exception('cport (index) is required')
        elif 'node' not in optional_args:
            logging.error('Node not found')
            raise Exception('Node not found')

        if optional_args['node'] == 'sender':
            return 0
        logging.debug('polling fio index {}'.format(optional_args['cport']))

        if len(fio.running_threads) < 1 or optional_args['cport'] not in fio.running_threads:
            raise Exception('fio is not running')

        proc = fio.running_threads[optional_args['cport']]
        proc.communicate(timeout=None)
        del fio.running_threads[optional_args['cport']]

        if optional_args['dstfile'] == None:
            return proc.returncode, None
        else:    
            return proc.returncode, os.path.getsize(optional_args.pop('dstfile'))        
        
    @classmethod
    def cleanup(cls, **optional_args):
        logging.debug('cleaning up fio threads')
        try:
            for _, proc in fio.running_threads.items():
                TransferTools.kill_proc_tree(proc.pid)
                proc.communicate()                
        finally:            
            fio.running_threads = {}