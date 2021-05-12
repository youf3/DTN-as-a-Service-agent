from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os

class msrsync(TransferTools):   

    running_thread = None

    def __init__(self,**optional_args ) -> None:
        return

    def run_sender(self, srcfile, **optional_args):
        raise NotImplementedError
        pass

    def free_port(self, port, **optional_args):
        raise NotImplementedError
        pass

    def run_receiver(self, address, dstfile, **optional_args):
        
        logging.debug('Running msrsync')
        logging.debug('args {}'.format(optional_args))

        if not os.path.isdir(address):
            raise Exception('address should be a path to srcdir')
        
        parallel = 1
        if 'parallel' in optional_args and type(optional_args['parallel']) == int:
            parallel = optional_args['parallel']        

        cmd = 'msrsync -p {} -P {} {}'.format(parallel, address, dstfile)
        logging.debug(cmd)
        proc = subprocess.Popen('exec ' + cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        msrsync.running_thread = proc
        return {'result': True}

    @classmethod
    def poll_progress(cls, **optional_args):        
        if msrsync.running_thread == None:
            raise Exception('msrsync is not running')

        msrsync.running_thread.communicate(timeout=None)
        
    @classmethod
    def cleanup(cls, **optional_args):
        logging.debug('cleaning up thread {}'.format(msrsync.running_thread.pid))
        TransferTools.kill_proc_tree(msrsync.running_thread.pid)
        msrsync.running_thread.communicate()