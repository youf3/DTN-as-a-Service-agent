from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os

class stress(TransferTools):   

    running_thread = None

    def run_sender(self, srcfile, **optional_args):
        raise NotImplementedError
        pass

    def free_port(self, port, **optional_args):
        raise NotImplementedError
        pass

    def run_receiver(self, address, dstfile, **optional_args):               
        logging.debug('Running stress test')
        logging.debug('args {}'.format(optional_args))

        if 'sequence' in optional_args and type(optional_args['sequence']) != dict:
            raise Exception('Sequence to generate fileio is required')
        try:
            sequence_t = sorted([int(i) for i in optional_args['sequence']])
        except Exception:
            raise Exception('Sequence has to be numbers')
        
        fsize = optional_args['size']
        iomode = 'write'
        if 'iomode' in optional_args:
            if optional_args['iomode'].lower() != 'read' and optional_args['iomode'].lower() != 'write':
                raise Exception('io mode has to be read or write')
            iomode = optional_args['iomode']

        os.makedirs(os.path.dirname(dstfile), exist_ok=True)
        with open('bench.fio', 'w') as fh:            
            fh.writelines('[global]\nname=fio-seq-write\nrw={}\nbs=1m\ndirect=1\nioengine=sync\niodepth=16'
            '\ngroup_reporting=1\ntime_based\nfilename={}\nsize={}\n\n'.format(iomode,dstfile, fsize))
            prev_time = 0        
            for i in range(0, len(sequence_t)-1):
                duration = sequence_t[i+1] - sequence_t[i]
                speed = optional_args['sequence'][str(sequence_t[i])]
                if speed != '0':
                    fh.writelines('[{0}]\nruntime={1}\nstartdelay={2}\nrate={3}\n\n'.format(i, duration,prev_time,speed ))                    
                prev_time = prev_time + duration
        
        proc = subprocess.Popen(['fio', 'bench.fio'], stdout = sys.stdout, stderr = sys.stdout)
        stress.running_thread = proc
        return {'result': True}

    @classmethod
    def poll_progress(cls, **optional_args):        
        if stress.running_thread == None:
            raise Exception('stress is not running')

        stress.running_thread.communicate(timeout=None)        
        
    @classmethod
    def cleanup(cls):
        logging.debug('cleaning up thread {}'.format(stress.running_thread.pid))
        try:
            TransferTools.kill_proc_tree(stress.running_thread.pid)
            stress.running_thread.communicate()
        finally:
            if os.path.exists('bench.fio'): os.remove('bench.fio')
            stress.running_thread = None
        