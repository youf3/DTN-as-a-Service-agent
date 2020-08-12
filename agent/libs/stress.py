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
        modes = ['read', 'write']
        #read_seq = write_seq = None
        seq = {}        
        
        logging.debug('Running stress test')
        logging.debug('args {}'.format(optional_args))

        try:
            for j in modes:
                seq[j] = sorted([int(i) for i in optional_args[j]])            
        except Exception:
            raise Exception('Sequence has to be numbers')        

        if len(seq) == 0:
            raise Exception('Either read or write is required')             
        
        fsize = optional_args['size']

        os.makedirs(os.path.dirname(dstfile), exist_ok=True)
        with open('bench.fio', 'w') as fh:            
            fh.writelines('[global]\nname=fio-seq-write\nbs=1m\ndirect=1\nioengine=sync\niodepth=16'
            '\ngroup_reporting=1\ntime_based\nfilename={}\nsize={}\n\n'.format(dstfile, fsize))            
            for i in modes:
                prev_time = 0
                for j in range(0, len(seq[i])-1):
                    duration = seq[i][j+1] - seq[i][j]
                    speed = optional_args[i][str(seq[i][j])]
                    if speed != '0':
                        fh.writelines('[{0}_{4}]\nruntime={1}\nstartdelay={2}\nrate={3}\nrw={4}\n\n'.format(j, duration,prev_time,speed,i))
                    prev_time = prev_time + duration
        
        proc = subprocess.Popen(['fio', 'bench.fio'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
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
            os.remove('bench.fio')
            stress.running_thread = None
        