from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os

class stress_cpu(TransferTools):   

    sysbench_running_thread = None

    def __init__(self, **optional_args) -> None:
        return

    def run_sender(self, srcfile, **optional_args):
        raise NotImplementedError
        pass

    def free_port(self, port, **optional_args):
        raise NotImplementedError
        pass

    def run_receiver(self, address, dstfile, **optional_args):
        seq = {}        
        
        logging.debug('Running stress cpu test')
        logging.debug('args {}'.format(optional_args))

        try:        
            seq['cpu'] = sorted([int(i) for i in optional_args['cpu']]) 
        except Exception:
            raise Exception('Sequence has to be numbers')

        cmd = ''
        for i in ['cpu']:
            prev_time = 0
            for j in range(0, len(seq[i])-1):
                threads = optional_args[i][str(seq[i][j])]
                if threads == '0' : continue
                duration = seq[i][j+1] - seq[i][j]
                cmd += 'sysbench --threads={0} --time={1} cpu run;'.format(threads, duration)                
    
        proc = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
        stress_cpu.sysbench_running_thread = proc
        return {'result': True}

    @classmethod
    def poll_progress(cls, **optional_args):
        if stress_cpu.sysbench_running_thread == None:
            raise Exception('stress_cpu is not running')

        stress_cpu.sysbench_running_thread.communicate(timeout=None)
        
    @classmethod
    def cleanup(cls, **optional_args):
        logging.debug('cleaning up thread {}'.format(stress_cpu.sysbench_running_thread.pid))
        try:
            TransferTools.kill_proc_tree(stress_cpu.sysbench_running_thread.pid)
            stress_cpu.sysbench_running_thread.communicate()
        finally:
            if os.path.exists('bench.fio'): os.remove('bench.fio')
            stress_cpu.sysbench_running_thread = None
        