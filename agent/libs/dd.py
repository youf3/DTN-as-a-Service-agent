from libs.TransferTools import TransferTools, TransferTimeout
import subprocess
import logging
import sys, os, time

class dd(TransferTools):
    running_cli_threads = {}
    
    def __init__(self, numa_scheme = 1, **optional_args) -> None:        
        super().__init__(numa_scheme = numa_scheme)         

    def run_sender(self, srcfile, **optional_args):        
        logging.debug('running nothing for dd sender'.format())
        logging.debug('args {}'.format(optional_args))      
        
        return {'result': True, 'srcfile' : srcfile, 'size' : os.path.getsize(srcfile)}

    def run_receiver(self, address, dstfile, **optional_args):
        logging.debug('args {}'.format(optional_args))

        srcfile = optional_args['srcfile']        
        logging.debug('running dd file from {} to {}'.format(address, srcfile, dstfile))        

        if dstfile is None:            
            return {'result' : False, 'error' : 'No file Specified'}

        else:
            iflags = 'direct'
            oflags = 'direct'
            if 'direct' in optional_args and optional_args['direct'] == False:
                iflags = iflags.replace('direct', '')
                oflags = oflags.replace('direct', '')
            
            if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
                blocksize = optional_args['blocksize']
            else:
                blocksize = '128M'
            
            cmd = 'dd if={} iflag={} of={} oflag={} bs={}'.format(srcfile, iflags, dstfile, oflags, blocksize)
        logging.debug(cmd)
        proc = subprocess.Popen('exec ' + cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        if 'numa_node' in optional_args:
            super().bind_proc_to_numa(proc, optional_args['numa_node'])
        dd.running_cli_threads[proc.pid] = proc
        return {'pid' : proc.pid, 'result': True, 'cport' : proc.pid}

    @classmethod
    def free_port(cls, pid, **optional_args):        
        err_thread = dd.running_cli_threads[pid]
        err_thread[0].kill()
        err_thread[0].kill()
        err_thread[0].communicate()             

    @classmethod
    def poll_progress(cls, **optional_args):
        if not 'pid' in optional_args:
            logging.error('pid not found')
            raise Exception('pid not found')        

        if 'timeout' not in optional_args:
            timeout = None
        else:
            timeout = optional_args['timeout']
            
        pid = optional_args.pop('pid')
        
        if optional_args['node'] == 'sender':
            logging.debug('sender not supported')
            return 0
        elif optional_args['node'] == 'receiver':
            threads = dd.running_cli_threads
            # logging.debug('threads: ' + str(threads))
            try:                
                proc = dd.running_cli_threads[pid]
                proc.communicate(timeout=timeout)
                threads.pop(pid)
                if optional_args['dstfile'] == None:
                    return proc.returncode, None
                else:    
                    return proc.returncode, os.path.getsize(optional_args.pop('dstfile'))
            except subprocess.TimeoutExpired:
                err_thread = threads.pop(pid)
                err_thread[0].kill()
                err_thread[0].kill()
                err_thread[0].communicate()                
                logging.error('receiver timed out on port %s' % pid)
                raise Exception('receiver timed out on port %s' % pid)
            
        else:
            logging.error('Node has to be either sender or receiver')
            raise Exception('Node has to be either sender or receiver')        

    @classmethod
    def cleanup(cls, **optional_args):        
        for i in dd.running_cli_threads.items():
            i[1].kill()
            i[1].kill()
            i[1].communicate()

        dd.running_cli_threads = {}        

        # nuttcp.cports = list(range(nuttcp_port, nuttcp_port+ 999))
        # nuttcp.dports = list(range(nuttcp_port + 1000, nuttcp_port + 1999))