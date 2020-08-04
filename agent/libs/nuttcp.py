from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os

class TransferTimeout(Exception):
    def __init__(self, msg, file):
        self.msg = msg
        self.file = file

    def __str__(self):
        return self.msg

class nuttcp(TransferTools):
    running_svr_threads = {}
    running_cli_threads = {}
    cports = list(range(30001, 31000))
    dports = list(range(31001, 32000))

    def run_sender(self, srcfile, **optional_args):
        cport = nuttcp.cports.pop()
        dport = nuttcp.dports.pop()
        logging.debug('running nuttcp server on cport {} file {} dport {}'.format(cport, srcfile, dport))
        logging.debug('args {}'.format(optional_args))
        
        filemode = ' -sdz'
        if 'direct' in optional_args and optional_args['direct'] == False:
            filemode = filemode.replace('d', '')

        if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
            filemode = filemode.replace('z', '')
        
        if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
            blocksize = optional_args['blocksize']
        else:
            blocksize = 8192

        cmd = 'nuttcp -S -1 -P {} -p {}{} -l{}k --nofork < {}'.format(cport, dport, filemode, blocksize, srcfile)
        logging.debug(cmd)
        proc = subprocess.Popen('exec ' + cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        nuttcp.running_svr_threads[cport] = [proc, dport, srcfile]
        if 'numa_node' in optional_args:
            super().bind_proc_to_numa(proc, optional_args['numa_node'])
        return {'cport' : cport, 'dport': dport, 'size' : os.path.getsize(srcfile), 'result': True}

    def run_receiver(self, address, dstfile, **optional_args):
        
        if 'cport' not in optional_args:
            logging.error('cport number not found')
            raise Exception('Control port not found')
        if 'dport' not in optional_args:
            logging.error('dport number not found')
            raise Exception('Data port not found')
        
        logging.debug('running nuttcp receiver on address {} file {} cport {} dport {}'.format(address, dstfile, optional_args['cport'], optional_args['dport']))        

        filemode = ' -sdz'
        if 'direct' in optional_args and optional_args['direct'] == False:
            filemode = filemode.replace('d', '')

        if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
            filemode = filemode.replace('z', '')

        if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
            blocksize = optional_args['blocksize']
        else:
            blocksize = 8192

        cport = optional_args['cport']
        dport = optional_args['dport']
        logging.debug('running nuttcp client on cport {} file {} dport {}'.format(cport, dstfile, dport))
        logging.debug('args {}'.format(optional_args))
        cmd = 'nuttcp -r -i 1 -P {} -p {}{} -l{}k --nofork {} > {}'.format(cport, dport, filemode, blocksize, address, dstfile)
        logging.debug(cmd)
        proc = subprocess.Popen('exec ' + cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        if 'numa_node' in optional_args:
            super().bind_proc_to_numa(proc, optional_args['numa_node'])
        nuttcp.running_cli_threads[cport] = [proc, dport]
        return {'cport' : cport, 'dport': dport, 'result': True}

    @classmethod
    def poll_progress(cls, **optional_args):
        if not 'cport' in optional_args:
            logging.error('Control port not found')
            raise Exception('Control port not found')
        elif not 'node' in optional_args:
            logging.error('Node not found')
            raise Exception('Node not found')

        if 'timeout' not in optional_args:
            timeout = None
        else:
            timeout = optional_args['timeout']
            
        cport = optional_args.pop('cport')        
        
        if optional_args['node'] == 'sender':
            threads = nuttcp.running_svr_threads
            # logging.debug('threads: ' + str(threads))            
            proc = threads[cport][0]
            try:
                proc.communicate(timeout=timeout)
                completed_thread = threads.pop(cport)
                nuttcp.cports.append(cport)
                nuttcp.dports.append(completed_thread[1])
                return proc.returncode
            except subprocess.TimeoutExpired:
                err_thread = threads.pop(cport)
                err_thread[0].kill()
                err_thread[0].kill()
                err_thread[0].communicate()
                nuttcp.cports.append(cport)
                nuttcp.dports.append(err_thread[1])
                logging.error('sender timed out on port %s' % cport)
                raise TransferTimeout('sender timed out on port %s' % cport, err_thread[2])
        elif optional_args['node'] == 'receiver':
            threads = nuttcp.running_cli_threads
            # logging.debug('threads: ' + str(threads))
            try:
                proc = nuttcp.running_cli_threads[cport][0]
                proc.communicate(timeout=timeout)
                threads.pop(cport)
                return proc.returncode, os.path.getsize(optional_args.pop('dstfile'))
            except subprocess.TimeoutExpired:
                err_thread = threads.pop(cport)
                err_thread[0].kill()
                err_thread[0].kill()
                err_thread[0].communicate()                
                logging.error('receiver timed out on port %s' % cport)
                raise Exception('receiver timed out on port %s' % cport)
            
        else:
            logging.error('Node has to be either sender or receiver')
            raise Exception('Node has to be either sender or receiver')        

    @classmethod
    def cleanup(cls):
        for i,j in nuttcp.running_svr_threads.items():
            j[0].kill()
            j[0].kill()
            j[0].communicate()
        
        nuttcp.running_svr_threads = {}

        for i,j in nuttcp.running_cli_threads.items():
            j[0].kill()
            j[0].kill()
            j[0].communicate()

        nuttcp.running_cli_threads = {}

        nuttcp.cports = list(range(30001, 31000))
        nuttcp.dports = list(range(31001, 32000))