from libs.TransferTools import TransferTools
import subprocess
import logging
import sys, os

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
        proc = subprocess.Popen(cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        nuttcp.running_svr_threads[cport] = [proc, dport]
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
        proc = subprocess.Popen(cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
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
            
        cport = optional_args.pop('cport')        
        
        if optional_args['node'] == 'sender':
            threads = nuttcp.running_svr_threads
            logging.debug('threads: ' + str(threads))            
            proc = threads[cport][0]
            proc.communicate(timeout=None)
            completed_thread = threads.pop(cport)
            nuttcp.cports.append(cport)
            nuttcp.dports.append(completed_thread[1])
            return proc.returncode
        elif optional_args['node'] == 'receiver':
            threads = nuttcp.running_cli_threads
            logging.debug('threads: ' + str(threads))
            proc = nuttcp.running_cli_threads[cport][0]
            proc.communicate(timeout=None)
            return proc.returncode, os.path.getsize(optional_args.pop('dstfile'))
        else:
            logging.error('Node has to be either sender or receiver')
            raise Exception('Node has to be either sender or receiver')        