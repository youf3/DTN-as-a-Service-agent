from libs.TransferTools import TransferTools, TransferTimeout
import subprocess
import logging
import sys, os, time

class ncat(TransferTools):
    running_svr_threads = {}
    running_cli_threads = {}
    cports = []
    
    def __init__(self, numa_scheme=1, begin_port=30001, max_ports=999) -> None:
        super().__init__(numa_scheme = numa_scheme)
        if ncat.cports == []:
            self.reset_ports(begin_port, max_ports)

    def reset_ports(self, begin_port, max_ports):
        ncat.cports = list(range(begin_port, begin_port + max_ports))

    def run_sender(self, srcfile, **optional_args):
        cport = ncat.cports.pop()
        logging.debug('running ncat server on cport {} file {}'.format(cport, srcfile))
        logging.debug('args {}'.format(optional_args))
        
        if srcfile is None:            
            duration = optional_args['duration']

            cmd = ['timeout', f'{duration}', 'ncat', '-l', '--send-only', f'{cport}']
            logging.debug(str(cmd))
            proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
            ncat.running_svr_threads[cport] = [proc, srcfile]
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            return {'cport' : cport, 'result': True}

        else:
            cmd = ['ncat', '-l', '--send-only', f'{cport}']
            logging.debug(str(cmd))
            with open(srcfile, 'rb') as file:
                proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=sys.stderr, stdin=file)
                ncat.running_svr_threads[cport] = [proc, srcfile]
                if 'numa_node' in optional_args:
                    super().bind_proc_to_numa(proc, optional_args['numa_node'])
            return {'cport' : cport, 'size' : os.path.getsize(srcfile), 'result': True}

    def run_receiver(self, address, dstfile, **optional_args):
        if 'cport' not in optional_args:
            logging.error('cport number not found')
            raise Exception('Control port not found')
        
        #logging.debug('running nuttcp receiver on address {} file {} cport {} dport {}'.format(address, dstfile, optional_args['cport'], optional_args['dport']))        

        if dstfile is None:
            cport = optional_args['cport']
            logging.debug('running ncat mem-to-mem client on cport {}'.format(cport))
            logging.debug('args {}'.format(optional_args))
            cmd = ['nc', '--recv-only', address, f'{cport}']
            logging.debug(str(cmd))
            proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            ncat.running_cli_threads[cport] = [proc]
            return {'cport' : cport, 'result': True}

        else:
            cport = optional_args['cport']
            logging.debug('running ncat client on cport {} file {}'.format(cport, dstfile))
            logging.debug('args {}'.format(optional_args))
            cmd = ['ncat', '--recv-only', address, f'{cport}']
            logging.debug(str(cmd))
            with open(dstfile, 'wb') as file:
                proc = subprocess.Popen(cmd, stdout=file, stderr=sys.stderr)
                if 'numa_node' in optional_args:
                    super().bind_proc_to_numa(proc, optional_args['numa_node'])
                ncat.running_cli_threads[cport] = [proc]
            return {'cport' : cport, 'result': True}

    @classmethod
    def free_port(cls, port, **optional_args):
        threads = ncat.running_svr_threads
        err_thread = threads.pop(port)
        err_thread[0].kill()
        err_thread[0].kill()
        err_thread[0].communicate()
        ncat.cports.append(port)

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
            threads = ncat.running_svr_threads
            # logging.debug('threads: ' + str(threads))            
            proc = threads[cport][0]
            try:
                proc.communicate(timeout=timeout)
                completed_thread = threads.pop(cport)
                ncat.cports.append(cport)
                return proc.returncode
            except subprocess.TimeoutExpired:
                filepath = threads[cport][1]
                ncat.free_port(cport)
                logging.error('sender timed out on port %s' % cport)
                raise TransferTimeout('sender timed out on port %s' % cport, filepath)
        elif optional_args['node'] == 'receiver':
            threads = ncat.running_cli_threads
            # logging.debug('threads: ' + str(threads))
            try:                
                proc = ncat.running_cli_threads[cport][0]
                proc.communicate(timeout=timeout)
                threads.pop(cport)
                if optional_args['dstfile'] == None:
                    return proc.returncode, None
                else:    
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
    def cleanup(cls, **optional_args):
        for i,j in ncat.running_svr_threads.items():
            j[0].kill()
            j[0].kill()
            j[0].communicate()
        
        ncat.running_svr_threads = {}

        for i,j in ncat.running_cli_threads.items():
            j[0].kill()
            j[0].kill()
            j[0].communicate()

        ncat.running_cli_threads = {}
        cls.reset_ports(cls, optional_args.get('begin_port', ncat.cports[0]), optional_args.get('max_ports', 999))
