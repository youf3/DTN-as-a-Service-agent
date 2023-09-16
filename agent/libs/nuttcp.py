from libs.TransferTools import TransferTools, TransferTimeout
import subprocess
import logging
import sys, os, time, re
from threading import Lock

class nuttcp(TransferTools):
    running_svr_threads = {}
    running_cli_threads = {}
    # cports = list(range(nuttcp_port, nuttcp_port+ 999))
    # dports = list(range(nuttcp_port + 1000, nuttcp_port + 1999))
    cports = []
    dports = []    
    port_lock = Lock()
    
    def __init__(self, numa_scheme = 1, nuttcp_port=30001) -> None:        
        super().__init__(numa_scheme = numa_scheme)
        if nuttcp.cports == []:
            self.reset_ports(nuttcp_port=nuttcp_port)

    def reset_ports(self, nuttcp_port):
        with self.port_lock:
            nuttcp.cports = list(range(nuttcp_port, nuttcp_port+ 999))
            nuttcp.dports = list(range(nuttcp_port + 1000, nuttcp_port + 1999))

    def run_sender(self, srcfile, **optional_args):
        time_start = time.time()
        with self.port_lock:
            cport = nuttcp.cports.pop()
            dport = nuttcp.dports.pop()
        logging.debug('running nuttcp server on cport {} file {} dport {}'.format(cport, srcfile, dport))
        
        if srcfile is None:            
            
            if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
                blocksize = optional_args['blocksize']
            else:
                blocksize = 8192

            cmd = ['nuttcp', '-S', '-1', f'-P{cport}', f'-p{dport}', f'-l{blocksize}k', '--nofork']
            logging.debug(str(cmd))
            proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
            nuttcp.running_svr_threads[cport] = [proc, dport, srcfile, time_start]
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            return {'cport' : cport, 'dport': dport, 'result': True}

        else:
            filemode = '-sdz'
            if 'direct' in optional_args and optional_args['direct'] == False:
                filemode = filemode.replace('d', '')

            if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
                filemode = filemode.replace('z', '')
            
            if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
                blocksize = optional_args['blocksize']
            else:
                blocksize = 8192

            if 'compression' in optional_args and optional_args['compression'] and optional_args['compression'].lower() in ['bzip2', 'gzip', 'lzma']:
                compression = optional_args['compression'].lower()
            else:
                compression = None

            cmd = ['nuttcp', '-S', '-b', '-1', f'-P{cport}', f'-p{dport}', filemode, f'-l{blocksize}k', '--nofork']

            if compression:
                # must use shell mode with subprocess
                cmd = [f'({compression} |'] + cmd + [f') < {srcfile}']
                logging.debug(str(cmd))
                proc = subprocess.Popen(' '.join(cmd), stderr=sys.stderr, shell=True)
            else:
                # no shell mode, take advantage of performance improvements
                logging.debug(str(cmd))
                with open(srcfile, 'rb') as file:
                    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=sys.stderr, stdin=file)

            nuttcp.running_svr_threads[cport] = [proc, dport, srcfile, time_start]
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            return {'cport' : cport, 'dport': dport, 'size' : os.path.getsize(srcfile), 'result': True}

    def run_receiver(self, address, dstfile, **optional_args):
        time_start = time.time()

        if 'cport' not in optional_args:
            logging.error('cport number not found')
            raise Exception('Control port not found')
        if 'dport' not in optional_args:
            logging.error('dport number not found')
            raise Exception('Data port not found')
        
        logging.debug('running nuttcp receiver on address {} file {} cport {} dport {}'.format(address, dstfile, optional_args['cport'], optional_args['dport']))
        if dstfile is None:
            
            if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
                blocksize = optional_args['blocksize']
            else:
                blocksize = 8192

            duration = optional_args['duration']
            cport = optional_args['cport']
            dport = optional_args['dport']
            logging.debug('running nuttcp mem-to-mem client on cport {} dport {}'.format(cport, dport))
            cmd = ['nuttcp', '-r', '-i1', f'-P{cport}', f'-p{dport}', f'-l{blocksize}k', f'-T{duration}', '-fparse', '--nofork', address]
            logging.debug(str(cmd))
            # PIPE stdout so we can keep the output for size calculations later
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            nuttcp.running_cli_threads[cport] = [proc, dport, time_start]
            return {'cport' : cport, 'dport': dport, 'result': True}

        else:
            filemode = '-sdz'
            if 'direct' in optional_args and optional_args['direct'] == False:
                filemode = filemode.replace('d', '')

            if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
                filemode = filemode.replace('z', '')

            if 'blocksize' in optional_args and type(optional_args['blocksize']) == int:
                blocksize = optional_args['blocksize']
            else:
                blocksize = 8192

            if 'compression' in optional_args and optional_args['compression'] and optional_args['compression'].lower() in ['bzip2', 'gzip', 'lzma']:
                compression = optional_args['compression'].lower()
            else:
                compression = None

            cport = optional_args['cport']
            dport = optional_args['dport']
            cmd = ['nuttcp', '-r', '-b', '-i1', f'-P{cport}', f'-p{dport}', filemode, f'-l{blocksize}k', '--nofork', address]

            if compression:
                cmd.extend([f' | {compression} -d > {dstfile}'])
                logging.debug(str(cmd))
                proc = subprocess.Popen(' '.join(cmd), shell=True, stderr=sys.stderr)
            else:
                logging.debug(str(cmd))
                with open(dstfile, 'wb') as file:
                    proc = subprocess.Popen(cmd, stdout=file, stderr=sys.stderr)

            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            nuttcp.running_cli_threads[cport] = [proc, dport, time_start]
            return {'cport' : cport, 'dport': dport, 'result': True}

    @classmethod
    def free_port(cls, port, **optional_args):
        threads = nuttcp.running_svr_threads
        err_thread = threads.pop(port)
        err_thread[0].kill()
        err_thread[0].kill()
        err_thread[0].communicate()
        with nuttcp.port_lock:
            nuttcp.cports.append(port)
            nuttcp.dports.append(err_thread[1])

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
                with nuttcp.port_lock:
                    completed_thread = threads.pop(cport)
                    nuttcp.cports.append(cport)
                    nuttcp.dports.append(completed_thread[1])
                return proc.returncode
            except subprocess.TimeoutExpired:
                filepath = threads[cport][2]
                nuttcp.free_port(cport)
                logging.error('sender timed out on port %s' % cport)
                raise TransferTimeout('sender timed out on port %s' % cport, filepath)
        elif optional_args['node'] == 'receiver':
            threads = nuttcp.running_cli_threads
            # logging.debug('threads: ' + str(threads))
            try:                
                proc = nuttcp.running_cli_threads[cport][0]
                out, err = proc.communicate(timeout=timeout)
                completed_thread = threads.pop(cport)
                if optional_args['dstfile'] == None:
                    # attempt to parse number of megabytes sent during the mem-mem test
                    if not out or b'megabytes' not in out:
                        if err:
                            logging.error(f"receiver failed on port {cport}: {err.decode()}")
                        return proc.returncode, None
                    transfer_stats = dict(re.findall(r'(\w+)=(\S+)', out.decode().splitlines()[-1]))
                    # make sure "megabytes" is a number
                    if not transfer_stats.get('megabytes', '').replace(".", "").isnumeric():
                        return proc.returncode, None
                    # return with bytes transferred
                    return proc.returncode, (int(float(transfer_stats.get('megabytes', '0.0'))) * 1024 * 1024)
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
        cls.reset_ports(cls, nuttcp_port = optional_args['nuttcp_port'])

        # nuttcp.cports = list(range(nuttcp_port, nuttcp_port+ 999))
        # nuttcp.dports = list(range(nuttcp_port + 1000, nuttcp_port + 1999))