from libs.TransferTools import TransferTools, TransferTimeout, TransferProcessing
import subprocess
import logging
import sys, os, time

class ncat(TransferTools):
    NONBLOCKING_TIMEOUT = 3
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

        if 'ipv6' in optional_args:
            ipv6 = bool(optional_args['ipv6'])
        else:
            ipv6 = False

        if srcfile is None:            
            duration = optional_args['duration']

            cmd = ['timeout', f'{duration}', 'ncat', '-l', '--send-only', f'{cport}']
            if ipv6:
                cmd.insert(3, '-6')
            logging.debug(' '.join(cmd))
            with open("/dev/zero", "rb", 0) as zero:
                proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, stdin=zero)
                ncat.running_svr_threads[cport] = [proc, srcfile]
                if 'numa_node' in optional_args:
                    super().bind_proc_to_numa(proc, optional_args['numa_node'])
            return {'cport' : cport, 'result': True}

        else:
            if 'compression' in optional_args and optional_args['compression'] and optional_args['compression'].lower() in ['bzip2', 'gzip', 'lzma']:
                compression = optional_args['compression'].lower()
            else:
                compression = None

            cmd = ['ncat', '-l', '--send-only', f'{cport}']
            if ipv6:
                cmd.insert(1, '-6')

            if compression:
                cmd = [f'({compression} |'] + cmd + [f') < {srcfile}']
                logging.debug(' '.join(cmd))
                proc = subprocess.Popen(' '.join(cmd), stderr=sys.stderr, shell=True)
            else:
                logging.debug(' '.join(cmd))
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
        
        if dstfile is None:
            cport = optional_args['cport']
            logging.debug('running ncat mem-to-mem client on cport {}'.format(cport))

            # use dd so we can report number of bytes going through
            cmd = ['ncat', '--recv-only', address, f'{cport}', '|', 'dd', '>', '/dev/null']
            logging.debug(' '.join(cmd))
            proc = subprocess.Popen(' '.join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if 'numa_node' in optional_args:
                super().bind_proc_to_numa(proc, optional_args['numa_node'])
            ncat.running_cli_threads[cport] = [proc]
            return {'cport' : cport, 'result': True}
        else:
            if 'compression' in optional_args and optional_args['compression'] and optional_args['compression'].lower() in ['bzip2', 'gzip', 'lzma']:
                compression = optional_args['compression'].lower()
            else:
                compression = None

            cport = optional_args['cport']
            logging.debug('running ncat client on cport {} file {}'.format(cport, dstfile))
            cmd = ['ncat', '--recv-only', address, f'{cport}']
            
            if compression:
                cmd.extend([f' | {compression} -d > {dstfile}'])
                logging.debug(' '.join(cmd))
                proc = subprocess.Popen(' '.join(cmd), shell=True, stderr=subprocess.PIPE)
            else:
                logging.debug(' '.join(cmd))
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
            proc = threads[cport][0]
            try:
                proc.communicate(timeout=timeout)
                completed_thread = threads.pop(cport)
                ncat.cports.append(cport)
                if optional_args['dstfile'] is None and proc.returncode == 124:
                    # mem-to-mem test timed out successfully, error isn't actually 124
                    return 0
                return proc.returncode
            except subprocess.TimeoutExpired:
                filepath = threads[cport][1]
                ncat.free_port(cport)
                logging.error('sender timed out on port %s' % cport)
                raise TransferTimeout('sender timed out on port %s' % cport, filepath)
        elif optional_args['node'] == 'receiver':
            threads = ncat.running_cli_threads
            if not timeout:
                # set a timeout so we don't block receiver requests with polls
                timeout = ncat.NONBLOCKING_TIMEOUT
            try:
                proc = ncat.running_cli_threads[cport][0]
                out, err = proc.communicate(timeout=timeout)
                threads.pop(cport)
                if optional_args['dstfile'] == None:
                    # attempt to parse number of bytes sent during the mem-mem test
                    if err and b'bytes' in err.splitlines()[-1]:
                        try:
                            num_bytes = int(err.splitlines()[-1].split()[0])
                            return proc.returncode, num_bytes
                        except:
                            logging.debug("unable to parse mem-to-mem bytes", exc_info=True)
                    return proc.returncode, None
                else:    
                    return proc.returncode, os.path.getsize(optional_args.pop('dstfile'))
            except subprocess.TimeoutExpired:
                # don't raise a generic exception - very possible that the transfer is still ongoing
                #
                # usually transfer will fail with a connection refused, connection timeout, etc.
                # that will not get caught by this exception
                #logging.debug(f"transfer on port {cport} still in progress")
                raise TransferProcessing("still transferring", optional_args.get('dstfile', ''))

                # err_thread = threads.pop(cport)
                # err_thread[0].kill()
                # err_thread[0].kill()
                # err_thread[0].communicate()                
                # logging.error('receiver timed out on port %s' % cport)
                # raise Exception('receiver timed out on port %s' % cport)
            
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
