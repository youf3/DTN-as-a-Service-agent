from libs.TransferTools import TransferTools
import subprocess
import logging
import sys

class nuttcp(TransferTools):
    running_svr_threads = {}
    running_cli_threads = {}

    def run_sender(self, port, srcfile, **optional_args):
        logging.debug('running nuttcp server on port {} file {} dport {}'.format(port, srcfile, optional_args['dport']))
        if 'dport' not in optional_args:
            logging.error('dport number not found')
            return False

        filemode = ' -sdz'
        if 'direct' in optional_args and optional_args['direct'] == False:
            filemode = filemode.replace('d', '')

        if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
            filemode = filemode.replace('z', '')

        dport = optional_args['dport']
        cmd = 'nuttcp -S -1 -P {} -p {}{} -l8m --nofork < {}'.format(port, dport, filemode, srcfile)
        logging.debug(cmd)        
        proc = subprocess.Popen(cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        self.running_svr_threads[port] = proc
        return True

    def run_receiver(self, address, port, dstfile, **optional_args):
        logging.debug('running nuttcp receiver on address {} port {} file {} dport {}'.format(address, port, dstfile, optional_args['dport']))
        if 'dport' not in optional_args:
            logging.error('dport number not found')
            return False

        filemode = ' -sdz'
        if 'direct' in optional_args and optional_args['direct'] == False:
            filemode = filemode.replace('d', '')

        if 'zerocopy' in optional_args and optional_args['zerocopy'] == False:
            filemode = filemode.replace('z', '')

        dport = optional_args['dport']
        cmd = 'nuttcp -r -i 1 -P {} -p {}{} -l8m --nofork {} > {}'.format(port, dport, filemode, address, dstfile)
        logging.debug(cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout = sys.stdout, stderr = sys.stderr)
        self.running_cli_threads[port] = proc
        return True

    @classmethod
    def poll_progress(cls, threads, port):        
        logging.debug('threads: ' + str(threads))
        proc = threads[port]
        proc.communicate(timeout=None)
        del threads[port]
        return proc.returncode