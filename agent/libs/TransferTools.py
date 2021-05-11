from abc import ABC, abstractmethod
import numa
from libs.Schemes import NumaScheme
import itertools
import os
import psutil
import signal

class TransferTimeout(Exception):
    def __init__(self, msg, file):
        self.msg = msg
        self.file = file

    def __str__(self):
        return self.msg

class TransferTools(ABC):    
 
    local_cpu_iterator = None

    def __init__(self, numa_scheme = 1):
        super().__init__()
        self.numa_scheme = NumaScheme(numa_scheme)        
    
    @abstractmethod
    def run_sender(self, srcfile, **optional_args):
        pass

    @abstractmethod
    def run_receiver(self, address, dstfile, **optional_args):
        pass

    @classmethod
    @abstractmethod
    def poll_progress(cls, port, **optional_args):
        pass

    @abstractmethod
    def free_port(cls, port, **optional_args):
        pass

    @classmethod
    @abstractmethod
    def cleanup(cls, **optional_args):
        pass

    def bind_proc_to_numa(self, proc, numa_num):
        if self.numa_scheme == NumaScheme.OS_CONTROLLED or not numa.available(): return
        else: 
            cores_to_bind = self.get_cpu(numa_num)
            os.sched_setaffinity(proc.pid, cores_to_bind)

    def get_cpu(self, numa_num):
        if self.numa_scheme == NumaScheme.BIND_TO_NUMA:
            return numa.node_to_cpus(numa_num)
        elif self.numa_scheme == NumaScheme.BIND_TO_CORE:
            if TransferTools.local_cpu_iterator == None:
                TransferTools.local_cpu_iterator = itertools.cycle(numa.node_to_cpus(numa_num))
            return TransferTools.local_cpu_iterator.next()
        else: raise Exception('Incorrect Numa Affinity Scheme')
        
    def kill_proc_tree(pid, sig=signal.SIGTERM, include_parent=True,
                   timeout=None, on_terminate=None):
        """Kill a process tree (including grandchildren) with signal
        "sig" and return a (gone, still_alive) tuple.
        "on_terminate", if specified, is a callabck function which is
        called as soon as a child terminates.
        """
        assert pid != os.getpid(), "won't kill myself"
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        if include_parent:
            children.append(parent)
        for p in children:
            p.send_signal(sig)
        gone, alive = psutil.wait_procs(children, timeout=timeout,
                                        callback=on_terminate)
        return (gone, alive)
