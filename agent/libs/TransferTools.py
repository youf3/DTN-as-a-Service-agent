from abc import ABC, abstractmethod
import numa
from libs.Schemes import NumaScheme
import itertools
import os

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
        