from abc import ABC, abstractmethod
 
class TransferTools(ABC):
 
    def __init__(self):        
        super().__init__()
    
    @abstractmethod
    def run_sender(self, port, srcfile, **optional_args):
        pass

    @abstractmethod
    def run_receiver(self, address, port, dstfile, **optional_args):
        pass

    @classmethod
    @abstractmethod
    def poll_progress(cls, port):
        pass