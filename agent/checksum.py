import argparse
import multiprocessing
import hashlib
import stat
import pathlib
import os

NUM_THREADS = 12

def get_type(mode):
    if stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        type = 'dir'
    else:
        type = 'file'
    return type

def list_files(path):
    contents = []

    for filename in list(pathlib.Path(path).glob('**/*')):
        filepath = os.path.join(path, filename)
        try:
            stat_res = os.stat(filename)
            ft = get_type(stat_res.st_mode)
            if ft == 'file':
                contents.append(str(filename))
        except FileNotFoundError:
            continue # skip files we can't read
    return contents

def split(pathlist, num_threads):
    i, mod = divmod(len(pathlist), num_threads)
    return (pathlist[j*i + min(j, mod):(j+1)*i + min(j+1, mod)] for j in range(num_threads))

def checksum_list(pathlist, id, shared):
    checksums = set()
    for path in pathlist[id]:
        checksums.add(hashlib.md5(open(path, 'rb').read()).hexdigest())
    shared[id] = checksums

def checksum(path):
    filelist = list_files(path)
    checksums = set()
    if len(filelist) > 100:
        manager = multiprocessing.Manager()
        # split into processes
        processes = []
        split_filelists = list(split(filelist, NUM_THREADS))
        shared_sums = manager.dict()

        # start threads
        for proc in range(NUM_THREADS):
            processes.append(multiprocessing.Process(target=checksum_list, args=(split_filelists, proc, shared_sums)))
            processes[-1].start()
        
        # join later
        for proc in processes:
            proc.join()

        # put values together
        for id in shared_sums:
            checksums.update(shared_sums[id])
    else:
        for chkfile in filelist:
            checksums.add(hashlib.md5(open(chkfile, 'rb').read()).hexdigest())
    
    return hashlib.md5(''.join(sorted(list(checksums))).encode('utf8')).hexdigest()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument("path", help="Directory path", type=str)
    args = parser.parse_args()    
    print(checksum(args.path))
