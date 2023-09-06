# coding: utf-8

#require mdadm, nvme-cli, mkfs.xfs, mdstat

import subprocess
import os, sys, glob
import re
import argparse
import mdstat
import getpass
import logging
import json
import time
import itertools

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split(r'(\d+)', text['device']) ]

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def run_command(cmd, ignore_stderr = False):
    if os.geteuid() != 0:
        cmd = 'sudo -S {}'.format(cmd)

    proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    logging.debug(cmd)
    outs, errs = proc.communicate()
    err = str(errs, 'UTF-8')
    if '[sudo] password for' in err:
        logging.debug('To query and modify NVMes, please input sudo password')
        password = getpass.getpass()
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        outs, errs = proc.communicate(bytes(password + '\n', 'UTF-8'))

    return [str(outs,'UTF-8'), str(errs,'UTF-8')]


class disk_manager():

    def __init__(self, isServer = False, mountpoint = '/data',
    password = None, nvmeof_numa = None):
        self.mountpoint = mountpoint
        self.password = password
        self.nvmeof_numa = nvmeof_numa

        if not isServer:
            self.scan_nvme()
        self.raids = []
        self.zfs_raids = []
        self.vol_iterator = None

    def scan_md_array(self, numa, transport):
        logging.debug('scanning array in numa ' + str(numa))
        if numa == None:
            devices = [x['device'].replace('/dev/','')
            for x in self.devices
            if x['transport'] == transport]
        else:
            devices = [x['device'].replace('/dev/','')
            for x in self.devices
            if x['numa'] == numa and x['transport'] == transport]

        arrays_devs = []
        raids = mdstat.parse()
        index = 0
        for rv in raids['devices']:
            disks_in_raid = raids['devices'][rv]['disks']
            #if any(x in disks_in_raid for x in devices):
            if any(x for y in disks_in_raid for x in devices if x in y):
                arrays_devs.append(rv)
                logging.debug('raid found {}'.format(rv))
                raid = {'device' : '/dev/' + rv, 'index' : index, 'numa' : numa, 'devices' : disks_in_raid}
                self.raids.append(raid)
                index += 1

    def scan_zfs_array(self, numa):
        logging.debug('scanning zfs in numa ' + str(numa))
        devices = [x['device'].replace('/dev/','') for x in self.devices if x['numa'] == numa]
        zfs_vols = self.get_zfs()
        index = 0
        for zfs_vol in zfs_vols:
            if any(x in devices for x in zfs_vol['devices']):
                raid = {'device' : zfs_vol['name'], 'index' : index, 'numa' : numa}
                self.zfs_raids.append(raid)
            index += 1


    def get_zfs(self):
        arrays = []
        out,err = run_command('zpool status')
        if err == 'no pools available\n':
            return []
        elif err != "":
            raise Exception(err)
        array = {}
        for line in out.split('\n')[6:-3]:
            if line.startswith('\t '):
                array['devices'].append(line.split()[0])
            else:
                if 'array' in locals():
                    arrays.append(array)
                array = {'name' : line.split()[0], 'devices' : []}
        arrays.append(array)

        return arrays

    def format_drive(self, device, filesystem):
        logging.debug('formatting {0} to {1}'.format(device, filesystem) )
        if filesystem == 'xfs':
            cmd = 'mkfs.xfs -f ' + device
        elif filesystem == 'ext2':
            cmd = 'mkfs.ext2 -F ' + device

        out, err = run_command(cmd)
        return out,err

    def umount(self, device):
        logging.debug('Unmounting ' + device)
        cmd = 'umount ' + device
        _,err = run_command(cmd)

        if err == '':
            logging.debug('Unmounted ' + device)
        elif 'target is busy' in err:
            raise Exception(err)
        elif 'not mounted' in err:
            logging.debug('Not mounted yet')

    def mount(self, device, index, raid=False, raidname='bigdisk'):
        if raid:
            mount_dir = self.mountpoint + '/{}'.format(raidname)
            if not os.path.exists(mount_dir):
                cmd = 'mkdir -p {0}'.format(mount_dir)
                _,err = run_command(cmd)
                if err != '':
                    raise Exception(err)

        else:
            mount_dir = self.mountpoint + '/disk' + str(index)
            # print('mounting {} to {}'.format(device, mount_dir))
            if not os.path.exists(mount_dir):
                cmd = 'mkdir -p {0}'.format(mount_dir)
                _,err = run_command(cmd)
                if err != '':
                    raise Exception(err)

        logging.debug('Mounting ' + device['device'] + ' to ' + mount_dir)
        cmd = 'mount -o noatime,nodiratime,largeio {0} {1}'.format(device['device'], mount_dir)
        _,err = run_command(cmd)
        if err != '':
            raise Exception(err)
        device['mounted'] = mount_dir
        #self.mounted.append(mount_dir)

    def create_zfs_raid(self, devices, raid_name):
        mount_dir =  '{0}/{1}'.format(self.mountpoint, raid_name)
        cmd = 'zpool create -m {0} {1} {2}'.format(mount_dir, raid_name, ' '.join(devices))
        _,err = run_command(cmd)
        if '' != err: raise Exception(err)


    def create_md_raid(self, devices, raid, raid_name):
        logging.debug('\nCreating RAID with md')
        cmd = ('mdadm --create {0} --level=raid0 --raid-devices={1} '
               '--chunk=128 {2}').format(raid_name, len(devices), ' '.join(devices))
        _,err = run_command(cmd)

        if '{} started'.format(raid_name) in err:
            logging.debug('RAID created')
        else:
            raise Exception(err)

    def format_and_mount(self, numa, filesystem, disk_conf, raidname, skip_input, transport, no_format):
        if numa == None: devices = [x for x in self.devices if x['transport'] == transport]
        else: devices = [x for x in self.devices if x['numa'] == numa and x['transport'] == transport]

        if len(devices) < 1:
            raise Exception('Cannot find any NVMe in NUMA {}'.format(numa))

        devlist = [x['device'] for x in devices]
        logging.debug(devlist)

        if disk_conf is None:
            if not skip_input and not no_format:
                print("Warning : following drives will be formatted into {0}".format(filesystem))
                print(' '.join(devlist))
                input()

            for device in devices:
                self.umount(device['device'])

                # self.clean_drive(device['device'])
                if no_format:
                    self.mount(device, self.devices.index(device))
                else:
                    _, err = self.format_drive(device['device'], filesystem)

                    if 'contains a mounted filesystem' in err:
                        logging.debug(device['device'] + ' is already mounted.')
                        raise Exception(err)

                    elif 'No such file or directory' in err:
                        print('Wrong device name ' + device['device'])
                        raise Exception(err)

                    elif 'mke2fs' in err:
                        self.mount(device, self.devices.index(device))

                    elif '' != err:
                        print('unknown error')
                        raise Exception(err)

                    else:
                        self.mount(device, self.devices.index(device))

        else:
            if not skip_input:
                print('Warning : following drives will be used for {0} RAID'.format(disk_conf))
                print(' '.join(devlist))
                input()

            for device in devices:
                self.umount(device['device'])
                # self.clean_drive(device['device'])

            if disk_conf == 'md':
                raidDevName='/dev/md/{}'.format(raidname)
                raid = {'device' : raidDevName, 'mounted' : self.mountpoint + '/' + raidname, 'index' : 0, 'numa' : numa}
                self.raids.append(raid)
                self.create_md_raid(devlist, raid, raidDevName)
                self.format_drive(raidDevName, filesystem)
                self.mount(raid, 0, raid=True, raidname=raidname)
            elif disk_conf == 'zfs':
                raidDevName=raidname
                self.raids.append({'device' : raidDevName, 'mounted' : self.mountpoint + '/' + raidDevName, 'index' : 1, 'numa' : numa})
                self.create_zfs_raid(devlist, raidDevName)

    def clean_drive(self, device):
        logging.debug('Wipeing filesystem signature for ' + device )
        cmd = 'wipefs --all --force ' + device
        _,err = run_command(cmd)

        if err != '': raise Exception(err)

        logging.debug('Zeroing superblocks')
        cmd = 'mdadm --zero-superblock ' + device
        _,err = run_command(cmd)
        if 'mdadm: Unrecognised md component device' in err:
            pass
        elif '' != err:
            print(cmd)
            raise Exception(err)


    #require mdadm, wipefs
    def stop_md_array(self, array):

        self.umount(array['device'])
        logging.debug('stopping ' + array['device'])
        cmd = 'mdadm --stop '+ array['device']
        _,err = run_command(cmd)

        if 'mdadm: stopped ' in err:
            logging.debug('stopped ' + array['device'])
            for device in array['devices']:
                self.clean_drive('/dev/' + device)
            for raid in self.raids:
                if raid['device'] == array['device']:
                    logging.debug('Deleting ' + array['device'])
                    self.raids.remove(raid)
        else:
            print('cannot stop md array')
            raise Exception(err)

    def stop_zfs_array(self, array):
        cmd = 'zpool destroy -f ' + array['device']
        _,err = run_command(cmd)
        if err != '':
            raise Exception(err)

        for raid in self.zfs_raids:
            if raid['device'] == array['device']:
                self.zfs_raids.remove(raid)

    def scan_nvme(self):

        devices = []
        nvme_path = '/sys/class/nvme'
        if not os.path.exists(nvme_path):
            self.devices = None
            return
        for nvme_dev in os.listdir(nvme_path):
            block_dev_paths = glob.glob('{}/{}/nvme*n*'.format(
                nvme_path, nvme_dev))
            for dev_path in block_dev_paths:
                block_dev = '/dev/' + dev_path.split('/')[-1]
                transport_file = '{}/{}/transport'.format(
                    nvme_path, nvme_dev)
                with open(transport_file) as f:
                    transport = f.read().replace('\n','')
                if transport == 'pcie':
                    #In linux 5.2rc4, numa_node in nvme does not report
                    #correct value and need to get it from pci bus
                    linuxVersion = float('.'.join(os.uname()[2].split('.')[0:2]))
                    if linuxVersion > 5:
                        busaddr = '{}/{}/address'.format(nvme_path, nvme_dev)
                        with open (busaddr) as f:
                            pciaddr = f.read().replace('\n', '')
                        numa_file = ('/sys/bus/pci/devices/{}/numa_node'
                        ''.format(pciaddr))
                    else:
                        numa_file = glob.glob('/sys/class/nvme/nvme*/{}/../device/numa_node'.format(
                            block_dev.replace('/dev/','')))[0]
                    with open(numa_file) as f:
                        numa = int(f.read())

                elif transport == 'rdma' or transport == 'tcp':
                    # block_dev = re.sub(r'c[\d]*', '', block_dev)
                    if self.nvmeof_numa == None:
                        numa_file = '{}/{}/numa_node'.format(nvme_path,
                        nvme_dev)
                        with open(numa_file) as f:
                            numa = int(f.read())
                    else:
                        numa = self.nvmeof_numa
                else:
                    raise Exception('Unknown nvme transport : {}'
                    ''.format(transport))
                devices.append(
                    {'raw_dev' : nvme_dev, 'device' : block_dev,
                    'numa' : numa, 'transport' : transport })

        devices.sort(key=natural_keys)
        self.devices = devices

    def prepare_disks(self, username, numa, transport, fs = 'xfs',
    disk_conf=None, raidname='bigdata', skip_input=False, no_format=False):
        self.scan_md_array(numa, transport)

        for nvme_array in self.raids:
            if not skip_input:
                print('Do you want to destroy array', nvme_array['device'] + '? (y/n)')
                ans = input()
            if skip_input or ans.lower() == 'y':
                self.stop_md_array(nvme_array)
            else:
                for device in nvme_array.split()[4:]:
                    device = '/dev/' + device.split('[')[0]
                    self.devices.remove(device)

        # self.scan_zfs_array(numa)
        # for zfs_array in self.zfs_raids:
        #     if not skip_input:
        #         print('Do you want to destroy array ', zfs_array['device'] + '? (y/n)')
        #         ans = input()
        #     if skip_input or ans.lower() == 'y':
        #         self.stop_zfs_array(zfs_array)
        #     else:
        #         for device in nvme_array.split()[4:]:
        #             device = '/dev/' + device.split('[')[0]
        #             self.devices.remove(device)

        self.format_and_mount(numa, fs, disk_conf, raidname, skip_input, transport, no_format)

        logging.debug('Changing user to ' + username)
        print('Changing user to ' + username)

        cmd = 'chown -R {0}:{0} {1}/*'.format(username, self.mountpoint)
        # print(cmd)
        _,err = run_command(cmd)
        if err != '': raise Exception(err)

    #insert mounted path to each device
    def find_paths(self, numa, transport='pcie'):
        if self.devices == None:
            return
        if numa == None:
            devices = [x for x in self.devices if x['transport'] == transport]
            self.scan_md_array(numa, transport)
            md_devices = [x for x in self.raids]
        else:
            devices = [x for x in self.devices if x['numa'] == numa and x['transport'] == transport]
            self.scan_md_array(numa, transport)
            md_devices = [x for x in self.raids if x['numa'] == numa]

        with open('/proc/mounts') as f:
            for line in f.readlines():
                elements = line.split()
                for device in (devices + md_devices):
                    if device['device'] in elements[0]:
                        device['mounted'] = elements[1]
        self.vol_iterator = itertools.cycle([x['mounted'] for x in devices if 'mounted' in x])

    def load_devices(self, devices):
        self.devices = devices

    def create_nvmeof(self, numa, traddr, transport='tcp', nqn='testnqn',
    adrfam='ipv4', trsvcid = 4420, num_null_blk = 0, inline_data_size=16384):
        import nvmet

        if transport != 'tcp' and transport != 'rdma':
            raise Exception('NVMeoF transport should either be tcp or rdma')

        _,err = run_command('modprobe -v nvmet-{}'.format(transport))
        if  'modprobe: FATAL: Module nvmet-{} not found'.format(transport) in err:
            raise Exception(err)

        if numa == -1:
            devices = [x['device'] for x in self.devices]
        else:
            devices = [x['device'] for x in self.devices if x['numa'] == numa]
        if len(devices) < 1:
            raise Exception('There is no NVMe attached at NUMA {}'.format(numa))

        if num_null_blk > 0:
            _,err = run_command('modprobe -v null_blk nr_devices={} gb=1024 irqmode=0'.format(num_null_blk))
            if err != '':
                raise Exception(err)

            for i in range(num_null_blk):
                # print('adding /dev/nullb{}'.format(i))
                devices.append('/dev/nullb{}'.format(i))

            time.sleep(1)

        root = nvmet.Root()
        root.clear_existing()

        nvmet.Host(nqn=nqn, mode='create')


        port = nvmet.Port(mode='create', portid = 1)
        port.set_attr('addr', 'trtype', transport)
        port.set_attr('addr', 'adrfam', adrfam)
        port.set_attr('addr', 'traddr', traddr)
        port.set_attr('addr', 'trsvcid', str(trsvcid))
        port.set_attr('param', 'inline_data_size', inline_data_size)

        subnqn=nqn
        subsystem = nvmet.Subsystem(nqn=subnqn)
        subsystem.set_attr('attr', 'allow_any_host', 1)
        for i in range(len(devices)):
            n = nvmet.Namespace(subsystem, nsid=i+1, mode='create')
            n.set_attr('device', 'path', devices[i])
            n.set_enable(1)
            i += 1
        port.add_subsystem(subnqn)


    def trim_drive(self, path):
        cmd = 'fstrim -v {}'.format(path)
        run_command(cmd)

    def discover_nvmeof(self, addr, trsvcid, transport='tcp'):
        nqns = {}
        _,err = run_command('modprobe -v nvme-{}'.format(transport))
        if  'modprobe: FATAL: Module nvme-{} not found'.format(transport) in err:
            raise Exception(err)

        # for trsvcid in trsvcids:
        command = 'nvme discover -t {} -a {} -s {}'.format(transport, addr, trsvcid)
        out,err = run_command(command)
        if err != '':
            raise Exception(err)

        for line in out.split('====Discovery Log Entry')[1].split('\n'):
            if 'subnqn: 'in line:
                subnqn = line.split()[1]
            elif 'trsvcid:' in line:
                trsvcid = int(line.split()[1])

        nqns[subnqn] = trsvcid

        return nqns


    def connect_nvmeof(self, addr, nqns, transport='tcp'):

        for nqn in nqns:
            command = 'nvme connect -t {} -a {} -s {} -n {}'.format(transport, addr, nqns[nqn], nqn)
            _,err = run_command(command)

            if err != '':
                raise Exception(err)

        time.sleep(2)
        self.scan_nvme()

        # print('disconnecting')
        # self.disconnect_nvmeof()

    def disconnect_nvmeof(self, nqn='testnqn'):
        command = 'nvme disconnect -n {}'.format(nqn)
        _,err = run_command(command)

        if err != '':
            raise Exception(err)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--fs", help="File system to use in NVMe SSD.",
    choices=['xfs','ext2'], default='xfs')
    parser.add_argument("--numa", help="Numa node for the disks",
    type=int, default=0)
    parser.add_argument("--raid", help="Raid type",
    choices=[None, 'md', 'zfs'], default=None)
    parser.add_argument("--raidname", help="RAID mountname",
    default='bigdisk', type=str)
    parser.add_argument("--mountpoint", help="Location to mount",
    default='/data', type=str)

    parser.add_argument("--username",
    help="Username who will own the filesystem",
    default=getpass.getuser(), type=str)
    parser.add_argument("--skip_input", help="Not asking for input", action="store_true", default=False)
    parser.add_argument("--list_disk",
    help="Do not format drives, just print mounts",
    action='store_true')
    #parser.add_argument("--nvmeof_numa", help="set NUMA for nvmeof",
    #type=int, default=None)
    parser.add_argument("--nvme_transport", help="NVME transport type",
    choices=['pcie', 'rdma', 'tcp'], default='pcie', type=str)
    parser.add_argument("--nvmeof_transport", help="NVMEof transport type",
    choices=['rdma', 'tcp'], default='tcp', type=str)
    parser.add_argument("-v", '--verbose', help="verbose output",
    action='store_true')
    parser.add_argument('--nvmeof_setup', help="Sets up nvmeof target with given ip",
                        type=str, metavar='IP', default=None)
    parser.add_argument('--nvmeof_connect', help="Connect to NVMeof Target with given ip",
                        type=str, metavar='IP')
    parser.add_argument("--num_nvmeof_disk", help="number of NVMeoF disks to connect", type=int, default=8)
    parser.add_argument('--no_mount', help="Do not mount NVMe when connecting NVMeoF target",
                        action="store_true", default=True)
    parser.add_argument('--null_dev', help="Number of null devices to use for NVMeoF target",
                        type=int, default=0)
    parser.add_argument("--inline_data_size", help="inline_data_size for NVMeoF", type=int, default=16384)
    parser.add_argument("--no_format", help="Do not format disks", action="store_true", default=False)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    dm = disk_manager(nvmeof_numa = None, mountpoint = args.mountpoint)

    if args.list_disk:
        dm.find_paths(args.numa)
    elif args.nvmeof_setup:
        if args.nvmeof_connect:
            raise Exception("Cannot setup and connect NVMeoF at the same time.")
        dm.create_nvmeof(args.numa, args.nvmeof_setup, transport=args.nvmeof_transport,
        num_null_blk=args.null_dev, inline_data_size=args.inline_data_size)
    elif args.nvmeof_connect:
        nqns = dm.discover_nvmeof(args.nvmeof_connect, 4420)
        dm.connect_nvmeof(args.nvmeof_connect, nqns, transport=args.nvmeof_transport)
        if not args.no_mount:
            dm.mountpoint='/data/nvmeof'
            dm.prepare_disks(args.username, args.numa, args.nvmeof_transport,
            fs = args.fs, disk_conf = args.raid, raidname = args.raidname,
            skip_input = args.skip_input)
    else:
        dm.prepare_disks(args.username, args.numa, args.nvme_transport,
        fs = args.fs, disk_conf = args.raid, raidname = args.raidname,
        skip_input = args.skip_input, no_format=args.no_format)
    logging.debug('Disks prepared:')
    if args.raid == None:
        if args.numa == -1:
            devices =  [x for x in dm.devices]
        else:
            devices =  [x for x in dm.devices if x['numa'] == args.numa]
    else:
        devices = [x for x in dm.raids if x['numa'] == args.numa]
    print(json.dumps(devices, indent=4, sort_keys=True))