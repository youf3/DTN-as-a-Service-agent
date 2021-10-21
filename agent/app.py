from ast import parse
import os, glob, shutil
import stat
import logging
import sys
import subprocess
import traceback
import ping3
import json
from pathlib import Path
from libs.TransferTools import TransferTools, TransferTimeout
from libs.Schemes import NumaScheme
import argparse
import hashlib
import socket
import psutil
import pwd
import nvmet
from functools import wraps
import jwt

logging.getLogger().setLevel(logging.DEBUG)
from flask import Flask, abort, jsonify, request, make_response
app = Flask(__name__)

from prometheus_flask_exporter import PrometheusMetrics, Counter
metrics = PrometheusMetrics(app, export_defaults=False)
metrics.info('app_info', 'Agent service for StarLight DTN-as-a-Service')

import importlib
import pkgutil

# import Diskmanager.py for NVMEoF functions
sys.path.append("/DTN_Testing_Framework/lib")
from DiskManager import disk_manager

MAX_FIO_JOBS=400
nuttcp_port = 30001

ORCHESTRATOR_REGISTRATION_PATH = "/orchestrator_registered"
orchestrator_registered = False

def import_submodules(package, recursive=True):
    """ Import all submodules of a module, recursively, including subpackages

    :param package: package (name or actual module)
    :type package: str | module
    :rtype: dict[str, types.ModuleType]
    """
    if isinstance(package, str):
        package = importlib.import_module(package)
    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))
    return results

loaded_modules = import_submodules('libs', False)
tools = [x.__name__ for x in TransferTools.__subclasses__()]

def load_config():
    try: app.config.from_envvar('CONF_FILE')
    except RuntimeError:
        pass
    finally:
        if 'FILE_LOC' not in app.config:
            logging.debug('FILE_LOC is not set, using default /data')
            app.config['FILE_LOC'] = '/data'

    # also check for a orchestrator_registered file
    global orchestrator_registered
    try:
        with open(ORCHESTRATOR_REGISTRATION_PATH) as f:
            orchestrator_registered = f.readline().strip()
    except:
        orchestrator_registered = False

def get_type(mode):
    if stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        type = 'dir'
    else:
        type = 'file'
    return type

def get_files(dirname):
    contents = []
    total = {'size': 0, 'dir': 0, 'file': 0}

    for filename in list(Path(dirname).glob('**/*')):
        filepath = os.path.join(dirname, filename)
        stat_res = os.stat(filepath)
        info = {}
        info['name'] = os.path.relpath(filename, dirname)
        info['mtime'] = stat_res.st_mtime
        ft = get_type(stat_res.st_mode)
        info['type'] = ft
        total[ft] += 1
        sz = stat_res.st_size
        info['size'] = sz
        total['size'] += sz
        contents.append(info)

    return contents

def prepare_file(jobname, filename, size):
    write_global=not os.path.exists(jobname)
    Path(os.path.dirname(filename)).mkdir(parents=True, exist_ok=True)
    with open(jobname, 'a') as fh:
        if write_global:
            fh.writelines('[global]\nname=fio-seq-write\nrw=write\nbs=1m\ndirect=1\nnumjobs=1\nioengine=libaio\niodepth=16\nthread=1\ngroup_reporting=1\n\n')
        option = '[{0}]\nsize={1}\nfilename={0}\n\n'.format(filename, size)
        fh.writelines(option)

def commit_write(jobs):
    for job in jobs:
        with open(job) as fh:
            logging.debug('Writing file using FIO job')
            logging.debug(''.join(fh.readlines()))
        ret_code = subprocess.run(['fio', job], stderr=subprocess.PIPE, stdout=subprocess.PIPE)    
        os.remove(job)
    return ret_code

def get_registration_data(given_addr, default_data_addr=None, default_interface=None):
    # we need hostname, management IP, dataplane IP, dataplane interface
    hostname = socket.gethostname()
    data_addr = app.config.get("DATA_ADDR", default_data_addr)
    data_int = app.config.get("DATA_INTERFACE", default_interface)
    all_addrs = psutil.net_if_addrs()
    if not data_addr and not data_int:
        # figure out dataplane interface by fastest interface
        interfaces = psutil.net_if_stats()
        interfaces = sorted([(interf, interfaces[interf].speed) for interf in interfaces 
                if interfaces[interf].isup and interf != 'lo'], key=lambda k: k[1])
        if not interfaces:
            raise ValueError("No valid interfaces found for dataplane")
        data_int = interfaces[-1][0] # fastest
    if not data_int and data_addr:
        # figure out dataplane interface from given address
        for name, sniclist in all_addrs.items():
            if data_addr in [snic.address for snic in sniclist]:
                data_int = name
                break
        if not data_int:
            raise ValueError("Dataplane address not found")
    if not data_addr and data_int:
        # figure out dataplane address from given interface
        if data_int not in all_addrs:
            raise ValueError(f"Invalid data interface {data_int}")
        data_addr = all_addrs.get(data_int)[0].address
    
    man_addr = app.config.get("MAN_ADDR")
    if not man_addr:
        man_addr = given_addr

    username = pwd.getpwuid(os.getuid()).pw_name
    # TODO generate auth token and pass it to the orchestrator
    # Note: this is NOT SECURE if not passed through https
    return {"name": hostname, "man_addr": man_addr, "data_addr": data_addr,
            "interface": data_int, "username": username, "jwt_token": generate_token()}

def fix_dm_mounts(diskmanager):
    # Fix device names so mounts work properly
    # first, get all NVME block devices
    block_devices = [blk for blk in os.listdir('/dev') if blk.startswith('nvme')]
    for idx, device in enumerate(diskmanager.devices):
        # get nvme drives with tcp transport
        if device.get('transport') == 'tcp':
            # find the matching block device and rewrite ['device']
            for blk in block_devices:
                if blk.startswith(device.get('raw_dev')) and 'p' in blk:
                    device['device'] = f"/dev/{blk}"
                    diskmanager.devices[idx] = device
                    # only apply the first partition
                    break
    return diskmanager

def generate_token():
    # generate a JWT with the hostname (can't be changed on client) and a secret (can be updated)
    composite_secret = socket.gethostname() + app.config.get("API_SECRET", "defaultsecretnotsecure")
    # sha1 hash so (configured) plaintext secret doesn't get sent out
    return hashlib.sha1(bytes(composite_secret, encoding='ascii')).hexdigest()

def authorize(f):
    @wraps(f)
    def check_orchestrator_token(*args, **kwargs):
        if not request.headers.get('Authorization'):
            abort(401)
        user = None
        try:
            jwtdata = request.headers['Authorization']
            jwtdata = str(jwtdata).replace('Bearer ', '')
            # TODO future releases can pass Jupyter user to the agent for permissions checks
            decoded = jwt.decode(jwtdata, generate_token(), 'HS256')
            #user = decoded.get('sub')
            #start_time = decoded.get('iat')
        except jwt.exceptions.InvalidSignatureError:
            logging.warn(f"Invalid JWT token")
            abort(401)
        except Exception as e:
            logging.error(f"Auth error: {str(e)}")
            abort(401)

        #return f(user, *args, **kwargs)
        return f(*args, **kwargs)
    return check_orchestrator_token

@app.route('/files/', defaults={'path': ''})
@app.route('/files/<path:path>')
@metrics.do_not_track()
@authorize
def list_files(path):
    try:
        contents = get_files(os.path.join(app.config['FILE_LOC'], path))
    except PermissionError:
        abort(403)
    except FileNotFoundError:
        abort(404)
    return jsonify(contents)

@app.route('/create_file/', methods=['POST'])
@metrics.counter('daas_agent_file_create', 'Number of files created')
@authorize
def create_file():

    fio_job_files = glob.glob("agent/scripts/*.fio")
    for fn in fio_job_files:
        os.remove(fn)

    param = request.get_json()
    file_cnt = 0
    fio_job_num = 0
    jobs = []

    for file_spec in param:
        job_file = os.path.join('agent/scripts/' , 'files{}.fio'.format(fio_job_num))
        if 'size' not in param[file_spec]:
            abort(make_response(jsonify(message='filename and size are required'), 400))
        filepath = os.path.join(app.config['FILE_LOC'] , file_spec)
        prepare_file(job_file, filepath, param[file_spec]['size'])

        file_cnt += 1
        if file_cnt > MAX_FIO_JOBS:
            fio_job_num += 1
            file_cnt = 0
            jobs.append(job_file)

    if job_file not in jobs:
        jobs.append(job_file)

    ret = commit_write(jobs)
    return jsonify(ret.returncode)


@app.route('/create_dir/', methods=['POST'])
@metrics.counter('daas_agent_dir_create', 'Number of dir created')
@authorize
def create_dir():
    dirs = request.get_json()

    for i in dirs:
        dirpath = os.path.join(app.config['FILE_LOC'] , i)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)    
    return ''

@app.route('/file/<path:path>', methods=['DELETE'])
@metrics.counter('daas_agent_file_delete', 'Number of files deleted')
@authorize
def delete_file(path):

    #TODO : check user permission
    
    #convert abs path to relpath
    if os.path.isabs(path):
        path = path[1:]

    filepath = os.path.join(app.config['FILE_LOC'], path)

    if path == '*':
        files = glob.glob(app.config['FILE_LOC'] + '/*', recursive=True)
        for fname in files:
            if os.path.isfile(fname):
                os.remove(fname)
            else:
                shutil.rmtree(fname)
        return ""

    else:
        if not os.path.exists(filepath): 
            abort(make_response(jsonify(message="Cannot find the specified file {}".format(filepath)), 400))

        try:
            if os.path.isfile(filepath):
                os.remove(filepath)
            else:
                shutil.rmtree(filepath)
        except Exception as e:
            abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))
        return ""

@app.route('/trim', methods=['GET'])
@metrics.counter('daas_agent_trim', 'Number of time NVME trim issued')
@authorize
def trim():
    proc = subprocess.run(['fstrim', '-va'])
    return {'returncode' : proc.returncode}


@app.route('/')
@metrics.do_not_track()
def check_running():
    return "The agent is running"

@app.route('/ping/<string:dst_ip>')
@metrics.do_not_track()
@authorize
def ping_host(dst_ip):
    logging.debug('pinging {}'.format(dst_ip))
    delay = ping3.ping(dst_ip)
    logging.debug('latency {}'.format(delay))
    return {'latency' : delay}

@app.route('/tools')
@metrics.do_not_track()
@authorize
def get_transfer_tools():    
    return jsonify(tools)

@app.route('/<string:tool>/poll')
@metrics.counter('daas_agent_polling', 'Number of polling for transfer',
labels={'status': lambda r: r.status_code})
@metrics.gauge('daas_agent_num_transfers', 'Number of transfers waiting to be finished')
@authorize
def poll(tool):
    data = request.get_json()

    if 'dstfile' in data and data['dstfile'] != None:
        data['dstfile'] = os.path.join(app.config['FILE_LOC'], data['dstfile'])
    
    if 'node' in data:
        logging.debug('polling {} {}'.format(data['node'], tool))
    else:
        logging.debug('polling {}'.format(tool))
    target_module = [x for x in loaded_modules if tool in x]    
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)    
    try:        
        retcode = target_tool_cls.poll_progress(**data)
        return jsonify(retcode)
    except TransferTimeout as e:        
            filepath = os.path.relpath(e.file, app.config['FILE_LOC'])
            abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1], file = filepath), 400))
    except Exception:
            abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))

@app.route('/sender/<string:tool>', methods=['POST'])
@metrics.counter('daas_agent_sender', 'Number of sender created',
labels={'status': lambda r: r.status_code})
@authorize
def run_sender(tool):
    if tool not in tools: abort(make_response(jsonify(message="transfer tool {} not found".format(tool)), 404))

    data = request.get_json()
    if not 'file' in data:
        abort(make_response(jsonify(message="file path is not found from request"), 400))

    if data['file'] == None:
        filename = None       

    else:
        filename = os.path.join(app.config['FILE_LOC'], data.get('file'))

        if not os.path.exists(filename): 
            abort(make_response(jsonify(message="file is not found"), 404))    

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if 'libs.' + tool == x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + tool), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    
    if 'numa_scheme' in data:
        tool_obj = target_tool_cls(numa_scheme = data['numa_scheme'], nuttcp_port = nuttcp_port)
    else:
        tool_obj = target_tool_cls(nuttcp_port = nuttcp_port)

    ret = tool_obj.run_sender(filename, **data)
    if not ret['result']:
        abort(make_response(jsonify(message="failed to run" + tool), 400))
    
    return jsonify(ret)

@app.route('/receiver/<string:tool>', methods=['POST'])
@metrics.counter('daas_agent_receiver', 'Number of receiver created',
labels={'status': lambda r: r.status_code})
@authorize
def run_receiver(tool):
    if tool not in tools: abort(404)

    data = request.get_json()    

    if data['file'] == None:
        filename = None
        if 'duration' not in data:
            # TODO : check if server is mem-to-mem
            abort(make_response(jsonify(message="Mem-to-mem transfer requires duration"), 404))
    else:
        filename = os.path.join(app.config['FILE_LOC'], data.pop('file'))    
        
    address = data.pop('address')    

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if 'libs.' + tool == x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + tool), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    tool_obj = target_tool_cls()

    if 'numa_scheme' in data:
        tool_obj = target_tool_cls(numa_scheme = data['numa_scheme'], nuttcp_port = nuttcp_port)
    else:
        tool_obj = target_tool_cls(nuttcp_port = nuttcp_port)

    try:
        ret = tool_obj.run_receiver(address, filename, **data)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))
    
    return jsonify(ret)

@app.route('/cleanup/<string:tool>', methods=['GET'])
@metrics.counter('daas_agent_cleanup', 'Number of cleanup',
labels={'status': lambda r: r.status_code})
@authorize
def cleanup(tool):
    if tool not in tools: abort(make_response(jsonify(message="transfer tool {} not found".format(tool)), 404))

    target_module = [x for x in loaded_modules if 'libs.' + tool == x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + tool), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)

    try:        
        retcode = target_tool_cls.cleanup(nuttcp_port=nuttcp_port)
        return jsonify(retcode)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))

@app.route('/free_port/<string:tool>/<int:port>', methods=['GET'])
@metrics.counter('daas_agent_free_port', 'Number of freeing port',
labels={'status': lambda r: r.status_code})
@authorize
def free_port(tool, port):
    if tool not in tools: abort(make_response(jsonify(message="transfer tool {} not found".format(tool)), 404))

    target_module = [x for x in loaded_modules if 'libs.' + tool == x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + tool), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)

    try:        
        retcode = target_tool_cls.free_port(port)
        return jsonify(retcode)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))

@app.route('/nvme/setup', methods=['POST'])
@authorize
def nvme_setup():
    data = request.get_json()
    ip = data.get('addr')
    numa = int(data.get('numa', 1))
    transport = data.get('transport', 'tcp')
    null_blk = data.get('null_blk', 0)
    inline_data_size = data.get('inline_data_size', 16384)
    dm = disk_manager(nvmeof_numa=numa)
    # check for an existing port first
    # TODO port ID hardcoded
    try:
        nvmet.Port(mode='lookup', portid=1)
    except nvmet.nvme.CFSNotFound:        
        dm.create_nvmeof(numa, ip, transport=transport, 
            num_null_blk=null_blk, inline_data_size=inline_data_size)

    devices =  [x for x in dm.devices if x['numa'] == numa or numa < 0]
    return {"devices": devices}

@app.route('/nvme/setup', methods=['DELETE'])
@authorize
def nvme_stop():
    # TODO move this functionality into the DiskManager library
    # remove subsystem and port
    try:
        existing_port = nvmet.Port(mode='lookup', portid=1)
        existing_port.delete()
        return {"result": "stopped"}
    except nvmet.nvme.CFSNotFound:
        return {"result": "not set up"}
    except PermissionError as e:
        return {"result": str(e)}, 403

@app.route('/nvme/devices')
@authorize
def nvme_devices():
    dm = disk_manager(nvmeof_numa=None)
    dm.scan_nvme()
    dm = fix_dm_mounts(dm) # quick patch device name for mounting
    # also get mounted partitions
    partitions = psutil.disk_partitions()
    mountpoints = []
    for idx, device in enumerate(dm.devices):
        for partition in partitions:
            if partition.device.startswith(device.get('device')):
                mountpoints.append({"device": device.get('device'), "partition": partition.mountpoint})
                dm.devices[idx]['mounted'] = partition.mountpoint
    return {"devices": dm.devices, "mountpoints": mountpoints}

@app.route('/nvme/connect', methods=['POST'])
@authorize
def nvme_connect():
    data = request.get_json()
    remote = data.get('remote_addr')
    transport = data.get('transport', 'tcp')
    mountpoint = data.get('mountpoint')
    username = data.get('username')
    numa = data.get('numa', 0)
    fs = data.get('fs', 'xfs')

    dm = disk_manager(nvmeof_numa=None, mountpoint=mountpoint)
    nqns = dm.discover_nvmeof(remote, 4420)
    dm.connect_nvmeof(remote, nqns, transport=transport)
    if mountpoint:
        # FIXME problem with mounting, device in /sys/class/nvme is different from /dev
        #    Exception: mount: /data/nvme/disk1: special device /dev/nvme1c1n1 does not exist

        # get nvme drive with tcp transport
        remote_id, remote_device = [(idx, dev) for idx, dev in enumerate(dm.devices) if dev.get('transport') == 'tcp'][0]
        # find the first partition - hacky but it works
        remote_partitions = [partition for partition in os.listdir('/dev')
                if remote_device.get('raw_dev') in partition and 'p' in partition]
        if remote_partitions:
            # in case the remote drive is not formatted
            remote_device['device'] = f"/dev/{remote_partitions[0]}"
            try:
                dm.mount(remote_device, remote_id)
            except Exception as e:
                return {"devices": dm.devices, "nqn": nqns, "mount_error": str(e)}, 400

    return {"devices": dm.devices, "nqn": nqns, "mountpoint": mountpoint}

@app.route('/nvme/connect', methods=['DELETE'])
@authorize
def nvme_disconnect():
    data = request.get_json()
    dm = disk_manager()
    # find the remote drive and unmount it
    dm.scan_nvme()
    dm = fix_dm_mounts(dm) # quick patch device name for mounting
    remote_drives = [device for device in dm.devices if device.get('transport') == 'tcp']
    if remote_drives:
        dm.umount(remote_drives[0].get('device'))
        dm.disconnect_nvmeof() # TODO determine nqn if not default
        return "disconnected"

# can't use the authorize decorator here
@app.route('/register', methods=['POST'])
def register_agent():
    global orchestrator_registered
    if orchestrator_registered:
        # we are already registered with an orchestrator, fail immediately
        abort(make_response(jsonify(message="Already registered with an orchestrator"), 400))

    # get address used to contact this DTN and use it as the management address
    data = request.get_json()
    addr = data.get("address")
    override_data_addr = data.get("data_addr")
    override_interface = data.get("interface")
    registration_data = get_registration_data(addr, override_data_addr, override_interface)

    # Set the registered orchestrator and save it
    orchestrator_registered = request.remote_addr
    with open(ORCHESTRATOR_REGISTRATION_PATH, 'w') as f:
        f.write(orchestrator_registered)

    # careful - get_registration_data returns a token that is used to authorize all other
    # API calls.
    return jsonify(registration_data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument("--flask_port", help="Set Flask port", type=int, default=5000)
    parser.add_argument("--nuttcp_port", help="Set nuttcp port", type=int, default=30001)
    args = parser.parse_args()    
    nuttcp_port = args.nuttcp_port

    load_config()    
    app.run('0.0.0.0', port=args.flask_port)
