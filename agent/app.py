import os, glob
import stat
import logging
import sys
import subprocess
import traceback
from libs.TransferTools import TransferTools
from libs.Schemes import NumaScheme

logging.getLogger().setLevel(logging.DEBUG)
from flask import Flask, abort, jsonify, request, make_response
app = Flask(__name__)

from prometheus_flask_exporter import PrometheusMetrics, Counter
metrics = PrometheusMetrics(app, export_defaults=False)
metrics.info('app_info', 'Agent service for StarLight DTN-as-a-Service')

import importlib
import pkgutil

MAX_FIO_JOBS=400

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

def get_type(mode):
    if stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        type = 'dir'
    else:
        type = 'file'
    return type

def get_files(dirname):
    contents = []
    total = {'size': 0, 'dir': 0, 'file': 0}

    for filename in os.listdir(dirname):
        filepath = os.path.join(dirname, filename)
        stat_res = os.stat(filepath)
        info = {}
        info['name'] = filename
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

@app.route('/files/', defaults={'path': ''})
@app.route('/files/<path:path>')
@metrics.do_not_track()
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

        prepare_file(job_file, os.path.join(app.config['FILE_LOC'] , file_spec), param[file_spec]['size'])

        file_cnt += 1
        if file_cnt > MAX_FIO_JOBS:
            fio_job_num += 1
            file_cnt = 0
            jobs.append(job_file)

    if job_file not in jobs:
        jobs.append(job_file)

    ret = commit_write(jobs)
    return jsonify(ret.returncode)

@app.route('/file/<path:path>', methods=['DELETE'])
@metrics.counter('daas_agent_file_delete', 'Number of files deleted')
def delete_file(path):

    #TODO : check user permission
    
    #convert abs path to relpath
    if os.path.isabs(path):
        path = path[1:]

    filepath = os.path.join(app.config['FILE_LOC'], path)

    if not os.path.exists(filepath): 
        abort(make_response(jsonify(message="Cannot find the specified file {}".format(filepath)), 400))

    try:
        os.remove(filepath)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))
    return ""

@app.route('/')
@metrics.do_not_track()
def check_running():
    return "The agent is running"

@app.route('/tools')
@metrics.do_not_track()
def get_transfer_tools():    
    return jsonify(tools)

@app.route('/<string:tool>/poll')
@metrics.counter('daas_agent_polling', 'Number of polling for transfer',
labels={'status': lambda r: r.status_code})
@metrics.gauge('daas_agent_num_transfers', 'Number of transfers waiting to be finished')
def poll(tool):
    data = request.get_json()

    if 'dstfile' in data:
        data['dstfile'] = os.path.join(app.config['FILE_LOC'], data['dstfile'])
    
    logging.debug('polling {} {}'.format(data['node'], tool))
    target_module = [x for x in loaded_modules if tool in x]    
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)    
    try:        
        retcode = target_tool_cls.poll_progress(**data)
        return jsonify(retcode)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))

@app.route('/sender/<tool>', methods=['POST'])
@metrics.counter('daas_agent_sender', 'Number of sender created',
labels={'status': lambda r: r.status_code})
def run_sender(tool):
    if tool not in tools: abort(make_response(jsonify(message="transfer tool {} found".format(tool) + target_module), 404))

    data = request.get_json()
    if not 'file' in data:
        abort(make_response(jsonify(message="file path is not found from request"), 400))

    filename = os.path.join(app.config['FILE_LOC'], data.pop('file'))    

    if not os.path.exists(filename): 
        abort(make_response(jsonify(message="file is not found"), 404))

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if tool in x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + target_module), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    
    if 'numa_scheme' in data:
        tool_obj = target_tool_cls(numa_scheme = data['numa_scheme'])
    else:
        tool_obj = target_tool_cls()

    ret = tool_obj.run_sender(filename, **data)
    if not ret['result']:
        abort(make_response(jsonify(message="failed to run" + tool), 400))
    
    return jsonify(ret)

@app.route('/receiver/<tool>', methods=['POST'])
@metrics.counter('daas_agent_receiver', 'Number of receiver created',
labels={'status': lambda r: r.status_code})
def run_receiver(tool):
    if tool not in tools: abort(404)

    data = request.get_json()    

    filename = os.path.join(app.config['FILE_LOC'], data.pop('file'))    
    address = data.pop('address')    

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if tool in x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + target_module), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    tool_obj = target_tool_cls()

    if 'numa_scheme' in data:
        tool_obj = target_tool_cls(numa_scheme = data['numa_scheme'])
    else:
        tool_obj = target_tool_cls()

    try:
        ret = tool_obj.run_receiver(address, filename, **data)
    except Exception:
        abort(make_response(jsonify(message=traceback.format_exc(limit=0).splitlines()[1]), 400))
    
    return jsonify(ret)

if __name__ == '__main__':
    load_config()    
    app.run('0.0.0.0')