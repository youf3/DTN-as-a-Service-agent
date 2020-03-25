import os
import stat
import logging
import sys
from libs.TransferTools import TransferTools

logging.getLogger().setLevel(logging.DEBUG)
from flask import Flask, abort, jsonify, request, make_response
app = Flask(__name__)

import importlib
import pkgutil

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
running_p = []

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

@app.route('/files/', defaults={'path': ''})
@app.route('/files/<path:path>')
def list_files(path):
    try:
        contents = get_files(os.path.join(app.config['FILE_LOC'], path))
    except PermissionError:
        abort(403)
    except FileNotFoundError:
        abort(404)
    return jsonify(contents)

@app.route('/')
def check_running():
    return "The agent is running"

@app.route('/tools')
def get_transfer_tools():    
    return jsonify(tools)

@app.route('/<string:tool>/<int:port>/poll')
def poll(tool, port):
    target_module = [x for x in loaded_modules if tool in x]    
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    retcode = target_tool_cls.poll_progress(target_tool_cls.running_cli_threads, port)
    return jsonify(retcode)

@app.route('/sender/<tool>', methods=['POST'])
def run_sender(tool):
    if tool not in tools: abort(404)

    data = request.get_json()
    if not('port' in data and 'file' in data):
        abort(make_response(jsonify(message="port and/or data is not found" + target_module), 400))

    port = data.pop('port')    
    filename = os.path.join(app.config['FILE_LOC'], data.pop('file'))    

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if tool in x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + target_module), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    tool_obj = target_tool_cls()

    ret = tool_obj.run_sender(port, filename, **data)
    if not ret:
        abort(make_response(jsonify(message="failed to run" + tool), 400))
    
    return jsonify({'result': True})

@app.route('/receiver/<tool>', methods=['POST'])
def run_receiver(tool):
    if tool not in tools: abort(404)

    data = request.get_json()
    if not('port' in data and 'file' in data):
        abort(make_response(jsonify(message="port and/or data is not found" + target_module), 400))

    port = data.pop('port')
    filename = os.path.join(app.config['FILE_LOC'], data.pop('file'))    
    address = data.pop('address')    

    # find the module for a tool and instantiate it
    target_module = [x for x in loaded_modules if tool in x]
    if len(target_module) > 1 :
        abort(make_response(jsonify(message="Duplicated transfer tool name" + target_module), 400))
    target_tool_cls = getattr(loaded_modules[target_module[0]], tool)
    tool_obj = target_tool_cls()

    ret = tool_obj.run_receiver(address, port, filename, **data)
    if not ret:
        abort(make_response(jsonify(message="failed to run" + tool), 400))
    
    return jsonify({'result': True})

if __name__ == '__main__':
    load_config()    
    app.run('0.0.0.0')