#!/usr/bin/env python
import glob
import os
import subprocess
import platform
import shutil
import argparse
import yaml
from os import path
from pathlib import Path

"""
    RESET TRIAL PARA STUDIO3T & NOSQLBOOSTER(en desarrollo)
    Para resetear Studio3t:
        python handle/resettrial.py --p s3t
"""

current_system = platform.system().lower()
current_system = "mac" if current_system == "darwin" else current_system

__config = None
base_path = Path(__file__).parent.parent
file_path = (base_path / 'data' / '_resources.yml')
__file = str(file_path)

valid_options = ("s3t", "nsb", "dg", "studio3t", "nosqlbooster", "datagrip", "all", )
aliases = {
    "s3t": "studio3t",
    "nsb": "nosqlbooster",
    "dg": "datagrip"
}

def _config():
    global __config
    if not __config:
        with open(__file, mode='r') as f:
            __config = yaml.load(f, Loader=yaml.FullLoader)
    return __config


if __name__ == "__main__":
    config = _config()
    parser =  argparse.ArgumentParser()
    parser.add_argument('--p', help='Programa a elegir o "all" para seleccionar ambos.', type=str, choices=valid_options)
    args = parser.parse_args()
    program = args.p

    program = aliases.get(program, program)
    if program == "all":
        list_apps = [app for app in config.keys()]
    else:
        list_apps = [program]

    for app in list_apps:
        prefs_to_restore = []
        save_prefs = config[app][current_system].get("save-prefs", [])
        for pref in save_prefs:
            try:
                source = path.expandvars(pref)
                source = path.expanduser(source)
                shutil.copy(src=source, dst='.')
                prefs_to_restore.append(source) 
            except:
                continue
        paths = config[app][current_system]["paths"]
        # Removing paths
        for _path in paths:
            formated_path = path.expandvars(_path)
            formated_path = path.expanduser(formated_path)
            files_match = glob.glob(formated_path)
            for file in files_match:
                try:
                    if path.isfile(file):
                        os.remove(file)
                    else:
                        shutil.rmtree(file)
                except:
                    pass
        # Regedit
        for reg in config[app][current_system].get("reg", []):
            _command = f"reg delete {reg} /f 2> nul"
            subprocess.run(_command, shell=True)
        # Restoring prefs
        for _path in prefs_to_restore:
            try:
                parts = _path.split('/')[:-1]
                parent_dir = path.join('/', *parts)
                if not path.exists(parent_dir):
                    os.makedirs(parent_dir)
                file_name = _path.split('/')[-1]
                shutil.copy(src=file_name, dst=_path)
                os.remove(file_name)
            except:
                continue
    print("\033[32;1;5;40mReset done!\033[0m")
