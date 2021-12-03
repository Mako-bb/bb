#!/usr/bin/env python3
import subprocess
import time
from os import path, remove

PATH_RESET_LOCK = "/tmp/reset-mongo-db.lock"


def check_service_name():
    service_name = "mongodb"
    out = subprocess.check_output(["systemctl", "list-unit-files"])
    if service_name not in str(out):
        service_name = "mongod"
    return service_name

def cmd(command):
    sp = subprocess.run(command, shell=True)

if __name__ == "__main__":
    service_name = check_service_name()
    spawn_command = f"service {service_name} restart"
    while True:
        if path.exists(PATH_RESET_LOCK):
            cmd(command=spawn_command)
            time.sleep(5)
            remove(PATH_RESET_LOCK)
        
        time.sleep(15)
