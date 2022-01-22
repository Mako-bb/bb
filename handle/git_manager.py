#!/usr/bin/env python
import socket
import subprocess
from datetime import datetime
import os
import sys

try:
    from settings import settings
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from settings import settings


fixtures = [
    "data/languages_validator.json",
]
TEMPLATE_COMMIT = """{filename}: Commit from {root_name} at {datetime} -> {custom_msg}"""
HOSTNAME = settings.DLV_ROOT_NAME or socket.gethostname()
DEFAULT_BRANCH = "master"


class GitManager():

    def __init__(self, file=None) -> None:
        self.__file = file

    @property
    def file(self):
        return self.__file

    @file.setter
    def file(self, new_value):
        self.__file = new_value

    def add(self, file) -> None:
        command = f"git add -f {file}"
        _ = self.__run_command(command=command.split())

    def pull(self) -> None:
        command = f"git pull origin {DEFAULT_BRANCH} --no-edit"
        _ = self.__run_command(command=command.split())
        command_status = "git status"
        output = self.__run_command(command=command_status.split())
        if "fix conflicts and run" in output:
            self._solve_conflicts()

    def push(self) -> None:
        command = f"git push origin {DEFAULT_BRANCH}"
        _ = self.__run_command(command=command.split())

    def commit(self, msg: str) -> None:
        command = ["git", "commit", "-m", msg]
        _ = self.__run_command(command=command)

    def _solve_conflicts(self) -> None:
        """Resuelve los conflictos de git.
        Solamente aprueba las diferencias sin descartar ninguna, realiza el commit y
        luego la sube.
        """
        with open(self.__file, "r") as file:
            data = file.read()

        parts = data.split("<<<<<<<")
        index_first_remain = parts[0].find("\n")
        first_remain = parts[0][index_first_remain:]
        _temp_parts = parts[1].split(">>>>>>>")
        index_last_remain = _temp_parts[1].find("\n")
        last_remain = _temp_parts[1][index_last_remain+1:]
        mid_problems = _temp_parts[0].split("=======")
        mid_problems[0] = mid_problems[0].replace('HEAD','',1)
    
        data = first_remain + "\n" + "\n".join(mid_problems) + "\n" + last_remain

        with open(self.__file, "w") as file:
            file.writelines(data)
        self.add(file=self.file)

        custom_msg = "FIX conflicts!"
        message_commit = TEMPLATE_COMMIT.format(filename=self.__file, root_name=HOSTNAME, datetime=datetime.now(), custom_msg=custom_msg)
        self.add(file=self.__file)
        self.commit(message_commit)

    def __checkout(self, branch: str) -> None:
        self.__run_command(f"git checkout {branch}")

    @classmethod
    def comprobe_new_changes(cls) -> bool:
        text = "Changes not staged for commit:"
        command = "git status"
        output = cls.__run_command(command=command.split())
        has_changes = False
        if text in output:
            has_changes = True
        return has_changes

    @classmethod
    def comprobe_diff(cls, file) -> None:
        """Comprueba si un archivo tiene diferencias.
        """
        command = ["git", "diff", file,]
        output = cls.__run_command(command=command)
        return True if output else False

    # TODO: tener en cuenta que los archivos no trackeados redirigen al stderr
    # y genera una excepción
    @classmethod
    def __run_command(cls, command: list, shell=True) -> str or None:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        return proc.stdout.read().decode("utf-8")


class GitClient():

    def __init__(self, file=None):
        self._instance = GitManager()

    def start(self):
        has_changes = self._instance.comprobe_new_changes()
        
        if has_changes:
            for file in fixtures:
                self._instance.file = file
                has_diff = self._instance.comprobe_diff(file)
                if has_diff:
                    self._instance.add(file)
                    message_commit = TEMPLATE_COMMIT.format(filename=file, root_name=HOSTNAME, datetime=datetime.now(), custom_msg="Adding changes from root.")
                    self._instance.commit(msg=message_commit)
            
            self._instance.pull()
            self._instance.push()


if __name__ == "__main__":
    client = GitClient()
    client.start()
