# -*- coding: utf-8 -*-
import platform
import subprocess
from common import config

def cmd(commando):
    subprocess.run(commando, shell=True)

if __name__ == "__main__":
    os = platform.system()
    if os == 'Linux':
        cmd("./split_itunes.sh")
    else:
        cmd("split_itunes.sh")
        
    countries = config()['ott_sites']['Itunes']['countries']

    for country in countries.keys():
        if country == False:
            country = "NO"

        cmd("python main.py Itunes --o scraping --c {}".format(str(country)))