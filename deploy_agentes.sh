#!/bin/bash
#
# Server's GitLab automation. Update master every one hour.
#
# Usage: bash deploy_agentes.sh
#

path_agentes="/home/bb/agentes"

function git_pull {
    user=$1
    host=$2
    ssh -i ${HOME}/.ssh/bb -l ${user} ${host} 'cd agentes; git pull --quiet origin master'
}

hosts="sg ca us de2 de3 nl gb gb2 ca2 de"

if [[ ${HOSTNAME} == "dlv-us" ]]; then
    for hostname in ${hosts}; do
        echo "Actualizando $hostname ..."
        git_pull bb ${hostname}
    done
else
    ssh -i bb.pem bb@167.172.228.104 "bash -c ${path_agentes}/deploy_agentes.sh"
fi
