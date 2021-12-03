#!/bin/bash
# Usar este script en un servidor de testing solamente.
path_agentes="~/agentes"
cd ${path_agentes}
git checkout master
git pull origin master > tmp-pull.log

if [[ $(tail -1 tmp-pull.log) != "Already up to date." ]]; then
    source env/bin/activate
    status_code=$($(python main.py 2> /dev/null); echo $?)
    if [ $status_code -eq 1 ]; then
        echo "Repositories will not be updated due to a bug - $(date)" >> ${path_agentes}/log/autopull.log
    elif [ $status_code -eq 2 ]; then
        echo "Updated repositories - $(date)" >> ${path_agentes}/log/autopull.log
        ssh -i ~/.ssh/bb -l bb us "sh -c ${path_agentes}/deploy_agentes.sh"
        echo -e "\033[32mDone\033[0m"
    fi
fi

git checkout platforms-dev
