#!/bin/bash
path_agentes=~/agentes
cd $path_agentes
path_logs=$path_agentes/log
LIMIT_DAYS=14

for _file in `ls ${path_logs}`; do
    _path_log="$path_logs/${_file}"
    if [ ! -f $_path_log ]; then
        continue
    fi
    datelog=`echo ${_file} | awk -F '[_]' '{ print $NF }' | tr -d [:alpha:] | tr -d [:punct:]`
    if [[ ${#datelog} -eq 8 ]]; then
        current_date=$(date -d "${datelog}" +%s)
        limit_date=$(date -d "-${LIMIT_DAYS} days" +%s)
        if [[ $current_date -le $limit_date ]]; then
            echo -e "Removing log: \t $(du -h ${_path_log})"
            rm -f ${_path_log}
        fi
    fi
done
