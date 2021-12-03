#!/bin/bash
path_agentes="/home/bb/agentes"
cd $path_agentes
path_logs=$path_agentes/log
LIMIT_DAYS=14

for _file in `ls ${path_logs}`; do
    # echo "$path_logs/${_file}"
    datelog=`echo ${_file} | awk -F '[_]' '{ print $NF }' | tr -d [:alpha:] | tr -d [:punct:]`
    if [[ ${#datelog} -eq 8 ]]; then
        current_date=$(date -d "${datelog}" +%s)
        limit_date=$(date -d "-${LIMIT_DAYS} days" +%s)
        _path_log="$path_logs/${_file}"
        if [[ $current_date -le $limit_date ]]; then
            echo Removing log ${_path_log}
            rm -f ${_path_log}
        fi
    fi
done
