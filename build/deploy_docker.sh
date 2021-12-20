#!/bin/bash

project_name="agentes"
only_up_services="false"
access_container="false"
build_image="false"
create_process="false"
default_port="27017"

help_dialog() {
    echo "USAGE:"
    echo -e "  bash $0 \033[1mREGION\033[0m"
    echo
    echo "OPTIONS:"
    echo -e "  --help, -h\t\tShow this help and exit."
    echo -e "  --build, -b\t\tBuikd Docker images."
    echo -e "  --up, -u\t\tUp services/containers."
    echo -e "  --create, -b\t\tCreate multiple containers."
    echo -e "  --enter, -e\t\tEnter the main container."
    echo
    echo "EXAMPLE:"
    echo -e "  \033[1mbash $0 -u ASIA\033[0m"
}

echo_msg () {
    echo -e "\033[32m$(date -Is) --- $1\033[0m"
}

echo_err () {
    echo -e "\033[31m$(date -Is) *** $1\033[0m"
}

determine_port() {
    for port in 270{17..35}; do
        output=$(netstat -an | grep "${port}" | grep LISTEN)
        if [[ -z "${output}" ]]; then
            default_port="${port}"
            break
        fi
    done
}

setting_vars() {
    export CONTAINER_NAME="${project_name}-$1"
    export MONGO_CONTAINER_NAME="mongo-$1"
    export CONTAINER_HOSTNAME="docker-contabo-$1"
    export DLV_ROOT_NAME_DOCKER="$1"
    export NETWORK_NAME="network-agentes-$1"
    export DATA_DB_CONTAINER="/opt/data/db/$1"
    export PROJECT_IMAGE="bb/${project_name}:latest"
    determine_port
    export MONGO_EXTERNAL_PORT="$?"
}

######################################
#               INIT                 #
######################################
if [[ $# -eq 1 ]]; then
    case $1 in
        -h|--help)
            help_dialog
            exit 0
        ;;
        -b|--build)
            build_image="true"
        ;;
        -c|--create)
            create_process="true"
        ;;
        *)
            echo_err "Wrong option. Type -h."
            exit 1
        ;;
    esac
elif [[ $# -eq 2 ]]; then
    case $1 in
        -u|--up)
            only_up_services="true"
        ;;
        -e|--enter)
            access_container="true"
        ;;
        *)
            echo_err "Wrong option. Type -h."
            exit 1
        ;;
    esac
else
    echo_err "Invalid number of arguments."
    help_dialog
    exit 1
fi


###################################################
#       BUILDING AND MANAGING PROCESS             #
###################################################
if [[ "$build_image" = "true" ]]; then
    docker-compose -f build/docker-compose.yml build
elif [[ "$create_process" = "true" ]]; then
    set -a
    source .env
    i=0
    for region in $DOCKER_REGION_1 $DOCKER_REGION_2; do
        ((i++))
        if [[ "$i" == '1' ]]; then
            compose_file=build/docker-compose.yml
        else
            compose_file=build/docker-compose-2.yml
        fi
        if [[ -z "$region" ]]; then
            echo_err "Region not specified."
        else
            setting_vars $region
            exists_container=$(docker container inspect ${CONTAINER_NAME} | grep "Error: No such container:")
            if [[ ! -z "$exists_container" ]]; then
                echo_err "A container with the same region already exists."
                continue
            fi
            ###############################
            echo -e "Deploying Docker..."
            ###############################
            docker-compose -f $compose_file up -d
        fi
    done
elif [[ "$only_up_services" = "true" ]]; then
    setting_vars $2
    exists_container=$(docker container inspect ${CONTAINER_NAME} | grep "Error: No such container:")
    if [[ ! -z "$exists_container" ]]; then
        echo_err "A container with the same region already exists."
        exit 1
    fi
    ###############################
    echo -e "Deploying Docker..."
    ###############################
    docker-compose -f build/docker-compose.yml up -d

elif [[ "${access_container}" = "true" ]]; then
    setting_vars $2
    exists_container=$(docker container inspect ${CONTAINER_NAME} | grep "Error: No such container:")
    if [[ $exists_container -ne 0 ]]; then
        echo_err "There is no container with the specified region."
        exit 1
    fi
    export MONGO_DOCKER_IP="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.Gateway}}{{end}}' ${MONGO_CONTAINER_NAME})"  # Usa la del Gateway en vez de IPAdress
    export MONGO_DOCKER_PORT="$(docker inspect --format='{{range $p, $conf := .NetworkSettings.Ports}}{{(index $conf 0).HostPort}}{{end}}' ${MONGO_CONTAINER_NAME})"
    export MONGODB_DOCKER_DATABASE_URI="mongodb://${MONGO_DOCKER_IP}:${MONGO_DOCKER_PORT}"
    docker exec -i -t -e MONGODB_DOCKER_DATABASE_URI=${MONGODB_DOCKER_DATABASE_URI} ${CONTAINER_NAME} /bin/bash -c "su bb"
fi
unset ACTIVATION_CODE
