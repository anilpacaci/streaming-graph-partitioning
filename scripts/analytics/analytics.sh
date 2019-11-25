#!/bin/bash

# default parameters incase interactive.conf is not defined
PROJECT_NAME="powerlyra"

PL_MASTER_NAME="powerlyra-master"
PL_WORKER_NAME="powerlyra-worker"

NETWORK_NAME="powerlyra-network"

IMAGE_TAG="127.0.0.1:5000/powerlyra"

IMAGE_FILE="../../containers/analytics"
SERVICE_COMPOSE_FILE="../../containers/analytics/docker-compose.yml"

SWARM_MANAGER_IP="192.168.152.51"

DOCKER_WORKER_NODES="/home/apacaci/docker_machines"


# include conf file if it is defined
if [ -f ./analytics.conf ]; then
    . analytics.conf
fi

# set variables

MASTER_SERVICE_NAME="${PROJECT_NAME}"_"${PL_MASTER_NAME}"
WORKER_SERVICE_NAME="${PROJECT_NAME}"_"${PL_WORKER_NAME}"

create_network () 
{
	printf "\\n\\n===> CREATE OVERLAY NETWORK"
    printf "$ docker network create  \\
                --driver overlay      \\
                %s\\n" "${NETWORK_NAME}"
    printf "\\n"

	docker network create               \
            --driver overlay            \
            ${NETWORK_NAME}

    echo "=> network is created"
}

remove_network ()
{
    printf "\\n\\n===> REMOVE OVERLAY NETWORK"
	printf "\\n"	

    echo "$ docker network rm ${NETWORK_NAME}"
    printf "\\n"
    if docker network rm ${NETWORK_NAME} ; then
        echo "=> network is removed"
    else
        echo "=> No problem"
    fi
}

build_and_push_image ()
{
    printf "\\n\\n===> BUILD IMAGE"
    printf "\\n"

	echo "$ docker build -t \"${IMAGE_TAG}\" \"${IMAGE_FILE}\""
    printf "\\n"
    docker build -t ${IMAGE_TAG} ${IMAGE_FILE}

    printf "\\n\\n===> START REGISTRY"
	echo "$ docker service create --name registry --constraint 'node.role == manager' --publish 5000:5000 registry:2"
	docker service create --name registry --constraint 'node.role == manager' --publish 5000:5000 registry:2
	
    printf "\\n\\n===> PUSH IMAGE TO REGISTRY"
    echo "$ docker push \"${IMAGE_TAG}\""
    printf "\\n"
    docker push ${IMAGE_TAG}
}

init_swarm()
{
    printf "\\n\\n===> SWARM INIT\\n"
    printf "\\n"
	
	echo "$ docker swarm init --advertise-addr \"${SWARM_MANAGER_IP}\""
    printf "\\n"
    docker swarm init --advertise-addr ${SWARM_MANAGER_IP}

	JOIN_COMMAND=$(docker swarm join-token worker)
	JOIN_COMMAND=${JOIN_COMMAND##*command:}
	JOIN_COMMAND=${JOIN_COMMAND%%[[:space:]]* }	

	printf "\\n\\n===> SWARM WORKER TOKEN\\n"
    echo "$ \"${JOIN_COMMAND}\""
    printf "\\n"

	printf "\\n\\n===> WORKERS JOINING THE SWARM\\n"
    echo "$ pdsh -w ^${DOCKER_WORKER_NODES} -R ssh \"${JOIN_COMMAND}\""
    printf "\\n"
	pdsh -w ^${DOCKER_WORKER_NODES} -R ssh "${JOIN_COMMAND}"

}

destroy_swarm()
{
    printf "\\n\\n===> DESTROY SWARM\\n"

    echo "$ pdsh -w ^${DOCKER_WORKER_NODES} -R ssh \"docker swarm leave\""
    printf "\\n"
    pdsh -w ^${DOCKER_WORKER_NODES} -R ssh "docker swarm leave"

	printf "\\n\\n===> MASTER NODE LEAVE\\n"
	echo "$ docker swarm leave -f"
    printf "\\n"
    docker swarm leave -f
}

start_service()
{
	printf "\\n\\n===> START POWERLYRA SERVICE \\n"

	echo "$ docker stack deploy -c \"${SERVICE_COMPOSE_FILE}\" \"${PROJECT_NAME}\" "
	printf "\\n"
	docker stack deploy -c ${SERVICE_COMPOSE_FILE} ${PROJECT_NAME}
}

stop_service()
{
    printf "\\n\\n===> STOP POWERLYRA SERVICE \\n"

    echo "$ docker service rm \"${WORKER_SERVICE_NAME}\" "
    printf "\\n"
    docker service rm ${WORKER_SERVICE_NAME}

	echo "$ docker service rm \"${MASTER_SERVICE_NAME}\" "
    printf "\\n"
    docker service rm ${MASTER_SERVICE_NAME}
}

run_experiments()
{
	printf "\\n\\n ===> RUN EXPERIMENTS"
	printf "\\n"
	if [ $# -eq 0 ] ; then
		echo "Supply relative path of the configuration file under /sgp/parameters/"
		exit 1
	fi

	EXPERIMENT_CONF=$1

	echo "docker exec -u mpi -it \"${MASTER_SERVICE_NAME}\".1.\$\(docker service ps -f name=\"${MASTER_SERVICE_NAME}\".1 \"${MASTER_SERVICE_NAME}\" -q --no-trunc \| head -n1\) /sgp/scripts/run_experiments.py \"${EXPERIMENT_CONF}\" "
	printf "\\n"
	docker exec -u mpi -it ${MASTER_SERVICE_NAME}.1.$(docker service ps -f name=${MASTER_SERVICE_NAME}.1 ${MASTER_SERVICE_NAME} -q --no-trunc | head -n1) /sgp/scripts/run_experiments.py ${EXPERIMENT_CONF}
}


generate_plots()
{
    printf "\\n\\n ===> GENERATE PLOTS"
    printf "\\n"
    if [ $# -eq 0 ] ; then
        echo "Supply relative path of the configuration file under /sgp/parameters/"
        exit 1
    fi

    # shift once to eliminate the first argument
    shift
    PARAMS=("$@")

    echo "Result plots will be generated for configurations:"

    for PARAM in "${PARAMS[@]}"
    do
        echo "Config file: ${PARAM}"
    done

    echo "\"${PARAMS[*]}\""

    echo "docker exec -u mpi -it \"${MASTER_SERVICE_NAME}\".1.\$\(docker service ps -f name=\"${MASTER_SERVICE_NAME}\".1 \"${MASTER_SERVICE_NAME}\" -q --no-trunc \| head -n1\) /sgp/scripts/gnuplot_generator.py ${PARAMS[*]} "
    printf "\\n"
    docker exec -u mpi -it ${MASTER_SERVICE_NAME}.1.$(docker service ps -f name=${MASTER_SERVICE_NAME}.1 ${MASTER_SERVICE_NAME} -q --no-trunc | head -n1) /sgp/scripts/gnuplot_generator.py ${PARAMS[*]}

}


usage()
{
    echo ' More info: https://github.com/anilpacaci/streaming-graph-partitioning'
    echo ''
    echo '=============================================================='
    echo ''

    echo "To run a set of experiments:"
    echo "	1. Initialize the docker cluster in swarm mode"
    echo "		$ ./analytics.sh init"
    echo ""
    echo "		starts a swarm master that is advertised with given IP and"
    echo "		and creates an overlay network "
    echo ""
    echo "	2. Build the PowerLyra container and deploy to local registry:"
    echo "		$ ./analytics.sh build"
    echo ""
    echo "		builds and deploys the specified docker image"
    echo ""
    echo "	3. Start PowerLyra cluster:"
    echo "		$ ./analytics.sh start"
    echo ""
    echo "		starts a container in the machines specified in the host file"
    echo ""
    echo "	4. Run experiments:"
    echo "		$ ./analytics.sh run config_file"
    echo ""
    echo "		runs a set of experiments described in the config file"
    echo ""
    echo "	5. Run experiments:"
    echo "		$ ./analytics.sh plot [config_file_1, ..., config_file_n]"
    echo ""
    echo "		generates result plots from the results of each experiment given by config files"
    echo ""
    echo "	6. Stop PowerLyra container:"
    echo "		$ ./analytics.sh stop"
    echo ""
    echo "		stops containers in the machines specified in the host file"
    echo ""
    echo "	7. Tear down docker cluster:"
    echo "		$ ./analytics.sh destroy"
    echo ""
    echo "		removes the overlay network and forces nodes to leave the swarm"
    echo ""
    echo "	8. Print this help message:"
    echo "		$ ./analytics.sh usage"
    echo ""
}

# Identify the command and run the corresponding function


COMMAND=$1
PARAM=$2
PARAMS=("$@")

case "$COMMAND" in
	(start) 
		start_service
		exit 0
	;;
	(stop)
		stop_service
		exit 0
	;;
	(build)
		build_and_push_image
		exit 0
	;;
	(run)
		if [ -z $PARAM ] 
		then
			echo "usage: ${0} ${COMMAND} config_file"
			exit 2
		fi
		run_experiments $PARAM
		exit 0
	;;
	(plot)
        if [ $# -eq 1 ]
        then
            echo "usage: ${0} ${COMMAND} [config_file_1, ..., config_file_n]"
            exit 2
        fi
        generate_plots "${PARAMS[@]}"
        exit 0
    ;;
	(init)
        init_swarm $PARAM
		create_network
        exit 0
	;;
	(destroy)
		remove_network
		destroy_swarm
		exit 0
	;;
	(usage)
		usage
		exit 0
	;;
	(*)
		echo "ERROR: unknown parameter \"${COMMAND}\""
		usage
		exit 2
	;;
esac
