#!/bin/bash

# default parameters incase swarm.conf is not defined
PROJECT_NAME="janusgraph"

NETWORK_NAME="janusgraph-network"

JANUSGRAPH_IMAGE_TAG="127.0.0.1:5000/janusgraph"

JANUSGRAPH_IMAGE_FILE="../../containers/interactive"
SERVICE_COMPOSE_FILE="../../containers/interactive/docker-compose-4nodes.yml"
MASTER_COMPOSE_FILE="../../containers/interactive/docker-compose-master.yml"

SWARM_MANAGER_IP="192.168.152.201"

DOCKER_WORKER_NODES="/home/apacaci/docker_machines"


# include conf file if it is defined
if [ -f ./swarm.conf ]; then
    . ./swarm.conf
fi

# set variables

MASTER_SERVICE_NAME="${PROJECT_NAME}"_"${JG_MASTER_NAME}"
WORKER_SERVICE_NAME="${PROJECT_NAME}"_"${JG_WORKER_NAME}"

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

	echo "$ docker build -t \"${JANUSGRAPH_IMAGE_TAG}\" \"${JANUSGRAPH_IMAGE_FILE}\""
    printf "\\n"
    docker build -t ${JANUSGRAPH_IMAGE_TAG} ${JANUSGRAPH_IMAGE_FILE}

	echo "$ docker build -t \"${MASTER_IMAGE_TAG}\" \"${MASTER_IMAGE_FILE}\""
    printf "\\n"
    docker build -t ${MASTER_IMAGE_TAG} ${MASTER_IMAGE_FILE}

    printf "\\n\\n===> START REGISTRY"
	echo "$ docker service create --name registry --constraint 'node.role == manager' --publish 5000:5000 registry:2"
	docker service create --name registry --constraint 'node.role == manager' --publish 5000:5000 registry:2
	
    printf "\\n\\n===> PUSH IMAGE TO REGISTRY"
    echo "$ docker push \"${JANUSGRAPH_IMAGE_TAG}\""
    printf "\\n"
    docker push ${JANUSGRAPH_IMAGE_TAG}

	echo "$ docker push \"${MASTER_IMAGE_TAG}\""
    printf "\\n"
    docker push ${MASTER_IMAGE_TAG}
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

	# sleep before checking cluster status
	sleep 5;
	
	while [ $(get_cluster_size) -lt ${JG_WORKER_COUNT} ]
	do
		echo "$(get_cluster_size)/${JG_WORKER_COUNT} Cassandra node is up, wait 10 seconds"
		sleep 10;
	done

	echo "$ docker stack deploy -c \"${MASTER_COMPOSE_FILE}\" \"${PROJECT_NAME}\" "
    printf "\\n"
    docker stack deploy -c ${MASTER_COMPOSE_FILE} ${PROJECT_NAME}
}

stop_service()
{
    printf "\\n\\n===> STOP POWERLYRA SERVICE \\n"

	for (( counter=1 ; counter <=JG_WORKER_COUNT ; counter++ ))
	do
    	echo "$ docker service rm \"${WORKER_SERVICE_NAME}${counter}\" "
    	printf "\\n"
    	docker service rm ${WORKER_SERVICE_NAME}${counter}
	done

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

run_command()
{
	local MACHINE_NAME=$1
	local COMMAND="$2"	

	echo $COMMAND

	printf "\\n\\n ===> EXECUTE COMMAND"
    printf "\\n"
    if [ $# -lt 2 ] ; then
        echo "Supply node name and command string"
        exit 1
    fi

	TASK_ID="$(docker service ps -q ${MACHINE_NAME} | head -n 1)"
	CONT_ID="$(docker inspect -f "{{.Status.ContainerStatus.ContainerID}}" ${TASK_ID})"	
	NODE_ID="$(docker inspect -f "{{.NodeID}}" ${TASK_ID})"
	NODE_IP="$(docker inspect -f {{.Status.Addr}} ${NODE_ID})"

	echo "ssh \"${NODE_IP}\" docker exec  \"${CONT_ID}\" bash  ${COMMAND} "

	func_result=$(ssh ${NODE_IP} docker exec ${CONT_ID} bash  ${COMMAND} 2>&1)

	echo "$func_result"
}

get_cluster_size()
{
	func_result="$(run_command janusgraph_worker1 'nodetool status' | grep 'UN' | wc -l)"

    echo "$func_result"
}

print_cluster_size()
{
	printf "\\n\\n===> CASSANDRA # of NODES \\n"

	func_result="$(get_cluster_size)"

	echo "$func_result"
}

usage()
{
    echo ' More info: https://github.com/anilpacaci/streaming-graph-partitioning'
    echo ''
    echo '=============================================================='
    echo ''

    echo "To run a set of experiments:"
    echo "	1. Initialize the docker cluster in swarm mode"
    echo "		$ ./swarm.sh init"
    echo ""
    echo "		starts a swarm master that is advertised with given IP and"
    echo "		and creates an overlay network "
    echo ""
    echo "	2. Build the JanusGraph container and deploy to local registry:"
    echo "		$ ./swarm.sh build"
    echo ""
    echo "		builds and deploys the specified docker image"
    echo ""
    echo "	3. Start JanusGraph cluster:"
    echo "		$ ./swarm.sh start"
    echo ""
    echo "		starts a container in the machines specified in the host file"
    echo ""
    echo "	4. Run experiments:"
    echo "		$ ./swarm.sh run config_file"
    echo ""
    echo "		runs a set of experiments described in the config file"
    echo ""
    echo "  5. Run command directly on container:"
    echo "      $ ./swarm.sh cmd service_name command"
    echo ""
    echo "      executes the given command directly on the container"
    echo ""    
	echo "	6. Stop JanusGraph container:"
    echo "		$ ./swarm.sh stop"
    echo ""
    echo "		stops containers in the machines specified in the host file"
    echo ""
    echo "	7. Tear down docker cluster:"
    echo "		$ ./swarm.sh destroy"
    echo ""
    echo "		removes the overlay network and forces nodes to leave the swarm"
    echo ""
    echo "	8. Print this help message:"
    echo "		$ ./swarm.sh usage"
    echo ""
}

# Identify the command and run the corresponding function


COMMAND=$1
PARAM1=$2
PARAM2="$3"

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
		if [ -z $PARAM1 ] 
		then
			echo "usage: ${0} ${COMMAND} config_file"
			exit 2
		fi
		run_experiments $PARAM1
		exit 0
	;;
	(cmd)
		if [ -z $PARAM1 ] || [ -z $PARAM2 ]
		then
			echo "usage: ${0} ${COMMAND} machine_name command"
			exit 2
		fi
		run_command $PARAM1 "$PARAM2"
		exit 0
	;;
	(size)
		print_cluster_size
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
		echo "ERROR: unknown parameter \"$COMMAND\""
		usage
		exit 2
	;;
esac
