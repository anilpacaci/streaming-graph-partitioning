#!/bin/sh

PROJECT_NAME="powerlyra"

PL_MASTER_NAME="powerlyra-master"
PL_WORKER_NAME="powerlyra-worker"

MASTER_SERVICE_NAME="${PROJECT_NAME}"_"${PL_MASTER_NAME}"
WORKER_SERVICE_NAME="${PROJECT_NAME}"_"${PL_WORKER_NAME}"

NETWORK_NAME="powerlyra-network"

IMAGE_TAG="127.0.0.1:5000/powerlyra"

IMAGE_FILE="../../containers/analytics"
SERVICE_COMPOSE_FILE="../../containers/analytics/docker-compose.yml"

SWARM_MANAGER_IP="192.168.152.51"

DOCKER_WORKER_NODES="/home/apacaci/docker_machines"

create_network () 
{
    printf "$ docker network create  \\
                --driver overlay      \\
                %s\\n" "${NETWORK_NAME}"
    printf "\\n"

	docker network create               \
            --driver overlay            \
            ${NETWORK_NAME}

    echo "=> network is created"

    delay
}

remove_network ()
{
    printf "\\n\\n===> REMOVE NETWORK"

    echo "$ docker network rm ${NETWORK_NAME}"
    printf "\\n"
    if docker network rm ${NETWORK_NAME} ; then
        echo "=> network is removed"
    else
        echo "=> No problem"
    fi

    delay
}

build_and_push_image ()
{
    printf "\\n\\n===> BUILD IMAGE"
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

