#!/bin/bash
set -e

# common function defined by original cassandra docker-entrypoint

_ip_address() {
    # scrape the first non-localhost IP address of the container
    # in Swarm Mode, we often get two IPs -- the container IP, and the (shared) VIP, and the container IP should always be first
    ip address | awk '
        $1 == "inet" && $NF != "lo" {
            gsub(/\/.+$/, "", $2)
            print $2
            exit
        }
    '
}

# "sed -i", but without "mv" (which doesn't work on a bind-mounted file, for example)
_sed-in-place() {
    local filename="$1"; shift
    local tempFile
    tempFile="$(mktemp)"
    sed "$@" "$filename" > "$tempFile"
    cat "$tempFile" > "$filename"
    rm "$tempFile"
}

_sed-append() {
	local filename="$1"; shift
	local tempFile
	tempFile="$(mktemp)"
	cat "$filename" > "$tempFile"
	echo "$@" >> "$tempFile"
	cat "$tempFile" > "$filename"
    rm "$tempFile"
}

# Commands to be executed as root

# Janusgraph specific configuration
JANUSGRAPH_STORAGE_HOSTNAME="$(_ip_address)"

# update storage hostname to point out cassandra address
if [ "$JANUSGRAPH_STORAGE_HOSTNAME" ]; then
    _sed-in-place "$JANUSGRAPH_HOME/conf/gremlin-server/janusgraph-cassandra-es-server.properties" \
        -r 's/^(# )?(storage\.hostname\s*=).*/\2 '$JANUSGRAPH_STORAGE_HOSTNAME'/'
fi

# # of partitions in the cluster
if [ "$JANUSGRAPH_CLUSTER_SIZE" ]; then
    _sed-in-place "$JANUSGRAPH_HOME/conf/gremlin-server/janusgraph-cassandra-es-server.properties" \
            -r 's/^(# )?(cluster\.max\-partitions\s*=).*/\2 '$JANUSGRAPH_CLUSTER_SIZE'/'
fi

# modified using original cassandra entrypoint

# first arg is `-f` or `--some-option`
# or there are no args
if [ "$#" -eq 0 ] || [ "${1#-}" != "$1" ]; then
	# set -- cassandra -f "$@"
	# to allow multiple background processes to be fired
	set -- cassandra "@"
fi

# allow the container to be started with `--user`
#if [ "$1" = 'cassandra' -a "$(id -u)" = '0' ]; then
#	find /var/lib/cassandra /var/log/cassandra "$CASSANDRA_CONF" \
#		\! -user cassandra -exec chown cassandra '{}' +
#	exec gosu cassandra "$BASH_SOURCE" "$@"
#fi

if [ "$1" = 'cassandra' ]; then
	: ${CASSANDRA_RPC_ADDRESS='0.0.0.0'}

	: ${CASSANDRA_LISTEN_ADDRESS='auto'}
	if [ "$CASSANDRA_LISTEN_ADDRESS" = 'auto' ]; then
		CASSANDRA_LISTEN_ADDRESS="$(_ip_address)"
	fi

	: ${CASSANDRA_PARTITIONER='org.apache.cassandra.dht.ByteOrderedPartitioner'}

	: ${CASSANDRA_NUM_TOKENS='1'}

	: ${CASSANDRA_BROADCAST_ADDRESS="$CASSANDRA_LISTEN_ADDRESS"}

	if [ "$CASSANDRA_BROADCAST_ADDRESS" = 'auto' ]; then
		CASSANDRA_BROADCAST_ADDRESS="$(_ip_address)"
	fi
	: ${CASSANDRA_BROADCAST_RPC_ADDRESS:=$CASSANDRA_BROADCAST_ADDRESS}

	if [ -n "${CASSANDRA_NAME:+1}" ]; then
		: ${CASSANDRA_SEEDS:="cassandra"}
	fi
	: ${CASSANDRA_SEEDS:="$CASSANDRA_BROADCAST_ADDRESS"}

	_sed-in-place "$CASSANDRA_CONF/cassandra.yaml" \
		-r 's/(- seeds:).*/\1 "'"$CASSANDRA_SEEDS"'"/'

	_sed-append "$CASSANDRA_CONF/cassandra.yaml" \
		"auto_bootstrap: false"
 
  _sed-in-place "$CASSANDRA_CONF/cassandra-env.sh" \
    -r 's/^(# )?(RMI_HOSTNAME=).*/\2'"$(_ip_address)"'/'

	for yaml in \
		broadcast_address \
		broadcast_rpc_address \
		cluster_name \
		endpoint_snitch \
		listen_address \
		num_tokens \
		rpc_address \
		start_rpc \
		partitioner \
		initial_token \
	; do
		var="CASSANDRA_${yaml^^}"
		val="${!var}"
		if [ "$val" ]; then
			_sed-in-place "$CASSANDRA_CONF/cassandra.yaml" \
				-r 's/^(# )?('"$yaml"':).*/\2 '"$val"'/'
		fi
	done

	for rackdc in dc rack; do
		var="CASSANDRA_${rackdc^^}"
		val="${!var}"
		if [ "$val" ]; then
			_sed-in-place "$CASSANDRA_CONF/cassandra-rackdc.properties" \
				-r 's/^('"$rackdc"'=).*/\1 '"$val"'/'
		fi
	done
fi

# start ssh server
/usr/sbin/sshd -D &
# run the original cassandra command

#check if cassandra succesfully started
while [ $(nodetool status | grep "$(_ip_address)" | grep UN | wc -l) -lt 1 ]
do
  sleep 1;
	if [ $(jps | grep Cassandra | wc -l) -lt 1 ]; then
		"$@";
	else
    	echo "Cassandra is not up, waiting 30 seconds"
    	sleep 30;
	fi
done

# start gremlin-server after all cassandra nodes are up
while [ $(nodetool status | grep UN | wc -l) -lt $JANUSGRAPH_CLUSTER_SIZE ]
do
	echo "Cassandra cluster is not ready, waiting 30 seconds"
	sleep 30;
done

echo "Waiting 60 seconds for schema disagreements to resolve"
sleep 60;

# cassandra cluster is ready, now setup the JanusGraph
# first let one node to initialize the keyspace to eliminate schema conflicts
if [ $CASSANDRA_INITIAL_TOKEN != "0" ]; then
  # not a seed note, wait until keyspace janusgraph is ready
  while [ $(nodetool describering janusgraph | grep TokenRange | wc -l) -lt $(($JANUSGRAPH_CLUSTER_SIZE + 1)) ]
  do
    echo "Keyspace janusgraph is not initialized, waiting 10 seconds"
    sleep 30;
  done
  echo "Keyspace janusgraph is initialized, start gremlin server"
  sleep 30;
else 
  # seed node, start gremlin server right away
  while [ $(nodetool describecluster | grep ": \[" | wc -l) -gt 1 ]
  do
    echo "Waiting cassandra schema to merge"
    sleep 10;
  done
  echo "Cassandra Seed node starting Gremlin Server"
fi

# start gremlin server
exec "$JANUSGRAPH_HOME/bin/gremlin-server.sh"
