#!/bin/bash

operation_count=100000
thread_count=128

# locator should point to remote-objects.yaml
locatorgremlin=conf/remote-objects.yaml
locator=$locatorgremlin

# configuration params for benchmark run
conf_pathsq3=conf/consumer-ldbc-sq3.properties
conf_path=$conf_pathsq3

# dataset location
dataset_location_sf30=/home/apacaci/ldbc-gremlin/ldbc_snb_datagen/datasets/sf30_updates
dataset_location_sf10=/home/apacaci/ldbc-gremlin/ldbc_snb_datagen/datasets/sf10_updates

dataset_location=$dataset_location_sf30

# DO NOT CHANGE
parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# DO NOT CHANGE Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDb

# DO NOT CHANGE jar file for the workload implementation
workload_impl_gremlin=lib/snb-interactive-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar
workload_impl=$workload_impl_gremlin

# DO NOT CHANGE first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "lib/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "graphName|$graph_name" -p "locator|$locator" -db $db -tc $thread_count -tcr 0.001 -cu true -ignore_scheduled_start_times true
