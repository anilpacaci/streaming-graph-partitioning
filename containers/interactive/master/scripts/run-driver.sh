#!/bin/bash

if [ $# -lt 4 ]
then
    echo "Missing argument, provide 'worker-count (number)', 'thread-count (number)', 'dataset-location (directory)', 'workload (onehop|twohop)', and result-directory (directory)"
    exit 2
fi

worker_count=$1

thread_count=$2

dataset_location=$3

workload=$4

result_dir=$5

if [ "$workload" == "twohop" ]; then
    echo "2-hop query workload"
    conf_path=/sgp/scripts/conf/ldbc-q12-twohop.properties
    operation_count=100000
else
    echo "1-hop query workload"
    conf_path=/sgp/scripts/conf/ldbc-q11-onehop.properties
    operation_count=1000000
fi

time_compression_ratio=0.0001

# locator should point to remote-objects.yaml
locator=/sgp/scripts/conf/remote-objects.yaml."$worker_count"

# DO NOT CHANGE
parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# DO NOT CHANGE Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDb

# DO NOT CHANGE jar file for the workload implementation
workload_impl=/sgp/scripts/lib/snb-interactive-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar

exec java -Djava.util.logging.config.file=logging.properties -cp "/sgp/scripts/lib/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" \
    com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path \
    -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "locator|$locator" -db $db \
    -tc $thread_count -tcr $time_compression_ratio -ignore_scheduled_start_times true -rd $result_dir
