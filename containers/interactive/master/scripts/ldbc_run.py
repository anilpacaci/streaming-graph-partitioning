#!/usr/bin/python

import subprocess
from enum import Enum
from jmxquery import JMXConnection, JMXQuery
import os
import json
import csv
import numpy

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

class WORKLOAD(Enum):
    ONE_HOP = 'onehop'
    TWO_HOP = 'twohop'

class LOAD(Enum):
    HIGH_LOAD = 'high'
    MEDIUM_LOAD = 'medium'

WORKER_HOSTNAME_PREFIX = 'worker'
THREADS_PER_WORKER_MEDIUM = 12
THREADS_PER_WORKER_HIGH = 24

# performs a JMX query to every worker in the cluster to obtain read counts
def cassandra_read_counter(worker_count):
    read_counts = []

    for i in worker_count:
        hostname = WORKER_HOSTNAME_PREFIX + str(i)
        jmxConnection = JMXConnection("service:jmx:rmi:///jndi/rmi://{}:7199/jmxrmi".format(hostname))
        jmxQuery = [JMXQuery("org.apache.cassandra.metrics:type=Keyspace,keyspace=janusgraph,name=ReadLatency")]
        metrics = jmxConnection.query(jmxQuery)
        read_count = 0
        for metric in metrics:
            if metric.attribute == 'Count':
                read_count = metric.value
                break
        read_counts.append(read_count)

    return read_counts

def compute_load(previous_counts, current_counts):
    if len(previous_counts) != len(current_counts):
        print('Cassandra Read Counts before and after workload processing do not match')
        return
    # pairwise subtraction
    load = []
    for index in range(len(previous_counts)):
        load.append(current_counts[index] - previous_counts[index])

    return load

# generates result directory name
def result_directory(graph_name, ingress, nworkers, workload, load):
    result_dir = os.path.join(result_volume, "{}-{}-{}-{}-{}".format(graph_name, ingress, nworkers, workload.value, load.value))
    return result_dir

# read tput from previous execution
def read_tput(result_dir):
    result_file = os.path.join(result_dir, 'LDBC-results.json')
    with open(result_file, 'r') as result_handle:
        results = json.load(result_handle)
        tput = results['throughput']
        return int(tput)


# run LDBC driver to execute specified workload
def run(graph_name, ingress, nworkers, dataset_location):
    print("Running experiments for graph: {}, ingress: {} and partitions: {}".format(graph_name, ingress, str(nworkers)))

    #onehop experiments
    print('Warm up for onehop workload run will be executed')
    onehop_previous_counts = cassandra_read_counter(nworkers)
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.ONE_HOP, LOAD.MEDIUM_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_MEDIUM), dataset_location, 'onehop', result_dir])
    onehop_current_counts = cassandra_read_counter(nworkers)
    onehop_load = compute_load(onehop_previous_counts, onehop_current_counts)

    # now take measurement runs
    # onehop calls, medium and
    print('Medium load for onehop workload run will be executed')
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.ONE_HOP, LOAD.MEDIUM_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_MEDIUM), dataset_location, 'onehop', result_dir])
    onehop_medium_tput = read_tput(result_dir)
    print('Medium load for onehop workload run will be executed')
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.ONE_HOP, LOAD.HIGH_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_HIGH), dataset_location, 'onehop', result_dir])
    onehop_high_tput = read_tput(result_dir)

    #onehop experiments
    print('Warm up for twohop workload run will be executed')
    twohop_previous_counts = cassandra_read_counter(nworkers)
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.TWO_HOP, LOAD.MEDIUM_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_MEDIUM), dataset_location, 'twohop', result_dir])
    twohop_current_counts = cassandra_read_counter(nworkers)
    twohop_load = compute_load(twohop_previous_counts, twohop_current_counts)

    # now take measurement runs
    # twohop calls, medium and
    print('Medium load for twohop workload run will be executed')
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.TWO_HOP, LOAD.MEDIUM_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_MEDIUM, dataset_location), 'twohop', result_dir])
    twohop_medium_tput = read_tput(result_dir)
    print('Medium load for twohop workload run will be executed')
    result_dir = result_directory(graph_name, ingress, nworkers, WORKLOAD.TWO_HOP, LOAD.HIGH_LOAD)
    subprocess.call(['/sgp/scripts/run-driver.sh', str(nworkers), str(int(nworkers) * THREADS_PER_WORKER_HIGH, dataset_location), 'twohop', result_dir])
    twohop_high_tput = read_tput(result_dir)

    # now parse and write results
    output_csv_file = os.path.join(result_volume, 'aggregated.csv')
    file_exists = os.path.isfile(output_csv_file)
    fieldnames = ['graph', 'ingress', 'partition', 'workload', 'tput_medium', 'tput_high', 'li_max', 'li_min', 'li_25', 'li_50', 'li_75']
    with open(output_csv_file, 'a') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        # now generate the new row for onehop workload
        writer.writerow({
            'graph' : graph_name,
            'ingress' : ingress,
            'partition': str(nworkers),
            'workload' : WORKLOAD.ONE_HOP.value,
            'tput_medium' : str(onehop_medium_tput),
            'tput_high' : str(onehop_high_tput),
            'li_max' : numpy.percentile(onehop_load, 100),
            'li_min' : numpy.percentile(onehop_load, 0),
            'li_25' : numpy.percentile(onehop_load, 25),
            'li_50' : numpy.percentile(onehop_load, 50),
            'li_75' : numpy.percentile(onehop_load, 75)
        })

        # now generate the new row for twohop workload
        writer.writerow({
            'graph' : graph_name,
            'ingress' : ingress,
            'partition': str(nworkers),
            'workload' : WORKLOAD.TWO_HOP.value,
            'tput_medium' : str(twohop_medium_tput),
            'tput_high' : str(twohop_high_tput),
            'li_max' : numpy.percentile(twohop_load, 100),
            'li_min' : numpy.percentile(twohop_load, 0),
            'li_25' : numpy.percentile(twohop_load, 25),
            'li_50' : numpy.percentile(twohop_load, 50),
            'li_75' : numpy.percentile(twohop_load, 75)
        })

