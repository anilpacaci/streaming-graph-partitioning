#!/usr/bin/python

# master script that orchestrates execution of data loading / run controlling / result generation

import sys
import json
import os
import subprocess
from jproperties import Properties
import numpy
from ldbc_run import run

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

WORKER_HOSTNAME_PREFIX = 'worker'
THREADS_PER_WORKER_MEDIUM = 12
THREADS_PER_WORKER_HIGH = 24

edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

partitions = [4, 8, 16, 32]

# resulting map that stores all dataset and result files
result_map = {}

# read input arguments
if len(sys.argv) < 3:
    print( "Supply parameter files for which plots will be generated")
    sys.exit()

COMMAND = sys.argv[1]
PARAMATER_FILE = sys.argv[2]
PARAMETER_FILES = sys.argv[1:]

# populates the database from given configuration file
# in a nutshell it calls partitioning info initialization scripts, then db loader script
def populate_db(parameter_file):
    # load partitioning information
    print ('/sgp/janusgraph/bin/gremlin.sh -e /sgp/scripts/SNBParser.groovy {}'.format(parameter_file))
    subprocess.call(['/sgp/janusgraph/bin/gremlin.sh', '-e', '/sgp/scripts/ADJParser.groovy', parameter_file])

# runs an experiment over a configuration given in the parameter file file and logs results for further processing
def run_experiment(parameter_file):
    # first load parameter file and extract related parameters
    config = Properties()
    with open(parameter_file, 'rb') as pf:
        config.load(pf, 'utf-8')

    graph_name = config.properties['graph.name']
    ingress = config.properties['partition.ingress']
    nworkers = config.properties['partition.count']
    dataset_location = os.path.join(dataset_volume, config.properties['input.base'])

    # call run to complete al runs on this configuration
    run(graph_name, ingress, nworkers, dataset_location)


if COMMAND == 'load':
    full_parameter_path = os.path.join(parameters_volume, PARAMATER_FILE)
    if not os.path.exists(full_parameter_path):
        # exit as there is no parameter file in the parameter volume
        print( "Parameter file {} does not exist in the parameter volume {}".format(PARAMATER_FILE, parameters_volume))
        sys.exit(2)
    populate_db(full_parameter_path)

elif COMMAND == 'run':
    full_parameter_path = os.path.join(parameters_volume, PARAMATER_FILE)
    if not os.path.exists(full_parameter_path):
        # exit as there is no parameter file in the parameter volume
        print ("Parameter file {} does not exist in the parameter volume {}".format(PARAMATER_FILE, parameters_volume))
        sys.exit(2)
    # file exists so run the experiment with given file
    run_experiment(full_parameter_path)


