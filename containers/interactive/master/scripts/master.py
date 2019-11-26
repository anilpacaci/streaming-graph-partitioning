#!/usr/bin/python

# master script that orchestrates execution of data loading / run controlling / result generation

import sys
import json
import os
import subprocess

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

partitions = [4, 8, 16, 32]

# resulting map that stores all dataset and result files
result_map = {}

# read input arguments
if len(sys.argv) < 3:
    print "Supply parameter files for which plots will be generated"
    sys.exit()

COMMAND = sys.argv[1]
PARAMATER_FILE = sys.argv[2]
PARAMETER_FILES = sys.argv[1:]

# populates the database from given configuration file
# in a nutshell it calls partitioning info initialization scripts, then db loader script
def populate_db(parameter_file):
    # load partitioning information
    print '/sgp/janusgraph/bin/gremlin.sh -e /sgp/scripts/SNBParser.groovy {}'.format(parameter_file)
    subprocess.call(['/sgp/janusgraph/bin/gremlin.sh', '-e', '/sgp/scripts/ADJParser.groovy', parameter_file])



if COMMAND == 'load':
    full_parameter_path = os.path.join(parameters_volume, PARAMATER_FILE)
    if not os.path.exists(full_parameter_path):
        # exit as there is no parameter file in the parameter volume
        print "Parameter file {} does not exist in the parameter volume {}".format(PARAMATER_FILE, parameters_volume)
        sys.exit(2)
    populate_db(full_parameter_path)

elif COMMAND == 'run':
    full_parameter_path = os.path.join(parameters_volume, PARAMATER_FILE)
    if not os.path.exists(full_parameter_path):
        # exit as there is no parameter file in the parameter volume
        print "Parameter file {} does not exist in the parameter volume {}".format(PARAMATER_FILE, parameters_volume)
        sys.exit(2)


