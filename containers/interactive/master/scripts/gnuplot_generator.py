#!/usr/bin/python

import pandas
import sys
import json
import os
from jproperties import Properties

import plot_library

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"


# resulting map that stores all dataset and result files
result_map = {}

# read input arguments
if len(sys.argv) < 2:
    print ("Supply parameter files for which plots will be generated")
    sys.exit()

# read input configuration
for parameter_file in sys.argv[1:]:
    print ("Read parameter file {}".format(parameter_file))

    full_parameter_path = os.path.join(parameters_volume, parameter_file)
    if not os.path.exists(full_parameter_path):
        # exit as there is no parameter file in the parameter volume
        print ("Parameter file {} does not exist in the parameter volume {}".format(parameter_file, parameters_volume))
        continue

    config = Properties()
    with open(full_parameter_path, 'rb') as pf:
        config.load(pf, 'utf-8')

    dataset_name = config.properties['graph.name']
    ingress = config.properties['partition.ingress']
    nworkers = int(config.properties['partition.count'])
    aggregated_results_filename = os.path.join(result_volume, "aggregated.csv")
    if os.path.exists(aggregated_results_filename) is not True:
        print("{} does not have a valid results file {}".format(dataset_name, aggregated_results_filename))
        continue
    aggregated_results = pandas.read_csv(aggregated_results_filename, header=0)
    filtered_per_graph = aggregated_results[aggregated_results['graph'] == dataset_name]
    result_map[dataset_name] = filtered_per_graph


# generate load imbalance
for dataset_name in result_map:
    plot_library.generate_load_imbalance(dataset_name, result_map[dataset_name])


# generate execution time scripts
# line graph where there is a file for each dataset/workload
for dataset_name in result_map:
    plot_library.generate_tput(dataset_name, result_map[dataset_name])
