#!/usr/bin/python

import pandas
import sys
import json
import os

import plot_library

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

DEFAULT_PARTITION = 64
DEFAULT_WORKLOAD = 'pagerank'

sgp_algorithms = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger", "random_ec", "ldg", "fennel", "metis"]
vertex_cut_algorithms = ["random", "dbh", "grid", "hdrf"]
hybrid_cut_algorithms = ["hybrid", "hybrid_ginger"]
edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

workloads = ["pagerank", "sssp", "connected_component"]

# resulting map that stores all dataset and result files
result_map = {}

# read input arguments
if len(sys.argv) < 2:
    print "Supply parameter files for which plots will be generated"
    sys.exit()

# read input configuration
for parameter_file in sys.argv[1:]:
    print "Read parameter file {}".format(parameter_file)
    with open(os.path.join(parameters_volume, parameter_file)) as parameter_handle:
        parameters_json = json.load(parameter_handle)
        run_config = parameters_json["runs"]
        ## read global parameters from the json file
        dataset_name = run_config["dataset-name"]
        aggregated_results_filename = os.path.join(result_volume, run_config["result-file"])
        if os.path.exists(aggregated_results_filename) is not True:
            print "{} does not have a valid results file {}".format(dataset_name, aggregated_results_filename)
            continue
        aggregated_results = pandas.read_csv(aggregated_results_filename, header=0)
        result_map[dataset_name] = aggregated_results

# now iterate over the result_map and create gnuplot data csvs for each result file
for dataset_name in result_map:
    plot_library.generate_rf_communication(dataset_name, result_map[dataset_name])

# generate load imbalance
for dataset_name in result_map:
    plot_library.generate_load_imbalance(dataset_name, result_map[dataset_name])

# generate replication factor data
# no need to iterate over workloads, just report PageRank
for dataset_name in result_map:
    plot_library.generate_rf(dataset_name, result_map[dataset_name])

# generate execution time scripts
# line graph where there is a file for each dataset/workload
for dataset_name in result_map:
    plot_library.generate_time(dataset_name, result_map[dataset_name])
