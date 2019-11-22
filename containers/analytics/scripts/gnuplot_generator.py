#!/usr/bin/python

import pandas
import numpy as np
import sys
import json

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
partitions = [8, 16, 32, 64, 128]

# resulting map that stores all dataset and result files
result_map = {}

# read input arguments
if len(sys.argv) < 2:
    print "Supply parameter files for which plots will be generated"
    sys.exit()

# read input configuration
for parameter_file in sys.argv[1:]:
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


ukdata = pandas.read_csv('/home/apacaci/experiments/powerlyra/logs/uk-li-percentile.csv', header=0)
# TODO: load-imbalance data could be in a separate csv. Check or update your existing log_parser so it is there by default

# generate rf-communication time figures
for dataset_name in result_map:
    dataset = result_map[dataset_name]




for workload in workloads:
    # create new data frame
    newDF = pandas.DataFrame(columns=['vc', 'Vertex-cut', 'hc', 'Hybrid-cut', 'ec', 'Edge-cut'])
    # extract data from the master table
    extracteddata = ukdata[ukdata['algorithm'] == workload][['ingress', 'rf', 'total_network']]
    for index, row in extracteddata.iterrows():
        if row['ingress'] in vertex_cut_algorithms:
            newDF = newDF.append({'vc' : row['rf'], 'Vertex-cut' : row['total_network']}, ignore_index=True)
        elif row['ingress'] in hybrid_cut_algorithms:
            newDF = newDF.append({'hc' : row['rf'], 'Hybrid-cut' : row['total_network']}, ignore_index=True)
        elif row['ingress'] in edge_cut_algorithms:
            newDF = newDF.append({'ec' : row['rf'], 'Edge-cut' : row['total_network']}, ignore_index=True)
    # export the data in csv for gnuplot
    newDF.to_csv('rf-comm-pr-uk.csv', sep=',', index=False)

# generate load imbalance
for workload in workloads:
    # create nre data frame
    newDF = pandas.DataFrame(columns=['ingress', 'min', 'max', '25', '50', '75'])
    # extract data from the master table
    extracteddata = ukdata[(ukdata['algorithm'] == workload) & (ukdata['partitions'] == DEFAULT_PARTITION)][['ingress', 'li_min', 'li_max', 'li_25', 'li_50', 'li_75']]
    for index, row in extracteddata.iterrows():
        newDF = newDF.append({'ingress': row['ingress'], 'min' : row['li_min'], 'max' : row['li_max'], '25' : row['li_25'], '50' : row['li_50'], '75' : row['li_75']}, ignore_index=True)
    # export the data in csv for gnuplot
    newDF.to_csv('uk-li-percentile', sep=',', index=False)

# generate replication factor data
# no need to iterate over workloads, just report PageRank
newDF = pandas.DataFrame(columns=['partitions'] + sgp_algorithms)
for partition in partitions:
    extracteddata = ukdata[(ukdata['algorithm'] == DEFAULT_WORKLOAD) & (ukdata['partitions'] == partition)][['ingress', 'rf']]
    sgpToReplicationFactor = dict(zip(extracteddata.ingress, extracteddata.rf))
    sgpToReplicationFactor['partitions'] = partition
    newDF = newDF.append(sgpToReplicationFactor, ignore_index=True)
# export the data in csv for gnuplot
newDF.to_csv('rf-uk.csv', sep=',', index=False)

# generate execution time scripts
# line graph where there is a file for each dataset/workload
for workload in workloads:
    newDF = pandas.DataFrame(columns=['partitions'] + sgp_algorithms)
    for partition in partitions:
        extracteddata = ukdata[(ukdata['algorithm'] == workload) & (ukdata['partitions'] == partition)][['ingress', 'total_time']]
        sgpToTime = dict(zip(extracteddata.ingress, extracteddata.total_time))
        sgpToTime['partitions'] = partition
        newDF = newDF.append(sgpToTime, ignore_index=True)
    # export data in csv for gnuplot
    newDF.to_csv('uk-' + workload + 'line.csv', sep=',', index=False)
