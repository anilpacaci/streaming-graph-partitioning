#!/usr/bin/python

import csv
import pandas
import numpy as np

DEFAULT_PARTITION = 64
DEFAULT_WORKLOAD = 'pagerank'

sgp_algorithms = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger", "random_ec", "ldg", "fennel", "metis"]
vertex_cut_algorithms = ["random", "dbh", "grid", "hdrf"]
hybrid_cut_algorithms = ["hybrid", "hybrid_ginger"]
edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

workloads = ["pagerank", "sssp", "connected_component"]
partitions = [8, 16, 32, 64, 128]

ukdata = pandas.read_csv('/home/apacaci/experiments/powerlyra/logs/uk2007-05.csv', header=0)
# TODO: load-imbalance data could be in a separate csv. Check or update your existing log_parser so it is there by default

# generate rf-communication time figures
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
    extracteddata = ukdata[(ukdata['algorithm'] == workload) and (ukdata['partitions'] == DEFAULT_PARTITION)][['ingress', 'li_min', 'li_max', 'li_25', 'li_50', 'li_75']]
    for index, row in extracteddata.iterrows():
        newDF = newDF.append({'ingress': row['ingress'], 'min' : row['li_min'], 'max' : row['li_max'], '25' : row['li_25'], '50' : row['li_50'], '75' : row['li_75']}, ignore_index=True)
    # export the data in csv for gnuplot
    newDF.to_csv('uk-li-percentile', sep=',', index=False)