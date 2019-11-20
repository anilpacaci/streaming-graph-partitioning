#!/usr/bin/python

import csv
import pandas
import numpy as np

sgp_algorithms = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger", "random_ec", "ldg", "fennel", "metis"]
vertex_cut_algorithms = ["random", "dbh", "grid", "hdrf"]
hybrid_cut_algorithms = ["hybrid", "hybrid_ginger"]
edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

workloads = ["pagerank", "sssp", "connected_component"]

ukdata = pandas.read_csv('/home/apacaci/experiments/powerlyra/logs/uk2007-05.csv', header=0)

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