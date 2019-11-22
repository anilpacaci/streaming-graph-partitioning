import pandas
import os
import subprocess

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

# path for scripts
RF_COMMUNICATION_SCRIPT = '/sgp/scripts/gnuplot/rf-comm.gnu'
RF_SCRIPT = 'sgp/scripts/gnuplot/rf.gnu'
LI_PERCENTILE_SCRIPT = 'sgp/scripts/gnuplot/li-percentile.gnu'

DEFAULT_PARTITION = 64
DEFAULT_WORKLOAD = 'pagerank'

sgp_algorithms = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger", "random_ec", "ldg", "fennel", "metis"]
vertex_cut_algorithms = ["random", "dbh", "grid", "hdrf"]
hybrid_cut_algorithms = ["hybrid", "hybrid_ginger"]
edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

workloads = ["pagerank", "sssp", "connected_component"]
partitions = [8, 16, 32, 64, 128]

# materialize the given dataset, call the gnuplot script and finally remove the materialized dataset
def gnuplot_call(script, inputDF, result_dataset_filename, output_filename):
    result_dataset_fullpath = os.path.join(result_volume, result_dataset_filename)
    output_fullpath = os.path.join(result_volume, output_filename)
    inputDF.to_csv(result_dataset_filename, sep=',', index=False)
    subprocess.call(['gnuplot', '-e', 'input=\'{}\';output=\'{}\''.format(result_dataset_fullpath, output_fullpath), script], cwd=result_volume)
    # delete the temp csv file
    os.remove(result_dataset_fullpath)
    print "Plot generated: {}".format(output_fullpath)

# plots replication factor against total network communication for a particular dataset/workload
def generate_rf_communication(dataset_name, dataset):
    for workload in workloads:
        newDF = pandas.DataFrame(columns=['vc', 'Vertex-cut', 'hc', 'Hybrid-cut', 'ec', 'Edge-cut'])
        extracteddata = dataset[dataset['algorithm'] == workload][['ingress', 'rf', 'total_network']]
        for index, row in extracteddata.iterrows():
            if row['ingress'] in vertex_cut_algorithms:
                newDF = newDF.append({'vc' : row['rf'], 'Vertex-cut' : row['total_network']}, ignore_index=True)
            elif row['ingress'] in hybrid_cut_algorithms:
                newDF = newDF.append({'hc' : row['rf'], 'Hybrid-cut' : row['total_network']}, ignore_index=True)
            elif row['ingress'] in edge_cut_algorithms:
                newDF = newDF.append({'ec' : row['rf'], 'Edge-cut' : row['total_network']}, ignore_index=True)
        # export the data in csv for gnuplot
        result_dataset_filename =  'rf-comm-{}-{}'.format(workload, dataset_name)
        output_filename = 'rf-comm-{}-{}'.format(workload, dataset_name)
        gnuplot_call(RF_COMMUNICATION_SCRIPT, newDF, result_dataset_filename, output_filename)

# plot load imbalance using default workload and default partitioning
def generate_load_imbalance(dataset_name, dataset):
    # create new data frame
    newDF = pandas.DataFrame(columns=['ingress', 'min', 'max', '25', '50', '75'])
    # extract data from the master table
    extracteddata = dataset[(dataset['algorithm'] == DEFAULT_WORKLOAD) & (dataset['partitions'] == DEFAULT_PARTITION)][['ingress', 'li_min', 'li_max', 'li_25', 'li_50', 'li_75']]
    for index, row in extracteddata.iterrows():
        newDF = newDF.append({'ingress': row['ingress'], 'min' : row['li_min'], 'max' : row['li_max'], '25' : row['li_25'], '50' : row['li_50'], '75' : row['li_75']}, ignore_index=True)
    # export the data in csv for gnuplot
    result_dataset_filename =  'li-percentile-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
    output_filename = 'li-percentile-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
    gnuplot_call(LI_PERCENTILE_SCRIPT, newDF, result_dataset_filename, output_filename)