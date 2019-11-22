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


# plots replication factor against total network communication for a particular dataset/workload
def generate_rf_communication(dataset_name, dataset, result_folder):
    for workload in workloads:
        extracteddata = dataset[dataset['algorithm'] == workload][['ingress', 'rf', 'total_network']]
        for index, row in extracteddata.iterrows():
            if row['ingress'] in vertex_cut_algorithms:
                newDF = newDF.append({'vc' : row['rf'], 'Vertex-cut' : row['total_network']}, ignore_index=True)
            elif row['ingress'] in hybrid_cut_algorithms:
                newDF = newDF.append({'hc' : row['rf'], 'Hybrid-cut' : row['total_network']}, ignore_index=True)
            elif row['ingress'] in edge_cut_algorithms:
                newDF = newDF.append({'ec' : row['rf'], 'Edge-cut' : row['total_network']}, ignore_index=True)
        # export the data in csv for gnuplot
        result_dataset_filename = os.path.join(result_volume, 'rf-comm-{}-{}'.format(workload, dataset_name))
        output_filename = os.path.join(result_volume, 'rf-comm-{}-{}'.format(workload, dataset_name))
        newDF.to_csv(result_dataset_filename, sep=',', index=False)
        # now execute gnuplot script
        subprocess.call(['gnuplot', '-e', 'input=\'{}\';output=\'{}\''.format(result_dataset_filename, output_filename), RF_COMMUNICATION_SCRIPT], cwd=result_volume)
        # delete the temp csv file
        os.remove(result_dataset_filename)
