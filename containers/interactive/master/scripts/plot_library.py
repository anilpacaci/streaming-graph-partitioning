import pandas
import os
import subprocess

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"
parameters_volume = "/sgp/parameters/"

# path for scripts
LI_PERCENTILE_SCRIPT = 'sgp/scripts/gnuplot/li-percentile.gnu'
TPUT_SCRIPT = 'sgp/scripts/gnuplot/tput-bar.gnu'

DEFAULT_PARTITION = 16
DEFAULT_WORKLOAD = 'onehop'

sgp_algorithms = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger", "random_ec", "ldg", "fennel", "metis"]
vertex_cut_algorithms = ["random", "dbh", "grid", "hdrf"]
hybrid_cut_algorithms = ["hybrid", "hybrid_ginger"]
edge_cut_algorithms = ["random_ec", "ldg", "fennel", "metis"]

workloads = ["onehop", "twohop"]
partitions = [4, 8, 16, 32]

# materialize the given dataset, call the gnuplot script and finally remove the materialized dataset
def gnuplot_call(script, inputDF, result_dataset_filename, output_filename):
    result_dataset_fullpath = os.path.join(result_volume, result_dataset_filename)
    output_fullpath = os.path.join(result_volume, output_filename)
    inputDF.to_csv(result_dataset_filename, sep=',', index=False)
    subprocess.call(['gnuplot', '-e', 'input=\'{}\';output=\'{}\''.format(result_dataset_fullpath, output_fullpath), script], cwd=result_volume)
    # delete the temp csv file
    os.remove(result_dataset_fullpath)
    print "Plot generated: {}".format(output_fullpath)


# plot load imbalance using default workload and default partitioning
def generate_load_imbalance(dataset_name, dataset):
    # create new data frame
    newDF = pandas.DataFrame(columns=['ingress', 'min', 'max', '25', '50', '75'])
    # extract data from the master table
    extracteddata = dataset[(dataset['algorithm'] == DEFAULT_WORKLOAD) & (dataset['partitions'] == DEFAULT_PARTITION)][['ingress', 'li_min', 'li_max', 'li_25', 'li_50', 'li_75']]
    for index, row in extracteddata.iterrows():
        newDF = newDF.append({'ingress': row['ingress'], 'min' : row['li_min'], 'max' : row['li_max'], '25' : row['li_25'], '50' : row['li_50'], '75' : row['li_75']}, ignore_index=True)
    # generate plot using the generated data
    result_dataset_filename =  'li-percentile-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
    output_filename = 'li-percentile-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
    gnuplot_call(LI_PERCENTILE_SCRIPT, newDF, result_dataset_filename, output_filename)


# plot time-line graph
def generate_tput(dataset_name, dataset):
    for workload in workloads:
        newDF = pandas.DataFrame(columns=['partitions'] + sgp_algorithms)
        for partition in partitions:
            extracteddata = dataset[(dataset['algorithm'] == workload) & (dataset['partitions'] == partition)][['ingress', 'total_time']]
            sgpToTime = dict(zip(extracteddata.ingress, extracteddata.total_time))
            sgpToTime['partitions'] = partition
            newDF = newDF.append(sgpToTime, ignore_index=True)
        # generate plot using the generated data
        result_dataset_filename =  'time-line-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
        output_filename = 'time-line-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name)
        gnuplot_call(TIME_LINE_SCRIPT, newDF, result_dataset_filename, output_filename)