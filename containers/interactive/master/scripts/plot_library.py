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
    print ("Plot generated: {}".format(output_fullpath))


# plot load imbalance using default workload and default partitioning
def generate_load_imbalance(dataset_name, dataset):
    # create new data frame
    newDF = pandas.DataFrame(columns=['ingress', 'min', 'max', '25', '50', '75'])
    # extract data from the master table
    for partition in partitions:
        extracteddata = dataset[(dataset['ingress'] == DEFAULT_WORKLOAD) & (dataset['partition'] == partition)][['ingress', 'li_min', 'li_max', 'li_25', 'li_50', 'li_75']]
        if extracteddata.ingress.nunique() is not len(edge_cut_algorithms):
            print("Graph: {} with {} partitions do not have results all four partitioning algorithms".format(dataset_name, str(partition)))
            continue
        # generate figure for this partition and graph
        for index, row in extracteddata.iterrows():
            newDF = newDF.append({'ingress': row['ingress'], 'min' : row['li_min'], 'max' : row['li_max'], '25' : row['li_25'], '50' : row['li_50'], '75' : row['li_75']}, ignore_index=True)
        # generate plot using the generated data
        result_dataset_filename =  'li-percentile-{}-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name, partition)
        output_filename = 'li-percentile-{}-{}-{}'.format(DEFAULT_WORKLOAD, dataset_name, partition)
        gnuplot_call(LI_PERCENTILE_SCRIPT, newDF, result_dataset_filename, output_filename)


# plot time-line graph
def generate_tput(dataset_name, dataset):
    for workload in workloads:
        newDF = pandas.DataFrame(columns=['ingress', 'medium', 'high'])
        for partition in partitions:
            extracteddata = dataset[(dataset['ingress'] == workload) & (dataset['partition'] == partition)][['ingress', 'tput_medium', 'tput_high']]
            if extracteddata.ingress.nunique() is not len(edge_cut_algorithms):
                print("Graph: {} with {} partitions do not have results all four partitioning algorithms".format(dataset_name, str(partition)))
                continue
            for index, row in extracteddata.iterrows():
                newDF = newDF.append({'ingress': row['ingress'], 'medium' : row['tput_medium'], 'high': row['tput_high']}, ignore_index=True)
            # generate plot using the generated data
            result_dataset_filename =  'tput-{}-{}-{}'.format(workload, dataset_name, partition)
            output_filename = 'tput-{}-{}-{}'.format(workload, dataset_name, partition)
            gnuplot_call(TPUT_SCRIPT, newDF, result_dataset_filename, output_filename)
