#!/usr/bin/python

import json
import os
import shlex
import sys

from log_parser import aggregate_logs

# volumes defined in the docker compose file
dataset_volume = "/sgp/datasets/"
result_volume = "/sgp/results/"

edge_cut_sgp = ["random_ec", "ldg", "fennel", "metis"]
vertex_cut_sgp = ["random", "dbh", "grid", "hdrf", "hybrid", "hybrid_ginger"]

natural_algorithms = ["pagerank", "sssp"]


# an object holding parameters for experiment and return the command line string to be executed
class PowerLyraRun:
	name			= ""
	machines		= 16
	cpu_per_node	= 1
	graph_nodes		= 1
	graph_edges		= 1
	algorithm		= "pagerank"
	graph_format	= "snap"
	ingress			= "random"
	lookup			= ""
	source			= -1
	directed		= "true"
	iterations		= -1
	engine			= "plsync"
	log_file		= ""

	def __init__(self, machines, cpu_per_node, graph_nodes, graph_edges, algorithm, ingress):
		self.machines = machines
		self.cpu_per_node = cpu_per_node
		self.graph_nodes = graph_nodes
		self.graph_edges = graph_edges
		self.algorithm = algorithm["name"]
		# graph format and engine is based on the cut model of the algorithm
		if ingress["name"] in edge_cut_sgp:
			self.graph_format = "adj_ec"
			if algorithm["name"] in natural_algorithms:
				self.engine = "plsyncec"
			else:
				self.engine = "plsync"
		if ingress["name"] in vertex_cut_sgp:
			self.graph_format = "snap"
			self.engine = "plsync"
		
		self.ingress = ingress["name"]
		
		if "lookup" in ingress:
			self.lookup = ingress["lookup"][str(machines)]

		if "source" in algorithm:	
			self.source = algorithm["source"]

		if "iterations" in algorithm: 
			self.iterations = algorithm["iterations"]
		
		# generate name from parameters
		self.name = algorithm["name"] + "-" + str(machines) + "-" + ingress["name"]
		self.log_file = os.path.join(log_folder, self.name)

	def produceCommandString(self):
		command = " mpiexec --mca btl_tcp_if_include eth0 "
		command += "-n {} -npernode {} ".format(str(self.machines), str(self.cpu_per_node))
		command += "-hostfile ~/machines "
		command += "/sgp/powerlyra/{} ".format(self.algorithm)
		# following command control the number of cpus per instance, it uses all cores for threading by default
		#command += "--ncpus {} ".format(str(self.cpu_per_node))
		# metis needs special parameters to set lookup file
		if self.ingress == "metis":
			command += "--graph_opts ingress={},nedges={},nverts={},lookup={} ".format(self.ingress, str(self.graph_edges), str(self.graph_nodes), self.lookup)
		else:
			command += "--graph_opts ingress={},nedges={},nverts={} ".format(self.ingress, str(self.graph_edges), str(self.graph_nodes))

		if self.algorithm.startswith("sssp"):
			command += "--source {} --directed {} ".format(self.source, self.directed)

		command += "--format {} ".format(self.graph_format)
		# provide proper graph based on the format
		if self.graph_format == "snap":
			command += "--graph {} ".format(snap_dataset)
		else:
			command += "--graph {} ".format(adj_dataset)

		# set iterations only if its provided, not equal to zero
		if self.iterations > -1:
			command += "--iterations {} ".format(str(self.iterations))

		command += "--engine {} ".format(self.engine)
		# finally record the output
		command += "2>&1 | tee {} ".format(self.log_file)
		return command

# read the command line arguments
if len(sys.argv) != 2:
	print "Supply configuration file as an argument"
	sys.exit()

# second argument is the parameters file
parameters = sys.argv[1]
		
# list to hold all the objects for this set of experiments
run_list = []

# generate proper host files
# os.system("/home/mpi/get_hosts > /home/mpi/machines")

# parse json files and populate PowerLyraRun objects
with open(parameters, 'rb') as parameters_file:
	parameters_json = json.load(parameters_file)
	run_config = parameters_json["runs"]
	
	## read global parameters from the json file
	snap_dataset = os.path.join(dataset_volume, run_config["snap-dataset"])
	adj_dataset  = os.path.join(dataset_volume, run_config["adj-dataset"])
	log_folder = run_config["log-folder"]
	aggregated_results_file = run_config["result-file"]
	nverts = run_config["nvertices"]
	nedges = run_config["nedges"]
	pernode = run_config["worker-per-node"]

	## read variable parameters
	workers_list = run_config["workers"]
	ingress_list = run_config["ingress"]
	algorithm_list = run_config["algorithm"]

	for worker in workers_list:
		for ingress in ingress_list:
			for algorithm in algorithm_list:
				run_list.append(PowerLyraRun(worker, pernode, nverts, nedges, algorithm, ingress))

	# run each command one by one
	for run in run_list:
		print "------------"
		print "!!! Executing on MPI Cluster: {}".format(run.produceCommandString())
		# result = os.system(run.produceCommandString())
		result = 1	
		print "!!! MPI Cluster returns: {}".format(result)
		print "------------"

	print "All runs are completed"

	print "!!! Calling log aggregater on {}".format(log_folder)

	aggregate_logs(log_folder, aggregated_results_file)

	print "!!! END"


