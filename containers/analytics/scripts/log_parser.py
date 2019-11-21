#!/usr/bin/python

import sys
import os
import re
import csv
import numpy

def mean(lst): 
    return sum(lst) / len(lst) 

def aggregate_logs(log_directory, output_csv_file):
	log_files		= []

	# get all the log files from log directory
	log_files = [os.path.join(log_directory,file) for file in os.listdir(log_directory) if os.path.isfile(os.path.join(log_directory, file))]

	with open(output_csv_file, 'w') as csv_file:
		fieldnames = ['file', 'algorithm', 'partitions', 'ingress', 'rf', 'total_ingress', 
		'compute_imbalance', 'li_max', 'li_min', 'li_25', 'li_50', 'li_75', 'total_time', 'total_network']
		writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
		writer.writeheader()

		# start processing the files
		for log_file in log_files:
			# variables that we try to extract from the log
			algorithm		= ""
			file			= ""
			nparts			= 0
			ingress			= ""
			rf				= 0
			ingress_time	= 0
			finalize_time	= 0
			iterations		= 0
			compute_balance	= [1]
			li_max			= 0
			li_min			= 0
			li_25			= 0
			li_50			= 0
			li_75			= 0
			gather_call		= 0 
			apply_call		= 0
			scatter_call	= 0
			execution_time	= 0
			breakdownx		= 0
			breakdownr		= 0
			breakdowng		= 0
			breakdowna		= 0
			breakdowns		= 0
			bytes_sent		= 0
			bytes_received	= 0
			calls_sent		= 0
			calls_received	= 0
			network_sent	= 0
	         
			print "Parsing {}".format(log_file)       
			file = log_file.split("/")[-1]
			algorithm = file.split("-")[0]

			try:

				with open(log_file, 'r') as logs:
					for log in logs:
						# now we need to check for each occurance of parameters that we try to parse
						match = re.search("Cluster of (.*) instances", log)
						if match != None:
							nparts = int(match.group(1))
						# match for ingress method
						match = re.search("ingress = (.*)", log)
						if match != None:
							ingress = match.group(1)
						# replication factor
						match = re.search("replication factor: (.*)", log)
						if match != None:
							rf = float(match.group(1))
						# ingress time
						match = re.search("Final Ingress \(second\): (.*)", log)
						if match != None:
							ingress_time = float(match.group(1))
						# finalize time
						match = re.search("Finalizing graph. Finished in (.*)", log)
						if match != None:
							finalize_time = float(match.group(1))
						# iterations
						match = re.search(": (\w*) iterations completed", log)
						if match != None:
							iterations = int(match.group(1))
						# compute balance array
						match = re.search("Compute Balance: (.*)", log)
						if match != None:
							compute_balance = map(float, match.group(1).split())
						# gas calls 
						match = re.search(" Total Calls\(G\|A\|S\): (.*)", log)
						if match != None:
							[gather_call, apply_call, scatter_call] = map(float, match.group(1).split("|"))
						# execution time
						match = re.search("Execution Time: (.*)", log)
						if match != None:
							execution_time = float(match.group(1))
						# Breakdown of timing
						match = re.search("Breakdown\(X\|R\|G\|A\|S\): (.*)", log)
						if match != None:
							[breakdownx, breakdownr, breakdowng, breakdowna, breakdowns] = map(float, match.group(1).split("|"))
						# bytes sent
						match = re.search("Bytes Sent: (.*)", log)
						if match != None:
							bytes_sent += int(match.group(1))
						# calls sent
						match = re.search("Calls Sent: (.*)", log)
						if match != None:
							calls_sent += int(match.group(1))
						# bytes received
						match = re.search("Bytes Received: (.*)", log)
						if match != None:
							bytes_received += int(match.group(1))
						# calls received
						match = re.search("Calls Received: (.*)", log)
						if match != None:
							calls_received += int(match.group(1))
						# network sent
						match = re.search("Network Sent: (.*)", log)
						if match != None:
							network_sent += int(match.group(1))

				# write result into csv
				writer.writerow({
					'file' : file,
					'algorithm' : algorithm,
					'partitions' : str(nparts), 
					'ingress' : ingress, 
					'rf' : str(rf), 
					'total_ingress' : str(ingress_time + finalize_time),
					'compute_imbalance' : str(max(compute_balance) / mean(compute_balance) ),
					'li_max' : numpy.percentile(compute_balance, 100),
					'li_min' : numpy.percentile(compute_balance, 0),
					'li_25' : numpy.percentile(compute_balance, 25),
					'li_50' : numpy.percentile(compute_balance, 50),
					'li_75': numpy.percentile(compute_balance, 75),
					'total_time' : str(execution_time), 
					'total_network' : str(network_sent)
					})
				print "!!! Done parsing {}".format(log_file)

			except (IOError, ValueError, ZeroDivisionError) as e:
				print "Could not parse: {}, skipping entry".format(log_file)
				# write result into csv
				writer.writerow({
					'file' : file,
					'algorithm' : algorithm,
					'partitions' : str(nparts), 
					'ingress' : ingress, 
					'rf' : "-", 
					'total_ingress' : "-",
					'compute_imbalance' : "-", 
					'total_time' : "-", 
					'total_network' : "-"
					})
			except:
				print "Unexpected error: {}".format(sys.exec_info()[0])
				raise
