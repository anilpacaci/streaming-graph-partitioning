set term eps font "Helvetica, 20" size 15,3
set datafile separator comma


set style data histogram
set style histogram cluster gap 1
set style fill solid
set boxwidth 0.9
set grid ytics

set ylabel "Throughput (queries \/ second)"

input_file = input
output_file = output.".eps"

# check arguments
print "reading: ".input_file

set output output_file

unset key
set title "USA-Road"
plot for[col=2:3] input_file using (column(col)):xtic(1) t columnhead(col)

print "Plot generated: ".output_file
