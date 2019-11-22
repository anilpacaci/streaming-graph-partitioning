set term eps font "Helvetica, 16"
set datafile separator comma
set key autotitle columnheader outside horizontal top

input_file = input
output_file = output.".eps"

# check arguments
print "reading: ".input_file

set yrange [0:220]
set xrange [0:14]

gb = 1024*1024*1024

set output output_file
set ylabel "Network Communication (GB)"


plot input_file using 1:($2)/gb with points lw 2 t columnhead(2), \
	 input_file using 3:($4)/gb with points lw 2 t columnhead(4), \
	 input_file using 5:($6)/gb with points lw 2 t columnhead(6)

print "Plot generated: ".output_file