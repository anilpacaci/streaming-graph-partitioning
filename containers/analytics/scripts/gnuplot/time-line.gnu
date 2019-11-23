set term eps font "Helvetica, 20" size 6, 4
set datafile separator comma

set xtics(8,16,32,64,128)

input_file = input
output_file = output.".eps"

# check arguments
print "reading: ".input_file

set output output_file

set ylabel "Execution Time (sec)"
set xlabel "Number of Partitions"

set key outside top center maxrows 2 

plot for[col=2:11] input_file using 1:(column(col)) with linespoints lw 2 t columnhead(col)

print "Plot generated: ".output_file