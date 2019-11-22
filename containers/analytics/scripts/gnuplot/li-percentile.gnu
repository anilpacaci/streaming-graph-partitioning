set term eps font "Helvetica, 16"
set datafile separator comma
set key autotitle columnheader outside horizontal top

input_file = input
output_file = output.".eps"

# check arguments
print "reading: ".input_file

set xrange [-0.5:9.5]

set xlabel "SGP Algorithm"
set ylabel "Computation Load (time)"

set output output_file
plot input_file using (column(0)):4:2:3:6:(0.4):xticlabels(1) with candlesticks lt 3 lw 2 t "Quantiles" whiskerbars, \
     "" using (column(0)):5:5:5:5:(0.4) with candlesticks lt -1 t "Median"

print "Plot generated: ".output_file