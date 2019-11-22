;/set term pdf font "Helvetica, 16" size 13, 2.7
set datafile separator comma
set key autotitle columnheader outside horizontal top

input_file = input
output_file = output.".eps"

# check arguments
print "reading: ".input_file

set xlabel "Number of Partitions"
set ylabel "Replication Factor"
set xtics(8,16,32,64,128)

set tmargin 2
set rmargin 1

set output output_file

set multiplot layout 1,3

set key vertical maxrows 1 at 400,4.3 font ",20"

set size 0.33, 0.9
set origin 0.02,0
set title "USA-Road"
plot for[col=2:11] "rf-usa.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset key
unset ylabel

set size 0.32, 0.9
set origin 0.35, 0
set title "Twitter"
plot for[col=2:11] "rf-twitter.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

set size 0.32, 0.9
set origin 0.67, 0
set title "UK2007-05"
plot for[col=2:11] "rf-uk.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset multiplot

print "Plot generated: ".output_file