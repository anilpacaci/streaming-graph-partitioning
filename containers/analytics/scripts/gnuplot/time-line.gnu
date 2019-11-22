set term eps font "Helvetica, 20" size 15,7
set datafile separator comma

set xtics(8,16,32,64,128)


set output "time-line.eps"

unset key

set multiplot layout 3,3

set size 0.33,0.32
set origin 0.02,0.02

set ylabel "Execution Time (sec)"

set xlabel "Number of Partitions"

set key vertical at 380,660 maxrows 1 

set label "{/*1.5 Twitter}" rotate at -20,50

plot for[col=2:11] "line/twitter-pr-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset key
unset ylabel
unset label

set origin 0.35,0.02
set size 0.32,0.32

plot for[col=2:11] "line/twitter-wcc-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

set origin 0.67,0.02
set size 0.32,0.32

plot for[col=2:11] "line/twitter-sssp-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)


set size 0.33,0.30
set origin 0.02,0.32

set ylabel "Execution Time (sec)"
unset xlabel

set label "{/*1.5 UK2007-05}" rotate at -10,20


plot for[col=2:11] "line/uk-pr-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset label

unset key
unset ylabel

set origin 0.35,0.32
set size 0.32,0.30

plot for[col=2:11] "line/uk-wcc-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

set origin 0.67,0.32
set size 0.32,0.30

plot for[col=2:11] "line/uk-sssp-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

set title "PageRank"

set size 0.33,0.34
set origin 0.02,0.62

set ylabel "Execution Time (sec)"

set label "{/*1.5 USA-Road}" rotate at -17,8

plot for[col=2:11] "line/usa-pr-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset key
unset ylabel
unset label

set title "WCC"

set origin 0.35,0.62
set size 0.32,0.34

plot for[col=2:11] "line/usa-wcc-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

set title "SSSP"

set origin 0.67,0.62
set size 0.32,0.34

plot for[col=2:11] "line/usa-sssp-line.csv" using 1:(column(col)) with linespoints lw 2 t columnhead(col)

unset multiplot