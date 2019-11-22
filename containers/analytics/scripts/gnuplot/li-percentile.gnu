set term eps font "Helvetica, 16"
set datafile separator comma
set key autotitle columnheader outside horizontal top

set xrange [-0.5:9.5]

set xlabel "SGP Algorithm"
set ylabel "Computation Load (time)"

set output "usa-li-percentile.eps"
plot "usa-li-percentile.csv" using (column(0)):4:2:3:6:(0.4):xticlabels(1) with candlesticks lt 3 lw 2 t "Quantiles" whiskerbars, \
     "" using (column(0)):5:5:5:5:(0.4) with candlesticks lt -1 t "Median"

set output "twitter-li-percentile.eps"
plot "twitter-li-percentile.csv" using (column(0)):4:2:3:6:(0.4):xticlabels(1) with candlesticks lt 3 lw 2 t "Quantiles" whiskerbars, \
     "" using (column(0)):5:5:5:5:(0.4) with candlesticks lt -1 t "Median"

set output "uk-li-percentile.eps"
plot "uk-li-percentile.csv" using (column(0)):4:2:3:6:(0.4):xticlabels(1) with candlesticks lt 3 lw 2 t "Quantiles" whiskerbars, \
     "" using (column(0)):5:5:5:5:(0.4) with candlesticks lt -1 t "Median"