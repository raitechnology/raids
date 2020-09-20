#!/usr/bin/gnuplot
  
set title "Rai DS redis-benchmark, GET Latency with Pipeline 1-200"
set ylabel "Avg. Latency in Microseconds"
set xlabel "Pipeline Count (redis-benchmark -t get -P <count> -c 1)"
set yrange [5:80]
set y2range [5:80]
set y2tics
set ytic 5 
set y2tic 5
set grid

plot "rb_slat.onload" using 1:(1000000/($2/$1)) with lines title "OpenOnload Bypass TCP", \
     "rb_slat.localhost" using 1:(1000000/($2/$1)) with lines title "Linux Kernel localhost TCP", \
     "rb_slat.kernel" using 1:(1000000/($2/$1)) with lines title "Linux Kernel TCP"
