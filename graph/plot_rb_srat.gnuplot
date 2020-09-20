#!/usr/bin/gnuplot
  
set title "Rai DS redis-benchmark, GET Rate with Pipeline 1-200"
set ylabel "Operations per Second (Millions)"
set xlabel "Pipeline Count (redis-benchmark -t get -P <count> -c 1)"
set yrange [0:6]
set y2range [0:6]
set y2tics
set ytic 1
set y2tic 1
set grid

plot "rb_slat.onload" using 1:($2/1000000) with lines title "OpenOnload Bypass TCP", \
     "rb_slat.localhost" using 1:($2/1000000) with lines title "Linux Kernel localhost TCP", \
     "rb_slat.kernel" using 1:($2/1000000) with lines title "Linux Kernel TCP"
