#!/usr/bin/gnuplot
  
set title "Rai DS Scaling using 'wrk' HTTP Load Generator"
set xlabel "Number of Threads (wrk -t1 -c50 -d60s http://addr:port/index.html)"
set ylabel "Operations per Second (Millions)"
set yrange [0:25]
set y2range [0:25]
set y2tics
set ytic 1
set y2tic 1
set xtic 2
set grid

plot "wrk_results.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP", \
     "wrk_results.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP"
