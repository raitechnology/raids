#!/usr/bin/gnuplot
  
set title "Rai DS redis-benchmark Basic Test, Pipeline 128 with 50 Connections"
set ylabel "Operations per Second (Millions)"
set xlabel "Redis Command (redis-benchmark -P 128 -c 50)"
set yrange [0:7]
set y2range [0:7]
set y2tics
set ytic 1
set y2tic 1
set grid
set style fill solid 1.0 border -1

plot "rb_s128.onload" using ($2/1000000):xtic(1) with histogram title "OpenOnload Bypass TCP", \
     "rb_s128.localhost" using ($2/1000000):xtic(1) with histogram title "Linux Kernel localhost TCP", \
     "rb_s128.kernel" using ($2/1000000):xtic(1) with histogram title "Linux Kernel TCP"
