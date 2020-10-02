#!/usr/bin/gnuplot
  
set title noenhanced
set title "Rai DS ds_test_api"
set ylabel "Operations per Second (Millions)"
set xlabel noenhanced
set xlabel "Redis Command (ds_test_api -x -n 1000000 -c)"
set yrange [0:10]
set y2range [0:10]
set y2tics
set ytic 1
set y2tic 1
set grid
set style fill solid 1.0 border -1

plot "test_api" using ($2/1000000):xtic(1) with histogram title "DS api command"
