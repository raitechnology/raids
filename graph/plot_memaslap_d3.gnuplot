#!/usr/bin/gnuplot
  
set title "Rai DS memaslap, scaling GET datasize 3, Solarflare x2522-25G"
set ylabel "Operations per Second (Millions)"
set xlabel "Number of Threads (memaslap -B -c 50, key=16, value=3)"
set yrange [0:130]
set y2range [0:130]
set y2tics
set ytic 10
set y2tic 10
set xtic 2
set grid
set key outside tmargin horizontal

plot "memaslap_p128_d3.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Multiget 128", \
     "memaslap_p64_d3.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Multiget 64", \
     "memaslap_p4_d3.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Multiget 4", \
     "memaslap_p1_d3.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Multiget 1", \
     "memaslap_p128_d3.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Multiget 128", \
     "memaslap_p64_d3.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Multiget 64", \
     "memaslap_p4_d3.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Multiget 4", \
     "memaslap_p1_d3.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Multiget 1"
