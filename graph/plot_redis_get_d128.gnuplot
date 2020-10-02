#!/usr/bin/gnuplot
  
set title "Rai DS redis-benchmark, scaling GET datasize 128, Solarflare x2522-25G"
set ylabel "Operations per Second (Millions)"
set xlabel "Number of Threads (redis-benchmark -t get -d 128 -c 50 -P <cnt>)"
set yrange [0:45]
set y2range [0:45]
set y2tics
set ytic 5
set y2tic 5
set xtic 2
set grid
set key outside tmargin horizontal

plot "redis_get_p128_d128.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Pipe 128", \
     "redis_get_p64_d128.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Pipe 64", \
     "redis_get_p4_d128.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Pipe 4", \
     "redis_get_p1_d128.onload" using 1:($2/1000000) with linespoints title "OpenOnload Bypass TCP Pipe 1", \
     "redis_get_p128_d128.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Pipe 128", \
     "redis_get_p64_d128.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Pipe 64", \
     "redis_get_p4_d128.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Pipe 4", \
     "redis_get_p1_d128.kernel" using 1:($2/1000000) with linespoints title "Linux Kernel TCP Pipe 1"
