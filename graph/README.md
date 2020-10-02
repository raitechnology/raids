# Running benchmarks

0. [Installing OpenOnload](#installing-openonload)
1. [Using wrk HTTPD loading](#using-wrk-httpd-loading)
2. [Using redis-benchmark](#using-redis-benchmark)
3. [Using nats-bench](#using-nats-bench)
4. [Using libmemcached memaslap](#using-libmemcached-memaslap)

After building Rai DS, the scripts used to start `ds_server` will also start
`kv_server` with default parameters to create the memory segment to attach to,
if it is not already started.

Many of these benchmarks use multiple ports for the same data store.  It is
possible to listen on the same port with multiple instances and use the kernel
load balancing with `SO_REUSEPORT` and also the OpenOnload bypass load
balancing with `EV_CLUSTER_SIZE`, but I found these are not as mature as using
multiple TCP ports.  The IP addresses used below are these directly connected
hosts:

    client 1 (i9 10980xe), port 1 : 192.168.25.21  <-->  server (3970x), port 1 : 192.168.25.22
    client 2 (i9 10980xe), port 1 : 192.168.26.91  <-->  server (3970x), port 2 : 192.168.26.22

## Installing OpenOnload

The method used to install the Solarflare OpenOnload driver, which includes
a `LD_PRELOAD` userspace library TCP stack which masquerades as a socket
interface.  And also includes Linux Kernel modules, one for the Solarflare
network card (`sfc.ko`) and another for the Onload component (`onload.ko`).

These can be built using the `rpmbuild` command by downloading the
OpenOnload from the Solarflare
[download](https://support.solarflare.com/wp/onload) area:

```console
$ sudo dnf install rpm-build make gcc-c++ kernel-devel kernel-headers
$ rpmbuild --rebuild onload-7.1.0.265-1.src.rpm
$ sudo dnf install rpmbuild/RPMS/x86_64/onload-*.rpm
```

## Using wrk HTTPD loading

On the server, the shell script [start_www.sh](start_www.sh) will run multiple
instances the `ds_server` using `taskset` to bind to separate CPUs on
incrementing ports, optionally using the `onload` accelerator.  On the
client(s), the shell script [start_wrk.sh](start_wrk.sh) will run multiple
`wrk` instances using `taskset` and optionally `onload` to connect to
incrementing ports on the server.  The results are collected by capturing the
output of `wrk` on the clients after running the scripts several times, each
time to scale the number of threads needed.

Clone the `wrk` repository and build it and copy it to the client machines,
it does not require configuration.

```console
$ cd raids
$ git clone https://github.com/wg/wrk
$ cd wrk ; make ; cd ..
```

The method used for this is to create a network of clients and servers, each
communicating on their own port, tied to their own CPU.  Here is a 4 CPU
configuration:

    wrk connect 48080 -- net interface 1 -- ds_server listening port 48080
    wrk connect 48081 -- net interface 1 -- ds_server listening port 48081
    wrk connect 48082 -- net interface 2 -- ds_server listening port 48082
    wrk connect 48083 -- net interface 2 -- ds_server listening port 48083

Each wrk instance is a separate program which can be on different hosts.  The
server uses the same shared memory segment, so the operations performed will be
seen in all instances, even though they are different processes on listening on
different ports.   The following is using 2 client hosts for each of the
interface ports, but it is possible to use more or less hosts, and combine
the statistics output by the `wrk` load driver.

Run 4 `ds_server` instances on port 48080 ... 48083 for the kernel networking
test (without Kernel bypass):

```console
[server]$ sh graph/start_www.sh 48080 4
Starting instance on CPU 1 on port 48080
 . . . (cut for brevity)
Starting instance on CPU 2 on port 48081
 . . .
Starting instance on CPU 3 on port 48082
 . . .
Starting instance on CPU 4 on port 48083
 . . .
<html><body>  Created   </body></html>
waiting for ctr-c to stop 4 instances
```

After the last instance is run, the script creates the `index.html` page by
using the `curl -X POST` command.  Run 2 `wrk` instances on client 1 and 2
instances on client 2.  The instances will run for 60 seconds.

```console
[client1]$ sh graph/start_wrk.sh 192.168.25.22 48080 2
Starting instance on CPU 1 to http://192.168.25.22:48080/index.html
Starting instance on CPU 2 to http://192.168.25.22:48081/index.html
svrpids = 3764145 3764148
waiting for 2 wrk instances, run time 60s
wrk pid 3764145 exit status 0
wrk pid 3764148 exit status 0
Requests/sec: 365188.85
complete
```

```console
[client2]$ sh graph/start_wrk.sh 192.168.26.22 48082 2
Starting instance on CPU 1 to http://192.168.26.22:48082/index.html
Starting instance on CPU 2 to http://192.168.26.22:48083/index.html
svrpids = 24296 24299
waiting for 2 wrk instances, run time 60s
wrk pid 24296 exit status 0
wrk pid 24299 exit status 0
Requests/sec: 388122.32
complete
```

The IP addresses used above will be the addresses assigned to the server.  The
last digits of the scripts, a 48080 4 on the server and 48080 2, 48082 2 on the
clients, tell the scripts how many instances to run and which ports to use.
Ideally, you want to start the clients at the same time in order to load the
server instances simultaneously.  Stop the servers by using 'ctrl-c' on the
console running them.  Repeat the above sequences, starting more servers and
more clients until you run out of CPUs available.

To use the Kernel Bypass with OpenOnload, repeat the above again with the
`USE_ONLOAD=yes` environment variable set in the consoles of both the server
and the client machines.  The `start_www.sh` and `start_wrk.sh` scripts both
look for `$USE_ONLOAD` and start the instances with the onload command when
it is set to `yes`.

After running the tests, there will be files in the working directory with
results in a format that can be combined and graphed: `wrk_results.kernel` and
`wrk_results.onload`, if onload is used.  These are the sums of the
requests/sec output by the `wrk` instances.  Both clients can be combined
together in a single file using the [merge2.sh](merge2.sh) script:

```console
[server]$ scp client1:~/raids/wrk_results.kernel wrk_results.kernel.1
[server]$ scp client2:~/raids/wrk_results.kernel wrk_results.kernel.2
[server]$ sh graph/merge2.sh wrk_results.kernel.1 wrk_results.kernel.2 > wrk_results.kernel
```

The merge script is stupid, it does not match the lines up in any way, it
just sums the two numbers and prints them to stdout.  If there are mismatched
lines from client failures or unequal runs, the data may not be be useful.

The files `wrk_results.{kernel,onload}` should be graphable using the `gnuplot` command:

```console
[server]$ gnuplot-qt -p graph/plot_www.gnulot
```

## Using redis-benchmark

Included in the graph directory is a patch for `redis-benchmark.c` that causes
it to stop processing when it receives a HUP signal.  The allows for more
accurate rate reporting.  Without this patch, it will continue until it
finishes the number of requests, which causes some processes to finish before
others skewing the rate higher when processing close to the limit of the
network bandwidth.  Patch `redis-benchmark.c` and compile it:

```console
$ git clone https://github.com/redis/redis
$ cd redis/src
$ patch < ~/raids/graph/redis-benchmark.c-diff
$ cd ..
$ make redis-benchmark
$ mkdir -p ~/bin ; cp src/redis-benchmark ~/bin
$ PATH=~/bin:$PATH ; export PATH
```

To create the barchart of the single threaded redis-benchmark results, start a redis server
using the `start_ds_server.sh` command, which will check the `USE_ONLOAD` variable and can
start multiple copies running on different ports.

```console
# set to yes for onload tcp or unset it for kernel tcp
[server]$ unset USE_ONLOAD
[server]$ sh graph/start_ds_server.sh 6379 1 redis
```

Capture localhost.

```console
[server]$ redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -P 128 -n 100000 --csv > rb_s128.localhost.csv
```

Capture kernel from a client machine.

```console
[client]$ redis-benchmark -h 192.168.25.22 -p 6379 -c 50 -P 128 -n 100000 --csv > rb_s128.kernel.csv
[client]$ scp rb_s128.kernel.csv server:~/raids/
```

Restart the server with onload.

```console
[server]$ USE_ONLOAD=yes ; export USE_ONLOAD
[server]$ sh graph/start_ds_server.sh 6379 1 redis
```

Capture onload tcp from a client machine.

```console
[client]$ onload '--profile=latency' taskset -c 1 redis-benchmark -h 192.168.25.22 -p 6379 -c 50 -P 128 -n 100000 --csv > rb_s128.onload.csv
[client]$ scp rb_s128.onload.csv server:~/raids/
```

The csv files can be converted from csv format to gnuplot format using the
`redis_histogram.sh` shell script.  The files
`rb_s128.{localhost,kernel,onload}` should be graphable using this `gnuplot`
command, which reads them from the current directory:

```console
[server]$ sh graph/redis_histogram.sh rb_s128.localhost.csv > rb_s128.localhost
[server]$ sh graph/redis_histogram.sh rb_s128.kernel.csv > rb_s128.kernel
[server]$ sh graph/redis_histogram.sh rb_s128.onload.csv > rb_s128.onload
[server]$ gnuplot-qt -p graph/plot_rb_s128.gnuplot
```

To create the pipeline graphs for latency, these commands will do.  Start a
`ds_server` redis instance.

```console
# set to yes for onload tcp or unset it for kernel tcp
[server]$ unset USE_ONLOAD
[server]$ sh graph/start_ds_server.sh 6379 1 redis
```

The clients are driven by a shell script, since the redis-benchmark commands
are run 200 times with different pipeline counts.  These are echoed to stdout
and also output to a files called `rb_slat.{localhost,kernel,onload}`,
depending on the `USE_ONLOAD` state and the address equal to `127.0.0.1`

```console
[server]$ sh graph/rb_slat.sh 127.0.0.1 200
```

The the clients, one with kernel, the other with onload.

```console
[client]$ unset USE_ONLOAD
[client]$ sh graph/rb_slat.sh 192.168.25.22 200
[client]$ scp rb_slat.kernel server:~/raids/
```

```console
[server]$ USE_ONLOAD=yes ; export USE_ONLOAD
[server]$ sh graph/start_ds_server.sh 6379 1 redis
```

```console
[client]$ USE_ONLOAD=yes ; export USE_ONLOAD
[client]$ sh graph/rb_slat.sh 192.168.25.22 200
[client]$ scp rb_slat.onload server:~/raids/
```

The plot commands, one for the rate graph and the other for the average latency
graph.  Both read the `rb_slat.{localhost,kernel,onload}` files in the working
directory.

```console
[server]$ gnuplot-qt -p graph/plot_rb_srat.gnuplot
[server]$ gnuplot-qt -p graph/plot_rb_slat.gnuplot
```

The GET scaling graphs are generated with a `ds_server` started on each
port, like wrk above, with the servers scaled from 2 to 36.  This requires
repeatedly the scripts to collect the data by changing the 4 below and the
2 on the client scripts.  The repeating the same thing again with
`USE_ONLOAD=yes` in the environment to generate the OpenOnload results.

```console
[server]$ sh graph/start_ds_server.sh 6379 4 redis
Starting instance on CPU 1 on port 6379
 . . . (cut for brevity)
Starting instance on CPU 2 on port 6380
 . . .
Starting instance on CPU 3 on port 6381
 . . .
Starting instance on CPU 4 on port 6382
 . . .
waiting for ctr-c to stop 4 instances
```

The clients on each of the machines connected to the ports are started by the
`start_rb.sh` script.  It takes the address, the port, the number of
`redis-benchmark` instances to run, the pipeline size, and the data size, which
uses 3 and 128 in the two graphs.

```console
[client1]$ sh graph/start_rb.sh
usage: graph/start_rb.sh address baseport NCPU pipesize datasize
where NCPU is the number of redis-benchmark instances to run
[client1]$ sh graph/start_rb.sh 192.168.25.22 6379 2 128 128
Starting instance on CPU 1 to connect to port 6379
Starting instance on CPU 2 to connect to port 6380
svrpids = 76676 76701
running 2 instances for 30 seconds
Stopping instance on CPU 1 port 6385 pid 76676
kill -HUP 76676
Stopping instance on CPU 2 port 6386 pid 76701
kill -HUP 76701
pid 76676 exit status 0
pid 76701 exit status 0
Requests/sec: 6873308.75
complete
```

```console
[client2]$ sh graph/start_rb.sh 192.168.26.22 6381 2 128 128
Starting instance on CPU 1 to connect to port 6381
Starting instance on CPU 2 to connect to port 6382
svrpids = 131080 131107
running 2 instances for 30 seconds
Stopping instance on CPU 1 port 6381 pid 131080
kill -HUP 131080
Stopping instance on CPU 2 port 6382 pid 131107
kill -HUP 131107
Requests/sec: 6962871.50
complete
[client2]$ cat redis_get_p128_d128.kernel
1 3517107.50
2 6962871.50
```

The requests/sec are capture in a file called `redis_get_p128_d128.kernel` and
`redis_get_p128_d128.onload`, depending on whether `USE_ONLOAD=yes` is in the
environment.  To graph these files, use the `merge2.sh` and
`plot_redis_get_d128.gnuplot` scripts:

```console
[server]$ scp client1:~/raids/redis_get_p128_d128.kernel redis_get_p128_d128.kernel.1
[server]$ scp client2:~/raids/redis_get_p128_d128.kernel redis_get_p128_d128.kernel.2
[server]$ sh graph/merge2.sh redis_get_p128_d128.kernel.? > redis_get_p128_d128.kernel
[server]$ gnuplot-qt -p graph/plot_redis_get_d128.gnuplot
```

Repeat the above with different cores, different pipelines, different
datasizes, setting `USE_ONLOAD=yes`, to construct these data files.  The
client1 and client2 examples used above are run simultaneously so that the
merging of the two data files represents concurrent execution.

## Using libmemcached memaslap

Install libmemcached, which includes memaslap:

```console
$ dnf install libmemcached
```




