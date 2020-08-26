# Rai Distribution Services

1. [Description of Rai DS](#description-of-rai-ds)
2. [Features of Rai DS](#features-of-rai-ds)
3. [How to Build Rai DS](#how-to-build-rai-ds)
4. [Installing Rai DS from COPR](#installing-rai-ds-from-copr)
5. [Testing Performance of Rai DS](#testing-performance-of-rai-ds)

## Description of Rai DS

Rai DS is a caching and distribution server and library built using [Rai
KV](https://github.com/raitechnology/raikv) shared memory and using [Rai
MD]((https://github.com/raitechnology/raimd) data structures.  It is intended
to provide protocol services to popular caching frameworks:  Redis and
Memcached, with additional services for bridging PubSub systems:  Http Websock,
NATS, RV, CAPR.  These maintain compatiblity with the original systems, but the
idea here is not for complete compatibility, but to extend the tools and
infrastructure from these worlds into the more vertical domain that [Rai
Technologies](https://www.raitechnology.com/) has existed for long time:
caching market data.

Although Rai DS `ds_server` provides services compatible with
[Redis](https://github.com/redis/redis) and
[Memcached](https://github.com/memcached/memcached), this is not a drop in
replacement for them.  There are some things that both services do better
because they target different types applications.  Both systems have a long
history of service performed at high performance levels.  Rai has traditionally
focused on PubSub systems, and that is true of Rai DS as well.  The type of
PubSub system Rai implements is a caching layer that is concerned with
maintaining last value consistency between publisher and subscriber.  This is
different from a fire and forget system like Redis and also different from a
persistent queue system like Kafka.  Rai DS aims to provide both fire and
forget semantics or last value consistency, depending the service type,
but not persistent queues, except for the Redis Stream data type.

### A Focus on Latency

The implementation of `ds_server` is an event driven single threaded, packing a
lot of protocol services that can be bound to a core(s) and accelerated with
[Solarflare onload](https://support.solarflare.com/wp/onload) or [Mellanox
VMA](https://www.mellanox.com/products/software/accelerator-software/vma).

One method of scaling up is to balance services bound to cores and utilizing
network hardware acceleration from the Solarflare and Mellanox, or native Linux
methods, such as the built-in load balancer that can forward TCP connections to
a cluster of processes all listening to the same port using
[SO_REUSEPORT](https://lwn.net/Articles/542629/).  There has also been a lot
of work accelerating containers, in order to isolate services from one another,
while using virtual hardware acceleration to direct traffic.

For these reasons, the design of Rai DS utilizes Rai KV shared memory that can
persist across multiple instances and/or local processing which can attach and
detach without disrupting each other.  This provides for seamless vertical
scaling based on external considerations of hardware acceleration or software
clustering, more instances can be started or stopped at any time.

## Features of Rai DS

1.  The current Redis command support is documented in the
    [Redis Command](doc/redis_cmd.adoc) asciidoc.  The document in
    this link is used to generate parser code used in `ds_server` and the 'C'
    DS API, because of it's structure and the way Redis defines the command
    arity and key positions.  The 'C' DS API does not communicate with
    `ds_server`, but interfaces directly with shared memory, providing
    nanosecond latency for Redis commands without pipelining them.  A call to 
    a function returns results immediately.  Since it is shared memory,
    all the operations are seen by Redis clients attached to the system over
    the network.  This also includes Redis PubSub commands (publish, subscribe)
    as well as the blocking commands (blpop), the stream commands (xread), and
    the monitor command.

    For example, reimplementing the `redis-benchmark` within the 'C' DS API:

      ```console
      $ ds_test_api
      cpu affinity 45
      PING: 33162611.85 req per second, 30.15 ns per req
      SET: 8661753.83 req per second, 115.45 ns per req
      GET: 9275413.08 req per second, 107.81 ns per req
      INCR: 7232089.78 req per second, 138.27 ns per req
      LPUSH: 7756708.67 req per second, 128.92 ns per req
      RPUSH: 7679394.47 req per second, 130.22 ns per req
      LPOP: 7294216.83 req per second, 137.09 ns per req
      RPOP: 7256950.40 req per second, 137.80 ns per req
      SADD: 7547661.42 req per second, 132.49 ns per req
      HSET: 6912941.58 req per second, 144.66 ns per req
      SPOP: 8100940.29 req per second, 123.44 ns per req
      LPUSH (for LRANGE): 7979828.80 req per second, 125.32 ns per req
      LRANGE_100: 481345.76 req per second, 2077.51 ns per req
      LRANGE_300: 169569.23 req per second, 5897.30 ns per req
      LRANGE_450: 113232.64 req per second, 8831.38 ns per req
      LRANGE_600: 85519.76 req per second, 11693.20 ns per req
      MSET (10 keys): 2242705.90 req per second, 445.89 ns per req
      Removing test keys
      ```

2.  Multi-threading and/or multi-process support.  The [Rai KV](https://github.com/raitechnology/raikv)
    library fully supports locking shared memory by multiple processes or
    multiple threads.  It does this by having each thread assigned a context
    within the shared memory that allows for communicating the [MCS
    Locking](https://lwn.net/Articles/590243/) of hash entries and tracking
    statistics.  When a process attaches to the shared memory, it connect to
    all other processes/threads in the system which are also registered to
    create a shared memory ring buffer between each of them.  This results in a
    mesh of all to all IPC network nodes of heterogeneous event based
    processing cores, that tracks the subscriptions, the streams blocking, the
    lists blocking, the monitor command, and other features that are necessary
    for a multi-threaded Redis implementation.  These same mechanisms allow
    other PubSub systems to work as well, since the IPC network is general
    enough to adapt and bridge different subscription regimes.  This is
    possible since the subjects are just KV keys and different subscription
    wildcards can all be translated to
    [PCRE2](https://pcre.org/current/doc/html/) expressions.

3.  Aggressive use of memory prefetching in `ds_server`.  A network that has
    even a couple of microseconds latency may have 1000s of requests in flight
    with a hundred or more clients, all independent.  This provides an
    opportunity to both order the requests by hash position and prefetch the
    memory locations to increase throughput.  The effect of prefetching can be
    dramatic, see the [Rai KV prefetching
    test](https://github.com/raitechnology/raikv#prefetching-hashtable-lookups).

4.  Memcached TCP and UDP support.  All memcached operations can operate on
    the same String data type that Redis commands do.  The UDP path is
    optimized better than memcached, utilizing {recv,send}mmsg calls.  This
    helps especially when lots of small items are requested from the cache.

5.  Http supports Redis commands using URIs.  The first command below returns a
    Redis RESP format response, the second command returns a Redis JSON
    response, and the third returns a String item as it is cached.  The latter
    is useful for caching Http objects, the mime type is determined by the key
    extension.  The last uses POST to set a key value.

      ```console
      # example of Redis RESP response, executing "get index.html", "lrange list 0 -1"
      $ curl 'http://127.0.0.1:48080/?get+index.html'
      $47
      <html>
      <body>
      hello world
      </body>
      </html>
      $ curl 'http://127.0.0.1:48080/?lrange+list+0+-1'
      *3
      $3
      one
      $3
      two
      $5
      three

      # example of JSONify response, executing "get index.html", "lrange list 0 -1"
      $ curl 'http://127.0.0.1:48080/js?get+index.html'
      "<html>\r\n<body>\r\nhello world\r\n</body>\r\n</html>\r\n"
      $ curl 'http://127.0.0.1:48080/js?lrange+list+0+-1'
      ["one","two","three"]

      # example of text/html result, executing "get index.html", get is implied
      $ curl http://127.0.0.1:48080/index.html                                 
      <html>
      <body>
      hello world
      </body>
      </html>

      # example of POST, "set README.md file:README.md" (the content-type is ignored)
      $ curl -X POST -H 'Content-Type: text/markdown' --data-binary '@README.md' http://127.0.0.1:48080/README.md
      <html><body>  Created   </body></html>
      ```

    The speed of processing small object requests very likely exceeds most
    general purpose web servers, while having the ability to update the data
    using Redis commands or PubSub feeds.

6.  Websocket Http upgrade supports Redis commands optionally returning
    JSONified results, like the Http support above.  A Websocket initializes
    with a request for a protocol.  This protocol can be "resp", "json", or
    "term".  If the "term" protocol is used, then the server "cooks" the input
    as if it were a terminal by echoing and processing control characters like
    arrow keys for editing a command.  This works with a client using
    [xterm.js](https://xtermjs.org) with the
    [attach](https://xtermjs.org/docs/api/addons/attach/) addon in the browser.

7.  Support for PubSub with last value caching, with updates using Redis,
    Websock, NATS, RV, CAPR.  Descriptions... Todo.

## How to Build Rai DS

Clone this and then update the submodules.

```console
$ git clone https://github.com/raitechnology/raids
$ cd raids
$ git submodule update --init --recursive
```

The dependencies for CentOS/Fedora are a make dependency called `dnf_depend`.
There is a `deb_depend` target for Ubuntu/Debian systems.

```console
$ make dnf_depend
sudo dnf -y install make gcc-c++ git redhat-lsb openssl-devel pcre2-devel liblzf-devel zlib-devel libbsd-devel
Package make-1:4.2.1-16.fc32.x86_64 is already installed.
Package gcc-c++-10.1.1-1.fc32.x86_64 is already installed.
Package git-2.26.2-1.fc32.x86_64 is already installed.
Package redhat-lsb-4.1-49.fc32.x86_64 is already installed.
Package openssl-devel-1:1.1.1g-1.fc32.x86_64 is already installed.
Package pcre2-devel-10.35-1.fc32.x86_64 is already installed.
Package liblzf-devel-3.6-19.fc32.x86_64 is already installed.
Package zlib-devel-1.2.11-21.fc32.x86_64 is already installed.
Package libbsd-devel-0.10.0-2.fc32.x86_64 is already installed.
Dependencies resolved.
Nothing to do.
Complete!
```

And finally, make.

```console
$ make
```

The binaries will work from the working directory, there are no config files.

Installing requires installing all of the submodules first by running `make
dist_rpm` or `make dist_dpkg` in each of the submodule directories then
installing via rpm or dpkg.  I'd recommend installing from COPR instead.

## Installing Rai DS from COPR

Current development RPM builds are installable from
[copr](https://copr.fedorainfracloud.org).

These should resolve all of the dependencies automatically.

```console
$ sudo dnf copr enable injinj/gold
$ sudo dnf install raids
```

## Testing Performance of Rai DS

Todo
