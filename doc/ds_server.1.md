% DS_SERVER(1) Rai User's Manual
% Chris Anderson
% August 11, 2020

# NAME

ds_server - Rai distribution server

# SYNOPSIS

ds_server [*options*]

# DESCRIPTION

The ds_server command attaches to shared memory segments created by 
kv_server in order to serve key values to remote clients using Redis,
Memcached, and/or HTTP protocols.

Multiple ds_server processes may run at the same time using different
ports or the same ports, as long as the -P option is used for setting
the SO_REUSEPORT socket property.  This option is not enabled by default
because it also prevents multiple instances from running and binding to
the same port, in the case that is desired.

Each instance attaached to the same shared memory is aware of any other
instance no matter which ports are being served.  This is necessary
because of the blocking Redis commands and the PubSub notifications.
These will cross process boundaries through posix shared memory pipes
located in /dev/shm.

# OPTIONS

-m map
:   The name of map file.  A prefix file, sysv, posix indicates the type of
shared memory.  The sysv prefix below is the default.  The file prefix uses
open(2) and mmap(2).   The posix prefix uses shm_open(2) and mmap(2).  The sysv
prefix uses shmget(2) and shmat(2).  All of these try to use 1GB huge page size
first, then 2MB huge page size, then the default page size.

        file:/path/raikv.shm
        posix:raikv.shm
        sysv:raikv.shm

-p redis-port
:   The TCP port to listen for Redis clients, default is 6379.

-d memcached-port
:   The TCP port to listen for Memcached clients, default is 11211.

-u memcached-udp-port
:   The UDP port to listen for Memcached UDP clients, default is 11211.

-w http-port
:   The TCP port to listen for HTTP / Websock clients, default is 48080.

-D  db-num
:   The default database number to use for keys, can be between 0 and 254.
Each database does not see the other databases keys, but the PubSub operations
are global (they are stored in database 255).

-x maximum-fd
:   The epoll(7) set is created with this number of fd slots, There is 1 fd
used for epoll, 2 fds used for signals and timers, and 1 fd each for the
listeners of the network protocols enabled.  The ulimit for the process may
need to be increased to allow the system to use them.

-k keepalive-timeout
:   The keepalive set on the TCP sockets.  This causes TCP pings every timeout
/ 3 and if 3 timeouts are missed, then the socket is closed.

-f [0|1]
:   Prefetch keys when there are multiple in flight.  Usually this is a win,
but can lose if the hot keys fit within the system caches, where prefetching
doesn't help.  The default is true (1).

-P
:   Allow multiple processes to bind to the same port.  The Linux kernel will
load balance the connections to each of the processes bound to the same port.

-4
:   Use only IPv4 addresses.  If not specified, then both IPv4 and IPv6 can be
used.

-s
:   Don't use SIGUSR1 notification.  To notify other processes that a message
is available, the other process is signaled.  Without a signal, the process
needs to busy poll (eats CPU) or timed poll (increases latency).

-b
:   Busy poll, never sleep always running.

-X
:   Do not listen to the default ports, only listen on ports which are
specified.

# SEE ALSO

The Rai DS source code and all documentation may be downloaded from
<https://github.com/raitechnology/raids>.

