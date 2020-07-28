#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raids/ev_service.h>
#ifdef REDIS_UNIX
#include <raids/ev_unix.h>
#endif
#include <raids/ev_memcached.h>
#include <raids/ev_http.h>
#include <raids/ev_nats.h>
#include <raids/ev_capr.h>
#include <raids/ev_rv.h>
#include <raids/ev_client.h>
#include <raids/kv_pubsub.h>

using namespace rai;
using namespace ds;
using namespace kv;

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
      return argv[ i + b ];
  return def; /* default value */
}

/* if multiple protos or just redis */
#define MULTI_PROTO

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvShm         shm;
  int           status = 0;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * pt = get_arg( argc, argv, 1, "-p", "6379" ),  /* redis */
#ifdef MULTI_PROTO
             * mc = get_arg( argc, argv, 1, "-d", "11211" ), /* memcached */
/* unix doesn't have a SO_REUSEPORT option */
#ifdef REDIS_UNIX
             * sn = get_arg( argc, argv, 1, "-u", "/tmp/raids.sock" ),/* unix */
#endif
             * hp = get_arg( argc, argv, 1, "-w", "48080" ), /* http/websock */
             * np = get_arg( argc, argv, 1, "-n", "4222" ), /* nats */
             * cp = get_arg( argc, argv, 1, "-c", "8866" ),  /* capr */
             * rv = get_arg( argc, argv, 1, "-r", "7500" ),  /* rv */
#endif
             * fd = get_arg( argc, argv, 1, "-x", "10000" ), /* max num fds */
             * ti = get_arg( argc, argv, 1, "-t", "16" ),    /* secs timeout */
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             /** si = get_arg( argc, argv, 1, "-s", "0" ),*/
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * cl = get_arg( argc, argv, 0, "-P", 0 ),
             * i4 = get_arg( argc, argv, 0, "-4", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s"
" [-m map] [-p redis] [-d memcd] [-u unix] [-w web] [-n nats] [-c capr]"
" [-x mfd] [-t secs] [-f pre] [-P] [-4] [-b]\n"
      "  -m map   = kv shm map name       (" KV_DEFAULT_SHM ")\n"
      "  -p redis = listen redis port     (6379)\n"
#ifdef MULTI_PROTO
      "  -d memcd = listen memcached port (11211)\n"
#ifdef REDIS_UNIX
      "  -u unix  = listen unix name      (/tmp/raids.sock)\n"
#endif
      "  -w web   = listen websocket      (48080)\n"
      "  -n nats  = listen nats port      (4222)\n"
      "  -c capr  = listen capr port      (8866)\n"
      "  -r rv    = listen rv port        (7500)\n"
#endif
      "  -x mfd   = max fds               (10000)\n"
      "  -t secs  = keep alive timeout    (16)\n"
      "  -f pre   = prefetch keys:      0 = no, 1 = yes (1)\n"
      "  -P    = set SO_REUSEPORT for clustering multiple instances\n"
      "  -4    = use only ipv4 listeners\n"
      "  -k    = don't use signal USR1 pub notification\n"
      "  -b    = busy poll\n"
      /*"  sin  = single thread:  0 = no, 1 = yes (0)\n"*/, argv[ 0 ] );
    return 0;
  }

  if ( shm.open( mn, 0 /* db */) != 0 )
    return 1;
  shm.print();

  EvPoll            poll;
  EvRedisListen     redis_sv( poll );
#ifdef MULTI_PROTO
#ifdef REDIS_UNIX
  EvRedisUnixListen redis_un( poll );
#endif
  EvMemcachedListen memcached_sv( poll );
  EvMemcachedUdp    memcached_udp_sv( poll );
  EvHttpListen      http_sv( poll );
  EvNatsListen      nats_sv( poll );
  EvCaprListen      capr_sv( poll );
  EvRvListen        rv_sv( poll );
#endif
  int               maxfd = atoi( fd ),
                    timeo = atoi( ti ),
                    tcp_opts = DEFAULT_TCP_LISTEN_OPTS,
                    udp_opts = DEFAULT_UDP_LISTEN_OPTS;

  if ( maxfd == 0 )
    maxfd = 10000;
  if ( cl == NULL ) { /* only do this if -P was used, solarflare onload does */
    tcp_opts &= ~OPT_REUSEPORT; /* broken clustering when it is set */
    udp_opts &= ~OPT_REUSEPORT; /* broken clustering when it is set */
  }
  if ( i4 != NULL ) {
    tcp_opts &= ~OPT_AF_INET6;
    udp_opts &= ~OPT_AF_INET6;
  }
  /* set timeouts */
  poll.wr_timeout_ns   = (uint64_t) timeo * 1000000000;
  poll.so_keepalive_ns = (uint64_t) timeo * 1000000000;

  if ( poll.init( maxfd, fe[ 0 ] == '1'/*, si[ 0 ] == '1'*/ ) != 0 ||
       poll.init_shm( shm ) != 0 ) {
    fprintf( stderr, "unable to init shm\n" );
    status = 3;
  }
  if ( redis_sv.listen( NULL, atoi( pt ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open tcp listen socket on %s\n", pt );
    status = 2; /* bad port or network error */
  }
#ifdef MULTI_PROTO
  if ( memcached_sv.listen( NULL, atoi( mc ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open memcached listen socket on %s\n", mc );
  }
  if ( memcached_udp_sv.listen( NULL, atoi( mc ), udp_opts ) != 0 ) {
    fprintf( stderr, "unable to open memcached udp listen socket on %s\n", mc );
  }
  memcached_udp_sv.init();
  if ( http_sv.listen( NULL, atoi( hp ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open http listen socket on %s\n", hp );
  }
#ifdef REDIS_UNIX
  if ( redis_un.listen( sn ) != 0 ) {
    fprintf( stderr, "unable to open unix listen socket on %s\n", sn );
  }
#endif
  if ( nats_sv.listen( NULL, atoi( np ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open nats listen socket on %s\n", np );
  }
  if ( capr_sv.listen( NULL, atoi( cp ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open capr listen socket on %s\n", cp );
  }
  if ( rv_sv.listen( NULL, atoi( rv ), tcp_opts ) != 0 ) {
    fprintf( stderr, "unable to open rv listen socket on %s\n", rv );
  }
#endif
  if ( status == 0 ) {
    printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
    printf( "max_fds:              %d\n", maxfd );
    printf( "keepalive_timeout:    %d\n", timeo );
    printf( "prefetch:             %s\n", fe[ 0 ] == '1' ? "true" : "false" );
    printf( "redis:                %s\n", pt );
#ifdef MULTI_PROTO
#ifdef REDIS_UNIX
    printf( "unix/redis:           %s\n", sn );
#endif
    printf( "memcached:            %s\n", mc );
    printf( "websocket:            %s\n", hp );
    printf( "nats:                 %s\n", np );
    printf( "capr:                 %s\n", cp );
    printf( "rv:                   %s\n", rv );
#endif
    printf( "SIGUSR1_notify:       %s\n", no != NULL ? "false" : "true" );
    printf( "cluster_soreuseport:  %s\n", cl != NULL ? "true" : "false" );
    printf( "ipv4_only:            %s\n", i4 != NULL ? "true" : "false" );
    printf( "busy_poll:            %s\n", bu != NULL ? "true" : "false" );
    fflush( stdout );
    sighndl.install();
    if ( bu != NULL ) {
      poll.pubsub->idle_push( EV_BUSY_POLL );
    }
    if ( no != NULL ) {
      poll.pubsub->flags &= ~KV_DO_NOTIFY;
    }
    for (;;) {
      if ( poll.quit >= 5 )
        break;
      int state = poll.dispatch(); /* 0 if idle, 1, 2, 3 if busy */
      poll.wait( state == EvPoll::DISPATCH_IDLE ? 100 : 0 );
      if ( sighndl.signaled && ! poll.quit )
        poll.quit++;
    }
    if ( fe[ 0 ] == '1' ) {
      size_t j = 0;
      for ( size_t i = 0; i <= EvPoll::PREFETCH_SIZE; i++ ) {
        if ( poll.prefetch_cnt[ i ] != 0 ) {
          printf( "pre[%lu] = %lu (=%lu) (t:%lu)\n", i, poll.prefetch_cnt[ i ],
                                      i * poll.prefetch_cnt[ i ],
                                      j += i * poll.prefetch_cnt[ i ] );
        }
      }
    }
    printf( "bye\n" );
  }
  shm.close();

  return status;
}
