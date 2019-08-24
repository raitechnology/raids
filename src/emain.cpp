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
#include <raids/ev_tcp.h>
#include <raids/ev_unix.h>
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

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvShm         shm;
  int           status = 0;

  const char * mn = get_arg( argc, argv, 1, "-m", "sysv2m:shm.test" ),
             * pt = get_arg( argc, argv, 1, "-p", "7379" ),  /* redis */
             * mc = get_arg( argc, argv, 1, "-m", "21211" ), /* memcached */
             * sn = get_arg( argc, argv, 1, "-u", "/tmp/raids.sock" ),/* unix */
             * hp = get_arg( argc, argv, 1, "-w", "48080" ), /* http/websock */
             * np = get_arg( argc, argv, 1, "-n", "42222" ), /* nats */
             * cp = get_arg( argc, argv, 1, "-c", "8866" ),  /* capr */
             * rv = get_arg( argc, argv, 1, "-r", "7501" ),  /* rv */
             * fd = get_arg( argc, argv, 1, "-x", "4096" ),  /* max num fds */
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             /** si = get_arg( argc, argv, 1, "-s", "0" ),*/
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s"
" [-m map] [-p redis] [-m memcd] [-u unix] [-w web] [-n nats] [-c capr]"
" [-x mfd] [-f pre] [-b]\n" /*"[-s sin]\n"*/
      "  map   = kv shm map name       (sysv2m:shm.test)\n"
      "  redis = listen redis port     (7379)\n"
      "  memcd = listen memcached port (21211)\n"
      "  unix  = listen unix name      (/tmp/raids.sock)\n"
      "  web   = listen websocket      (48080)\n"
      "  nats  = listen nats port      (42222)\n"
      "  capr  = listen capr port      (8866)\n"
      "  rv    = listen rv port        (7501)\n"
      "  mfd   = max fds               (4096)\n"
      "  pre   = prefetch keys:      0 = no, 1 = yes (1)\n"
      "  -k    = don't use signal USR1 pub notification\n"
      "  -b    = busy poll\n"
      /*"  sin  = single thread:  0 = no, 1 = yes (0)\n"*/, argv[ 0 ] );
    return 0;
  }

  if ( shm.open( mn ) != 0 )
    return 1;
  shm.print();

  EvPoll            poll;
  EvRedisListen     redis_sv( poll );
  EvRedisUnixListen redis_un( poll );
  EvMemcachedListen memcached_sv( poll );
  EvHttpListen      http_sv( poll );
  EvNatsListen      nats_sv( poll );
  EvCaprListen      capr_sv( poll );
  EvRvListen        rv_sv( poll );
  int               maxfd = atoi( fd );

  if ( maxfd == 0 )
    maxfd = 4096;
  if ( poll.init( maxfd, fe[ 0 ] == '1'/*, si[ 0 ] == '1'*/ ) != 0 ||
       poll.init_shm( shm ) != 0 ) {
    fprintf( stderr, "unable to init shm\n" );
    status = 3;
  }
  if ( redis_sv.listen( NULL, atoi( pt ) ) != 0 ) {
    fprintf( stderr, "unable to open tcp listen socket on %s\n", pt );
    status = 2; /* bad port or network error */
  }
  if ( memcached_sv.listen( NULL, atoi( mc ) ) != 0 ) {
    fprintf( stderr, "unable to open memcached listen socket on %s\n", mc );
  }
  if ( http_sv.listen( NULL, atoi( hp ) ) != 0 ) {
    fprintf( stderr, "unable to open http listen socket on %s\n", hp );
  }
  if ( redis_un.listen( sn ) != 0 ) {
    fprintf( stderr, "unable to open unix listen socket on %s\n", sn );
  }
  if ( nats_sv.listen( NULL, atoi( np ) ) != 0 ) {
    fprintf( stderr, "unable to open nats listen socket on %s\n", np );
  }
  if ( capr_sv.listen( NULL, atoi( cp ) ) != 0 ) {
    fprintf( stderr, "unable to open capr listen socket on %s\n", cp );
  }
  if ( rv_sv.listen( NULL, atoi( rv ) ) != 0 ) {
    fprintf( stderr, "unable to open rv listen socket on %s\n", rv );
  }
  if ( status == 0 ) {
    printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
    printf( "max_fds:              %d\n", maxfd );
    printf( "prefetch:             %s\n", fe[ 0 ] == '1' ? "true" : "false" );
    /*printf( "single_thread:        %s\n", si[ 0 ] == '1' ? "true" : "false" );*/
    printf( "redis:                %s\n", pt );
    printf( "unix/redis:           %s\n", sn );
    printf( "memcached:            %s\n", mc );
    printf( "websocket:            %s\n", hp );
    printf( "nats:                 %s\n", np );
    printf( "capr:                 %s\n", cp );
    printf( "rv:                   %s\n", rv );
    printf( "SIGUSR1 notify:       %s\n", no != NULL ? "false" : "true" );
    printf( "busy poll:            %s\n", bu != NULL ? "true" : "false" );
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
      bool idle = poll.dispatch(); /* true if idle, false if busy */
      poll.wait( idle ? 100 : 0 );
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
