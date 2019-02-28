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
#include <raids/ev_http.h>
#include <raids/ev_nats.h>
#include <raids/ev_client.h>

using namespace rai;
using namespace ds;
using namespace kv;

static const char *
get_arg( int argc, char *argv[], int n, int b, const char *f, const char *def )
{
  /* [1]=map [2]=port [3]=sock [4]=http [5]=nats [6]=prefetch [7]=single */
  if ( n > 0 && argc > n && argv[ 1 ][ 0 ] != '-' )
    return argv[ n ];
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

  const char * mn = get_arg( argc, argv, 1, 1, "-m", "sysv2m:shm.test" ),
             * pt = get_arg( argc, argv, 2, 1, "-p", "8888" ),
             * sn = get_arg( argc, argv, 3, 1, "-u", "/tmp/raids.sock" ),
             * hp = get_arg( argc, argv, 4, 1, "-w", "48080" ),
             * np = get_arg( argc, argv, 5, 1, "-n", "42222" ),
             * fd = get_arg( argc, argv, 6, 1, "-x", "4096" ),
             * fe = get_arg( argc, argv, 7, 1, "-f", "1" ),
             /** si = get_arg( argc, argv, 8, 1, "-s", "0" ),*/
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s"
" [-m map] [-p port] [-u unix] [-w web] [-n nats] [-x mfd] [-f pre]\n" /*"[-s sin]\n"*/
      "  map  = kv shm map name  (sysv2m:shm.test)\n"
      "  port = listen tcp port  (8888)\n"
      "  unix = listen unix name (/tmp/raids.sock)\n"
      "  web  = listen www port  (48080)\n"
      "  nats = listen nats port (42222)\n"
      "  mfd  = max fds          (4096)\n"
      "  pre  = prefetch keys:  0 = no, 1 = yes (1)\n"
      /*"  sin  = single thread:  0 = no, 1 = yes (0)\n"*/, argv[ 0 ] );
    return 0;
  }

  if ( shm.open( mn ) != 0 )
    return 1;
  shm.print();

  EvPoll            poll;
  EvRedisListen     redis_sv( poll );
  EvRedisUnixListen redis_un( poll );
  EvHttpListen      http_sv( poll );
  EvNatsListen      nats_sv( poll );
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
  if ( http_sv.listen( NULL, atoi( hp ) ) != 0 ) {
    fprintf( stderr, "unable to open http listen socket on %s\n", hp );
  }
  if ( redis_un.listen( sn ) != 0 ) {
    fprintf( stderr, "unable to open unix listen socket on %s\n", sn );
  }
  if ( nats_sv.listen( NULL, atoi( np ) ) != 0 ) {
    fprintf( stderr, "unable to open nats listen socket on %s\n", np );
  }
  if ( status == 0 ) {
    printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
    printf( "max_fds:              %d\n", maxfd );
    printf( "prefetch:             %s\n", fe[ 0 ] == '1' ? "true" : "false" );
    /*printf( "single_thread:        %s\n", si[ 0 ] == '1' ? "true" : "false" );*/
    printf( "listening:            %s\n", pt );
    printf( "unix:                 %s\n", sn );
    printf( "www:                  %s\n", hp );
    printf( "nats:                 %s\n", np );
    fflush( stdout );
    sighndl.install();
    for (;;) {
      if ( poll.quit >= 5 )
        break;
      bool idle = poll.dispatch(); /* true if idle, false if busy */
      poll.wait( idle ? 100 : 0 );
      if ( sighndl.signaled && ! poll.quit )
        poll.quit++;
    }
    if ( fe[ 0 ] == '1' )
      for ( size_t i = 0; i < EvPoll::PREFETCH_SIZE; i++ ) {
        if ( poll.prefetch_cnt[ i ] != 0 )
          printf( "pre[%lu] = %lu\n", i, poll.prefetch_cnt[ i ] );
      }
    printf( "bye\n" );
  }
  shm.close();

  return status;
}
