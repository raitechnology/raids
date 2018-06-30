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
#include <raikv/shm_ht.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;
using namespace kv;

static HashTab *
shm_attach( const char *mn,  uint32_t &ctx_id )
{
  HashTabGeom geom;
  HashTab *map = HashTab::attach_map( mn, 0, geom );
  if ( map != NULL ) {
    ctx_id = map->attach_ctx( ::getpid(), 0 );
    fputs( print_map_geom( map, ctx_id ), stdout );
    fflush( stdout );
  }
  return map;
}

static void
shm_close( HashTab *map,  uint32_t ctx_id )
{
  //print_stats();
  if ( ctx_id != MAX_CTX_ID ) {
    map->detach_ctx( ctx_id );
    ctx_id = MAX_CTX_ID;
  }
  delete map;
}

static const char *
get_arg( int argc, char *argv[], int n, int b, const char *f, const char *def )
{
  /* [1]=map [2]=port [3]=sock [4]=http [5]=prefetch [6]=single */
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
  HashTab * map = NULL;
  uint32_t  ctx_id;
  int       status = 0;

  const char * mn = get_arg( argc, argv, 1, 1, "-m", "sysv2m:shm.test" ),
             * pt = get_arg( argc, argv, 2, 1, "-p", "8888" ),
             * sn = get_arg( argc, argv, 3, 1, "-u", "/tmp/raids.sock" ),
             * hp = get_arg( argc, argv, 4, 1, "-w", "48080" ),
             * fe = get_arg( argc, argv, 5, 1, "-f", "1" ),
             * si = get_arg( argc, argv, 6, 1, "-s", "0" ),
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s [-m map] [-p port] [-u unix] [-w web] [-f pre] [-s sin]\n"
            "  map  = kv shm map name (sysv2m:shm.test)\n"
            "  port = listen tcp port (8888)\n"
            "  unix = listen unix port (/tmp/raids.sock)\n"
            "  web  = listen www port (48080)\n"
            "  pre  = prefetch keys:  0 = no, 1 = yes (1)\n"
            "  sin  = single thread:  0 = no, 1 = yes (0)\n", argv[ 0 ] );
    return 0;
  }

  map = shm_attach( mn, ctx_id );
  if ( map == NULL ) /* bad map name, doesn't exist */
    status = 1;
  else {
    EvPoll       poll( map, ctx_id );
    EvTcpListen  sv( poll );
    EvUnixListen un( poll );
    EvHttpListen ht( poll );
    poll.init( 4 * 1024, fe[ 0 ] == '1', si[ 0 ] == '1' );
    if ( sv.listen( NULL, atoi( pt ) ) != 0 ) {
      fprintf( stderr, "unable to open tcp listen socket on %s\n", pt );
      status = 2; /* bad port or network error */
    }
    if ( ht.listen( NULL, atoi( hp ) ) != 0 ) {
      fprintf( stderr, "unable to open http listen socket on %s\n", hp );
    }
    if ( un.listen( sn ) != 0 ) {
      fprintf( stderr, "unable to open unix listen socket on %s\n", sn );
    }
    if ( status == 0 ) {
      printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
      printf( "max_fds:              %d\n", 4 * 1024 );
      printf( "prefetch:             %s\n", fe[ 0 ] == '1' ? "true" : "false" );
      printf( "single_thread:        %s\n", si[ 0 ] == '1' ? "true" : "false" );
      printf( "listening:            %s\n", pt );
      printf( "unix:                 %s\n", sn );
      printf( "www:                  %s\n", hp );
      fflush( stdout );
      sighndl.install();
      while ( poll.quit < 5 ) {
	poll.wait( 100 );
        if ( sighndl.signaled )
          poll.quit++;
	poll.dispatch();
      }
      if ( fe[ 0 ] == '1' )
        for ( size_t i = 0; i < EvPoll::PREFETCH_SIZE; i++ ) {
          if ( poll.prefetch_cnt[ i ] != 0 )
            printf( "pre[%lu] = %lu\n", i, poll.prefetch_cnt[ i ] );
        }
      printf( "bye\n" );
    }
    shm_close( map, ctx_id );
  }

  return status;
}
