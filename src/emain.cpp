#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raids/ev_net.h>
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
    ctx_id = map->attach_ctx( ::getpid() );
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
  /* [1]=map [2]=port [3]=prefetch, no flags */
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
             * fe = get_arg( argc, argv, 3, 1, "-f", "1" ),
             * si = get_arg( argc, argv, 4, 1, "-s", "0" ),
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s [-m map] [-p port] [-f pre] [-s sin]\n"
            "  map  = kv shm map name (sysv2m:shm.test)\n"
            "  port = listen tcp port (8888)\n"
            "  pre  = prefetch keys:  0 = no, 1 = yes (1)\n"
            "  sin  = single thread:  0 = no, 1 = yes (0)\n", argv[ 0 ] );
    return 0;
  }

  map = shm_attach( mn, ctx_id );
  if ( map == NULL ) /* bad map name, doesn't exist */
    status = 1;
  else {
    EvPoll   poll( map, ctx_id );
    EvListen sv( poll );
    poll.init( 4 * 1024, fe[ 0 ] == '1', si[ 0 ] == '1' );
    if ( sv.listen( NULL, atoi( pt ) ) != 0 )
      status = 2; /* bad port or network error */
    else {
      printf( "max fds:              %d\n", 4 * 1024 );
      printf( "prefetch:             %s\n", fe[ 0 ] == '1' ? "true" : "false" );
      printf( "single_thread:        %s\n", si[ 0 ] == '1' ? "true" : "false" );
      printf( "listening:            %s\n", pt );
      sighndl.install();
      while ( poll.quit < 5 ) {
	poll.wait( 100 );
        if ( sighndl.signaled )
          poll.quit++;
	poll.dispatch_service();
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
