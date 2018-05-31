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
get_arg( int argc, char *argv[], int n, const char *f, const char *def )
{
  if ( argc > n && argv[ 1 ][ 0 ] != '-' ) /* [1]=map [2]=port, no flags */
    return argv[ n ];
  for ( int i = 1; i < argc - 1; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
      return argv[ i + 1 ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  HashTab * map = NULL;
  uint32_t  ctx_id;
  int       status = 0;

  const char *mn = get_arg( argc, argv, 1, "-m", "sysv2m:shm.test" );
  map = shm_attach( mn, ctx_id );
  if ( map == NULL ) /* bad map name, doesn't exist */
    status = 1;
  else {
    EvPoll   poll( map, ctx_id );
    EvListen sv( poll );
    poll.init( 4 * 1024 );
    const char *pt = get_arg( argc, argv, 2, "-p", "8888" );
    if ( sv.listen( NULL, atoi( pt ) ) != 0 )
      status = 2; /* bad port or network error */
    else {
      printf( "listening:            %s\n", pt );
      sighndl.install();
      while ( poll.quit < 5 ) {
	poll.wait( 100 );
        if ( sighndl.signaled )
          poll.quit++;
	poll.dispatch_service();
      }
      printf( "bye\n" );
    }
    shm_close( map, ctx_id );
  }

  return status;
}
