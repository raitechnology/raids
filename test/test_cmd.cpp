#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <raids/redis_cmd.h>
#include <raids/redis_cmd_db.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;

/* examples from http://blog.wjin.org/posts/redis-communication-protocol.html */
//static char auth_str[]     = "AUTH";
static char echo_str[]     = "ECHO";
static char ping_str[]     = "PING";
static char get_str[]      = "GET";
static char strlen_str[]   = "STRLEN";
static char hset_str[]     = "HSET";
static char pexpire_str[]  = "PEXPIRE";
static char publish_str[]  = "PUBLISH";
static char info_str[]     = "INFO";
static char rename_str[]   = "RENAME";

static struct {
  char   * ex;
  size_t   len;
  RedisCmd cmd;
} examples[] = {
  //{ auth_str,    sizeof( auth_str)     - 1, AUTH_CMD },
  { echo_str,    sizeof( echo_str )    - 1, ECHO_CMD },
  { ping_str,    sizeof( ping_str )    - 1, PING_CMD },
  { get_str,     sizeof( get_str )     - 1, GET_CMD },
  { strlen_str,  sizeof( strlen_str )  - 1, STRLEN_CMD },
  { hset_str,    sizeof( hset_str )    - 1, HSET_CMD },
  { pexpire_str, sizeof( pexpire_str ) - 1, PEXPIRE_CMD },
  { publish_str, sizeof( publish_str ) - 1, PUBLISH_CMD },
  { info_str,    sizeof( info_str )    - 1, INFO_CMD },
  { rename_str,  sizeof( rename_str )  - 1, RENAME_CMD }
};

int
main( int, char ** )
{
  uint32_t readonly = 0, write = 0, arity_cnt[ 8 ];
  int16_t arity, first, last, step;
  double start, end;
  uint64_t h;
  RedisCmd cmd;

  ::memset( arity_cnt, 0, sizeof( arity_cnt ) );
  start = kv_current_monotonic_time_s();
  for ( int repeat = 0; repeat < 10 * 1000 * 1000; repeat++ ) {
    for ( size_t i = 0; i < sizeof( examples ) / sizeof( examples[ 0 ] ); i++ ) {
      h   = get_redis_cmd_hash( examples[ i ].ex, examples[ i ].len );
      cmd = get_redis_cmd( h );
      if ( cmd != examples[ i ].cmd ) {
        printf( "failed: %s (%d)\n", examples[ i ].ex, cmd );
      }
      const uint16_t mask = cmd_db[ cmd ].flags;
      if ( ( mask & CMD_READ_FLAG ) != 0 )
        readonly++;
      else if ( ( mask & CMD_WRITE_FLAG ) != 0 )
        write++;
      arity = cmd_db[ cmd ].arity;
      arity_cnt[ arity > 0 ? arity : -arity ]++;
    }
  }
  end = kv_current_monotonic_time_s();
  printf( "speed: %.3f M/s %.03fns\n", 10.0 / ( end - start ) * 10.0,
          ( end - start ) * 10.0 );
  printf( "readonly %u write %u arity 1: %u, 2: %u\n", readonly, write,
          arity_cnt[ 1 ], arity_cnt[ 2 ] );

  for ( size_t i = 0; i < sizeof( examples ) / sizeof( examples[ 0 ] ); i++ ) {
    h   = get_redis_cmd_hash( examples[ i ].ex, examples[ i ].len );
    cmd = get_redis_cmd( h );
    printf( "%s: ", cmd_db[ cmd ].name );
    const char *comma = "";
    const uint16_t mask = cmd_db[ cmd ].flags;
    if ( ( mask & CMD_ADMIN_FLAG ) != 0 ) {
      printf( "admin" );
      comma = ",";
    }
    if ( ( mask & CMD_READ_FLAG ) != 0 ) {
      printf( "%sread", comma );
      comma = ",";
    }
    else if ( ( mask & CMD_READ_FLAG ) != 0 ) {
      printf( "%swrite", comma );
      comma = ",";
    }
    if ( ( mask & CMD_MOVABLE_FLAG ) != 0 )
      printf( "%smovable", comma );
    arity = cmd_db[ cmd ].arity;
    first = cmd_db[ cmd ].first;
    last  = cmd_db[ cmd ].last;
    step  = cmd_db[ cmd ].step;
    printf( " arity: %d, first: %d, last %d, step: %d\n", arity, first, last,
            step );
  }
  return 0;
}

