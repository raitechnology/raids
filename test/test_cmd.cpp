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
static char auth_str[]     = "AUTH";
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
  { auth_str,    sizeof( auth_str)     - 1, AUTH_CMD },
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
  RedisCmd cmd;

  ::memset( arity_cnt, 0, sizeof( arity_cnt ) );
  start = kv_current_monotonic_time_s();
  for ( int repeat = 0; repeat < 10 * 1000 * 1000; repeat++ ) {
    for ( size_t i = 0; i < sizeof( examples ) / sizeof( examples[ 0 ] ); i++ ) {
      if ( (cmd = get_redis_cmd( examples[ i ].ex, examples[ i ].len )) !=
           examples[ i ].cmd ) {
        printf( "failed: %s\n", examples[ i ].ex );
      }
      const uint16_t mask = get_cmd_flag_mask( cmd );
      if ( test_cmd_mask( mask, CMD_READONLY_FLAG ) != 0 )
        readonly++;
      else if ( test_cmd_mask( mask, CMD_WRITE_FLAG ) != 0 )
        write++;
      get_cmd_arity( cmd, arity, first, last, step );
      arity_cnt[ arity > 0 ? arity : -arity ]++;
    }
  }
  end = kv_current_monotonic_time_s();
  printf( "speed: %.3f M/s %.03fns\n", 10.0 / ( end - start ) * 10.0,
          ( end - start ) * 10.0 );
  printf( "readonly %u write %u arity 1: %u, 2: %u\n", readonly, write,
          arity_cnt[ 1 ], arity_cnt[ 2 ] );

  for ( size_t i = 0; i < sizeof( examples ) / sizeof( examples[ 0 ] ); i++ ) {
    cmd = get_redis_cmd( examples[ i ].ex, examples[ i ].len );
    printf( "%s: ", cmd_db[ cmd ].name );
    const char *comma = "";
    for ( uint8_t fl = 0; fl < cmd_flag_cnt; fl++ ) {
      if ( test_cmd_flag( cmd, (RedisCmdFlag) fl ) ) {
        printf( "%s%s", comma, cmd_flag[ fl ] );
        comma = ",";
      }
    }
    get_cmd_arity( cmd, arity, first, last, step );
    printf( " arity: %d, first: %d, last %d, step: %d\n", arity, first, last,
            step );
    printf( "%s\n", cmd_db[ cmd ].attr );
  }
  return 0;
}

