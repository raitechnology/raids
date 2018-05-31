#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/redis_msg.h>
#include <raikv/util.h>
#include <raikv/work.h>

using namespace rai;
using namespace ds;
using namespace kv;

/* examples from http://blog.wjin.org/posts/redis-communication-protocol.html */
static char ex_simple_str[]  = "+hello world\r\n";
static char ex_error_str[]   = "-error\r\n";
static char ex_integer[]     = ":123\r\n";
static char ex_mt_string[]   = "$0\r\n\r\n";
static char ex_null_string[] = "$-1\r\n";
static char ex_mt_array[]    = "*0\r\n";
static char ex_null_array[]  = "*-1\r\n";
static char ex_int_array[]   = "*3\r\n:1\r\n:2\r\n:3\r\n";
static char ex_str_array[]   = "*1\r\n$7\r\nCOMMAND\r\n";
static char ex_mix_array[]   = "*2\r\n$3\r\nfoo\r\n:1\r\n";

static struct {
  char * ex;
  size_t len;
} examples[] = {
  { ex_simple_str,  sizeof( ex_simple_str )  - 1 },
  { ex_error_str,   sizeof( ex_error_str )   - 1 },
  { ex_integer,     sizeof( ex_integer )     - 1 },
  { ex_mt_string,   sizeof( ex_mt_string )   - 1 },
  { ex_null_string, sizeof( ex_null_string ) - 1 },
  { ex_mt_array,    sizeof( ex_mt_array )    - 1 },
  { ex_null_array,  sizeof( ex_null_array )  - 1 },
  { ex_int_array,   sizeof( ex_int_array )   - 1 },
  { ex_str_array,   sizeof( ex_str_array )   - 1 },
  { ex_mix_array,   sizeof( ex_mix_array )   - 1 }
};

int
main( int argc, char *argv[] )
{
  char buf[ 1024 ];
  size_t sz;
  RedisMsg m;
  WorkAllocT<8192> wrk;
  double start, end;
  RedisMsgStatus x;

  start = kv_current_monotonic_time_s();
  for ( int j = 0; j < 1000 ; j++ ) {
    for ( int repeat = 0; repeat < 1000; repeat++ ) {
      for ( size_t i = 0; i < sizeof( examples ) /
                              sizeof( examples[ 0 ] ); i++ ) {
        sz = examples[ i ].len;
        x = m.unpack( examples[ i ].ex, sz, wrk );
        if ( x != REDIS_MSG_OK || sz != examples[ i ].len )
          printf( "unpack sz %lu != len %lu\n", sz, examples[ i ].len );
        /*if ( m.to_json( buf ) )
          printf( "%s\n", buf );*/
        sz = sizeof( buf );
        x = m.pack( buf, sz );
        if ( x != REDIS_MSG_OK || sz != examples[ i ].len )
          printf( "pack   sz %lu != len %lu\n", sz, examples[ i ].len );
      }
    }
    wrk.reset();
  }
  end = kv_current_monotonic_time_s();
  printf( "speed: %.3f M/s %.03fns\n", 1.0 / ( end - start ) * 10.0,
          ( end - start ) * 100.0 );
  return 0;
}

