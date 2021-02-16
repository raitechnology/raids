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
static char ex_white[]       = "\n*2\r\n $3\r\nfoo\r\n\r\n:1\r\n";

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
}, examples2[] = {
  { ex_white,  sizeof( ex_white )  - 1 }
};

static struct {
  const char * str;
  size_t       len;
  int64_t      ival;
} num_str[] = {
  { "0", 1, 0 },
  { "-1", 2, -1 },
  { "1234", 4, 1234 },
  { "9223372036854775807", 19, 9223372036854775807 },
  { "-9223372036854775808", 20, (int64_t) 0x8000000000000000LL },
  { "1000000000000000000000", 22, 0 },
  { "92e7", 4, (int64_t) 92e7 },
  { "92233720368547758078", 20, 0 },
  { "-9223372036854775809", 20, 0 }
};

int
main( int, char ** )
{
  char buf[ 1024 ];
  size_t sz;
  RedisMsg m;
  WorkAllocT<8192> wrk;
  double start, end;
  RedisMsgStatus x;
  int64_t ival;
  size_t j;

  for ( size_t i = 0; i < sizeof( examples2 ) /
                          sizeof( examples2[ 0 ] ); i++ ) {
    sz = examples2[ i ].len;
    x = m.unpack( examples2[ i ].ex, sz, wrk );
    if ( x != DS_MSG_STATUS_OK || sz != examples2[ i ].len ) {
      printf( "unpack sz %lu != len %lu\n", sz, examples2[ i ].len );
      return 1;
    }
    wrk.reset();
  }

  start = kv_current_monotonic_time_s();
  for ( j = 0; j < 1000 ; j++ ) {
    for ( int repeat = 0; repeat < 1000; repeat++ ) {
      for ( size_t i = 0; i < sizeof( examples ) /
                              sizeof( examples[ 0 ] ); i++ ) {
        sz = examples[ i ].len;
        x = m.unpack( examples[ i ].ex, sz, wrk );
        if ( x != DS_MSG_STATUS_OK || sz != examples[ i ].len )
          printf( "unpack sz %lu != len %lu\n", sz, examples[ i ].len );
        /*if ( m.to_json( buf ) )
          printf( "%s\n", buf );*/
        sz = sizeof( buf );
        x = m.pack2( buf, sz );
        if ( x != DS_MSG_STATUS_OK || sz != examples[ i ].len )
          printf( "pack   sz %lu != len %lu\n", sz, examples[ i ].len );
      }
    }
    wrk.reset();
  }
  end = kv_current_monotonic_time_s();
  printf( "unpack/pack2 speed: %.3f M/s %.03fns\n",
          1.0 / ( end - start ) * 10.0,
          ( end - start ) * 100.0 );
  start = kv_current_monotonic_time_s();
  for ( j = 0; j < 1000 ; j++ ) {
    for ( int repeat = 0; repeat < 1000; repeat++ ) {
      for ( size_t i = 0; i < sizeof( examples ) /
                              sizeof( examples[ 0 ] ); i++ ) {
        sz = examples[ i ].len;
        x = m.unpack( examples[ i ].ex, sz, wrk );
        if ( x != DS_MSG_STATUS_OK || sz != examples[ i ].len )
          printf( "unpack sz %lu != len %lu\n", sz, examples[ i ].len );
        /*if ( m.to_json( buf ) )
          printf( "%s\n", buf );*/
        sz = m.pack_size();
        m.pack( buf );
        if ( x != DS_MSG_STATUS_OK || sz != examples[ i ].len )
          printf( "pack   sz %lu != len %lu\n", sz, examples[ i ].len );
      }
    }
    wrk.reset();
  }
  end = kv_current_monotonic_time_s();
  printf( "unpack/pack_size/pack speed: %.3f M/s %.03fns\n",
          1.0 / ( end - start ) * 10.0,
          ( end - start ) * 100.0 );

  for ( j = 0; j < sizeof( num_str ) / sizeof( num_str[ 0 ] ); j++ ) {
    x = (RedisMsgStatus) RedisMsg::str_to_int( num_str[ j ].str,
                                               num_str[ j ].len, ival );
    if ( x == DS_MSG_STATUS_OK ) {
      if ( ival != num_str[ j ].ival )
        printf( "failed: %s (%ld)\n", num_str[ j ].str, ival );
      else {
        printf( "str_to_int(%s) ok\n", num_str[ j ].str );
        sz = int64_to_string( ival, buf );
        buf[ sz ] = '\0';
        if ( ::strcmp( num_str[ j ].str, buf ) != 0 )
          printf( "failed: %s %s\n", num_str[ j ].str, buf );
        else
          printf( "int64_to_string(%s) ok\n", buf );
      }
    }
    else {
      printf( "did not parse: %s (%d/%s)\n", num_str[ j ].str,
              x, ds_msg_status_description( x ) );
    }
  }
  return 0;
}

