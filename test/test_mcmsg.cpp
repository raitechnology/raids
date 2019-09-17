#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/memcached_exec.h>
#include <raikv/util.h>
#include <raikv/work.h>

using namespace rai;
using namespace ds;
using namespace kv;

/* examples from http://blog.wjin.org/posts/redis-communication-protocol.html */
static char ex_set_str[]   = "\r\nSET key 0 60 11\r\nhello world\r\n";
static char ex_get_str[]   = "GET key key2\r\n";
static char ex_gat_str[]   = "GAT 90 key key2\r\n";
static char ex_touch_str[] = "TOUCH key 60\r\n";
static char ex_cas_str[]   = "CAS key 0 60 11 12345678\r\nhello world\r\n";
static char ex_incr_str[]  = "INCR key 1\r\n";
static char ex_vers_str[]  = "VERSION\r\n";
static char ex_quit_str[]  = "QUIT\r\n";

static char ex_ok_str[]    = "OK\r\n";
static char ex_int_str[]   = "53\r\n";
static char ex_end_str[]   = "END\r\n";
static char ex_value_str[] = "VALUE key 0 11 1\r\nhello world\r\n";
static char ex_valu2_str[] = "VALUE key2 0 11\r\nworld hello\r\n";
static char ex_err_str[]   = "CLIENT_ERROR bad args\r\n";

static struct {
  char * ex;
  size_t len;
} examples[] = {
  { ex_set_str,  sizeof( ex_set_str )  - 1 },
  { ex_get_str,  sizeof( ex_get_str )  - 1 },
  { ex_gat_str,  sizeof( ex_gat_str )  - 1 },
  { ex_touch_str,  sizeof( ex_touch_str )  - 1 },
  { ex_cas_str,  sizeof( ex_cas_str )  - 1 },
  { ex_incr_str,  sizeof( ex_incr_str )  - 1 },
  { ex_vers_str,  sizeof( ex_vers_str )  - 1 },
  { ex_quit_str,  sizeof( ex_quit_str )  - 1 }
}, results[] = {
  { ex_ok_str, sizeof( ex_ok_str ) - 1 },
  { ex_int_str, sizeof( ex_int_str ) - 1 },
  { ex_end_str, sizeof( ex_end_str ) - 1 },
  { ex_value_str, sizeof( ex_value_str ) - 1 },
  { ex_valu2_str, sizeof( ex_valu2_str ) - 1 },
  { ex_err_str, sizeof( ex_err_str ) - 1 }
};

int
main( int, char ** )
{
  size_t sz;
  MemcachedMsg m;
  MemcachedRes r;
  kv::ScratchMem wrk;
  MemcachedStatus x;
  size_t i;

  for ( i = 0; i < sizeof( examples ) / sizeof( examples[ 0 ] ); i++ ) {
    sz = examples[ i ].len;
    x = m.unpack( examples[ i ].ex, sz, wrk );
    printf( "unpack=%d/%s sz %lu len %lu \"%.*s\"\n",
            x, memcached_status_string( x ), sz, examples[ i ].len,
            (int) m.msglen, m.msg );
    if ( x == MEMCACHED_OK )
      m.print();
    wrk.reset();
  }
  for ( i = 0; i < sizeof( results ) / sizeof( results[ 0 ] ); i++ ) {
    sz = results[ i ].len;
    x = r.unpack( results[ i ].ex, sz, wrk );
    printf( "unpack=%d/%s sz %lu len %lu \"%.*s\"\n",
            x, memcached_status_string( x ), sz, results[ i ].len,
            (int) r.msglen, r.msg );
    if ( x == MEMCACHED_OK )
      r.print();
    wrk.reset();
  }

  return 0;
}

