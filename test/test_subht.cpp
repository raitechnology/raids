#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raids/redis_pubsub.h>
#include <raikv/route_ht.h>
#include <raids/nats_map.h>

using namespace rai;
using namespace ds;
using namespace kv;

const char *str[] = {
  "hello", "world", "this", "is", "one", "way", "street", "behind",
  "mailbox", "working", "before", "jumble", "world", "super", "kayak", "gong"
};
static const size_t nstr = sizeof( str ) / sizeof( str[ 0 ] );

size_t
split_args( char *s,  char **args,  size_t *len )
{
  size_t cnt = 0;
  for (;;) {
    while ( *s != '\0' && *s <= ' ' ) s++;
    if ( *s == '\0' ) return cnt;
    args[ cnt ] = s;
    while ( *s > ' ' ) s++;
    len[ cnt ] = (size_t) ( s - args[ cnt ] );
    cnt++;
  }
}

int
main( int argc, char ** )
{
  int cnt = 0;
  size_t i;

  if ( argc == 1 ) {
    RedisSubMap map;
    RedisSubRoute * rt;
    for ( i = 0; i < 100; i++ ) {
      const char *s = str[ i % 16 ];
      size_t l = ::strlen( s );
      uint32_t h = kv_crc_c( s, l, 0 );
      if ( map.put( h, s, l ) == REDIS_SUB_EXISTS )
        cnt++;
    }
    for ( i = 0; i < 1000000; i++ ) {
      const char *s = str[ i % 16 ];
      size_t l = ::strlen( s );
      uint32_t h = kv_crc_c( s, l, 0 );
      if ( map.updcnt( h, s, l, rt ) == REDIS_SUB_OK )
        cnt++;
    }
  }
  else {
    NatsSubMap map;
    char * args[ 16 ];
    size_t cnt, len[ 16 ];
    char buf[ 1024 ];
    printf( "[sup]> " ); fflush( stdout );
    while ( fgets( buf, sizeof( buf ), stdin ) != NULL ) {
      switch ( buf[ 0 ] ) {
        case 's':
          cnt = split_args( &buf[ 2 ], args, len );
          if ( cnt != 2 )
            printf( "s <subj> <sid>\n" );
          else {
            NatsStr subj( args[ 0 ], len[ 0 ] );
            NatsStr sid( args[ 1 ], len[ 1 ] );
            map.put( subj, sid );
          }
          break;

        case 'u':
          cnt = split_args( &buf[ 2 ], args, len );
          if ( cnt != 1 && cnt != 2 )
            printf( "u <sid> [max-msgs]\n" );
          else {
            NatsStr  sid( args[ 0 ], len[ 0 ] );
            uint32_t max_msgs;
            if ( cnt == 2 )
              max_msgs = atoi( args[ 1 ] );
            else
              max_msgs = 0;
            map.expire_sid( sid, max_msgs );
          }
          break;

        case 'p':
          cnt = split_args( &buf[ 2 ], args, len );
          if ( cnt != 2 )
            printf( "p <subj> <msg>\n" );
          else {
            NatsStr subj( args[ 0 ], len[ 0 ] );
            NatsStr sid;
            NatsLookup pub;
            NatsSubStatus status = map.lookup_publish( subj, pub );
            if ( status != NATS_NOT_FOUND ) {
              for ( ; pub.iter.get( sid ); pub.iter.incr() ) {
                printf( "msg subj=%.*s sid=%.*s msg=%.*s\n",
                        (int) subj.len, subj.str,
                        (int) sid.len, sid.str,
                        (int) len[ 1 ], args[ 1 ] );
              }
            }
            if ( status == NATS_EXPIRED )
              map.expire_publish( subj, pub );
          }
          break;
      }
      map.print();
      printf( "[sup]> " ); fflush( stdout );
    }
  }
  printf( "cnt %d\n", cnt );
  return 0;
}


