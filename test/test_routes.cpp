#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/route_db.h>

using namespace rai;
using namespace ds;

void print_routedb( RouteDB &rte )
{
  uint32_t i;
  if ( rte.xht != NULL ) {
    printf( "xht:\n" );
    for ( i = 0; i <= rte.xht->tab_mask; i++ ) {
      if ( rte.xht->tab[ i ].is_used() ) {
        uint32_t h = rte.xht->tab[ i ].hash,
                 v = rte.xht->tab[ i ].val,
                 cnt, *routes;
        cnt = rte.decompress_routes( v, routes, false );
        printf( "[%u][%x] %x(%s) ", i, h, v,
                rte.dc.is_not_encoded( v )?"str":"enc");
        printf( "[ " );
        for ( uint32_t j = 0; j < cnt; j++ )
          printf( "%u ", routes[ j ] );
        printf( "]\n" );
      }
    }
  }
  if ( rte.zht != NULL ) {
    printf( "zht:\n" );
    for ( i = 0; i <= rte.zht->tab_mask; i++ ) {
      if ( rte.zht->tab[ i ].is_used() ) {
        uint32_t  h = rte.zht->tab[ i ].hash,
                  v = rte.zht->tab[ i ].val;
        CodeRef * p = (CodeRef *) (void *) &rte.code_buf[ v ];
        printf( "[%u][%x] hash:%x off:%u ref:%u ecnt:%u rcnt:%u\n", i, h,
                p->hash, v, p->ref, p->ecnt, p->rcnt );
      }
    }
  }
  printf( "code_end %u, code_size %u, code_free %u\n",
          rte.code_end, rte.code_size, rte.code_free );
  printf( "code_spc_size %u, route_spc_size %u\n",
          rte.code_spc_size, rte.route_spc_size );
  if ( rte.xht != NULL )
    printf( "xht %u/%u\n", rte.xht->elem_count, rte.xht->tab_size() );
  if ( rte.xht != NULL )
    printf( "zht %u/%u\n", rte.zht->elem_count, rte.zht->tab_size() );
}

int
split_args( char *start,  char *end,  char **args,  size_t *len,
            size_t maxargs )
{
  char *p;
  size_t n;
  for ( p = start; ; p++ ) {
    if ( p >= end )
      return 0;
    if ( *p > ' ' )
      break;
  }
  n = 0;
  args[ 0 ] = p;
  for (;;) {
    if ( ++p == end || *p <= ' ' ) {
      len[ n ] = p - args[ n ];
      if ( ++n == maxargs )
        return n;
      while ( p < end && *p <= ' ' )
        p++;
      if ( p == end )
        break;
      args[ n ] = p;
    }
  }
  return n;
}

int
main( int, char ** )
{
  RouteDB rte;
  char  * args[ 30 ];
  size_t  arglen[ 30 ];
  char    buf[ 1024 ];

  for (;;) {
    printf( "[add, del, get, print, gc] [subject] [route]\n> " );
    if ( fgets( buf, sizeof( buf ), stdin ) == NULL )
      break;
    int n = split_args( buf, &buf[ ::strlen( buf ) ], args, arglen, 30 );
    if ( n >= 2 ) {
      uint32_t hash = kv_crc_c( args[ 1 ], arglen[ 1 ], 0 );
      if ( n >= 3 ) {
        uint32_t r = atoi( args[ 2 ] );
        if ( arglen[ 0 ] == 3 && ::strncmp( args[ 0 ], "add", 3 ) == 0 ) {
          for ( int i = 2; ; ) {
            rte.add_route( hash, r );
            if ( ++i == n )
              break;
            r = atoi( args[ i ] );
          }
          printf( "ok\n" );
        }
        else if ( arglen[ 0 ] == 3 && ::strncmp( args[ 0 ], "del", 3 ) == 0 ) {
          for ( int i = 2; ; ) {
            rte.del_route( hash, r );
            if ( ++i == n )
              break;
            r = atoi( args[ i ] );
          }
          printf( "ok\n" );
        }
        else {
          printf( "what?\n" );
        }
      }
      else if ( n == 2 ) {
        if ( arglen[ 0 ] == 3 && ::strncmp( args[ 0 ], "get", 3 ) == 0 ) {
          uint32_t *routes;
          n = rte.get_route( hash, routes );
          if ( n == 0 )
            printf( "not found\n" );
          else {
            printf( "%u [", n );
            for ( int i = 0; i < n; i++ )
              printf( "%u ", routes[ i ] );
            printf( "]\n" );
          }
        }
        else {
          printf( "what?\n" );
        }
      }
    }
    else if ( n == 1 && ::strncmp( args[ 0 ], "print", 5 ) == 0 ) {
      print_routedb( rte );
    }
    else if ( n == 1 && ::strncmp( args[ 0 ], "gc", 2 ) == 0 ) {
      rte.gc_code_ref_space();
    }
  }
  return 0;
}

