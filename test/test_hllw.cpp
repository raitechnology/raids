#include <stdio.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <math.h>
#include <raikv/key_hash.h>
#define HLL_GLOBAL_VARS
#include <raids/redis_hyperloglog.h>

using namespace rai;
using namespace ds;

typedef HyperLogLog<14> HLLCountEst14;

static const char *algo( int i ) {
  switch ( i ) {
    case 0: return "aes:1";
    case 1: return "aes:2";
    case 2: return "spooky:3";
    case 3: return "spooky:4";
    case 4: return "murmur:5";
    case 5: return "murmur:6";
    default: return "xxx";
  }
}

int
main( int, char ** )
{
  HLLCountEst14 he[ 6 ];
  char buf[ 1024 ];
  size_t len;
  uint64_t h[ 6 ];
  double e, r, d;
  int i, cnt = 0;
  bool b;

  for ( i = 0; i < 6; i++ )
    he[ i ].init( i );
 
  while ( fgets( buf, sizeof( buf ), stdin ) ) {
    len = ::strlen( buf );
    while ( len > 0 && buf[ len - 1 ] <= ' ' )
      buf[ --len ] = '\0';
    if ( len > 0 ) {
      for ( i = 0; i < 6; i++ )
        h[ i ] = 0;
      kv_hash_aes128( buf, len, &h[ 0 ], &h[ 1 ] );
      kv_hash_spooky128( buf, len, &h[ 2 ], &h[ 3 ] );
      kv_hash_murmur128( buf, len, &h[ 4 ], &h[ 5 ] );
      for ( i = 0; i < 6; i++ )
        he[ i ].add( h[ i ] );
      cnt++;
    }
  }
  double expected_error = 1.04 / sqrt( 1 << 14 ),
         min_error = (double) cnt - (double) cnt * expected_error,
         max_error = (double) cnt + (double) cnt * expected_error;
  printf( "loglog size %" PRIu64 "\n", he[ 0 ].size() );
  printf( "%d words, expected_error: +-%.1f, min_error: %.1f, max_error: %.1f\n",
          cnt, (double) cnt * expected_error, min_error, max_error );
  for ( i = 0; i < 6; i++ ) {
    e = he[ i ].estimate( b );
    d = (double) cnt - e;
    r = 100 * fabs( d ) / (double) cnt;

    printf( "%8s: %.1f estimate%s(diff=%.1f) ratio %.2f (%s)\n",
            algo( he[ i ].hash_algo ), e,
            e < min_error || e > max_error ? "! " : " ", d, r,
            b ? "linear" : "hyperloglog" );
  }
  e = ( he[ 0 ].estimate( b ) + he[ 1 ].estimate( b ) ) / 2.0;
  d = (double) cnt - e;
  r = 100 * fabs( d ) / (double) cnt;
  printf( "%8s: %.1f estimate (diff=%.1f) ratio %.2f\n", "aes",  e, d, r );
  e = ( he[ 1 ].estimate( b ) + he[ 2 ].estimate( b ) ) / 2.0;
  d = (double) cnt - e;
  r = 100 * fabs( d ) / (double) cnt;
  printf( "%8s: %.1f estimate (diff=%.1f) ratio %.2f\n", "spooky",  e, d, r );
  e = ( he[ 3 ].estimate( b ) + he[ 4 ].estimate( b ) ) / 2.0;
  d = (double) cnt - e;
  r = 100 * fabs( d ) / (double) cnt;
  printf( "%8s: %.1f estimate (diff=%.1f) ratio %.2f\n", "murmur",  e, d, r );
  return 0;
}

