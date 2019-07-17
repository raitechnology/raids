#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>
#include <raikv/key_hash.h>
#define HLL_GLOBAL_VARS
#include <raids/redis_hyperloglog.h>

using namespace rai;
using namespace ds;

typedef HyperLogLog<14> HLLCountEst14;

uint64_t ht[ 4 * 1024 * 1024 ];
uint64_t mask = 4 * 1024 * 1024 - 1;

int
main( int argc, char *argv[] )
{ 
  HLLCountEst14 he1, he2, he3, he4, he5, he6;
  uint8_t *bloom;
  char buf[ 1024 ];
  size_t len, blsize, n, m;
  uint64_t h1, h2;
  int filter[ 6 ], hit, hit2, miss, miss2, total, total2, cnt, blfilter;
  
  he1.init( 0 );
  he2.init( 0 );
  he3.init( 0 );
  he4.init( 0 );
  he5.init( 0 );
  he6.init( 0 );
  blsize = sizeof( he1 ) * 6;
  printf( "size %ld\n", blsize );
  bloom = (uint8_t *) malloc( blsize );
  ::memset( bloom, 0, blsize );

  for ( int i = 1; i < argc; i++ ) {
    size_t len = ::strlen( argv[ i ] );
    h1 = h2 = 0;
    kv_hash_aes128( argv[ i ], len, &h1, &h2 );
    he1.add( h1 );
    he2.add( h2 );
    ht[ h1 & mask ] = h1;
    n = h1 % ( blsize * 8 );
    m = 1 << ( n % 8 );
    n /= 8;
    bloom[ n ] |= m;
    he3.add( ( h1 >> 32 ) | ( h2 << 32 ) );
    he4.add( ( h1 << 32 ) | ( h2 >> 32 ) );
    he5.add( ( h1 >> 48 ) | ( h2 << 16 ) );
    he6.add( ( h1 << 16 ) | ( h2 >> 48 ) );
  }
  ::memset( filter, 0, sizeof( filter ) );
  hit = hit2 = miss = miss2 = total = total2 = cnt = blfilter = 0;

  while ( fgets( buf, sizeof( buf ), stdin ) ) {
    len = ::strlen( buf );
    while ( len > 0 && buf[ len - 1 ] <= ' ' )
      buf[ --len ] = '\0';
    if ( len > 0 ) {
      h1 = h2 = 0;
      kv_hash_aes128( buf, len, &h1, &h2 );
      /*kv_hash_spooky128( buf, len, &h3, &h4 );
      kv_hash_murmur128( buf, len, &h5, &h6 );*/
      bool match = false;
      if ( ! he1.test( h1 ) )      filter[ 0 ]++;
      else if ( ! he2.test( h2 ) ) filter[ 1 ]++;
      else if ( ! he3.test( ( h1 >> 32 ) | ( h2 << 32 ) ) ) filter[ 2 ]++;
      else if ( ! he4.test( ( h1 << 32 ) | ( h2 >> 32 ) ) ) filter[ 3 ]++;
      else if ( ! he5.test( ( h1 >> 48 ) | ( h2 << 16 ) ) ) filter[ 4 ]++;
      else if ( ! he6.test( ( h1 << 16 ) | ( h2 >> 48 ) ) ) filter[ 5 ]++;
      else if ( ht[ h1 & mask ] == h1 ) {
	match = true;
        hit++;
      }
      else
        miss++;
      if ( ! match )
        total++;
      cnt++;

      n = h1 % ( blsize * 8 );
      m = 1 << ( n % 8 );
      n /= 8;
      match = false;
      if ( ( bloom[ n ] & m ) == 0 )
        blfilter++;
      else if ( ht[ h1 & mask ] == h1 ) {
	match = true;
        hit2++;
      }
      else
        miss2++;
      if ( ! match )
        total2++;
    }
  }
  printf( "sub %d cnt %d hit %d miss %d/%d %.2f%%\n", argc, cnt, hit, miss,
          total, (double) miss * 100.0 / (double) total );
  printf( "bloom hit %d miss %d/%d %.2f%%\n", hit2, miss2,
          total2, (double) miss2 * 100.0 / (double) total2 );
  for ( int i = 0; i < 6; i++ )
    printf( "filter[%d] = %d %.2f%%\n", i, filter[ i ],
      (double) filter[ i ] * 100.0 / (double) total );
  return 0;
}
