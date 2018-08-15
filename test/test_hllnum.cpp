#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <raikv/key_hash.h>
#define HLL_GLOBAL_VARS
#include <raids/redis_hyperloglog.h>

using namespace rai;
using namespace ds;

/*typedef HyperLogLog<16> HLLCountEst16;
typedef HyperLogLog<15> HLLCountEst15;*/
typedef HyperLogLog<14> HLLCountEst14;
/*typedef HyperLogLog<11> HLLCountEst11;
typedef HyperLogLog<12> HLLCountEst12;
typedef HyperLogLog<13> HLLCountEst13;*/

struct Stats {
  uint64_t mink, maxk, mine, maxe;
  double   minerr, maxerr;
  Stats() : mink( 0 ), maxk( 0 ),
            mine( 0 ), maxe( 0 ), minerr( 0 ), maxerr( 0 ) {}

  void check( double estimate,  uint64_t actual ) {
    uint64_t est = (uint64_t) round( estimate );
    double err = ( (double) actual - (double) est ) * 100.0 / (double) actual;

    if ( err < this->minerr ) {
      this->minerr = err;
      this->mink = actual;
      this->mine = est;
    }
    else if ( err > this->maxerr ) {
      this->maxerr = err;
      this->maxk = actual;
      this->maxe = est;
    }
  }
  void print( const char *s ) {
    printf( "%s: minerr %.1f%% (%lu-%lu) maxerr %.1f%% (%lu-%lu)\n",
            s, this->minerr, this->mink, this->mine, this->maxerr,
            this->maxk, this->maxe );
  }
};

int
main( int , char ** )
{
  HLLCountEst14 he1;
  HLLCountEst14 he2;
  HLLCountEst14 he3;
  HLLCountEst14 he4;
  HLLCountEst14 he5;
  HLLCountEst14 he6;
  uint64_t j, k, h1, h2, h3, h4, h5, h6;
  //Stats s1, s2, s3, s4, s5;
  double e1, e2, e3, e4, e5, e6, kf;
  bool l1, l2, l3, l4, l5, l6;
  //const char *s1, *s2, *s3, *s4, *s5, *s6;
  FILE *fp[ 12 ];
  int n;
  //static const char *islin[ 2 ] = { "H", "L" };

  he1.init( 0 ); he2.init( 1 ); he3.init( 2 ); he4.init( 3 );
  he5.init( 4 ); he6.init( 5 );
  for ( n = 0; n < 12; n++ ) {
    char fn[ 16 ];
    ::sprintf( fn, "out%u.la%d_n", he1.ht_bits, n );
    fp[ n ] = fopen( fn, "w" );
  }
  printf( "size6 = %lu\n", he1.size() + he2.size() + he3.size() + he4.size() +
          he5.size() + he6.size() );
  printf( "size4 = %lu\n", he1.size() + he2.size() + he3.size() + he4.size() );
  printf( "size2 = %lu\n", he1.size() + he2.size() );
  printf( "size  = %lu\n", he1.size() );
  for ( k = 0; k < 100ULL * 1000ULL; k++ ) {
    //e1.zero(); e2.zero(); e3.zero();
    h1 = 0;
    h2 = 0;
    h3 = 0;
    h4 = 0;
    h5 = 0;
    h6 = 0;
    j  = k + 1234567890ULL;
    kv_hash_aes128( &j, sizeof( j ), &h1, &h2 );
    kv_hash_spooky128( &j, sizeof( j ), &h3, &h4 );
    kv_hash_murmur128( &j, sizeof( j ), &h5, &h6 );
    //h1 &= HLL_HASH_MASK; h2 &= HLL_HASH_MASK; h3 &= HLL_HASH_MASK;
    he1.add( h1 );
    he2.add( h2 );
    he3.add( h3 );
    he4.add( h4 );
    he5.add( h5 );
    he6.add( h6 );
    //if ( ( k + 1 ) % 1000000 == 0 ) {
/*    s1.check( e1 = he1.estimate(), k+1 );
    s2.check( e2 = he2.estimate(), k+1 );
    s3.check( e3 = he3.estimate(), k+1 );
    s4.check( e4 = he4.estimate(), k+1 );
    s5.check( ( e1 + e2 + e3 + e4 ) / 4.0, k+1 );*/
    e1 = he1.estimate( l1 ); //s1 = islin[ l1?1:0 ];
    e2 = he2.estimate( l2 ); //s2 = islin[ l2?1:0 ];
    e3 = he3.estimate( l3 ); //s3 = islin[ l3?1:0 ];
    e4 = he4.estimate( l4 ); //s4 = islin[ l4?1:0 ];
    e5 = he5.estimate( l5 ); //s5 = islin[ l5?1:0 ];
    e6 = he6.estimate( l6 ); //s6 = islin[ l6?1:0 ];
    kf = (double) ( k + 1 );
    //if ( ( k + 1 ) % 100000 == 0 ) {
      fprintf( fp[ 0 ], "%lu %.0f\n", k + 1, e1 - kf );
      fprintf( fp[ 1 ], "%lu %.0f\n", k + 1, e2 - kf );
      fprintf( fp[ 2 ], "%lu %.0f\n", k + 1, e3 - kf );
      fprintf( fp[ 3 ], "%lu %.0f\n", k + 1, e4 - kf );
      fprintf( fp[ 4 ], "%lu %.0f\n", k + 1, e5 - kf );
      fprintf( fp[ 5 ], "%lu %.0f\n", k + 1, e6 - kf );
      fprintf( fp[ 6 ], "%lu %.0f\n", k + 1,
               ( e1 + e2 + e3 + e4 + e5 + e6 ) / 6.0 - kf );
      fprintf( fp[ 7 ], "%lu %.0f\n", k + 1,
               ( e1 + e2 + e3 + e4 ) / 4.0 - kf );
      fprintf( fp[ 8 ], "%lu %.0f\n", k + 1,
               ( e1 + e2 + e5 + e6 ) / 4.0 - kf );
      fprintf( fp[ 9 ], "%lu %.0f\n", k + 1,
               ( e1 + e2 ) / 2.0 - kf );
      fprintf( fp[ 10 ], "%lu %.0f\n", k + 1,
               ( e3 + e4 ) / 2.0 - kf );
      fprintf( fp[ 11 ], "%lu %.0f\n", k + 1,
               ( e5 + e6 ) / 2.0 - kf );
    //}
  }
  for ( n = 0; n < 12; n++ )
    fclose( fp[ n ] );
#if 0
  printf( "k: %lu, e: %.1f  %.1f  %.1f,%.1f,%1.f,%.1f\n",
           k, ( e1 + e2 + e3 + e4 ) / 4.0,
              ( e1 + e3 ) / 2.0, e1, e2, e3, e4 );
  s1.print( "1" );
  s2.print( "2" );
  s3.print( "3" );
  s4.print( "4" );
  s5.print( "5" );
  he5.merge( he1 );
  printf( "merge1: %.1f == %.1f\n", he5.estimate(), e1 = he1.estimate() );
  he5.merge( he2 );
  printf( "merge2: %.1f == %.1f\n", he5.estimate(), e1 += he2.estimate() );
  he5.merge( he3 );
  printf( "merge3: %.1f == %.1f\n", he5.estimate(), e1 += he3.estimate() );
  he5.merge( he4 );
  printf( "merge4: %.1f == %.1f\n", he5.estimate(), e1 += he4.estimate() );
#endif
  return 0;
}

