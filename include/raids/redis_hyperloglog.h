#ifndef __rai_raids__redis_hyperloglog_h__
#define __rai_raids__redis_hyperloglog_h__

namespace rai {
namespace ds {

template <const uint32_t HT_BITS>
struct HyperLogLog {
  static const uint32_t LZ_BITS    = 6, /* bitsize for leading zeros register */
                        HASH_WIDTH = 64 - HT_BITS; /* word size of LZ calc */
  static const uint16_t LZ_MASK    = ( 1 << LZ_BITS ) - 1;
  static const uint64_t HASH_MASK  = ( (uint64_t) 1 << HASH_WIDTH ) - 1;
  static const uint32_t HTSZ       = 1 << HT_BITS, /* num registers */
                        HT_MASK    = HTSZ - 1;
  /* alpha from http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf */
  static inline double alpha( void ) {
    return 0.7213 / ( 1.0 + 1.079 / (double) HTSZ );
  }
  /* precalculated tables for estimates */
  static double lz_sum[ LZ_MASK + 1 ]; /* table for hyperloglog: 1/1<<LZ */
  static double ht_beta[ HTSZ ];       /* table for hyperloglog: log(ez) */
  static double ht_lin[ HTSZ ];        /* table for linear counting -m*log(n) */

  uint16_t size8;     /* sizeof this / 8 */
  uint8_t  ht_bits,   /* HT_BITS */
           hash_algo; /* 0=aes:1, 0=aes:2, 2=spooky:1, 3=spooky:2, 4=mur:1, 5=mur:2 */
  uint32_t popcnt; /* how many ht[] entries are used */
  double   sum;    /* sum of lz_sum[ n ] or 1 / ( 1 << ht[ n ] ) */
  uint8_t  ht[ ( HTSZ * LZ_BITS + 7 ) / 8 ]; /* ht % HTSZ = LZ (registers) */
  /* beta from https://arxiv.org/pdf/1612.02284.pdf */
  static double beta( double ez ) {
    double zl = log( ez + 1 );
    return -0.370393911 * ez +
            0.070471823 * zl +
            0.17393686  * pow( zl, 2 ) +
            0.16339839  * pow( zl, 3 ) +
           -0.09237745  * pow( zl, 4 ) +
            0.03738027  * pow( zl, 5 ) +
           -0.005384159 * pow( zl, 6 ) +
            0.00042419  * pow( zl, 7 );
  }
  size_t size( void ) const {
    return this->size8 * 8;
  }
  /* zero stuff, init tables if necessary */
  void init( uint8_t algo ) {
    this->size8     = sizeof( *this ) / 8; /* when <= 1<<18 */
    this->ht_bits   = HT_BITS;
    this->hash_algo = algo;
    this->popcnt    = 0;
    this->sum       = 0;
    ::memset( this->ht, 0, sizeof( this->ht ) );
    if ( lz_sum[ 1 ] == 0.0 )
      ginit();
  }
  /* init global tables */
  static void ginit( void ) {
    if ( lz_sum[ 1 ] == 0.0 ) {
      uint32_t i;
      /* linear counting is an estimate of card based on how full ht[] is */
      for ( i = 1; i < HTSZ; i++ ) {
        double n = (double) ( HTSZ - i ) / (double) HTSZ;
        ht_lin[ i ] = -(double) HTSZ * log( n );
      }
      /* hyperloglog counting is based on the sum of 1/lz,
       * where lz is pow2 counting of leading zeros in the hash */
      for ( i = 1; i < sizeof( lz_sum ) / sizeof( lz_sum[ 0 ] ); i++ )
        lz_sum[ i ] = 1.0 / (double) ( (uint64_t) 1 << i );
      for ( i = 1; i < sizeof( ht_beta ) / sizeof( ht_beta[ 0 ] ); i++ )
        ht_beta[ i ] = beta( (double) i );
    }
  }
  static inline uint16_t get_u16( const void *p ) {
    uint16_t i; ::memcpy( &i, p, sizeof( i ) ); return i;
  }
  static inline void put_u16( uint16_t i,  void *p ) {
    ::memcpy( p, &i, sizeof( i ) );
  }
  /* return 1 if added, 0 if not added */
  int add( uint64_t h ) {
    uint64_t lzwd = ( h & HASH_MASK ) >> HT_BITS; /* the upper bits of hash */
    uint32_t reg  = ( h & HT_MASK ) * LZ_BITS, /* register bit position */
             off  = reg / 8,                   /* 8 bit address and shift */
             shft = reg % 8;
    uint16_t hv = get_u16( &this->ht[ off ] );
    uint16_t v, w, m, lz;

    v  = hv;                      /* the 6 bit reg val plus surrounding bits */
    m  = ~( LZ_MASK << shft ) & v; /* m = surrounding bits, reg masked to 0 */
    v  = ( v >> shft ) & LZ_MASK;  /* extract the value from surrounding */
    lz = __builtin_clzl( lzwd ) + 1; /* the new value */
    lz -= 64 + HT_BITS - HASH_WIDTH; /* minus the hash table bits */
    w  = ( v > lz ? v : lz );      /* maximum of old and new */
    if ( v == w )                  /* no change */
      return 0;
    hv = m | ( w << shft );        /* put the value back into bits blob */
    put_u16( hv, &this->ht[ off ] );
    if ( v == 0 )                  /* new entry */
      this->popcnt++;
    this->sum += lz_sum[ w ] - lz_sum[ v ]; /* remove old and add new */
    return 1;
  }
  /* return true if hash exists in hll */
  bool test( uint64_t h ) const {
    uint64_t lzwd = ( h & HASH_MASK ) >> HT_BITS; /* the upper bits of hash */
    uint32_t reg  = ( h & HT_MASK ) * LZ_BITS, /* register bit position */
             off  = reg / 8,                   /* 8 bit address and shift */
             shft = reg % 8;
    uint16_t hv   = get_u16( &this->ht[ off ] );
    uint16_t v, lz;

    v  = ( hv >> shft ) & LZ_MASK; /* extract the value from surrounding */
    lz = __builtin_clzl( lzwd ) + 1; /* the new value */
    lz -= 64 + HT_BITS - HASH_WIDTH; /* minus the hash table bits */
    return v >= lz;
  }
  /* return the current estimate, islin = is linear counting */
  double estimate( bool &islin ) const {
    /* linear counting threshold 95% full */
    if ( this->popcnt < HTSZ * 950 / 1000 ) {
      islin = true;
      return ht_lin[ this->popcnt ];
    }
    islin = false;

    uint32_t empty = HTSZ - this->popcnt;
    double   sum2  = this->sum + (double) empty; /* 1/1<<0 == 1 */
    return alpha() * (double) HTSZ * (double) this->popcnt /
           ( ht_beta[ empty ] + sum2 );
  }
  static inline uint64_t get_u64( const void *p ) {
    uint64_t i; ::memcpy( &i, p, sizeof( i ) ); return i;
  }
  /* merge hll into this */
  void merge( const HyperLogLog &hll ) {
    uint32_t bitsleft = 0;
    uint64_t hbits = 0, mbits = 0;
    uint16_t x, y;
    /* reg is the bit offset of each register */
    for ( uint32_t reg = 0; reg < HTSZ * LZ_BITS; reg += LZ_BITS ) {
      const uint32_t off  = reg / 8,
                     shft = reg % 8;
      if ( bitsleft < LZ_BITS ) {  /* 64 bit shift words, little endian style */
        hbits = get_u64( &hll.ht[ off ] );
        mbits = get_u64( &this->ht[ off ] );
        hbits >>= shft;
        mbits >>= shft;
        bitsleft = 64 - shft;
      }
      x = hbits & LZ_MASK; /* 6 bit register at reg offset */
      y = mbits & LZ_MASK;
      if ( y < x ) {       /* if merge is larger, take that value */
        if ( y == 0 )      /* new entry */
          this->popcnt++;
        this->sum += lz_sum[ x ] - lz_sum[ y ]; /* update sum */
        /* splice in the new bits */
        uint16_t hv = get_u16( &this->ht[ off ] );
        hv = ( ~( LZ_MASK << shft ) & hv ) | (uint16_t) ( x << shft );
        put_u16( hv, &this->ht[ off ] );
      }
      hbits >>= LZ_BITS; /* next register */
      mbits >>= LZ_BITS;
      bitsleft -= LZ_BITS;
    }
  }
};

#ifdef HLL_GLOBAL_VARS
template <const uint32_t I> double HyperLogLog<I>::lz_sum[];
template <const uint32_t I> double HyperLogLog<I>::ht_lin[];
template <const uint32_t I> double HyperLogLog<I>::ht_beta[];
#endif

}
}

#endif
