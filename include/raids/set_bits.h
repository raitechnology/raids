#ifndef __rai_raids__set_bits_h__
#define __rai_raids__set_bits_h__

#include <raikv/util.h>

namespace rai {
namespace ds {

struct SetBits {
  size_t     last, /* last uint64_t word cleared, incr as bits are added */
             size; /* count of uint64_t words allocated, initially work[] */
  uint64_t * bits, /* the bits, allocted if work[] bits exhausted */
             work[ 1024 / 64 ]; /* static bits */

  SetBits() : last( 0 ) {
    this->size = sizeof( this->work ) / sizeof( this->work[ 0 ] );
    this->bits = this->work;
    this->work[ 0 ] = 0;
  }
  ~SetBits() {
    if ( this->bits != this->work )
      ::free( this->bits );
  }
  void reset( void ) {
    this->last = 0;
    this->bits[ 0 ] = 0;
  }
  bool alloc( size_t sz ) {
    void * p;
    if ( ( sz & ( sz - 1 ) ) != 0 ) /* power of 2 alloc */
      sz = (size_t) 1 << ( 64 - kv_clzl( sz ) );
    if ( this->bits == this->work ) { /* copy from work[] */
      if ( (p = ::malloc( sizeof( this->bits[ 0 ] ) * sz )) != NULL )
        ::memcpy( p, this->work, sizeof( this->work ) );
    }
    else /* use realloc for copy */
      p = ::realloc( this->bits, sizeof( this->bits[ 0 ] ) * sz );
    if ( p != NULL ) {
      this->bits = (uint64_t *) p;
      this->size = sz;
    }
    return p != NULL;
  }
  /* clear bits up to off */
  bool extend( size_t off ) {
    if ( off >= this->size )
      if ( ! this->alloc( off ) )
        return false;
    while ( this->last < off )
      this->bits[ ++this->last ] = 0;
    return true;
  }
  /* returns true/false if existing bit is set, then sets it */
  bool test_set( size_t j ) {
    size_t   off  = j >> 6;
    uint64_t mask = (uint64_t) 1 << ( j & 63 );
    if ( ! this->extend( off ) )
      return false;
    if ( ( this->bits[ off ] & mask ) != 0 )
      return true;
    this->bits[ off ] |= mask;
    return false;
  }
  bool set( size_t j ) {
    size_t   off  = j >> 6;
    uint64_t mask = (uint64_t) 1 << ( j & 63 );
    if ( ! this->extend( off ) )
      return false;
    this->bits[ off ] |= mask;
    return true;
  }
  /* not operator, flip all the bits set: bits = ~bits; */
  bool flip( size_t upto ) {
    if ( ! this->extend( upto >> 6 ) )
      return false;
    for ( size_t k = 0; k * 64 < upto; k++ ) {
      uint64_t w = this->bits[ k ];
      w = ~w;
      if ( ( k + 1 ) * 64 > upto )
        w &= ( ( 1 << ( upto & 63 ) ) - 1 );
      this->bits[ k ] = w;
    }
    return true;
  }
  /* next() is post-increment, externally:
   *   for ( i = 0; bits.next( i ); i++ ) { bit i is set } */
  bool next( size_t &j ) const {
    size_t k = j >> 6;
    if ( k > this->last )
      return false;
    for (;;) {
      uint64_t w = this->bits[ k ] >> ( j & 63 );
      int x = kv_ffsl( w );
      if ( x == 0 ) {
        j = ++k << 6;
        if ( k > this->last )
          return false;
      }
      else {
        j += x - 1;
        return true;
      }
    }
    return false;
  }
  /* prev() is pre-decrement, internally:
   *   for ( i = count; bits.prev( i ); ) { bit i is set } */
  bool prev( size_t &j ) const {
    if ( j == 0 )
      return false;
    size_t k = ( j - 1 ) >> 6;
    if ( k > this->last ) {
      k = this->last;
      j = k * 64 + 64;
    }
    for (;;) {
      uint64_t w = this->bits[ k ];
      if ( ( j & 63 ) > 0 )
        w <<= ( 64 - ( j & 63 ) );
      if ( w == 0 ) {
        if ( k == 0 )
          return false;
        j = k-- << 6;
      }
      else {
        int x = kv_clzl( w );
        j -= x + 1;
        return true;
      }
    }
    return false;
  }
};

}
}
#endif
