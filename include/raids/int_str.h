#ifndef __rai_raids__int_str_h__
#define __rai_raids__int_str_h__

#ifdef __cplusplus
extern "C" {
#endif

size_t ds_uint_digits( uint64_t v );
size_t ds_int_digits( int64_t v );
size_t ds_uint_to_string( uint64_t v,  char *buf,  size_t len );
size_t ds_int_to_string( int64_t v,  char *buf,  size_t len );
int ds_string_to_int( const char *str,  size_t sz,  int64_t *ival );
int ds_string_to_uint( const char *str,  size_t sz,  uint64_t *ival );
int ds_string_to_dbl( const char *str,  size_t sz,  double *fval );

#ifdef __cplusplus
}

#include <raikv/util.h>
#include <raikv/work.h>

namespace rai {
namespace ds {
#if 0
/* integer to string routines */
static inline uint64_t negate( int64_t v ) {
  if ( (uint64_t) v == ( (uint64_t) 1 << 63 ) )
    return ( (uint64_t) 1 << 63 );
  return (uint64_t) -v;
}
static inline size_t uint_digits( uint64_t v ) {
  for ( size_t n = 1; ; n += 4 ) {
    if ( v < 10 )    return n;
    if ( v < 100 )   return n + 1;
    if ( v < 1000 )  return n + 2;
    if ( v < 10000 ) return n + 3;
    v /= 10000;
  }
}
static inline size_t int_digits( int64_t v ) {
  if ( v < 0 ) return 1 + uint_digits( negate( v ) );
  return uint_digits( v );
}
/* does not null terminate (most strings have lengths, not nulls) */
static inline size_t uint_to_str( uint64_t v,  char *buf,  size_t len ) {
  for ( size_t pos = len; v >= 10; ) {
    const uint64_t q = v / 10, r = v % 10;
    buf[ --pos ] = '0' + r;
    v = q;
  }
  buf[ 0 ] = '0' + v;
  return len;
}
static inline size_t uint_to_str( uint64_t v,  char *buf ) {
  return uint_to_str( v, buf, uint_digits( v ) );
}
static inline size_t int_to_str( int64_t v,  char *buf,  size_t len ) {
  if ( v < 0 ) {
    buf[ 0 ] = '-';
    return 1 + uint_to_str( negate( v ), &buf[ 1 ], len - 1 );
  }
  return uint_to_str( v, buf, len );
}
static inline size_t int_to_str( int64_t v,  char *buf ) {
  return int_to_str( v, buf, int_digits( v ) );
}
#endif
enum StrCvtStatus {
  STR_CVT_OK             = 0,
  STR_CVT_INT_OVERFLOW   = 1,
  STR_CVT_BAD_INT        = 2,
  STR_CVT_FLOAT_OVERFLOW = 3,
  STR_CVT_BAD_FLOAT      = 4
};
/* str length sz to int */
int string_to_int( const char *str,  size_t sz,  int64_t &ival ) noexcept;
/* str length sz to uint */
int string_to_uint( const char *str,  size_t sz,  uint64_t &ival ) noexcept;
/* str length sz to double */
int string_to_dbl( const char *str,  size_t sz,  double &fval ) noexcept;

static inline size_t crlf( char *b,  size_t i ) {
  b[ i ] = '\r'; b[ i + 1 ] = '\n'; return i + 2;
}

}
}
#endif /* __cplusplus */
#endif
