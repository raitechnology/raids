#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/decimal.h>
#define DECNUMDIGITS 34
#include <libdecnumber/bid/decimal64.h>
#include <libdecnumber/bid/decimal128.h>

#if 0
static int endian_mismatch = 2;
static int
test_gcc_libdecnumber_endian( void )
{
  _Decimal64 a = 0, b;
  uint64_t aa, bb;
  decNumber n;
  decContext ctx64;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decNumberFromUInt32( &n, 0 );
  decimal64FromNumber( (decimal64 *) &b, &n, &ctx64 );

  memcpy( &aa, &a, sizeof( aa ) );
  memcpy( &bb, &b, sizeof( bb ) );
  printf( "gcc: %lx, libdecnumber %lx\n", aa, bb );
  return a != b;
}

static void
gcc_decimal64( Decimal64Storage *fp )
{
  for (;;) {
    switch ( endian_mismatch ) {
      case 0: return;
      case 1: *fp = __builtin_bswap64( *fp ); return;
      case 2: endian_mismatch = test_gcc_libdecnumber_endian(); break;
    }
  }
}

static void
gcc_decimal128( Decimal128Storage *fp )
{
  uint64_t n0, n1;
  for (;;) {
    switch ( endian_mismatch ) {
      case 0: return;
      case 1: n0 = __builtin_bswap64( fp->n[ 0 ] );
              n1 = __builtin_bswap64( fp->n[ 1 ] );
              fp->n[ 0 ] = n1;
              fp->n[ 1 ] = n0;
              return;
      case 2: endian_mismatch = test_gcc_libdecnumber_endian(); break;
    }
  }
}
#endif

void
dec64_itod( Decimal64Storage *fp,  int i )
{
  *(_Decimal64 *) fp = i;
}

void
dec64_ftod( Decimal64Storage *fp,  double f )
{
  *(_Decimal64 *) fp = f;
}

void
dec64_from_string( Decimal64Storage *fp,  const char *str )
{
  decContext ctx64;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64FromString( (decimal64 *) fp, str, &ctx64 );
  /*gcc_decimal64( fp );*/
}

void
dec64_zero( Decimal64Storage *fp )
{
  *(_Decimal64 *) fp = 0;
}

size_t
dec64_to_string( const Decimal64Storage *fp,  char *str )
{
  Decimal64Storage fp2 = *fp;
  /*gcc_decimal64( &fp2 );*/
  /* this adds garbage at the end of NaN (as far as I can tell) */
  decimal64ToString( (const decimal64 *) &fp2, str );
  if ( str[ 0 ] == '-' ) {
    if ( str[ 1 ] == 'N' || str[ 1 ] == 'I' ) {
      str[ 4 ] = '\0'; /* -NaN, -Inf */
      return 4;
    }
  }
  else {
    if ( str[ 0 ] == 'N' || str[ 0 ] == 'I' ) {
      str[ 3 ] = '\0'; /* NaN, Inf */
      return 3;
    }
  }
  return strlen( str );
}

void
dec64_sum( Decimal64Storage *out,  const Decimal64Storage *l,
           const Decimal64Storage *r )
{
  *(_Decimal64 *) out = *(const _Decimal64 *) l +
                        *(const _Decimal64 *) r;
#if 0
  decContext ctx64;
  decNumber lhs, rhs, fp;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64ToNumber( (decimal64 *) l, &lhs );
  decimal64ToNumber( (decimal64 *) r, &rhs );
  decNumberAdd( &fp, &lhs, &rhs, &ctx64 );
  decimal64FromNumber( (decimal64 *) out, &fp, &ctx64 );
#endif
}

void
dec64_mul( Decimal64Storage *out,  const Decimal64Storage *l,
           const Decimal64Storage *r )
{
  *(_Decimal64 *) out = *(const _Decimal64 *) l *
                        *(const _Decimal64 *) r;
}

int
dec64_eq( const Decimal64Storage *l, const Decimal64Storage *r )
{
  return *(const _Decimal64 *) l == *(const _Decimal64 *) r;
}

int
dec64_lt( const Decimal64Storage *l, const Decimal64Storage *r )
{
  return *(const _Decimal64 *) l < *(const _Decimal64 *) r;
}

int
dec64_gt( const Decimal64Storage *l, const Decimal64Storage *r )
{
  return *(const _Decimal64 *) l > *(const _Decimal64 *) r;
}


void
dec128_itod( Decimal128Storage *fp,  int i )
{
  *(_Decimal128 *) fp = i;
}

void
dec128_ftod( Decimal128Storage *fp,  double f )
{
  *(_Decimal128 *) fp = f;
}

void
dec128_from_string( Decimal128Storage *fp,  const char *str )
{
  decContext ctx128;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128FromString( (decimal128 *) fp, str, &ctx128 );
  /*gcc_decimal128( fp );*/
}

void
dec128_zero( Decimal128Storage *fp )
{
  *(_Decimal128 *) fp = 0;
}

size_t
dec128_to_string( const Decimal128Storage *fp,  char *str )
{
  Decimal128Storage fp2 = *fp;
  /*gcc_decimal128( &fp2 );*/
  /* this adds garbage at the end of NaN (as far as I can tell) */
  decimal128ToString( (const decimal128 *) &fp2, str );
  if ( str[ 0 ] == '-' ) {
    if ( str[ 1 ] == 'N' || str[ 1 ] == 'I' ) {
      str[ 4 ] = '\0'; /* -NaN, -Inf */
      return 4;
    }
  }
  else {
    if ( str[ 0 ] == 'N' || str[ 0 ] == 'I' ) {
      str[ 3 ] = '\0'; /* NaN, Inf */
      return 3;
    }
  }
  return strlen( str );
}

void
dec128_sum( Decimal128Storage *out,  const Decimal128Storage *l,
            const Decimal128Storage *r )
{
  *(_Decimal128 *) out = *(const _Decimal128 *) l +
                         *(const _Decimal128 *) r;
}

void
dec128_mul( Decimal128Storage *out,  const Decimal128Storage *l,
            const Decimal128Storage *r )
{
  *(_Decimal128 *) out = *(const _Decimal128 *) l *
                         *(const _Decimal128 *) r;
}

int
dec128_eq( const Decimal128Storage *l, const Decimal128Storage *r )
{
  return *(const _Decimal128 *) l == *(const _Decimal128 *) r;
}

int
dec128_lt( const Decimal128Storage *l, const Decimal128Storage *r )
{
  return *(const _Decimal128 *) l < *(const _Decimal128 *) r;
}

int
dec128_gt( const Decimal128Storage *l, const Decimal128Storage *r )
{
  return *(const _Decimal128 *) l > *(const _Decimal128 *) r;
}

