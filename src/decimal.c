#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/decimal.h>
#define DECNUMDIGITS 34
#include <libdecnumber/bid/decimal64.h>
#include <libdecnumber/bid/decimal128.h>

#if 0
static void
print( const char *what,  Dec128Store *d )
{
  char buf[ 128 ];
  dec128_to_string( d, buf );
  printf( "%s : %s\n", what, buf );
}

int
main( int argc, char *argv[] )
{
  Dec128Store e, d, f, g, h;
  dec128_itod( &d, 101 );
  print( "d = int(101)", &d );
  dec128_from_string( &e, "101.101" );
  print( "e = str(101.101)", &e );
  printf( "e > d : %d\n", dec128_gt( &e, &d ) );
  printf( "e < d : %d\n", dec128_lt( &e, &d ) );
  printf( "e == d : %d\n", dec128_eq( &e, &d ) );
  dec128_sum( &f, &e, &d );
  print( "f = d + e", &f );
  dec128_itod( &g, 10 );
  print( "g = int(10)", &g );
  dec128_mul( &h, &g, &f );
  print( "h = g * f", &h );
  dec128_div( &h, &f, &g );
  print( "h = f / g", &h );
  dec128_mod( &h, &f, &g );
  print( "h = f % g", &h );
  dec128_itod( &g, 2 );
  print( "g = int(2)", &g );
  dec128_pow( &h, &f, &g );
  print( "h = f ^ g", &h );
  printf( "h > f : %d\n", dec128_gt( &h, &f ) );
  printf( "h < f : %d\n", dec128_lt( &h, &f ) );
  printf( "h == f : %d\n", dec128_eq( &h, &f ) );
  printf( "h == h : %d\n", dec128_eq( &h, &h ) );
  return 0;
}
#endif

void
dec64_itod( Dec64Store *fp,  int i )
{
  /**(_Decimal64 *) fp = i;*/
  decContext ctx64;
  decNumber n;
  decNumberFromInt32( &n, i );
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64FromNumber( (decimal64 *) fp, &n, &ctx64 );
}

void
dec64_ftod( Dec64Store *fp,  double f )
{
  *(_Decimal64 *) fp = f;
}

void
dec64_from_string( Dec64Store *fp,  const char *str )
{
  decContext ctx64;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64FromString( (decimal64 *) fp, str, &ctx64 );
}

void
dec64_zero( Dec64Store *fp )
{
  /**(_Decimal64 *) fp = 0;*/
  dec64_itod( fp, 0 );
}

size_t
dec64_to_string( const Dec64Store *fp,  char *str )
{
  Dec64Store fp2 = *fp;
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

static void
dec64_binop( Dec64Store *out,  const Dec64Store *l,
              const Dec64Store *r,  char op )
{
  decContext ctx64;
  decNumber lhs, rhs, fp;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64ToNumber( (decimal64 *) l, &lhs );
  decimal64ToNumber( (decimal64 *) r, &rhs );
  switch ( op ) {
    default:
    case '+': decNumberAdd( &fp, &lhs, &rhs, &ctx64 ); break;
    case '-': decNumberSubtract( &fp, &lhs, &rhs, &ctx64 ); break;
    case '*': decNumberMultiply( &fp, &lhs, &rhs, &ctx64 ); break;
    case '/': decNumberDivide( &fp, &lhs, &rhs, &ctx64 ); break;
    case '%': decNumberRemainder( &fp, &lhs, &rhs, &ctx64 ); break;
    case 'p': decNumberPower( &fp, &lhs, &rhs, &ctx64 ); break;
  }
  decimal64FromNumber( (decimal64 *) out, &fp, &ctx64 );
}

void
dec64_sum( Dec64Store *out,  const Dec64Store *l,
               const Dec64Store *r )
{
  dec64_binop( out, l, r, '+' );
}

void
dec64_diff( Dec64Store *out,  const Dec64Store *l,
                const Dec64Store *r )
{
  dec64_binop( out, l, r, '-' );
}

void
dec64_mul( Dec64Store *out,  const Dec64Store *l,
               const Dec64Store *r )
{
  dec64_binop( out, l, r, '*' );
}

void
dec64_div( Dec64Store *out,  const Dec64Store *l,
               const Dec64Store *r )
{
  dec64_binop( out, l, r, '/' );
}

void
dec64_mod( Dec64Store *out,  const Dec64Store *l,
               const Dec64Store *r )
{
  dec64_binop( out, l, r, '%' );
}

void
dec64_pow( Dec64Store *out,  const Dec64Store *l,
               const Dec64Store *r )
{
  dec64_binop( out, l, r, 'p' );
}

static void
dec64_unaryop( Dec64Store *out,  const Dec64Store *l,  char op )
{
  decContext ctx64;
  decNumber lhs, fp;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  if ( op == 'f' )
    decContextSetRounding( &ctx64, DEC_ROUND_DOWN );
  else if ( op == 'r' )
    decContextSetRounding( &ctx64, DEC_ROUND_HALF_UP );
  decimal64ToNumber( (decimal64 *) l, &lhs );
  switch ( op ) {
    default:
    case 'l': decNumberLn( &fp, &lhs, &ctx64 ); break; /* ln */
    case 'L': decNumberLog10( &fp, &lhs, &ctx64 ); break; /* log10 */
    case 'r': /* round */
    case 'f': decNumberToIntegralValue( &fp, &lhs, &ctx64 ); break; /* floor */
  }
  decimal64FromNumber( (decimal64 *) out, &fp, &ctx64 );
}

void
dec64_ln( Dec64Store *out,  const Dec64Store *l )
{
  dec64_unaryop( out, l, 'l' );
}

void
dec64_log10( Dec64Store *out,  const Dec64Store *l )
{
  dec64_unaryop( out, l, 'L' );
}

void
dec64_floor( Dec64Store *out,  const Dec64Store *l )
{
  dec64_unaryop( out, l, 'f' );
}

void
dec64_round( Dec64Store *out,  const Dec64Store *l )
{
  dec64_unaryop( out, l, 'r' );
}

int
dec64_isinf( const Dec64Store *fp )
{
  decContext ctx64;
  decNumber val;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64ToNumber( (decimal64 *) fp, &val );
  switch ( decNumberClass( &val, &ctx64 ) ) {
    case DEC_CLASS_NEG_INF:
    case DEC_CLASS_POS_INF: return 1;
    default: return 0;
  }
}

int
dec64_isnan( const Dec64Store *fp )
{
  decContext ctx64;
  decNumber val;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64ToNumber( (decimal64 *) fp, &val );
  switch ( decNumberClass( &val, &ctx64 ) ) {
    case DEC_CLASS_SNAN:
    case DEC_CLASS_QNAN: return 1;
    default: return 0;
  }
}

int
dec64_compare( const Dec64Store *l,  const Dec64Store *r )
{
  decContext ctx64;
  decNumber lhs, rhs, fp;
  decContextDefault( &ctx64, DEC_INIT_DECIMAL64 );
  decimal64ToNumber( (decimal64 *) l, &lhs );
  decimal64ToNumber( (decimal64 *) r, &rhs );
  decNumberSubtract( &fp, &lhs, &rhs, &ctx64 );
  switch ( decNumberClass( &fp, &ctx64 ) ) {
    case DEC_CLASS_NEG_INF:
    case DEC_CLASS_NEG_NORMAL:
    case DEC_CLASS_NEG_SUBNORMAL:
    case DEC_CLASS_NEG_ZERO: return -1;
    case DEC_CLASS_POS_NORMAL:
    case DEC_CLASS_POS_SUBNORMAL:
    case DEC_CLASS_POS_INF:  return 1;
    case DEC_CLASS_POS_ZERO: return 0;
    default:
    case DEC_CLASS_SNAN:
    case DEC_CLASS_QNAN:     return 2;
  }
}

int
dec64_eq( const Dec64Store *l, const Dec64Store *r )
{
  return dec64_compare( l, r ) == 0;
}

int
dec64_lt( const Dec64Store *l, const Dec64Store *r )
{
  return dec64_compare( l, r ) < 0;
}

int
dec64_gt( const Dec64Store *l, const Dec64Store *r )
{
  return dec64_compare( l, r ) > 0;
}

void
dec128_itod( Dec128Store *fp,  int i )
{
  /**(_Decimal128 *) fp = i;*/
  decContext ctx128;
  decNumber n;
  decNumberFromInt32( &n, i );
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128FromNumber( (decimal128 *) fp, &n, &ctx128 );
}

void
dec128_ftod( Dec128Store *fp,  double f )
{
  *(_Decimal128 *) fp = f;
}

void
dec128_from_string( Dec128Store *fp,  const char *str )
{
  decContext ctx128;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128FromString( (decimal128 *) fp, str, &ctx128 );
}

void
dec128_zero( Dec128Store *fp )
{
  /**(_Decimal128 *) fp = 0;*/
  dec128_itod( fp, 0 );
}

size_t
dec128_to_string( const Dec128Store *fp,  char *str )
{
  Dec128Store fp2 = *fp;
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

static void
dec128_binop( Dec128Store *out,  const Dec128Store *l,
              const Dec128Store *r,  char op )
{
  decContext ctx128;
  decNumber lhs, rhs, fp;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128ToNumber( (decimal128 *) l, &lhs );
  decimal128ToNumber( (decimal128 *) r, &rhs );
  switch ( op ) {
    default:
    case '+': decNumberAdd( &fp, &lhs, &rhs, &ctx128 ); break;
    case '-': decNumberSubtract( &fp, &lhs, &rhs, &ctx128 ); break;
    case '*': decNumberMultiply( &fp, &lhs, &rhs, &ctx128 ); break;
    case '/': decNumberDivide( &fp, &lhs, &rhs, &ctx128 ); break;
    case '%': decNumberRemainder( &fp, &lhs, &rhs, &ctx128 ); break;
    case 'p': decNumberPower( &fp, &lhs, &rhs, &ctx128 ); break;
  }
  decimal128FromNumber( (decimal128 *) out, &fp, &ctx128 );
}

void
dec128_sum( Dec128Store *out,  const Dec128Store *l,
               const Dec128Store *r )
{
  dec128_binop( out, l, r, '+' );
}

void
dec128_diff( Dec128Store *out,  const Dec128Store *l,
                const Dec128Store *r )
{
  dec128_binop( out, l, r, '-' );
}

void
dec128_mul( Dec128Store *out,  const Dec128Store *l,
               const Dec128Store *r )
{
  dec128_binop( out, l, r, '*' );
}

void
dec128_div( Dec128Store *out,  const Dec128Store *l,
               const Dec128Store *r )
{
  dec128_binop( out, l, r, '/' );
}

void
dec128_mod( Dec128Store *out,  const Dec128Store *l,
               const Dec128Store *r )
{
  dec128_binop( out, l, r, '%' );
}

void
dec128_pow( Dec128Store *out,  const Dec128Store *l,
               const Dec128Store *r )
{
  dec128_binop( out, l, r, 'p' );
}

static void
dec128_unaryop( Dec128Store *out,  const Dec128Store *l,  char op )
{
  decContext ctx128;
  decNumber lhs, fp;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  if ( op == 'f' )
    decContextSetRounding( &ctx128, DEC_ROUND_DOWN );
  else if ( op == 'r' )
    decContextSetRounding( &ctx128, DEC_ROUND_HALF_UP );
  decimal128ToNumber( (decimal128 *) l, &lhs );
  switch ( op ) {
    default:
    case 'l': decNumberLn( &fp, &lhs, &ctx128 ); break; /* ln */
    case 'L': decNumberLog10( &fp, &lhs, &ctx128 ); break; /* log10 */
    case 'r': /* round */
    case 'f': decNumberToIntegralValue( &fp, &lhs, &ctx128 ); break; /* floor */
  }
  decimal128FromNumber( (decimal128 *) out, &fp, &ctx128 );
}

void
dec128_ln( Dec128Store *out,  const Dec128Store *l )
{
  dec128_unaryop( out, l, 'l' );
}

void
dec128_log10( Dec128Store *out,  const Dec128Store *l )
{
  dec128_unaryop( out, l, 'L' );
}

void
dec128_floor( Dec128Store *out,  const Dec128Store *l )
{
  dec128_unaryop( out, l, 'f' );
}

void
dec128_round( Dec128Store *out,  const Dec128Store *l )
{
  dec128_unaryop( out, l, 'r' );
}

int
dec128_isinf( const Dec128Store *fp )
{
  decContext ctx128;
  decNumber val;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128ToNumber( (decimal128 *) fp, &val );
  switch ( decNumberClass( &val, &ctx128 ) ) {
    case DEC_CLASS_NEG_INF:
    case DEC_CLASS_POS_INF: return 1;
    default: return 0;
  }
}

int
dec128_isnan( const Dec128Store *fp )
{
  decContext ctx128;
  decNumber val;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128ToNumber( (decimal128 *) fp, &val );
  switch ( decNumberClass( &val, &ctx128 ) ) {
    case DEC_CLASS_SNAN:
    case DEC_CLASS_QNAN: return 1;
    default: return 0;
  }
}

int
dec128_compare( const Dec128Store *l,  const Dec128Store *r )
{
  decContext ctx128;
  decNumber lhs, rhs, fp;
  decContextDefault( &ctx128, DEC_INIT_DECIMAL128 );
  decimal128ToNumber( (decimal128 *) l, &lhs );
  decimal128ToNumber( (decimal128 *) r, &rhs );
  decNumberSubtract( &fp, &lhs, &rhs, &ctx128 );
  switch ( decNumberClass( &fp, &ctx128 ) ) {
    case DEC_CLASS_NEG_INF:
    case DEC_CLASS_NEG_NORMAL:
    case DEC_CLASS_NEG_SUBNORMAL:
    case DEC_CLASS_NEG_ZERO: return -1;
    case DEC_CLASS_POS_NORMAL:
    case DEC_CLASS_POS_SUBNORMAL:
    case DEC_CLASS_POS_INF:  return 1;
    case DEC_CLASS_POS_ZERO: return 0;
    default:
    case DEC_CLASS_SNAN:
    case DEC_CLASS_QNAN:     return 2;
  }
}

int
dec128_eq( const Dec128Store *l, const Dec128Store *r )
{
  return dec128_compare( l, r ) == 0;
}

int
dec128_lt( const Dec128Store *l, const Dec128Store *r )
{
  return dec128_compare( l, r ) < 0;
}

int
dec128_gt( const Dec128Store *l, const Dec128Store *r )
{
  return dec128_compare( l, r ) > 0;
}

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

#endif
