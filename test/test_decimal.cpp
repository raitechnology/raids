#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raimd/decimal.h>

using namespace rai;
using namespace md;

int
main( int , char ** )
{
  char buf[ 64 ], buf2[ 64 ];
  Decimal64  fp, fp2, fp3;
  Decimal128 gp, gp2, gp3;
  
  fp = Decimal64::ftod( 10.0 );
  fp.to_string( buf );
  fp2.from_string( "10.01", 5 );
  fp2.to_string( buf2 );
  printf( "fp = %s fp2 = %s\n", buf, buf2 );
  fp3 = fp + fp2;
  fp3.to_string( buf );
  printf( "sum = %s\n", buf );
  fp3 = fp * fp2;
  fp3.to_string( buf );
  printf( "mul = %s\n", buf );

  gp = Decimal128::ftod( 10.0 );
  gp.to_string( buf );
  gp2.from_string( "10.01", 5 );
  gp2.to_string( buf2 );
  printf( "gp = %s gp2 = %s\n", buf, buf2 );
  gp3 = gp + gp2;
  gp3.to_string( buf );
  printf( "sum = %s\n", buf );
  gp3 = gp * gp2;
  gp3.to_string( buf );
  printf( "mul = %s\n", buf );

  gp.zero();
  gp.to_string( buf );
  printf( "zero = %s\n", buf );
  gp += Decimal128::parse_len( "0\n", 1 );
  ::memset( buf, 0, sizeof( buf ) );
  gp.to_string( buf );
  printf( "addzero = %s\n", buf );

  return 0;
}

