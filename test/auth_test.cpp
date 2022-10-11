#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <raids/http_auth.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;
using namespace kv;

static void
test_wiki( void ) noexcept
{
  HtDigestDB      db;
  HttpServerNonce svr;
  HttpDigestAuth  auth( NULL, &db );
  size_t len;

  db.set_realm( "testrealm@host.com" );
  db.add_user_pass( "Mufasa", "Circle Of Life", NULL );
  ::strcpy( svr.nonce, "dcd98b7102dd2f0e8b11d0f600bfb0c093" );
  ::strcpy( svr.opaque, "5ccc069c403ebaf9f0171e9517f40e41" );
  /*svr.regenerate();*/
  len = auth.gen_server( svr, false );
  printf( "server: %s\n", auth.out_buf );

  char cnonce[ 64 ];
  /*uint64_t x = svr.rand.next();
  i = bin_to_base64( &x, sizeof( x ), cnonce, false );
  cnonce[ i ] = '\0';*/
  ::strcpy( cnonce, "0a4f113b" );
  HttpDigestAuth auth2( svr.nonce, &db );
  auth2.parse_auth( auth.out_buf, len, false );
  len = auth2.gen_client( "Mufasa", "Circle Of Life", 1, cnonce,
                          "/dir/index.html", "GET", 3 );
  printf( "client: %s\n", auth2.out_buf );

  HttpDigestAuth auth3( svr.nonce, &db );
  if ( ! auth3.parse_auth( auth2.out_buf, len, true ) ) {
    printf( "check fields failed: %s\n", auth3.error() );
  }
  else if ( ! auth3.check_auth( "GET", 3, true ) ) {
    printf( "check auth failed: %s\n", auth3.error() );
  }
  else {
    printf( "check passed\n\n" );
  }
}

static void
test_auto( void ) noexcept
{
  HtDigestDB      db;
  HttpServerNonce svr;
  HttpDigestAuth  auth( NULL, &db );
  char hostname[ 256 ];
  size_t len;

  ::gethostname( hostname, sizeof( hostname ) );
  db.set_realm( "raids", hostname );
  db.add_user_pass( "danger", "highvoltage", NULL );
  svr.regenerate();
  len = auth.gen_server( svr, false );
  printf( "server: %s\n", auth.out_buf );

  char cnonce[ 64 ];
  uint64_t x = svr.rand.next();
  size_t i = bin_to_base64( &x, sizeof( x ), cnonce, false );
  cnonce[ i ] = '\0';
  HttpDigestAuth auth2( svr.nonce, &db );
  auth2.parse_auth( auth.out_buf, len, false );
  len = auth2.gen_client( "danger", "highvoltage", 1, cnonce,
                          "/dir/index.html", "GET", 3 );
  printf( "client: %s\n", auth2.out_buf );

  HttpDigestAuth auth3( svr.nonce, &db );
  if ( ! auth3.parse_auth( auth2.out_buf, len, true ) ) {
    printf( "check fields failed: %s\n", auth3.error() );
  }
  else if ( ! auth3.check_auth( "GET", 3, true ) ) {
    printf( "check auth failed: %s\n", auth3.error() );
  }
  else {
    printf( "check passed\n\n" );
  }
}

int
main( int, char ** )
{
  test_wiki();
  test_auto();
  return 0;
}
