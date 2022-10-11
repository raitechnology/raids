/* Http header that has MD5 digest authorization:
    (see https://en.wikipedia.org/wiki/Digest_access_authentication)

   Authorization: Digest username="admin", realm="hpz@RAICACHE", \
                  nonce="86030bad0b620ad19f6992693f85f9c4f7", uri="/", \
                  algorithm=MD5, response="57e05cba5127e2b8ebd393c4442bd3be", \
                  qop=auth, nc=00000012, cnonce="3ece0ec152e94b80"

   Check is performed by calculating;

   HA1=MD5(username:realm:password)  ; password is a shared secret
   HA2=MD5(method:uri)               ; method = GET, etc
   response=MD5(HA1:nonce:nc:cnonce:qop:HA2)

   And response is compared to verify.

   The pair "nc:cnonce" must be different for each request to prevent
   replay attacks, clients should increment nc for each request.

   HA1 can be created by the 'htdigest' program.  Format of .htdigest:

   user1:Realm:5ea41921c65387d904834f8403185412
   user2:Realm:734418f1e487083dc153890208b79379

   where HA1 is the md5 of user:realm:password
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/http_auth.h>
#include <raikv/os_file.h>
#include <raikv/key_hash.h>

static void MD5( const void *data,  size_t len,  uint8_t digest[16] ) noexcept;

using namespace rai;
using namespace ds;
using namespace kv;

const char *
HttpDigestAuth::error( void ) const noexcept
{
  switch ( this->errcode ) {
    case HT_AUTH_NOAUTH:    return "Not authenticated";
    case HT_AUTH_OK:        return "Ok";
    case HT_AUTH_USERNAME:  return "Missing username";
    case HT_AUTH_REALM:     return "Missing realm";
    case HT_AUTH_URI:       return "Missing URI";
    case HT_AUTH_RESPONSE:  return "Missing response";
    case HT_AUTH_COUNT:     return "Missing client serial increment";
    case HT_AUTH_CNONCE:    return "Missing client nonce";
    case HT_AUTH_STALE:     return "Nonce is stale";
    case HT_AUTH_NOUSER:    return "Unknown user";
    case HT_AUTH_NOREALM:   return "Unknown realm";
    case HT_AUTH_PASSFAIL:  return "Password failed";
    default:                return "Other error";
  }
}

bool
HttpDigestAuth::check_fields( void ) noexcept
{
  if ( this->username == NULL ) {
    this->errcode = HT_AUTH_USERNAME;
    return false;
  }
  if ( this->realm == NULL ) {
    this->errcode = HT_AUTH_REALM;
    return false;
  }
  if ( this->uri == NULL ) {
    this->errcode = HT_AUTH_URI;
    return false;
  }
  if ( this->response == NULL ) {
    this->errcode = HT_AUTH_RESPONSE;
    return false;
  }
  if ( this->nc == NULL ) {
    this->errcode = HT_AUTH_COUNT;
    return false;
  }
  if ( this->cnonce == NULL ) {
    this->errcode = HT_AUTH_CNONCE;
    return false;
  }
  if ( this->nonce != NULL &&
       ::strcmp( this->nonce, this->mynonce ) != 0 ) {
    this->errcode = HT_AUTH_STALE;
    return false;
  }
  /* set the current increment to check for replay */
  for ( size_t i = 0; this->nc[ i ] != '\0'; i++ ) {
    uint8_t b = 0;
    if ( this->nc[ i ] >= '0' && this->nc[ i ] <= '9' )
      b = this->nc[ i ] - '0';
    else if ( this->nc[ i ] >= 'a' && this->nc[ i ] <= 'f' )
      b = this->nc[ i ] - 'a' + 10;
    else if ( this->nc[ i ] >= 'A' && this->nc[ i ] <= 'F' )
      b = this->nc[ i ] - 'A' + 10;
    this->client_incr = ( this->client_incr << 4 ) | b;
  }
  this->errcode = HT_AUTH_OK;
  return true;
}

size_t
HttpDigestAuth::match( size_t i,  const char *val,  size_t len,
                       char **ptr,  size_t *ptr_len ) noexcept
{
  size_t j, k;
  bool saw_equals = false;
  char match_char = 0;

  if ( i + len >= this->buf_len )
    return i;
  for ( j = 1; j < len; j++ ) {
    if ( this->buf[ i + j ] != val[ j ] &&
         this->buf[ i + j ] != ( val[ j ] - ( 'a' - 'A' ) ) )
      return i;
  }
  if ( ptr == NULL ) /* no value matched */
    return i + j;

  /* find ="blah", */
  for ( i += j; i < this->buf_len; i++ ) {
    switch ( this->buf[ i ] ) {
      case ' ': case '\n': case '\r': break;
      case '=': saw_equals = true; break;
      case '\"':
        i++;
        if ( saw_equals ) {
          match_char = '\"';
          goto break_loop;
        }
        return i;
      default:
        if ( saw_equals ) {
          match_char = ',';
          goto break_loop;
        }
        return i;
    }
  }
break_loop:;
  /* find '\"' or ',' */
  for ( k = i; k < this->buf_len; k++ ) {
    if ( this->buf[ k ] == match_char ) {
      this->buf[ k ] = '\0';
      goto matched;
    }
  }
  /* at end of buffer */
  if ( match_char == ',' ) {
matched:;
    if ( ptr != NULL )
      *ptr = &this->buf[ i ];
    if ( ptr_len != NULL )
      *ptr_len = k - i;
    return k;
  }
  return i;
}

bool
HttpDigestAuth::parse_auth( const void *p,  size_t len,
                            bool check ) noexcept
{
  static const char algorithm_val[] = "algorithm",/* 1 */
                    cnonce_val[]    = "cnonce",   /* 2 */
                    digest_val[]    = "digest",   /* 3 */
                    nonce_val[]     = "nonce",    /* 4 */
                    nc_val[]        = "nc",       /* 5 */
                    opaque_val[]    = "opaque",   /* 6 */
                    qop_val[]       = "qop",      /* 7 */
                    realm_val[]     = "realm",    /* 8 */
                    response_val[]  = "response", /* 9 */
                    uri_val[]       = "uri",      /* 10 */
                    username_val[]  = "username"; /* 11 */

  size_t i, j;

  this->clear();
  if ( len + 1 >= sizeof( this->tmp_buf ) ) {
    this->buf = (char *) ::malloc( len + 1 );
    this->out_buf = this->tmp_buf;
    this->max_out = sizeof( this->tmp_buf );
  }
  else {
    this->buf = this->tmp_buf;
    this->out_buf = &this->buf[ len + 1 ];
    this->max_out = sizeof( this->tmp_buf ) - ( len + 1 );
  }
  ::memcpy( this->buf, p, len );
  this->buf[ len ] = '\0';
  this->buf_len = len;

  for ( i = 0; i < len; i = j + 1 ) {
    j = i;
    #define S( s ) s, sizeof( s ) - 1
    switch ( this->buf[ i ] ) {
      case 'a': case 'A': /* algorithm */
        j = this->match( i, S( algorithm_val ), &this->algorithm );
        break;

      case 'c': case 'C': /* cnonce */
        j = this->match( i, S( cnonce_val ), &this->cnonce, &this->cnonce_len );
        break;

      case 'd': case 'D': /* digest */
        j = this->match( i, S( digest_val ) );
        break;

      case 'n': case 'N': /* nonce, nc */
        j = this->match( i, S( nonce_val ), &this->nonce, &this->nonce_len );
        if ( i == j )
          j = this->match( i, S( nc_val ), &this->nc, &this->nc_len );
        break;
      case 'o': case 'O': /* opaque */
        j = this->match( i, S( opaque_val ), &this->opaque, &this->opaque_len );
        break;
      case 'q': case 'Q': /* qop */
        j = this->match( i, S( qop_val ), &this->qop );
        break;
      case 'r': case 'R': /* realm, response */
        if ( this->realm == NULL )
          j = this->match( i, S( realm_val ), &this->realm, &this->realm_len );
        if ( i == j )
          j = this->match( i, S( response_val ), &this->response );
        break;
      case 'u': case 'U': /* uri, username */
        if ( this->username == NULL )
          j = this->match( i, S( username_val ), &this->username,
                           &this->username_len );
        if ( i == j )
          j = this->match( i, S( uri_val ), &this->uri, &this->uri_len );
        break;

      default: break;
    }
    #undef S
  }
  if ( ! check )
    this->errcode = HT_AUTH_OK;
  else
    this->check_fields();
  return this->errcode == HT_AUTH_OK;
}

void
HttpDigestAuth::make_out_buf( size_t len ) noexcept
{
  len += 1024;
  if ( this->out_buf == NULL || this->out_buf < this->tmp_buf ||
       this->out_buf >= &this->tmp_buf[ sizeof( this->tmp_buf ) ] ) {
    this->out_buf = (char *) ::realloc( this->out_buf, len );
  }
  else {
    char * old = this->out_buf;
    this->out_buf = (char *) ::malloc( len );
    ::memcpy( this->out_buf, old, this->max_out );
  }
  this->max_out = len;
}

static char hexchar[] = { '0', '1', '2', '3', '4', '5', '6', '7',
                          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
size_t
HttpDigestAuth::cpy( size_t i,  const char *s,  char c,  size_t len ) noexcept
{
  if ( i + len + 16 >= this->max_out )
    this->make_out_buf( i + len );
  ::memcpy( &this->out_buf[ i ], s, len );
  i += len;
  if ( c != 0 )
    this->out_buf[ i++ ] = c;
  this->out_buf[ i ] = '\0';
  return i;
}

static const size_t md5sz = 128 / 8;
size_t
HttpDigestAuth::cpy_MD5( size_t i,  const uint8_t *bits,  char c ) noexcept
{
  if ( i + md5sz * 2 + 16 >= this->max_out )
    this->make_out_buf( i + md5sz * 2 );
  for ( uint32_t k = 0; k < md5sz; k++ ) {
    this->out_buf[ i++ ] = hexchar[ ( bits[ k ] >> 4 ) & 0xfU ];
    this->out_buf[ i++ ] = hexchar[ bits[ k ] & 0xfU ];
  }
  if ( c != 0 )
    this->out_buf[ i++ ] = c;
  this->out_buf[ i ] = '\0';
  return i;
}

char *
HttpDigestAuth::compute_HA1( const char *username,  const char *realm,
                             const char *passwd ) noexcept
{
  uint8_t bits[ 128 / 8 ];
  size_t  i;

  if ( username == NULL )
    username = "no-user";
  if ( realm == NULL )
    realm = "no-realm";
  if ( passwd == NULL )
    passwd = "no-passwd";

  i = this->cpy( 0, username,':' );
  i = this->cpy( i, realm, ':' );
  i = this->cpy( i, passwd );
  MD5( this->out_buf, i, bits );
  i = this->cpy_MD5( 0, bits );
  this->out_buf[ i ] = '\0';

  return this->out_buf;
}

bool
HttpDigestAuth::check_user( void ) noexcept
{
  HtUserHA1 *ent = NULL;
  if ( this->dig_db != NULL ) {
    size_t len = ::strlen( this->username );
    uint32_t h = kv_crc_c( this->username, len, 0 );
    ent = this->dig_db->find( h, this->username, len );
  }
  if ( ent == NULL ) {
    this->errcode = HT_AUTH_NOUSER;
  }
  else if ( this->realm == NULL || this->dig_db->realm == NULL ||
       ::strcmp( this->realm, this->dig_db->realm ) != 0 ) {
    this->errcode = HT_AUTH_NOREALM;
  }
  else {
    this->errcode = HT_AUTH_OK;
  }
  return this->errcode == HT_AUTH_OK;
}

size_t
HttpDigestAuth::gen_client( const char *user,  const char *passwd,  
                            uint32_t cnt,  const char *cnonce,
                            const char *uri,  const char *method,
                            size_t method_len ) noexcept
{
  uint8_t bits[ 128 / 8 ];
  char count_buf[ 16 ],
       response[ 40 ],
     * nc = count_buf;
  size_t i = 0, k, nc_len = sizeof( count_buf ) - 1;

  if ( this->realm != NULL && this->nonce != NULL ) {
    nc[ nc_len ] = '\0';
    while ( cnt >= 10 ) {
      nc[ --nc_len ] = ( cnt % 10 ) + '0';
      cnt /= 10;
    }
    nc[ --nc_len ] = cnt + '0';
    while ( nc_len + 8 >= sizeof( count_buf ) )
      nc[ --nc_len ] = '0';
    nc     = &nc[ nc_len ];
    nc_len = ( sizeof( count_buf ) - nc_len ) - 1;

    /* HA1 */
    i = this->cpy( 0, user, ':' );
    i = this->cpy( i, this->realm, ':', this->realm_len );
    i = this->cpy( i, passwd );
    MD5( this->out_buf, i, bits );
    i = this->cpy_MD5( 0, bits, ':' );

    /* HA1:nonce:nc:cnonce:auth:HA2 */
    i = this->cpy( i, this->nonce, ':', this->nonce_len );
    i = this->cpy( i, nc, ':', nc_len );
    i = this->cpy( i, cnonce,':' );
    i = this->cpy( i, "auth:", 0, 5 );

    k = this->cpy( i, method, ':', method_len );
    k = this->cpy( k, uri );
    MD5( &this->out_buf[ i ], k - i, bits ); 
    i = this->cpy_MD5( i, bits );

    MD5( this->out_buf, i, bits ); 
    size_t response_len = 0;
    for ( size_t k = 0; k < md5sz; k++ ) {
      response[ response_len++ ] = hexchar[ ( bits[ k ] >> 4 ) & 0xfU ];
      response[ response_len++ ] = hexchar[ bits[ k ] & 0xfU ];
    }
    response[ response_len ] = '\0';

    /* the Authorization: field */
    #define S( s ) s, 0, sizeof( s ) - 1
    i = this->cpy( 0, S( "Authorization: Digest username=\"" ) );
    i = this->cpy( i, user );
    i = this->cpy( i, S( "\", realm=\"" ) );
    i = this->cpy( i, this->realm, 0, this->realm_len );
    i = this->cpy( i, S( "\", nonce=\"" ) );
    i = this->cpy( i, this->nonce, 0, this->nonce_len );
    i = this->cpy( i, S( "\", uri=\"" ) );
    i = this->cpy( i, uri );
    i = this->cpy( i, S( "\", algorithm=MD5, response=\"" ) );
    i = this->cpy( i, response, 0, response_len );
    i = this->cpy( i, S( "\", qop=auth, nc=" ) );
    i = this->cpy( i, nc, 0, nc_len );
    i = this->cpy( i, S( ", cnonce=\"" ) );
    i = this->cpy( i, cnonce );
    if ( this->opaque_len > 0 ) {
      i = this->cpy( i, S( "\", opaque=\"" ) );
      i = this->cpy( i, this->opaque, 0, this->opaque_len );
    }
    i = this->cpy( i, S( "\"\r\n" ) );
  }
  this->out_buf[ i ] = '\0';
  return i;
}

size_t
HttpDigestAuth::gen_server( const HttpServerNonce &svr,  bool stale ) noexcept
{
  size_t i = 0;

  if ( this->dig_db != NULL ) {
    i = this->cpy( 0, S( "WWW-Authenticate: Digest realm=\"" ) );
    i = this->cpy( i, this->dig_db->realm );
    i = this->cpy( i, S( "\", qop=auth, stale=" ) );
    if ( stale )
      i = this->cpy( i, S( "true" ) );
    else
      i = this->cpy( i, S( "false" ) );
    i = this->cpy( i, S( ", nonce=\"" ) );
    i = this->cpy( i, svr.nonce );
    if ( svr.opaque[ 0 ] != '\0' ) {
      i = this->cpy( i, S( "\", opaque=\"" ) );
      i = this->cpy( i, svr.opaque );
    }
    i = this->cpy( i, S( "\"\r\n" ) );
    #undef S
  }
  this->out_buf[ i ] = '\0';
  return i;
}

void
HttpDigestAuth::log_auth( void ) noexcept
{
  if ( this->username != NULL )
    printf( "x username=\"%s\"\n", this->username );
  if ( this->realm != NULL )
    printf( "x realm=\"%s\"\n", this->realm );
  if ( this->nonce != NULL && this->cnonce != NULL )
    printf( "x nonce=\"%s\" mynonce=\"%s\"\n", this->nonce, this->mynonce );
  if ( this->uri != NULL )
    printf( "x uri=\"%s\"\n", this->uri );
  if ( this->response != NULL )
    printf( "x response=\"%s\"\n", this->response );
  if ( this->qop != NULL )
    printf( "x qop=\"%s\"\n", this->qop );
  if ( this->nc != NULL )
    printf( "x nc=\"%s\"\n", this->nc );
  if ( this->cnonce != NULL )
    printf( "x cnonce=\"%s\"\n", this->cnonce );
  if ( this->opaque != NULL )
    printf( "x opaque=\"%s\"\n", this->opaque );
  if ( this->algorithm != NULL )
    printf( "x algorithm=\"%s\"\n", this->algorithm );
}

bool
HttpDigestAuth::check_auth( const char *method,  size_t method_len,
                            bool trace ) noexcept
{
  uint8_t bits[ 128 / 8 ];
  size_t i, k;

  if ( trace ) {
    this->log_auth();
  }

  HtUserHA1 *ent = NULL;
  if ( this->dig_db != NULL ) {
    size_t len = ::strlen( this->username );
    uint32_t h = kv_crc_c( this->username, len, 0 );
    ent = this->dig_db->find( h, this->username, len );
  }
  if ( ent == NULL ) {
    this->errcode = HT_AUTH_NOUSER;
    goto failed;
  }
  /* HA1:nonce:nc:cnonce:auth:HA2 */
  i = this->cpy( 0, ent->ha1,':' );
  i = this->cpy( i, this->mynonce,':' );
  i = this->cpy( i, this->nc, ':', this->nc_len );
  i = this->cpy( i, this->cnonce, ':', this->cnonce_len );
  i = this->cpy( i, "auth:", 0, 5 );

  /* MD5(GET:uri) = HA2 */
  k = this->cpy( i, method, ':', method_len );
  k = this->cpy( k, this->uri, 0, this->uri_len );
  MD5( &this->out_buf[ i ], k - i, bits ); 
  i = this->cpy_MD5( i, bits );

  /* response */
  MD5( this->out_buf, i, bits ); 
  i = this->cpy_MD5( 0, bits );
  this->out_buf[ i ] = '\0';

  if ( ::strcmp( this->out_buf, this->response ) == 0 ) {
    if ( trace )
      printf( "x success \"%s\"\n", this->response );
    this->errcode = HT_AUTH_OK;
    return true;
  }
  /* MD5(MD5(username:realm:password):nonce:cnonce) = HA1 */
  i = this->cpy( 0, ent->ha1, ':' );
  i = this->cpy( i, this->mynonce,':' );
  i = this->cpy( i, this->cnonce, 0, this->cnonce_len );
  MD5( this->out_buf, i, bits );

  /* HA1:nonce:nc:cnonce:auth:HA2 */
  i = this->cpy_MD5( 0, bits, ':' );
  i = this->cpy( i, this->mynonce,':' );
  i = this->cpy( i, this->nc, ':', this->nc_len );
  i = this->cpy( i, this->cnonce, ':', this->cnonce_len );
  i = this->cpy( i, "auth:", 0, 5 );

  /* MD5(GET:uri) = HA2 */
  k = this->cpy( i, method, ':', method_len );
  k = this->cpy( k, this->uri, 0, this->uri_len );
  MD5( &this->out_buf[ i ], k - i, bits ); 
  i = this->cpy_MD5( i, bits );
  
  /* response */
  MD5( this->out_buf, i, bits ); 
  i = this->cpy_MD5( 0, bits );
  this->out_buf[ i ] = '\0';

  if ( ::strcmp( this->out_buf, this->response ) == 0 ) {
    if ( trace )
      printf( "x auth-int success \"%s\"\n", this->response );
    this->errcode = HT_AUTH_OK;
    return true;
  }
  this->errcode = HT_AUTH_PASSFAIL;
failed:;
  if ( trace )
    printf( "x failed: %s\n", this->error() );
  return false;
}

void
HttpServerNonce::regenerate( bool do_opaque ) noexcept
{
  uint64_t buf[ NONCE_BYTES / 8 ];
  size_t i;

  ::memset( this->replay, 0, sizeof( this->replay ) );
  for ( i = 0; i < NONCE_BYTES / 8; i++ )
    buf[ i ] = this->rand.next();
  i = bin_to_base64( buf, sizeof( buf ), this->nonce, false );
  this->nonce[ i ] = '\0';
  if ( do_opaque ) {
    buf[ 0 ] = this->rand.next();
    i = bin_to_base64( buf, sizeof( buf[ 0 ] ), this->opaque, false );
    this->opaque[ i ] = '\0';
  }
}

bool
HttpServerNonce::check_replay( HttpDigestAuth &auth ) noexcept
{
  uint32_t h = ( kv_crc_c( auth.username, ::strlen( auth.username ), 0 ) ^
                     kv_crc_c( auth.cnonce, ::strlen( auth.cnonce ), 0 ) ) %
                   ( sizeof( this->replay ) / sizeof( this->replay[ 0 ] ) );
  uint32_t incr = auth.client_incr;
  if ( this->replay[ h ]++ > incr )
    return true;
  return false;
}
/* return true if loaded least one entry, throw error f can't parse */
bool
HtDigestDB::update_if_modified( const char *fn, const char *realm ) noexcept
{
  os_stat statbuf;
  bool changed = false;

  if ( os_fstat( fn, &statbuf ) == 0 ) {
    if ( this->last_modified != (uint64_t) statbuf.st_mtime ) {
      changed = this->load( fn, realm );
      this->last_modified = (uint64_t) statbuf.st_mtime;
    }
  }
  return changed;
}
/* return true if loaded least one entry */
bool
HtDigestDB::load( const char *fn,  const char *realm ) noexcept
{
  FILE * fp = fopen( fn, "rb" );
  if ( fp == NULL )
    return false;

  char buf[ 1024 ];
  uint32_t n;
  char *re, *ha1;
  uint32_t count = 0;

  if ( realm != NULL )
    this->set_realm( realm );
  if ( this->realm != NULL )
    realm = this->realm;

  while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
    n = ::strlen( buf );
    while ( n > 0 && buf[ n-1 ] <= ' ' )
      buf[ --n ] = '\0';
    if ( (re = ::strchr( buf, ':' )) != NULL ) {
      *re++ = '\0';
      if ( (ha1 = ::strchr( re, ':' )) != NULL ) {
        *ha1++ = '\0';
        if ( realm == NULL ) {
          this->set_realm( re );
          realm = this->realm;
        }
        if ( ::strcmp( realm, re ) == 0 ) {
          if ( this->add_user_HA1( buf, ha1 ) )
            count++;
        }
      }
    }
  }
  fclose( fp );
  return count != 0;
}

bool
HtDigestDB::set_realm( const char *realm,  const char *host ) noexcept
{
  size_t len = ::strlen( realm ), hostlen = 0;
  if ( host != NULL ) {
    hostlen = ::strlen( host ) + 1;
  }
  this->realm = (char *) ::realloc( this->realm, len + hostlen + 1 );
  if ( this->realm == NULL )
    return false;
  ::memcpy( this->realm, realm, len );
  if ( host != NULL ) {
    this->realm[ len++ ] = '@';
    ::memcpy( &this->realm[ len ], host, hostlen - 1 );
  }
  this->realm[ len + hostlen ] = '\0';
  return true;
}

/* add user to db */
bool
HtDigestDB::add_user_pass( const char *username,  const char *passwd,
                           const char *realm ) noexcept
{
  HttpDigestAuth auth;

  if ( realm == NULL && this->realm[ 0 ] == '\0' )
    return false;
  if ( realm != NULL )
    this->set_realm( realm );
  realm = this->realm;
  return this->add_user_HA1( username,
                             auth.compute_HA1( username, realm, passwd ) );
}

bool
HtDigestDB::add_user_HA1( const char *username,  const char *ha1 ) noexcept
{
  RouteLoc    loc;
  size_t      len = ::strlen( username );
  uint32_t    h   = kv_crc_c( username, len, 0 );
  HtUserHA1 * u   = this->upsert( h, username, len, loc );

  if ( u == NULL )
    return false;
  if ( loc.is_new )
    u->ha1 = NULL;
  len = ::strlen( ha1 );
  u->ha1 = (char *) ::realloc( u->ha1, len + 1 );
  ::memcpy( u->ha1, ha1, len );
  u->ha1[ len ] = '\0';
  return true;
}

HtDigestDB::~HtDigestDB() noexcept
{
  if ( this->realm != NULL )
    ::free( this->realm );
  RouteLoc loc;
  for ( HtUserHA1 *u = this->first( loc ); u != NULL; u = this->next( loc ) ) {
    if ( u->ha1 != NULL )
      ::free( u->ha1 );
  }
}


/* Data structure for MD5 (Message Digest) computation */
typedef struct {
  uint32_t i[2];
  uint32_t buf[4];
  uint8_t in[64];
} MD5_CTX;

static void
MD5Init( MD5_CTX *mdContext ) noexcept
{
  mdContext->i[0] = mdContext->i[1] = (uint32_t)0;

  /* Load magic initialization constants.
   */
  mdContext->buf[0] = (uint32_t)0x67452301;
  mdContext->buf[1] = (uint32_t)0xefcdab89;
  mdContext->buf[2] = (uint32_t)0x98badcfe;
  mdContext->buf[3] = (uint32_t)0x10325476;
}

static void
Transform ( uint32_t *buf, const uint32_t *x ) noexcept
{
/* F, G and H are basic MD5 functions: selection, majority, parity */
#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

/* ROTATE_LEFT rotates x left n bits */
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4 */
/* Rotation is separate from addition to prevent recomputation */
#define FF(a, b, c, d, x, s, ac) \
  {(a) += F ((b), (c), (d)) + (x) + (uint32_t)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define GG(a, b, c, d, x, s, ac) \
  {(a) += G ((b), (c), (d)) + (x) + (uint32_t)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define HH(a, b, c, d, x, s, ac) \
  {(a) += H ((b), (c), (d)) + (x) + (uint32_t)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define II(a, b, c, d, x, s, ac) \
  {(a) += I ((b), (c), (d)) + (x) + (uint32_t)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }

  uint32_t a = buf[0], b = buf[1], c = buf[2], d = buf[3];
  static const uint32_t S11 = 7, S12 = 12, S13 = 17, S14 = 22,
                        S21 = 5, S22 = 9,  S23 = 14, S24 = 20,
                        S31 = 4, S32 = 11, S33 = 16, S34 = 23,
                        S41 = 6, S42 = 10, S43 = 15, S44 = 21;
  /* Round 1 */
  FF ( a, b, c, d, x[ 0], S11, 0xd76aa478U ); /* 1 */
  FF ( d, a, b, c, x[ 1], S12, 0xe8c7b756U ); /* 2 */
  FF ( c, d, a, b, x[ 2], S13, 0x242070dbU ); /* 3 */
  FF ( b, c, d, a, x[ 3], S14, 0xc1bdceeeU ); /* 4 */
  FF ( a, b, c, d, x[ 4], S11, 0xf57c0fafU ); /* 5 */
  FF ( d, a, b, c, x[ 5], S12, 0x4787c62aU ); /* 6 */
  FF ( c, d, a, b, x[ 6], S13, 0xa8304613U ); /* 7 */
  FF ( b, c, d, a, x[ 7], S14, 0xfd469501U ); /* 8 */
  FF ( a, b, c, d, x[ 8], S11, 0x698098d8U ); /* 9 */
  FF ( d, a, b, c, x[ 9], S12, 0x8b44f7afU ); /* 10 */
  FF ( c, d, a, b, x[10], S13, 0xffff5bb1U ); /* 11 */
  FF ( b, c, d, a, x[11], S14, 0x895cd7beU ); /* 12 */
  FF ( a, b, c, d, x[12], S11, 0x6b901122U ); /* 13 */
  FF ( d, a, b, c, x[13], S12, 0xfd987193U ); /* 14 */
  FF ( c, d, a, b, x[14], S13, 0xa679438eU ); /* 15 */
  FF ( b, c, d, a, x[15], S14, 0x49b40821U ); /* 16 */
  /* Round 2 */
  GG ( a, b, c, d, x[ 1], S21, 0xf61e2562U ); /* 17 */
  GG ( d, a, b, c, x[ 6], S22, 0xc040b340U ); /* 18 */
  GG ( c, d, a, b, x[11], S23, 0x265e5a51U ); /* 19 */
  GG ( b, c, d, a, x[ 0], S24, 0xe9b6c7aaU ); /* 20 */
  GG ( a, b, c, d, x[ 5], S21, 0xd62f105dU ); /* 21 */
  GG ( d, a, b, c, x[10], S22,  0x2441453U ); /* 22 */
  GG ( c, d, a, b, x[15], S23, 0xd8a1e681U ); /* 23 */
  GG ( b, c, d, a, x[ 4], S24, 0xe7d3fbc8U ); /* 24 */
  GG ( a, b, c, d, x[ 9], S21, 0x21e1cde6U ); /* 25 */
  GG ( d, a, b, c, x[14], S22, 0xc33707d6U ); /* 26 */
  GG ( c, d, a, b, x[ 3], S23, 0xf4d50d87U ); /* 27 */
  GG ( b, c, d, a, x[ 8], S24, 0x455a14edU ); /* 28 */
  GG ( a, b, c, d, x[13], S21, 0xa9e3e905U ); /* 29 */
  GG ( d, a, b, c, x[ 2], S22, 0xfcefa3f8U ); /* 30 */
  GG ( c, d, a, b, x[ 7], S23, 0x676f02d9U ); /* 31 */
  GG ( b, c, d, a, x[12], S24, 0x8d2a4c8aU ); /* 32 */
  /* Round 3 */
  HH ( a, b, c, d, x[ 5], S31, 0xfffa3942U ); /* 33 */
  HH ( d, a, b, c, x[ 8], S32, 0x8771f681U ); /* 34 */
  HH ( c, d, a, b, x[11], S33, 0x6d9d6122U ); /* 35 */
  HH ( b, c, d, a, x[14], S34, 0xfde5380cU ); /* 36 */
  HH ( a, b, c, d, x[ 1], S31, 0xa4beea44U ); /* 37 */
  HH ( d, a, b, c, x[ 4], S32, 0x4bdecfa9U ); /* 38 */
  HH ( c, d, a, b, x[ 7], S33, 0xf6bb4b60U ); /* 39 */
  HH ( b, c, d, a, x[10], S34, 0xbebfbc70U ); /* 40 */
  HH ( a, b, c, d, x[13], S31, 0x289b7ec6U ); /* 41 */
  HH ( d, a, b, c, x[ 0], S32, 0xeaa127faU ); /* 42 */
  HH ( c, d, a, b, x[ 3], S33, 0xd4ef3085U ); /* 43 */
  HH ( b, c, d, a, x[ 6], S34,  0x4881d05U ); /* 44 */
  HH ( a, b, c, d, x[ 9], S31, 0xd9d4d039U ); /* 45 */
  HH ( d, a, b, c, x[12], S32, 0xe6db99e5U ); /* 46 */
  HH ( c, d, a, b, x[15], S33, 0x1fa27cf8U ); /* 47 */
  HH ( b, c, d, a, x[ 2], S34, 0xc4ac5665U ); /* 48 */
  /* Round 4 */
  II ( a, b, c, d, x[ 0], S41, 0xf4292244U ); /* 49 */
  II ( d, a, b, c, x[ 7], S42, 0x432aff97U ); /* 50 */
  II ( c, d, a, b, x[14], S43, 0xab9423a7U ); /* 51 */
  II ( b, c, d, a, x[ 5], S44, 0xfc93a039U ); /* 52 */
  II ( a, b, c, d, x[12], S41, 0x655b59c3U ); /* 53 */
  II ( d, a, b, c, x[ 3], S42, 0x8f0ccc92U ); /* 54 */
  II ( c, d, a, b, x[10], S43, 0xffeff47dU ); /* 55 */
  II ( b, c, d, a, x[ 1], S44, 0x85845dd1U ); /* 56 */
  II ( a, b, c, d, x[ 8], S41, 0x6fa87e4fU ); /* 57 */
  II ( d, a, b, c, x[15], S42, 0xfe2ce6e0U ); /* 58 */
  II ( c, d, a, b, x[ 6], S43, 0xa3014314U ); /* 59 */
  II ( b, c, d, a, x[13], S44, 0x4e0811a1U ); /* 60 */
  II ( a, b, c, d, x[ 4], S41, 0xf7537e82U ); /* 61 */
  II ( d, a, b, c, x[11], S42, 0xbd3af235U ); /* 62 */
  II ( c, d, a, b, x[ 2], S43, 0x2ad7d2bbU ); /* 63 */
  II ( b, c, d, a, x[ 9], S44, 0xeb86d391U ); /* 64 */

  buf[0] += a;
  buf[1] += b;
  buf[2] += c;
  buf[3] += d;
}

static void
MD5Update( MD5_CTX *mdContext, const uint8_t *inBuf, uint32_t inLen ) noexcept
{
  uint32_t in[16];
  int mdi;
  unsigned int i, ii;

  /* compute number of bytes mod 64 */
  mdi = (int)((mdContext->i[0] >> 3) & 0x3F);

  /* update number of bits */
  if ((mdContext->i[0] + ((uint32_t)inLen << 3)) < mdContext->i[0])
    mdContext->i[1]++;
  mdContext->i[0] += ((uint32_t)inLen << 3);
  mdContext->i[1] += ((uint32_t)inLen >> 29);

  while (inLen--) {
    /* add new character to buffer, increment mdi */
    mdContext->in[mdi++] = *inBuf++;

    /* transform if necessary */
    if (mdi == 0x40) {
      for (i = 0, ii = 0; i < 16; i++, ii += 4)
        in[i] = (((uint32_t)mdContext->in[ii+3]) << 24) |
                (((uint32_t)mdContext->in[ii+2]) << 16) |
                (((uint32_t)mdContext->in[ii+1]) << 8) |
                ((uint32_t)mdContext->in[ii]);
      Transform (mdContext->buf, in);
      mdi = 0;
    }
  }
}

static void
MD5Final( uint8_t *digest,  MD5_CTX *mdContext ) noexcept
{
  static uint8_t PADDING[64] = {
    0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };

  uint32_t in[16];
  int mdi;
  uint32_t i, ii;
  uint32_t padLen;

  /* save number of bits */
  in[14] = mdContext->i[0];
  in[15] = mdContext->i[1];

  /* compute number of bytes mod 64 */
  mdi = (int)((mdContext->i[0] >> 3) & 0x3F);

  /* pad out to 56 mod 64 */
  padLen = (mdi < 56) ? (56 - mdi) : (120 - mdi);
  MD5Update (mdContext, PADDING, padLen);

  /* append length in bits and transform */
  for (i = 0, ii = 0; i < 14; i++, ii += 4)
    in[i] = (((uint32_t)mdContext->in[ii+3]) << 24) |
            (((uint32_t)mdContext->in[ii+2]) << 16) |
            (((uint32_t)mdContext->in[ii+1]) << 8) |
            ((uint32_t)mdContext->in[ii]);
  Transform (mdContext->buf, in);

  /* store buffer in digest */
  for (i = 0, ii = 0; i < 4; i++, ii += 4) {
    digest[ii]   = (uint8_t)(mdContext->buf[i] & 0xFF);
    digest[ii+1] = (uint8_t)((mdContext->buf[i] >> 8) & 0xFF);
    digest[ii+2] = (uint8_t)((mdContext->buf[i] >> 16) & 0xFF);
    digest[ii+3] = (uint8_t)((mdContext->buf[i] >> 24) & 0xFF);
  }
}

static void
MD5( const void *data,  size_t len,  uint8_t digest[16] ) noexcept
{
  const uint8_t *ptr = (const uint8_t *) data;
  MD5_CTX ctx;
  MD5Init( &ctx );
  for (;;) {
    uint32_t sz = len & (size_t) 0xffffffffU;
    MD5Update( &ctx, ptr, sz );
    if ( len == (size_t) sz )
      break;
    len -= (size_t) sz;
    ptr = &ptr[ sz ];
  }
  MD5Final( digest, &ctx );
}

