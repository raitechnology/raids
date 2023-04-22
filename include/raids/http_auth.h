/* Copyright (c) 2016 Rai Technology.  All rights reserved.
 *  http://www.raitechnology.com */
#ifndef __rai_raids__http_auth_h__
#define __rai_raids__http_auth_h__

#include <raikv/route_ht.h>
#include <raikv/util.h>

namespace rai {
namespace ds {

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

struct HtDigestDB;
struct HttpServerNonce;

enum HtAuthCode {
  HT_AUTH_NOAUTH = 0,
  HT_AUTH_OK,
  HT_AUTH_USERNAME,
  HT_AUTH_REALM,
  HT_AUTH_URI,
  HT_AUTH_RESPONSE,
  HT_AUTH_COUNT,
  HT_AUTH_CNONCE,
  HT_AUTH_STALE,
  HT_AUTH_NOUSER,
  HT_AUTH_NOREALM,
  HT_AUTH_PASSFAIL
};

struct HttpDigestAuth {
  uint32_t     client_incr;/* counter incremented by the client (nc field) */
  HtAuthCode   errcode;
  char       * cnonce,     /* client nonce, used to check for replay */
             * opaque,     /* keeps state information svr -> client -> svr */
             * username,   /* username requesting auth */
             * realm;      /* realm of username:passwd */
  char       * nonce,      /* pointers into buf after parse */
             * uri,
             * response,
             * qop,       /* auth */
             * nc,
             * algorithm; /* MD5 */
  size_t       cnonce_len,
               nonce_len,
               nc_len,
               realm_len,
               username_len,
               uri_len,
               opaque_len;
  const char * mynonce; /* should match nonce above */
  HtDigestDB * dig_db;
  size_t       buf_len,      /* size of data in buf[] */
               max_out;
  char       * buf,
             * out_buf,
               tmp_buf[ 2 * 1024 ];

  const char *error( void ) const noexcept;
  size_t match( size_t i,  const char *val,  size_t len,  char **ptr = NULL,
                size_t *ptr_len = NULL ) noexcept;
  void make_out_buf( size_t len ) noexcept;
  size_t cpy( size_t i,  const char *s,  char c,  size_t len ) noexcept;
  size_t cpy( size_t i,  const char *s,  char c = 0 ) {
    return this->cpy( i, s, c, ::strlen( s ) );
  }
  size_t cpy_MD5( size_t i,  const uint8_t *bits,  char c = 0 ) noexcept;

  bool check_fields( void ) noexcept;

  HttpDigestAuth( const char *non = NULL,  HtDigestDB *db = NULL ) :
      mynonce( non ), dig_db( db ), buf_len( 0 ), max_out( 0 ), buf( 0 ),
      out_buf( 0 ) { this->clear(); }
  ~HttpDigestAuth() { this->release(); }

  void release( void ) {
    char * end_tmp = &this->tmp_buf[ sizeof( this->tmp_buf ) ];
    if ( this->buf != NULL ) {
      if ( this->buf < this->tmp_buf || this->buf >= end_tmp )
        ::free( this->buf );
    }
    if ( this->out_buf != NULL ) {
      if ( this->out_buf < this->tmp_buf || this->out_buf >= end_tmp )
        ::free( this->out_buf );
    }
  }
  void clear( void ) {
    this->release();
#if __GNUC__ >= 9
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
    ::memset( (void *) &this->client_incr, 0,
              (char *) (void *) &this->mynonce -
              (char *) (void *) &this->client_incr );
#if __GNUC__ >= 9
#pragma GCC diagnostic pop
#endif
    this->buf_len      = 0;
    this->max_out      = sizeof( this->tmp_buf );
    this->buf          = NULL;
    this->out_buf      = this->tmp_buf;
    this->tmp_buf[ 0 ] = '\0';
  }

  /* if failed to parse, or nonce does't match mynonce, returns false */
  bool parse_auth( const void *p,  size_t len,  bool check = true ) noexcept;
  /* if user/realm not found, returns false */
  bool check_user( void ) noexcept;

  void set_nonce( char *non ) {
    this->nonce = non;
  }
  /* gen client request fields */
  size_t gen_client( const char *user,  const char *passwd,
                     uint32_t cnt,  const char *cnonce,
                     const char *uri,  const char *method = "GET",
                     size_t method_len = 3 ) noexcept;
  size_t gen_server( const HttpServerNonce &svr,  bool stale ) noexcept;
  /* log the fields as minor */
  void log_auth( void ) noexcept;
  /* if MD5 hash computation doesn't match response, returns false */
  bool check_auth( const char *method = "GET",  size_t method_len = 3,
                   bool trace = false ) noexcept;
  /* return MD5 string computed in outBuf */
  char *compute_HA1( const char *username,  const char *realm,
                     const char *passwd ) noexcept;
};

struct HttpServerNonce {
  static const size_t NONCE_BYTES = 16, OPAQUE_BYTES = 8;
  kv::rand::xorshift1024star rand;
  uint32_t replay[ 64 * 1024 ];
  char     nonce[ 64 ];
  char     opaque[ 64 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  HttpServerNonce() {
    ::memset( this->replay, 0, sizeof( this->replay ) );
    ::memset( this->nonce, 0, sizeof( this->nonce ) );
    ::memset( this->opaque, 0, sizeof( this->opaque ) );
    this->rand.init();
  }

  void regenerate( bool do_opaque = false ) noexcept;
  bool check_replay( HttpDigestAuth &auth ) noexcept;
};

struct HtUserHA1 {
  char   * ha1;
  uint32_t hash;
  uint16_t len;
  char     value[ 2 ];
};

struct HtDigestDB : public kv::RouteVec<HtUserHA1> {
  char   * realm;
  uint64_t last_modified;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  HtDigestDB() : realm( 0 ), last_modified( 0 ) {}
  ~HtDigestDB() noexcept;
  /* init realm[] */
  bool set_realm( const char *realm,  const char *host = NULL ) noexcept;
  /* return true if loaded least one entry */
  bool load( const char *fn,  const char *realm ) noexcept;
  /* update database if file is newer, (or first-time initialize db) */
  bool update_if_modified( const char *fn,  const char *realm ) noexcept;
  /* add user pass to db */
  bool add_user_pass( const char *username,  const char *passwd,
                      const char *realm ) noexcept;
  /* add user ha1 to db */
  bool add_user_HA1( const char *username,  const char *ha1 ) noexcept;
};

}
}
#endif
