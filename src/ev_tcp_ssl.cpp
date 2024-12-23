#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <raids/ev_tcp_ssl.h>

using namespace rai;
using namespace ds;
using namespace kv;

/*
#include <raimd/md_msg.h>
using namespace md;
static void
print_bytes( const char *where,  const void *b,  size_t b_len ) noexcept
{
  printf( "%s:\n", where );
  MDOutput mout;
  mout.print_hex( b, b_len );
}*/

static void
init_ssl_library( void ) noexcept
{
  /* SSL library initialisation */
  static int library_init;
  if ( ! library_init ) {
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    /*ERR_load_BIO_strings();*/
    ERR_load_crypto_strings();
    library_init = 1;
  }
}

void
SSL_Context::release_ctx( void ) noexcept
{
  if ( this->ctx != NULL ) {
    SSL_CTX_free( this->ctx );
    this->ctx = NULL;
  }
}

bool
SSL_Context::init_config( const SSL_Config &cfg ) noexcept
{
  char errbuf[ 256 ];

  init_ssl_library();
  this->ctx = SSL_CTX_new( cfg.is_client ? SSLv23_client_method() :
                                           SSLv23_server_method() );
  if ( this->ctx == NULL ) {
    fprintf( stderr, "SSL_CTX_new()\n" );
    return false;
  }
  if ( cfg.cert_file != NULL &&
       SSL_CTX_use_certificate_file( this->ctx, cfg.cert_file,
                                     SSL_FILETYPE_PEM ) <= 0 ) {
    ERR_error_string_n( ERR_get_error(), errbuf, sizeof( errbuf ) );
    fprintf( stderr, "Failed to load certificate: %s: %s", cfg.cert_file,
             errbuf );
    return false;
  }
  if ( cfg.key_file != NULL &&
       SSL_CTX_use_PrivateKey_file( this->ctx, cfg.key_file,
                                    SSL_FILETYPE_PEM ) <= 0 ) {
    ERR_error_string_n( ERR_get_error(), errbuf, sizeof( errbuf ) );
    fprintf( stderr, "Failed to load private key: %s: %s", cfg.key_file,
             errbuf );
    return false;
  }
  if ( ! cfg.no_verify ) {
    int opts = SSL_VERIFY_PEER;
    if ( cfg.verify_peer || cfg.is_client )
      opts |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    SSL_CTX_set_verify( this->ctx, opts, NULL );

    if ( ( cfg.ca_cert_file != NULL || cfg.ca_cert_dir != NULL ) &&
         SSL_CTX_load_verify_locations( this->ctx, cfg.ca_cert_file,
                                        cfg.ca_cert_dir ) <= 0 ) {
      ERR_error_string_n( ERR_get_error(), errbuf, sizeof( errbuf ) );
      fprintf( stderr, "Failed to configure CA certificate(s) file/directory: %s",
               errbuf );
      return false;
    }
  }
  SSL_CTX_set_options( this->ctx, SSL_OP_ALL|SSL_OP_NO_SSLv2|SSL_OP_NO_SSLv3 );
  return true;
}
#if 0
  /* Load certificate and private key files, and check consistency  */
  int err = SSL_CTX_use_certificate_file( this->ctx, cfg.cert_file,
                                          SSL_FILETYPE_PEM );
  if ( err != 1 ) {
    fprintf( stderr, "SSL_CTX_use_certificate_file failed\n" );
    return false;
  }
  /* Indicate the key file to be used */
  err = SSL_CTX_use_PrivateKey_file( this->ctx, cfg.key_file,
                                     SSL_FILETYPE_PEM );
  if ( err != 1 ) {
    fprintf( stderr, "SSL_CTX_use_PrivateKey_file failed\n" );
    return false;
  }
  /* Make sure the key and certificate file match. */
  err = SSL_CTX_check_private_key( this->ctx );
  if ( err != 1 ) {
    fprintf( stderr, "SSL_CTX_check_private_key failed\n" );
    return false;
  }
bool
SSL_Context::init_client( const SSL_Config &cfg ) noexcept
{
  int err;
  init_ssl_library();
  this->ctx = SSL_CTX_new( SSLv23_client_method() );

  if ( this->ctx == NULL ) {
    fprintf( stderr, "SSL_CTX_new()\n" );
    return false;
  }
  SSL_CTX_set_verify( this->ctx, SSL_VERIFY_PEER, NULL );
  err = SSL_CTX_load_verify_locations( this->ctx, cfg.ca_cert_file,
                                       cfg.ca_cert_dir );
  if ( err != 1 ) {
    fprintf( stderr, "SSL_CTX_load_verify_locations failed\n" );
    return false;
  }
  /* Recommended to avoid SSLv2 & SSLv3 */
  SSL_CTX_set_options( this->ctx, SSL_OP_ALL|SSL_OP_NO_SSLv2|SSL_OP_NO_SSLv3 );
  return true;
}
#endif
void
SSL_Context::init_accept( SSL_Connection &conn ) noexcept
{
  if ( this->ctx != NULL ) {
    conn.rbio = BIO_new( BIO_s_mem() );
    conn.wbio = BIO_new( BIO_s_mem() );
    conn.ssl = SSL_new( this->ctx );
    SSL_set_accept_state( conn.ssl );
    SSL_set_bio( conn.ssl, conn.rbio, conn.wbio );
  }
  conn.is_connect = false;
}

void
SSL_Context::init_connect( SSL_Connection &conn ) noexcept
{
  if ( this->ctx != NULL ) {
    conn.rbio = BIO_new( BIO_s_mem() );
    conn.wbio = BIO_new( BIO_s_mem() );
    conn.ssl = SSL_new( this->ctx );
    SSL_set_connect_state( conn.ssl );
    SSL_set_bio( conn.ssl, conn.rbio, conn.wbio );
  }
  conn.is_connect = true;
}

void
SSL_Connection::release_ssl( void ) noexcept
{
  if ( this->ssl != NULL )
    SSL_free( this->ssl );
  if ( this->save != NULL )
    ::free( this->save );

  this->ssl  = NULL;
  this->rbio = NULL;
  this->wbio = NULL;
  this->save = NULL;
  this->save_len = 0;
}

SSL_Connection::Status
SSL_Connection::get_ssl_status( int n ) noexcept
{
  int e = SSL_get_error( this->ssl, n );
  const char * s = "Other";
  switch ( e ) {
    case SSL_ERROR_NONE:        return CONN_OK;
    case SSL_ERROR_WANT_WRITE:  return CONN_WRITE;
    case SSL_ERROR_WANT_READ:   return CONN_READ;
    case SSL_ERROR_ZERO_RETURN: return CONN_CLOSED;
    case SSL_ERROR_SYSCALL:     s = "Syscall error"; break;
    default: break;
  }
  fprintf( stderr, "SSL error: %s/%d\n", s, e );
  ERR_print_errors_fp( stderr );
  return CONN_ERROR;
}

bool
SSL_Connection::drain_wbio( void ) noexcept
{
  for (;;) {
    char * buf = this->alloc( 1024 );
    int    n   = BIO_read( this->wbio, buf, 1024 );
    if ( n <= 0 ) {
      if ( ! BIO_should_retry( this->wbio ) )
        return false;
      return true;
    }
    this->sz += (size_t) n;
    this->send_ssl_off += (size_t) n;
  }
}

bool
SSL_Connection::ssl_init_io( void ) noexcept
{
  if ( ! this->init_finished )
    this->init_finished = ( SSL_is_init_finished( this->ssl ) != 0 );
  if ( ! this->init_finished ) {
    int n;
    if ( ! this->is_connect )
      n = SSL_accept( this->ssl );
    else
      n = SSL_connect( this->ssl );
    switch ( this->get_ssl_status( n ) ) {
      case CONN_OK:    break;
      case CONN_CLOSED:
      case CONN_ERROR: return false;
      default:
        if ( ! this->drain_wbio() )
          return false;
        break;
    }
    this->init_finished = ( SSL_is_init_finished( this->ssl ) != 0 );
  }
  if ( this->init_finished ) {
    if ( this->save_len > 0 ) {
      char * buf = this->save;
      size_t len = this->save_len;
      this->save     = NULL;
      this->save_len = 0;
      if ( ! this->write_buf( buf, len ) )
        return false;
      ::free( buf );
    }
    this->ssl_init_finished();
    if ( ! this->write_buffers() )
      return false;
  }
  if ( this->pending() != 0 )
    this->idle_push_write();
  return true;
}

void
SSL_Connection::ssl_init_finished( void ) noexcept
{
}

bool
SSL_Connection::ssl_read( void ) noexcept
{
  int n;
  if ( this->recv_ssl_off < this->bytes_recv ) {
    size_t buflen = this->bytes_recv - this->recv_ssl_off;
    if ( buflen > this->len ) {
      fprintf( stderr, "bad enc len\n" );
      return false;
    }
    size_t moff = this->len - buflen;
    n = BIO_write( this->rbio, &this->recv[ moff ], buflen );
    if ( n <= 0 )
      return false; /* if write fails, assume unrecoverable */

    this->recv_ssl_off += (size_t) n;
    if ( buflen == (size_t) n )
      this->len -= buflen;
    else {
      size_t xoff = moff + (size_t) n,
             xsz  = this->len - xoff;
      ::memmove( &this->recv[ moff ], &this->recv[ xoff ], xsz );
      this->len = moff + xsz;
    }
    /* finish init */
    if ( ! this->init_finished ) {
      if ( ! this->ssl_init_io() )
        return false;
      if ( ! this->init_finished ) {
        this->pop( EV_PROCESS );
        return true;
      }
    }
    if ( this->recv_ssl_off < this->bytes_recv ) {
      if ( this->pending() == 0 )
        return false;
      this->pushpop( EV_READ_LO, EV_PROCESS ); /* write, then read again */
      return true;
    }
  }
  /* read decrypted data */
  for (;;) {
    if ( this->len == this->recv_size ) {
      if ( ! this->resize_recv_buf( sizeof( this->recv_buf ) ) )
        return false;
    }
    n = SSL_read( this->ssl, &this->recv[ this->len ],
                  this->recv_size - this->len );
    if ( n <= 0 )
      break;
    this->len += (size_t) n;
  }
  if ( this->off == this->len )
    this->pop( EV_PROCESS );

  switch ( this->get_ssl_status( n ) ) {
    case CONN_OK:    break;
    case CONN_CLOSED:
    case CONN_ERROR: return false;
    default:
      if ( ! this->drain_wbio() )
        return false;
      break;
  }
  return true;
}

void
SSL_Connection::read( void ) noexcept
{
  this->EvConnection::read();
  if ( this->ssl != NULL ) {
    if ( ! this->ssl_read() )
      this->pushpop( EV_CLOSE, EV_PROCESS );
    else if ( this->pending() > 0 )
      this->push( EV_WRITE );
  }
}

void
SSL_Connection::save_write( void ) noexcept
{
  this->concat_iov();

  size_t wr_len = this->bytes_sent + this->wr_pending;
  if ( wr_len > this->send_ssl_off ) {
    size_t          iov_len = this->iov[ 0 ].iov_len;
    const uint8_t * base    = (const uint8_t *) this->iov[ 0 ].iov_base;

    if ( this->bytes_sent < this->send_ssl_off ) {
      iov_len -= ( this->send_ssl_off - this->bytes_sent );
      base     = &base[ this->send_ssl_off - this->bytes_sent ];
    }

    this->save = (char *)
      ::realloc( this->save, this->save_len + iov_len );
    ::memcpy( &this->save[ this->save_len ], base, iov_len );
    this->save_len += iov_len;

    if ( this->iov[ 0 ].iov_len == iov_len ) {
      this->reset();
      this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
      return;
    }
    this->iov[ 0 ].iov_len = this->send_ssl_off - this->bytes_sent;
    this->wr_pending = this->iov[ 0 ].iov_len;
  }
  this->EvConnection::write();
}

bool
SSL_Connection::write_buf( const void *buf,  size_t len ) noexcept
{
  size_t off = 0;
  for (;;) {
    int n = SSL_write( this->ssl, &((const char *) buf)[ off ], len );
    if ( n < 0 )
      return false;
    if ( (size_t) n == len )
      return true;
    off += (size_t) n;
    len -= (size_t) n;
    if ( ! this->drain_wbio() )
      return false;
  }
}

bool
SSL_Connection::write_buffers( void ) noexcept
{
  if ( this->sz > 0 )
    this->flush();

  size_t enc_off = this->send_ssl_off - this->bytes_sent,
         i, j, k = this->idx, z;

  for ( i = 0; i < k; i++ ) {
    iovec & io = this->iov[ i ];
    if ( io.iov_len > enc_off )
      break;
    enc_off -= io.iov_len;
  }
  if ( i < k ) {
    this->idx = i;
    iovec * tmp = (struct iovec *)
                  this->alloc_temp( sizeof( iovec ) * ( k - i ) );
    if ( enc_off > 0 ) {
      iovec & io = this->iov[ i ];
      char  * base = (char *) io.iov_base;
      size_t  enc_len = io.iov_len - enc_off;

      this->idx = ++i;
      this->wr_pending -= enc_len;
      io.iov_len = enc_off;

      tmp[ 0 ].iov_base = &base[ enc_off ];
      tmp[ 0 ].iov_len  = enc_len;
      z = 1;
    }
    else {
      tmp = (struct iovec *)
            this->alloc_temp( sizeof( struct iovec ) * ( k - i ) );
      z = 0;
    }

    for ( j = i; j < k; j++ ) {
      iovec & io = this->iov[ j ];
      tmp[ z++ ] = io;
      this->wr_pending -= io.iov_len;
    }

    for ( j = 0; j < z; j++ ) {
      iovec & io = tmp[ j ];
      if ( ! this->write_buf( io.iov_base, io.iov_len ) )
        return false;
    }
  }
  return true;
}

void
SSL_Connection::write( void ) noexcept
{
  if ( this->ssl != NULL ) {
    if ( ! this->init_finished ) {
      this->save_write();
      return;
    }
    else {
      if ( ! this->write_buffers() || ! this->drain_wbio() ) {
        this->push( EV_CLOSE );
        return;
      }
    }
  }
  this->EvConnection::write();
}
