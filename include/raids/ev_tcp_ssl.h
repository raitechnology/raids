#ifndef __rai_raids__ev_tcp_aes_h__
#define __rai_raids__ev_tcp_aes_h__

#include <raikv/ev_net.h>
#include <openssl/ssl.h>

namespace rai {
namespace ds {

struct SSL_Connection;

struct SSL_Config {
  const char * cert_file,
             * key_file,
             * ca_cert_file,
             * ca_cert_dir;
  bool         is_client,
               verify_peer;
  SSL_Config( const char *c,  const char *k,  const char *cf,  const char *cd,
              bool is_cl,  bool vpeer )
    : cert_file( c ), key_file( k ), ca_cert_file( cf ), ca_cert_dir( cd ),
      is_client( is_cl ), verify_peer( vpeer ) {}
};

struct SSL_Context {
  SSL_CTX * ctx; /* pem files */

  void * operator new( size_t, void *ptr ) { return ptr; }
  SSL_Context() : ctx( 0 ) {}
  bool init_config( const SSL_Config &cfg ) noexcept;
  void init_accept( SSL_Connection &conn ) noexcept;
  void init_connect( SSL_Connection &conn ) noexcept;
};

struct SSL_Connection : public kv::EvConnection {
  enum Status {
    CONN_ERROR = -1,
    CONN_OK    = 0,
    CONN_WRITE = 1,
    CONN_READ  = 2
  };
  SSL    * ssl;
  BIO    * rbio, /* reads decryption */
         * wbio; /* write encryption  */
  size_t   recv_ssl_off,
           send_ssl_off;
  char   * save;
  size_t   save_len;
  bool     init_finished,
           is_connect;

  SSL_Connection( kv::EvPoll &p,  uint8_t st )
    : kv::EvConnection( p, st ), ssl( 0 ), rbio( 0 ), wbio( 0 ),
      recv_ssl_off( 0 ), send_ssl_off( 0 ), save( 0 ), save_len( 0 ),
      init_finished( false ), is_connect( false ) {}

  Status get_ssl_status( int n ) noexcept;

  bool init_ssl_accept( SSL_Context *ctx ) noexcept {
    this->init_finished = false;
    this->is_connect    = false;
    this->recv_ssl_off  = 0;
    this->send_ssl_off  = 0;

    if ( ctx != NULL ) {
      ctx->init_accept( *this );
      return this->ssl_init_io();
    }
    return true;
  }
  bool init_ssl_connect( SSL_Context *ctx ) noexcept {
    this->init_finished = false;
    this->is_connect    = true;
    this->recv_ssl_off  = 0;
    this->send_ssl_off  = 0;

    if ( ctx != NULL ) {
      ctx->init_connect( *this );
      return this->ssl_init_io();
    }
    return true;
  }
  void release_ssl( void ) noexcept;
  bool drain_wbio( void ) noexcept;
  bool ssl_init_io( void ) noexcept;
  bool ssl_read( void ) noexcept;
  bool write_buf( const void *buf,  size_t len ) noexcept;
  virtual void read( void ) noexcept;
  virtual void write( void ) noexcept;
  void save_write( void ) noexcept;
};

}
}

#endif
