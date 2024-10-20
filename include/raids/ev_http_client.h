#ifndef __rai_raids__ev_http_client_h__
#define __rai_raids__ev_http_client_h__

#include <raimd/md_msg.h>
#include <raikv/ev_tcp.h>
#include <raikv/util.h>
#include <raids/ev_tcp_ssl.h>
#include <raids/ws_frame.h>

namespace rai {
namespace ds {

struct Val {
  const char * str;
  size_t       len;

  Val() : str( 0 ), len( 0 ) {}
  Val( const Val &v ) : str( v.str ), len( v.len ) {}
  Val( const char *s ) : str( s ), len( ::strlen( s ) ) {}
  Val( const char *s,  size_t l ) : str( s ), len( l ) {}
  void zero( void ) { this->str = NULL; this->len = 0; }
  void copy( const Val &v ) { this->str = v.str; this->len = v.len; }
  void move( Val &v ) { this->copy( v ); v.zero(); }
  bool is_empty( void ) const { return this->len == 0; }
  bool equals( const Val &v ) const { return this->len == v.len &&
                                     ::memcmp( this->str, v.str, v.len ) == 0; }
  uint32_t hash( void ) const { return kv_crc_c( this->str, this->len, 0 ); }
};

struct Pair {
  Val x, y;

  void * operator new( size_t, void *ptr ) { return ptr; }
  Pair() {}
  Pair( const Pair &p )
    : x( p.x ), y( p.y ) {}
  Pair( const char *a,  size_t al,  const char *b,  size_t bl )
    : x( a, al ), y( b, bl ) {}

  bool is_empty( void ) const { return this->x.is_empty(); }
  uint32_t hash( void ) const { return this->x.hash(); }
  void move( Pair &p ) { this->x.move( p.x ); this->y.move( p.y ); }
};

struct VarHT {
  md::MDMsgMem mem;
  Pair  * p;
  size_t  used, sz;

  VarHT() : p( 0 ), sz( 0 ) {}
  VarHT &add( const Pair &pair ) noexcept;
  bool   get( const Val &var,  Val &val ) const noexcept;
  void   resize( void ) noexcept;
};

struct HttpRsp;
struct WSClientMsg {
  ds::WebSocketFrame frame;
  void             * data;
  size_t             len;
};

struct HttpClientCB {
  HttpClientCB() {}
  virtual void on_http_msg( HttpRsp &rsp ) noexcept;
  virtual void on_switch( HttpRsp &rsp ) noexcept;
  virtual void on_ws_msg( WSClientMsg &msg ) noexcept;
};

struct HttpClient : public ds::SSL_Connection, public kv::RouteNotify {
  kv::RoutePublish & sub_route;
  HttpClientCB * cb;
  kv::rand::xoroshiro128plus
                 rand;
  uint64_t       ws_mask,
                 ws_bytes_sent;
  bool           is_websock;

  void * operator new( size_t, void *ptr ) { return ptr; }

  HttpClient( kv::EvPoll &p,  kv::RoutePublish &r,  kv::EvConnectionNotify *n ) noexcept;
  HttpClient( kv::EvPoll &p ) noexcept;

  void send_request( const char *tmplate,  VarHT &ht ) noexcept;
  bool process_http( void ) noexcept;
  bool process_websock( void ) noexcept;
  void send_ws_pong( WSClientMsg &ping ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct HttpRsp {
  static const size_t STR_SZ = 128;
  char         wsacc[ STR_SZ ], wspro[ STR_SZ ],
               content_type[ STR_SZ ];
  size_t       wsacclen,
               content_length,
               hdr_len,
               http_code_length;
  const char * data,
             * hdr,
             * http_code_string; /* OK */
  int          opts, /* parse the hdrs that are useful */
               http_code; /* 200 */

  enum Opts {
    HTTP_1_1   = 1, /* HTTP/1.1 found */
    UPGRADE    = 2, /* Connection: upgrade */
    KEEP_ALIVE = 4, /* Connection: keep-alive */
    CLOSE      = 8, /* Connection: close */
    WEBSOCKET  = 16 /* Upgrade: websocket */
  };

  HttpRsp() : wsacclen( 0 ), content_length( 0 ), hdr_len( 0 ),
              http_code_length( 0 ), data( 0 ), hdr( 0 ), http_code_string( 0 ),
              opts( 0 ), http_code( 0 ) {
    this->wsacc[ 0 ] = this->wspro[ 0 ] =
    this->content_type[ 0 ] = '\0';
  }
  bool parse_version( const char *line,  size_t len ) noexcept;
  void parse_header( const char *line,  size_t len ) noexcept;
};

}
}

#endif
