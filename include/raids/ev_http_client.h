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

struct HtReqArgs {
  struct {
    const char *str;
    size_t      len;
  } arg[ 10 ];
  HtReqArgs & add( uint32_t i,  const char *s,  size_t l ) {
    this->arg[ i ].str = s;
    this->arg[ i ].len = l;
    return *this;
  }
  size_t template_size( const char *m,  const char *e ) noexcept;
  void template_copy( const char *m,  const char *e,  char *o ) noexcept;
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

struct HttpClientParameters {
  const char  * addr;
  int           port,
                opts;
  const struct  addrinfo *ai;
  const char  * k;
  uint32_t      rte_id;
  SSL_Context * ctx;
  bool          is_websock;

  HttpClientParameters( const char *a = NULL,  int p = 80,
                        int o = kv::DEFAULT_TCP_CONNECT_OPTS )
    : addr( a ), port( p ), opts( o ),
      ai( NULL ), k( 0 ), rte_id( 0 ), ctx( 0 ), is_websock( false ) {}
};

static const char ws_key[] = "dGhlIHNhbXBsZSBub25jZQ==",
                  ws_acc[] = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

struct HttpClient : public SSL_Connection, public kv::RouteNotify {
  enum HtState {
    HT_INIT    = 0,
    HT_SSL     = 1,
    HT_DATA    = 2,
    HT_WEBSOCK = 3,
    HT_CLOSE   = 4
  };

  kv::RoutePublish & sub_route;
  HttpClientCB * cb;
  char         * host;
  size_t         host_len;
  kv::rand::xoroshiro128plus
                 rand;
  uint64_t       ws_mask,
                 ws_bytes_sent;
  HtState        ht_state,
                 ht_target;
  struct Arg {
    const char *value;
    size_t len;
  };
  Arg args[ 32 ];
  size_t args_count;

  Arg *a( const char *v,  size_t l ) {
    Arg &s = this->args[ this->args_count++ ];
    s.value = v;
    s.len   = l;
    return &s;
  }
  Arg *a( const char *v ) { return a( v, ::strlen( v ) ); }

  void * operator new( size_t, void *ptr ) { return ptr; }

  HttpClient( kv::EvPoll &p,  kv::RoutePublish &r,  kv::EvConnectionNotify *n ) noexcept;
  HttpClient( kv::EvPoll &p ) noexcept;

  void initialize_state( void ) noexcept;
  bool ht_connect( HttpClientParameters &p,
                   kv::EvConnectionNotify *n,  HttpClientCB *c ) noexcept;
  virtual int connect( kv::EvConnectParam &param ) noexcept;
  void send_request( const char *tmplate,  VarHT &ht ) noexcept;
  void send_request2( const Arg *a,  ... ) noexcept;
  void send_request3( const char *tmplate,  HtReqArgs &args ) noexcept;
  bool process_http( void ) noexcept;
  bool process_websock( void ) noexcept;
  void send_ws_pong( WSClientMsg &ping ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual void ssl_init_finished( void ) noexcept;
  virtual void send_websocket_upgrade( void ) noexcept;
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

extern uint32_t ws_debug;

}
}

#endif
