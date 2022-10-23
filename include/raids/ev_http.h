#ifndef __rai_raids__ev_http_h__
#define __rai_raids__ev_http_h__

#include <raikv/ev_tcp.h>
#include <raids/ev_tcp_ssl.h>
#include <raids/redis_exec.h>
#include <raids/term.h>

namespace rai {
namespace ds {

struct EvHttpListen : public kv::EvTcpListen {
  kv::RoutePublish & sub_route;
  SSL_Context        ssl_ctx;
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvHttpListen( kv::EvPoll &p, kv::RoutePublish &sr ) noexcept;
  EvHttpListen( kv::EvPoll &p ) noexcept;
  virtual EvSocket *accept( void ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept {
    return this->kv::EvTcpListen::listen2( ip, port, opts, "http_listen",
                                           this->sub_route.route_id );
  }
};

struct EvPrefetchQueue;

struct HttpReq {
  static const size_t STR_SZ = 128;
  char         wsver[ STR_SZ ], wskey[ STR_SZ ], wspro[ STR_SZ ],
               content_type[ STR_SZ ];
  size_t       wskeylen,
               content_length,
               method_len,
               path_len,
               authorize_len;
  const char * method,
             * path,
             * data,
             * authorize;
  int          opts; /* parse the hdrs that are useful */

  enum Opts {
    HTTP_1_1   = 1, /* HTTP/1.1 found */
    UPGRADE    = 2, /* Connection: upgrade */
    KEEP_ALIVE = 4, /* Connection: keep-alive */
    CLOSE      = 8, /* Connection: close */
    WEBSOCKET  = 16 /* Upgrade: websocket */
  };

  HttpReq() : wskeylen( 0 ), content_length( 0 ), method_len( 0 ), path_len( 0 ),
              authorize_len( 0 ), method( 0 ), path( 0 ), data( 0 ),
              authorize( 0 ), opts( 0 ) {
    this->wsver[ 0 ] = this->wskey[ 0 ] = this->wspro[ 0 ] =
    this->content_type[ 0 ] = '\0';
  }
  bool parse_version( const char *line,  size_t len ) noexcept;
  void parse_header( const char *line,  size_t len ) noexcept;
  static size_t decode_uri( const char *s,  const char *e,  char *q,
                            size_t qlen ) noexcept;
};

struct HttpOut {
  const char * hdr[ 16 ]; /* calculate size of hdrs for alloc */
  size_t       len[ 16 ],
               off,
               size;
  void push( const char *s,  size_t l ) {
    this->hdr[ this->off ] = s;
    this->len[ this->off++ ] = l;
    this->size += l;
  }
  size_t cat( char *s ) {
    const char *t = s;
    for ( size_t i = 0; ; ) {
      size_t j = this->len[ i ];
      ::memcpy( s, this->hdr[ i++ ], j );
      s = &s[ j ];
      if ( i == this->off )
        return s - t;
    }
  }
};

struct WSMsg {
  char * inptr;
  size_t inoff,
         inlen,
         msgcnt,
         nlcnt;
};

struct HttpServerNonce;
struct HtDigestDB;
struct HttpDigestAuth;

struct EvHttpConnection : public SSL_Connection {
  char            * wsbuf;   /* decoded websocket frames */
  size_t            wsoff,   /* start offset of wsbuf */
                    wslen,   /* length of wsbuf used */
                    wsalloc, /* sizeof wsbuf alloc */
                    wsmsgcnt;
  uint64_t          websock_off;/* on output pointer that frames msgs with ws */
  HttpServerNonce * svr_nonce;
  HtDigestDB      * digest_db;
  int               term_int;
  bool              is_using_term;
  Term              term;

  EvHttpConnection( kv::EvPoll &p,  const uint8_t t )
    : SSL_Connection( p, t ), wsbuf( 0 ), wsoff( 0 ), wslen( 0 ),
      websock_off( 0 ), svr_nonce( 0 ), digest_db( 0 ), term_int( 0 ),
      is_using_term( false ) {}
  void initialize_state( HttpServerNonce *svr = NULL,  HtDigestDB *db = NULL ) {
    this->wsbuf         = NULL;
    this->wsoff         = 0;
    this->wslen         = 0;
    this->wsalloc       = 0;
    this->wsmsgcnt      = 0;
    this->websock_off   = 0;
    this->term_int      = 0;
    this->is_using_term = false;
    this->svr_nonce     = svr;
    this->digest_db     = db;
    this->term.zero();
  }
  void init_http_response( const HttpReq &hreq,  HttpOut &hout,
                           int opts,  int code ) noexcept;
  void send_404_not_found( const HttpReq &hreq,  int opts ) noexcept;
  void send_404_bad_type( const HttpReq &hreq ) noexcept;
  void send_201_created( const HttpReq &hreq ) noexcept;
  void send_401_unauthorized( const HttpReq &hreq,
                              HttpDigestAuth &auth ) noexcept;
  bool process_websock( void ) noexcept;
  bool process_http( void ) noexcept;
  bool flush_term( void ) noexcept;
  bool frame_websock( void ) noexcept;
  virtual bool frame_websock2( void ) noexcept;
  bool send_ws_upgrade( const HttpReq &wshdr ) noexcept;
  bool send_ws_pong( const char *payload,  size_t pay_len ) noexcept;
  size_t recv_wsframe( char *start,  char *end ) noexcept;
  static const char *get_mime_type( const char *path, size_t len,
                                    size_t &mlen,  bool &is_gzip ) noexcept;
  virtual bool process_get( const HttpReq &hreq ) noexcept;
  virtual bool process_get_file( const char *path,  size_t path_len ) noexcept;
  virtual void process_wsmsg( WSMsg &wmsg ) noexcept = 0;
  virtual bool process_post( const HttpReq &hreq ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
};

struct EvHttpService : public EvHttpConnection, public RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvHttpService( kv::EvPoll &p,  const uint8_t t,  kv::RoutePublish &sr )
    : EvHttpConnection( p, t ),
      RedisExec( *sr.map, sr.ctx_id, sr.dbx_id, *this, sr, *this, p.timer ) {}

  virtual bool process_get( const HttpReq &hreq ) noexcept;
  virtual void process_wsmsg( WSMsg &wmsg ) noexcept;
  virtual bool process_post( const HttpReq &hreq ) noexcept;
  virtual bool frame_websock2( void ) noexcept;
  /* EvSocket */
  virtual void process_close( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( kv::PeerMatchArgs &ka ) noexcept;
};

/*    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *   +-+-+-+-+-------+-+-------------+-------------------------------+
 *   |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
 *   |I|S|S|S|  (4)  |A|     (7)     |             (16/63)           |
 *   |N|V|V|V|       |S|             |   (if payload len==126/127)   |
 *   | |1|2|3|       |K|             |                               |
 *   +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
 *   |     Extended payload length continued, if payload len == 127  |
 *   + - - - - - - - - - - - - - - - +-------------------------------+
 *   |                               |Masking-key, if MASK set to 1  |
 *   +-------------------------------+-------------------------------+
 *   | Masking-key (continued)       |          Payload Data         |
 *   + - - - - - - - - - - - - - - - +-------------------------------+
 *   :                       Payload Data                            :
 *   +---------------------------------------------------------------+ */
struct WebSocketFrame {
  static const uint32_t MAX_HEADER_SIZE = 14;

  uint64_t payload_len;
  uint32_t mask;
  uint8_t  opcode;
  bool     fin;

  enum Opcode {
    WS_CONTINUATION = 0, /* 0 -> 7 data frames */
    WS_TEXT         = 1,
    WS_BINARY       = 2,
    WS_CLOSE        = 8, /* 8 -> 15 control frames */
    WS_PING         = 9,
    WS_PONG         = 10
  };

  void set( uint64_t len, uint32_t msk,  uint8_t op,  bool fn ) {
    this->payload_len = len;
    this->mask        = msk;
    this->opcode      = op;
    this->fin         = fn;
  }
  uint32_t hdr_size( void ) const {
    uint32_t sz = ( this->mask ? 4 : 0 );
    switch ( this->opcode ) {
      case WS_CLOSE: return 1;
      /*case WS_PING:
      case WS_PONG:*/
      default: return sz + ( ( this->payload_len < 126 ) ? 2 :
                             ( this->payload_len <= 0xffffU ) ? 4 : 10 );
    }
  }
  uint64_t frame_size( void ) const {
    return (uint64_t) this->hdr_size() + this->payload_len;
  }
  /* extract header and return size, 0 if not enough data, 1 is WS_CLOSE */
  uint64_t decode( const void *p,  uint64_t buflen ) {
    const uint8_t *buf = (const uint8_t *) p;
    uint64_t i = 0;
    uint8_t  x;

    if ( buflen < 1 )
      return 0;
    x = buf[ i++ ];
    this->fin = ( ( x & 0x80U ) != 0 );
    this->opcode = x & 0xfU;
    if ( this->opcode == WS_CLOSE )
      return 1;
    if ( buflen < i + 1 )
      return 0;

    x = buf[ i++ ];
    this->mask = ( ( x & 0x80U ) != 0 ? 1 : 0 );

    switch ( x & 0x7f ) {
      default:
        this->payload_len = x & 0x7fU;
        break;
      case 126:
        if ( buflen < i + 2 )
          return 0;
        this->payload_len = kv_bswap16( *(const uint16_t *) &buf[ i ] );
        i += 2;
        break;
      case 127:
        if ( buflen < i + 8 )
          return 0;
        this->payload_len = kv_bswap64( *(const uint64_t *) &buf[ i ] );
        i += 8;
        break;
    }
    if ( this->mask ) {
      if ( buflen < i + 4 )
        return 0;
      this->mask = *(const uint32_t *) &buf[ i ];
      i += 4;
    }
    return i;
  }
  /* xor mask bits */
  void apply_mask( void *p ) {
    uint32_t bits[ 64 / 4 ];
    size_t j;
    for ( size_t i = 0; i < this->payload_len; i += j ) {
      if ( i + sizeof( bits ) > this->payload_len )
        j = this->payload_len - i;
      else
        j = sizeof( bits );
      ::memcpy( bits, &((uint8_t *) p)[ i ], j );
      for ( size_t k = 0; k * sizeof( bits[ 0 ] ) < j; k++ )
        bits[ k ] ^= this->mask;
      ::memcpy( &((uint8_t *) p)[ i ], bits, j );
    }
  }
  /* encode header and return size */
  uint64_t encode( void *p ) const {
    uint64_t  off = 0;
    uint8_t * buf = (uint8_t *) p;
    buf[ off ] = ( this->fin ? 0x80 : 0 ) | this->opcode;
    if ( this->opcode == WS_CLOSE )
      return 1;
    if ( this->payload_len < 126 ) {
      buf[ off+1 ] = (uint8_t) this->payload_len | ( this->mask ? 0x80U : 0 );
      off += 2;
    }
    else {
      if ( this->payload_len <= 0xffffU ) {
        buf[ off+1 ] = 126 | ( this->mask ? 0x80U : 0 );
        off += 2;
      }
      else {
        buf[ off+1 ] = 127 | ( this->mask ? 0x80U : 0 );
        buf[ off+2 ] = 0; buf[ off+3 ] = 0; buf[ off+4 ] = 0;
        buf[ off+5 ] = (uint8_t) ( ( this->payload_len >> 32 ) & 0xffU );
        buf[ off+6 ] = (uint8_t) ( ( this->payload_len >> 24 ) & 0xffU );
        buf[ off+7 ] = (uint8_t) ( ( this->payload_len >> 16 ) & 0xffU );
        off += 8;
      }
      buf[ off++ ] = (uint8_t) ( ( this->payload_len >> 8 ) & 0xffU );
      buf[ off++ ] = (uint8_t) ( this->payload_len & 0xffU );
    }
    if ( this->mask != 0 ) {
      *(uint32_t *) &buf[ off ] = this->mask;
      off += 4;
    }
    return off;
  }
};

}
}
#endif
