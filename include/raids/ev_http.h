#ifndef __rai_raids__ev_http_h__
#define __rai_raids__ev_http_h__

#include <raids/ev_tcp.h>
#include <raids/redis_exec.h>
#include <raids/term.h>

namespace rai {
namespace ds {

struct EvHttpListen : public EvTcpListen {
  EvListenOps ops;
  EvHttpListen( EvPoll &p ) : EvTcpListen( p, this->ops ) {}
  virtual void accept( void );
  int listen( const char *ip,  int port ) {
    return this->EvTcpListen::listen( ip, port, "http-listen" );
  }
};

struct EvPrefetchQueue;

struct EvHttpServiceOps : public EvConnectionOps {
  bool client_matches( PeerData &pd,  PeerMatchArgs &ka );
  virtual int client_list( PeerData &pd,  PeerMatchArgs &ka,
                           char *buf,  size_t buflen );
  virtual bool client_kill( PeerData &pd,  PeerMatchArgs &ka );
};

struct EvHttpService : public EvConnection, public RedisExec {
  char           * wsbuf;   /* decoded websocket frames */
  size_t           wsoff,   /* start offset of wsbuf */
                   wslen,   /* length of wsbuf used */
                   wsalloc, /* sizeof wsbuf alloc */
                   wsmsgcnt;
  uint64_t         websock_off;  /* on output pointer that frames msgs with ws */
  int              term_int;
  bool             is_not_found, /* a 404 page closes socket */
                   is_using_term;
  Term             term;
  EvHttpServiceOps ops;
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvHttpService( EvPoll &p ) : EvConnection( p, EV_HTTP_SOCK, this->ops ),
    RedisExec( *p.map, p.ctx_id, *this, p.sub_route, *this ),
    wsbuf( 0 ), wsoff( 0 ), wslen( 0 ), websock_off( 0 ),
    term_int( 0 ), is_not_found( false ), is_using_term( false ) {}
  void initialize_state( void ) {
    this->wsbuf         = NULL;
    this->wsoff         = 0;
    this->wslen         = 0;
    this->wsalloc       = 0;
    this->wsmsgcnt      = 0;
    this->websock_off   = 0;
    this->term_int      = 0;
    this->is_not_found  = false;
    this->is_using_term = false;
    this->term.zero();
  }
  void process( void );
  bool on_msg( EvPublish &pub );
  bool timer_expire( uint64_t tid,  uint64_t event_id );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  bool flush_term( void );
  void write( void ); /* override write() in EvConnection */
  bool frame_websock( void );
  bool frame_websock2( void );
  bool send_file( const char *get,  size_t hdrlen );
  bool send_ws_upgrade( const char *wsver, const char *wskey,
                        size_t wskeylen,  const char *wspro );
  bool send_ws_pong( const char *payload,  size_t len );
  size_t recv_wsframe( char *start,  char *end );
  void release( void );
  void push_free_list( void );
  void pop_free_list( void );
  void process_close( void ) {}
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
        this->payload_len = __builtin_bswap16( *(const uint16_t *) &buf[ i ] );
        i += 2;
        break;
      case 127:
        if ( buflen < i + 8 )
          return 0;
        this->payload_len = __builtin_bswap64( *(const uint64_t *) &buf[ i ] );
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
