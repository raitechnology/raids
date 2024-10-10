#ifndef __rai_raids__ws_frame_h__
#define __rai_raids__ws_frame_h__

namespace rai {
namespace ds {
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
  void apply_mask( void *p ) {
    this->apply_mask2( p, 0, this->payload_len );
  }
  /* xor mask bits */
  size_t apply_mask2( void *p,  size_t j,  size_t len ) {
    uint32_t bits[ 64 / 4 ], m = 0x01020304;
    const uint8_t * u = (uint8_t *) &m;
    size_t i = 0;
    if ( u[ 0 ] == 0x01 ) { /* if big */
      u = (uint8_t *) &this->mask;
      m = ( ( (uint32_t) u[ 0 ] << 24 ) | ( (uint32_t) u[ 1 ] << 16 ) |
            ( (uint32_t) u[ 2 ] << 8 )  |   (uint32_t) u[ 3 ] );
    }
    else {
      m = this->mask;
    }
    if ( j > 0 ) {
      j = sizeof( bits ) - j;
      if ( j > len )
        j = len;
      size_t x = sizeof( bits ) - j;
      ::memcpy( &((uint8_t *) bits)[ x ], p, j );
      for ( size_t k = 0; k < 64 / 4; k++ )
        bits[ k ] ^= m;
      ::memcpy( p, &((uint8_t *) bits)[ x ], j );
      i = j;
    }
    for ( ; i + sizeof( bits ) <= len; i += sizeof( bits ) ) {
      ::memcpy( bits, &((uint8_t *) p)[ i ], sizeof( bits ) );
      for ( size_t k = 0; k < 64 / 4; k++ )
        bits[ k ] ^= m;
      ::memcpy( &((uint8_t *) p)[ i ], bits, sizeof( bits ) );
    }
    j = len - i;
    if ( j > 0 ) {
      ::memcpy( bits, &((uint8_t *) p)[ i ], j );
      for ( size_t k = 0; k < 64 / 4; k++ )
        bits[ k ] ^= m;
      ::memcpy( &((uint8_t *) p)[ i ], bits, j );
    }
    return j;
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
      buf[ off++ ] = (uint8_t)   ( this->payload_len & 0xffU );
    }
    if ( this->mask != 0 ) {
      ::memcpy( &buf[ off ], &this->mask, 4 );
      off += 4;
    }
    return off;
  }
};

}
}
#endif
