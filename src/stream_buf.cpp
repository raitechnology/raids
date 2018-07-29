#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <raids/stream_buf.h>
#include <raids/redis_msg.h>

using namespace rai;
using namespace ds;

static inline size_t
crlf( char *b,  size_t i ) {
  b[ i ] = '\r'; b[ i + 1 ] = '\n'; return i + 2;
}

StreamBuf::BufList *
StreamBuf::alloc_buf_list( BufList *&hd,  BufList *tl,  size_t len,
                           size_t pad )
{
  BufList *p = (BufList *) this->alloc_temp( sizeof( BufList ) + len + pad );
  if ( p == NULL )
    return NULL;
  if ( tl != NULL )
    tl->next = p;
  else
    hd = p;
  p->next = NULL;
  p->off  = pad;
  p->used = 0;
  return p;
}

bool
StreamBuf::BufQueue::append_buf( size_t len )
{
  size_t pad = ( this->hd == NULL ) ? 48 : 0,
         alsz = 1000 - pad;

  if ( alsz < (size_t) len )
    alsz = len;
  BufList * p = this->strm.alloc_buf_list( this->hd, this->tl, alsz, pad );
  if ( p == NULL )
    return false;

  this->finish_tail();
  this->buflen = alsz;
  this->tl     = p;
  this->bufp   = p->buf( 0 );
  return true;
}

size_t
StreamBuf::BufQueue::append_string( const void *str,  size_t len,
                                    const void *str2,  size_t len2 )
{
  size_t itemlen = len + len2,
         d       = RedisMsg::uint_digits( itemlen );

  if ( itemlen + d + 5 > this->buflen - this->used )
    if ( ! this->append_buf( itemlen + d + 5 ) )
      return 0;

  this->bufp[ this->used++ ] = '$';
  this->used += RedisMsg::uint_to_str( itemlen, &this->bufp[ this->used ], d );
  this->used = crlf( this->bufp, this->used );
  ::memcpy( &this->bufp[ this->used ], str, len );
  if ( len2 > 0 )
    ::memcpy( &this->bufp[ this->used + len ], str2, len2 );
  this->used = crlf( this->bufp, this->used + len + len2 );

  return this->total + this->used;
}

size_t
StreamBuf::BufQueue::append_nil( void )
{
  if ( 5 > this->buflen - this->used )
    if ( ! this->append_buf( 5 ) )
      return 0;
  this->bufp[ this->used ] = '$';
  this->bufp[ this->used+1 ] = '-';
  this->bufp[ this->used+2 ] = '1';
  this->used = crlf( this->bufp, this->used + 3 );

  return this->total + this->used;
}

size_t
StreamBuf::BufQueue::prepend_array( size_t nitems )
{
  size_t    itemlen = RedisMsg::uint_digits( nitems ),
                 /*  '*'   4      '\r\n' (nitems = 1234) */
            len     = 1 + itemlen + 2;
  char    * hdr;
  BufList * p;
  if ( this->hd != NULL && this->hd->off >= len ) {
    p = this->hd;
    p->off  -= len;
    p->used += len;
  }
  else {
    p = (BufList *) this->strm.alloc_temp( sizeof( BufList ) + len );
    if ( p == NULL )
      return 0;
    p->off  = 0;
    p->used = len;
  }
  hdr = p->buf( 0 );
  hdr[ 0 ] = '*';
  RedisMsg::uint_to_str( nitems, &hdr[ 1 ], itemlen );
  crlf( hdr, len - 2 );

  if ( p != this->hd ) {
    p->next = this->hd;
    this->hd = p;
    if ( this->tl == NULL )
      this->tl = p;
  }
  this->total += len;

  return this->total + this->used;
}

size_t
StreamBuf::BufQueue::prepend_cursor_array( size_t curs,  size_t nitems )
{
  size_t    curslen = RedisMsg::uint_digits( curs ),
            clenlen = RedisMsg::uint_digits( curslen ),
            itemlen = RedisMsg::uint_digits( nitems ),
            len     = /* '*2\r\n$'    1    '\r\n'  0     '\r\n' (curs=0) */
                           5      + clenlen + 2 + curslen + 2 +
                      /* '*'    4     '\r\n'  (nitems = 1234) */
                          1 + itemlen + 2,
            i;
  char    * hdr;
  BufList * p;
  if ( this->hd != NULL && this->hd->off >= len ) {
    p = this->hd;
    p->off  -= len;
    p->used += len;
  }
  else {
    p = (BufList *) this->strm.alloc_temp( sizeof( BufList ) + len );
    if ( p == NULL )
      return 0;
    p->off  = 0;
    p->used = len;
  }
  hdr = p->buf( 0 );
  hdr[ 0 ] = '*';
  hdr[ 1 ] = '2';
  crlf( hdr, 2 );
  hdr[ 4 ] = '$';
  i  = 5 + RedisMsg::uint_to_str( curslen, &hdr[ 5 ], clenlen );
  i  = crlf( hdr, i );
  i += RedisMsg::uint_to_str( curs, &hdr[ i ], curslen );
  i  = crlf( hdr, i );
  hdr[ i ] = '*';
  i += 1 + RedisMsg::uint_to_str( nitems, &hdr[ 1 + i ], itemlen );
  crlf( hdr, i );

  if ( p != this->hd ) {
    p->next = this->hd;
    this->hd = p;
    if ( this->tl == NULL )
      this->tl = p;
  }
  this->total += len;

  return this->total + this->used;
}
