#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <raids/stream_buf.h>
#include <raids/int_str.h>

using namespace rai;
using namespace ds;

char *
StreamBuf::alloc_temp( size_t amt ) noexcept
{
  char *spc = (char *) this->tmp.alloc( amt );
  if ( spc == NULL ) {
    this->alloc_fail = true;
    return NULL;
  }
  return spc;
}

void
StreamBuf::expand_iov( void ) noexcept
{
  void *p;
  p = this->alloc_temp( sizeof( struct iovec ) * this->vlen * 2 );
  ::memcpy( p, this->iov, sizeof( struct iovec ) * this->vlen );
  this->iov   = (struct iovec *) p;
  this->vlen *= 2;
}

static inline size_t
crlf( char *b,  size_t i ) {
  b[ i ] = '\r'; b[ i + 1 ] = '\n'; return i + 2;
}

StreamBuf::BufList *
StreamBuf::alloc_buf_list( BufList *&hd,  BufList *&tl,  size_t len,
                           size_t pad ) noexcept
{
  BufList *p = (BufList *) this->alloc_temp( sizeof( BufList ) + len + pad );
  if ( p == NULL )
    return NULL;
  if ( tl != NULL )
    tl->next = p;
  else
    hd = p;
  tl = p;
  p->next   = NULL;
  p->off    = pad;
  p->used   = 0;
  p->buflen = len + pad;
  return p;
}

StreamBuf::BufList *
StreamBuf::BufQueue::append_buf( size_t len ) noexcept
{
  size_t pad = ( this->hd == NULL ) ? 48 : 0,
         alsz = 928 - pad;
  if ( alsz < len )
    alsz = len;
  return this->strm.alloc_buf_list( this->hd, this->tl, alsz, pad );
}

size_t
StreamBuf::BufQueue::append_string( const void *str,  size_t len,
                                    const void *str2,  size_t len2 ) noexcept
{
  size_t itemlen = len + len2,
         d       = uint_digits( itemlen );
  BufList * p = this->get_buf( itemlen + d + 5 );

  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used++ ] = '$';
  p->used += uint_to_str( itemlen, &bufp[ p->used ], d );
  p->used = crlf( bufp, p->used );
  ::memcpy( &bufp[ p->used ], str, len );
  if ( len2 > 0 )
    ::memcpy( &bufp[ p->used + len ], str2, len2 );
  p->used = crlf( bufp, p->used + len + len2 );

  return p->used;
}

size_t
StreamBuf::BufQueue::append_nil( bool is_null ) noexcept
{
  BufList * p = this->get_buf( 5 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used ]   = ( is_null ? '*' : '$' );
  bufp[ p->used+1 ] = '-';
  bufp[ p->used+2 ] = '1';
  p->used = crlf( bufp, p->used + 3 );

  return p->used;
}

size_t
StreamBuf::BufQueue::append_zero_array( void ) noexcept
{
  BufList * p = this->get_buf( 5 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used ]   = '*';
  bufp[ p->used+1 ] = '0';
  p->used = crlf( bufp, p->used + 2 );

  return p->used;
}

size_t
StreamBuf::BufQueue::append_bytes( const void *buf,  size_t len ) noexcept
{
  BufList * p = this->get_buf( len );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  ::memcpy( &bufp[ p->used ], buf, len );
  p->used += len;
  return p->used;
}

size_t
StreamBuf::BufQueue::append_uint( uint64_t val ) noexcept
{
  size_t d = uint_digits( val );
  BufList * p = this->get_buf( d + 3 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used++ ] = ':';
  p->used += uint_to_str( val, &bufp[ p->used ], d );
  p->used = crlf( bufp, p->used );
  return p->used;
}

size_t
StreamBuf::BufQueue::prepend_array( size_t nitems ) noexcept
{
  size_t    itemlen = uint_digits( nitems ),
                 /*  '*'   4      '\r\n' (nitems = 1234) */
            len     = 1 + itemlen + 2;
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
    p->off    = 0;
    p->used   = len;
    p->buflen = len;
  }
  char * bufp = p->buf( 0 );
  bufp[ 0 ] = '*';
  uint_to_str( nitems, &bufp[ 1 ], itemlen );
  crlf( bufp, len - 2 );

  if ( p != this->hd ) {
    p->next = this->hd;
    this->hd = p;
    if ( this->tl == NULL )
      this->tl = p;
  }

  return p->used;
}

size_t
StreamBuf::BufQueue::prepend_cursor_array( size_t curs,
                                           size_t nitems ) noexcept
{
  size_t    curslen = uint_digits( curs ),
            clenlen = uint_digits( curslen ),
            itemlen = uint_digits( nitems ),
            len     = /* '*2\r\n$'    1    '\r\n'  0     '\r\n' (curs=0) */
                           5      + clenlen + 2 + curslen + 2 +
                      /* '*'    4     '\r\n'  (nitems = 1234) */
                          1 + itemlen + 2,
            i;
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
    p->off    = 0;
    p->used   = len;
    p->buflen = len;
  }
  char * bufp = p->buf( 0 );
  bufp[ 0 ] = '*';
  bufp[ 1 ] = '2';
  crlf( bufp, 2 );
  bufp[ 4 ] = '$';
  i  = 5 + uint_to_str( curslen, &bufp[ 5 ], clenlen );
  i  = crlf( bufp, i );
  i += uint_to_str( curs, &bufp[ i ], curslen );
  i  = crlf( bufp, i );
  bufp[ i ] = '*';
  i += 1 + uint_to_str( nitems, &bufp[ 1 + i ], itemlen );
  crlf( bufp, i );

  if ( p != this->hd ) {
    p->next = this->hd;
    this->hd = p;
    if ( this->tl == NULL )
      this->tl = p;
  }

  return p->used;
}
