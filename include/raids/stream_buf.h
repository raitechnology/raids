#ifndef __rai_raids__stream_buf_h__
#define __rai_raids__stream_buf_h__

#include <sys/uio.h>
#include <raikv/work.h>

namespace rai {
namespace ds {

struct StreamBuf {
  /* buffering items */
  struct BufList {
    BufList * next;
    size_t    off,
              used;
    char    * buf( size_t x ) const {
      return &((char *) (void *) &this[ 1 ])[ this->off + x ];
    }
  };
  /* allocate a BufList */
  BufList *alloc_buf_list( BufList *&hd,  BufList *tl,  size_t len,
                           size_t pad = 0 );
  /* queue of items, usually an array */
  struct BufQueue {
    StreamBuf & strm;
    BufList   * hd,
              * tl;
    char      * bufp;
    size_t      buflen,
                used,
                total;
    BufQueue( StreamBuf &s ) : strm( s ), hd( 0 ), tl( 0 ), bufp( 0 ),
                               buflen( 0 ), used( 0 ), total( 0 ) {}
    /* put used into this->tl */
    size_t finish_tail( void ) {
      if ( this->tl != NULL )
        this->tl->used += this->used;
      this->total += this->used;
      this->used   = 0;
      return this->total;
    }
    /* allocate a new BufList that fits at least len and append to list */
    bool append_buf( size_t len );
    /* a nil string: $-1\r\n or *-1\r\n if is_null true */
    size_t append_nil( bool is_null = false );
    /* one string item, appended with decorations: $<strlen>\r\n<string>\r\n */
    size_t append_string( const void *str,  size_t len,  const void *str2=0,
                          size_t len2=0 );
    /* an int, appended with decorations: :<val>\r\n */
    size_t append_uint( uint64_t val );
    /* arbitrary bytes, no formatting */
    size_t append_bytes( const void *buf,  size_t len );
    /* make array: [item1, item2, ...], prepends decorations *<arraylen>\r\n */
    size_t prepend_array( size_t nitems );
    /* cursor is two arrays: [cursor,[items]] */
    size_t prepend_cursor_array( size_t curs,  size_t nitems );
  };

  static const size_t BUFSIZE = 1024;
  size_t wr_pending;  /* how much is in send buffers total */
  char * out_buf;     /* current buffer to fill, up to BUFSIZE */
  size_t sz,          /* sz bytes in out_buf */
         idx,         /* head data in iov[] to send */
         woff;        /* offset of iov[] sent, tail, idx >= woff */
  bool   alloc_fail;  /* if alloc send buffers below failed */

  //static const size_t vlen = 4096;
  kv::WorkAllocT< 4096 > tmp;
  struct iovec iovbuf[ 2 * 1024 ], /* vec of send buffers */
             * iov;
  size_t       vlen;

  StreamBuf() { this->reset(); }

  void release( void ) {
    this->reset();
    this->tmp.release_all();
  }

  size_t pending( void ) const { /* how much is read to send */
    return this->wr_pending + this->sz;
  }
  void reset( void ) {
    this->iov        = this->iovbuf;
    this->vlen       = sizeof( this->iovbuf ) / sizeof( this->iovbuf[ 0 ] );
    this->wr_pending = 0;
    this->out_buf    = NULL;
    this->sz         = 0;
    this->idx        = 0;
    this->woff       = 0;
    this->alloc_fail = false;
    this->tmp.reset();
  }
  void expand_iov( void ) {
    void *p;
    p = this->alloc_temp( sizeof( struct iovec ) * this->vlen * 2 );
    ::memcpy( p, this->iov, sizeof( struct iovec ) * this->vlen );
    this->iov   = (struct iovec *) p;
    this->vlen *= 2;
  }
  void flush( void ) { /* move work buffer to send iov */
    if ( this->idx == this->vlen )
      this->expand_iov();
    this->iov[ this->idx ].iov_base  = this->out_buf;
    this->iov[ this->idx++ ].iov_len = this->sz;

    this->wr_pending += this->sz;
    this->out_buf     = NULL;
    this->sz          = 0;
  }
  void append_iov( void *p,  size_t amt ) {
    if ( this->out_buf != NULL && this->sz > 0 )
      this->flush();
    if ( this->idx == this->vlen )
      this->expand_iov();
    this->iov[ this->idx ].iov_base  = p;
    this->iov[ this->idx++ ].iov_len = amt;
    this->wr_pending += amt;
  }
  void append_iov( BufQueue &q ) {
    if ( q.hd != NULL ) {
      BufList *p = q.hd;
      if ( p->next != NULL || this->out_buf == NULL ||
           this->sz + p->used > BUFSIZE ) {
        for (;;) {
          this->append_iov( p->buf( 0 ), p->used );
          if ( (p = p->next) == NULL )
            break;
        }
      }
      else {
        this->append( p->buf( 0 ), p->used );
      }
    }
  }
  char *alloc_temp( size_t amt ) {
    char *spc = (char *) this->tmp.alloc( amt );
    if ( spc == NULL ) {
      this->alloc_fail = true;
      return NULL;
    }
    return spc;
  }
  char *alloc( size_t amt ) {
    if ( this->out_buf != NULL && this->sz + amt > BUFSIZE )
      this->flush();
    if ( this->out_buf == NULL ) {
      this->out_buf = (char *) this->alloc_temp( amt < BUFSIZE ? BUFSIZE : amt );
      if ( this->out_buf == NULL )
        return NULL;
    }
    return &this->out_buf[ this->sz ];
  }
  void append( const void *p,  size_t amt ) {
    char *b = this->alloc( amt );
    if ( b != NULL ) {
      ::memcpy( b, p, amt );
      this->sz += amt;
    }
    else {
      this->alloc_fail = true;
    }
  }
};

}
}
#endif
