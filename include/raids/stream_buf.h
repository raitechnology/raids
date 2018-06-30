#ifndef __rai_raids__stream_buf_h__
#define __rai_raids__stream_buf_h__

#include <sys/uio.h>
#include <raikv/work.h>

namespace rai {
namespace ds {

struct StreamBuf {
  static const size_t BUFSIZE = 1024;
  size_t wr_pending;  /* how much is in send buffers */
  char * out_buf;
  size_t sz,
         idx,         /* data in iov[] to send */
         woff;        /* offset of iov[] sent */
  bool   alloc_fail;

  static const size_t vlen = 4096;
  struct iovec   iov[ vlen ]; /* vec of send buffers */
  kv::WorkAllocT< 4096 > tmp;

  StreamBuf() { this->reset(); }

  void release( void ) {
    this->reset();
    this->tmp.release_all();
  }

  void reset( void ) {
    this->wr_pending = 0;
    this->out_buf    = NULL;
    this->sz         = 0;
    this->idx        = 0;
    this->woff       = 0;
    this->alloc_fail = false;
    this->tmp.reset();
  }
  void flush( void ) {
    this->iov[ this->idx ].iov_base  = this->out_buf;
    this->iov[ this->idx++ ].iov_len = this->sz;

    this->wr_pending += this->sz;
    this->out_buf     = NULL;
    this->sz          = 0;
  }
  void append_iov( void *p,  size_t sz ) {
    if ( this->out_buf != NULL )
      this->flush();
    this->iov[ this->idx ].iov_base  = p;
    this->iov[ this->idx++ ].iov_len = sz;
    this->wr_pending += sz;
  }
  char *alloc_temp( size_t sz ) {
    char *spc = (char *) this->tmp.alloc( sz );
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