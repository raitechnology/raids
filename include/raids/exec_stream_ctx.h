#ifndef __rai_raids__exec_stream_ctx_h__
#define __rai_raids__exec_stream_ctx_h__

#include <raimd/md_stream.h>

namespace rai {
namespace ds {

struct ExecStreamCtx {
  RedisExec      & exec;
  kv::KeyCtx     & kctx;
  EvKeyCtx       & ctx;
  md::StreamData * x,
                   tmp[ 2 ];
  int              n;

  ExecStreamCtx( RedisExec &e,  EvKeyCtx &c )
    : exec( e ), kctx( e.kctx ), ctx( c ), x( 0 ), n( 0 ) {
    c.state |= EKS_NO_UPDATE;
  }

  kv::KeyStatus get_key_read( void )  { return this->do_key_fetch( true ); }
  kv::KeyStatus get_key_write( void ) { return this->do_key_fetch( false ); }

  kv::KeyStatus do_key_fetch( bool readonly ) {
    kv::KeyStatus status = this->exec.exec_key_fetch( this->ctx, readonly );
    if ( status == KEY_OK ) {
      if ( this->ctx.type != md::MD_STREAM ) {
        if ( this->ctx.type == md::MD_NODATA )
          return readonly ? KEY_NOT_FOUND : KEY_IS_NEW;
        return KEY_NO_VALUE;
      }
    }
    return status;
  }

  bool create( size_t count,  size_t ndata ) {
    md::StreamGeom geom;
    geom.add( NULL, ndata, count, 8, 1, 8, 1 );
    void * data    = NULL;
    size_t datalen = geom.asize();
    if ( (this->ctx.kstatus = this->kctx.resize( &data, datalen )) == KEY_OK ) {
      this->x = geom.make_new( &this->tmp[ this->n++%2 ], data );
      this->ctx.type   = md::MD_STREAM;
      this->ctx.flags |= EKF_IS_NEW;
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool tombstone( void ) {
    this->ctx.kstatus = this->kctx.tombstone();
    return this->ctx.kstatus == KEY_OK;
  }

  bool open( void ) {
    void * data    = NULL;
    size_t datalen = 0;
    if ( (this->ctx.kstatus = this->kctx.value( &data, datalen )) == KEY_OK ) {
      this->x = md::StreamGeom::make_existing( &this->tmp[ this->n++%2 ],
                                               data, datalen );
      if ( this->x == NULL )
        return false;
      this->x->open();
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool open_readonly( void ) {
    uint8_t  lhdr[ md::LIST_HDR_OOB_SIZE ];
    void   * data    = NULL;
    size_t   datalen = 0;
    uint64_t llen    = sizeof( lhdr );

    this->ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
    if ( ctx.kstatus == KEY_OK ) {
      this->x = md::StreamGeom::make_existing( &this->tmp[ this->n++%2 ],
                                               data, datalen );
      if ( this->x == NULL )
        return false;
      this->x->open( lhdr, llen );
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool validate_value( void ) {
    if ( this->ctx.kstatus == KEY_NOT_FOUND )
      return true;
    this->ctx.kstatus = this->kctx.validate_value();
    return this->ctx.kstatus == KEY_OK;
  }

  bool realloc( size_t add_str,  size_t add_grp,  size_t add_pnd ) {
    kv::MsgCtxBuf  tmpm;
    kv::MsgCtx   * msg;
    void         * data = NULL;
    md::StreamGeom geom;
    size_t         datalen;

    geom.add( this->x, add_str, add_str?1:0, add_grp, add_grp?1:0,
                       add_pnd, add_pnd?1:0 );
    datalen = geom.asize();

    msg = new ( tmpm ) kv::MsgCtx( this->kctx );
    msg->set_key( this->ctx.kbuf );
    msg->set_hash( this->ctx.hash1, this->ctx.hash2 );
    this->ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );

    /* copy old into new */
    if ( this->ctx.kstatus == KEY_OK ) {
      md::StreamData * old_x = this->x;
      this->x = geom.make_new( &this->tmp[ this->n++%2 ], data );
      old_x->copy( *this->x );
      this->ctx.kstatus = this->kctx.load( *msg );
    }
    return this->ctx.kstatus == KEY_OK;
  }
};

}
}
#endif
