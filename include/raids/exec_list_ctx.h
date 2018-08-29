#ifndef __rai_raids__exec_list_ctx_h__
#define __rai_raids__exec_list_ctx_h__

namespace rai {
namespace ds {

template <class LIST_CLASS, MDType LIST_TYPE>
struct ExecListCtx {
  RedisExec   & exec;
  kv::KeyCtx  & kctx;
  RedisKeyCtx & ctx;
  LIST_CLASS  * x,
                tmp[ 2 ];
  size_t        retry;
  int           n;

  ExecListCtx( RedisExec &e,  RedisKeyCtx &c )
    : exec( e ), kctx( e.kctx ), ctx( c ), x( 0 ), retry( 0 ), n( 0 ) {}

  kv::KeyStatus get_key_read( void )  { return this->do_key_fetch( true ); }
  kv::KeyStatus get_key_write( void ) { return this->do_key_fetch( false ); }

  kv::KeyStatus do_key_fetch( bool readonly ) {
    kv::KeyStatus status = this->exec.exec_key_fetch( this->ctx, readonly );
    if ( status == KEY_OK ) {
      if ( this->ctx.type != LIST_TYPE ) {
        if ( this->ctx.type == MD_NODATA )
          return readonly ? KEY_NOT_FOUND : KEY_IS_NEW;
        return KEY_NO_VALUE;
      }
    }
    return status;
  }

  bool create( size_t count,  size_t ndata ) {
    void * data    = NULL;
    size_t datalen = LIST_CLASS::alloc_size( count, ndata );
    if ( (this->ctx.kstatus = this->kctx.resize( &data, datalen )) == KEY_OK ) {
      this->x = new ( &this->tmp[ this->n++%2 ] ) LIST_CLASS( data, datalen );
      this->x->init( count, ndata );
      this->ctx.type = LIST_TYPE;
      this->ctx.is_new = true;
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool open( void ) {
    void * data    = NULL;
    size_t datalen = 0;
    if ( (this->ctx.kstatus = this->kctx.value( &data, datalen )) == KEY_OK ) {
      this->x = new ( &this->tmp[ this->n++%2 ] ) LIST_CLASS( data, datalen );
      this->x->open();
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool open_readonly( void ) {
    uint8_t  lhdr[ LIST_HDR_OOB_SIZE ];
    void   * data    = NULL;
    size_t   datalen = 0;
    uint64_t llen    = sizeof( lhdr );

    this->ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
    if ( ctx.kstatus == KEY_OK ) {
      this->x = new ( &this->tmp[ this->n++%2 ] ) LIST_CLASS( data, datalen );
      this->x->open( lhdr, llen );
    }
    return this->ctx.kstatus == KEY_OK;
  }

  bool validate_value( void ) {
    this->ctx.kstatus = this->kctx.validate_value();
    return this->ctx.kstatus == KEY_OK;
  }

  bool realloc( size_t addsz ) {
    kv::MsgCtxBuf tmpm;
    kv::MsgCtx  * msg;
    void        * data    = NULL;
    size_t        count   = 2,
                  ndata   = addsz + this->retry,
                  datalen = this->x->resize_size( count, ndata );

    this->retry += 16; /* in case of miscalculation */
    msg = new ( tmpm ) kv::MsgCtx( this->kctx.ht, this->kctx.thr_ctx );
    msg->set_key( this->ctx.kbuf );
    msg->set_hash( this->ctx.hash1, this->ctx.hash2 );
    this->ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );

    /* copy old into new */
    if ( this->ctx.kstatus == KEY_OK ) {
      LIST_CLASS * old_x = this->x;
      this->x = new ( &this->tmp[ this->n++%2 ] ) LIST_CLASS( data, datalen );
      this->x->init( count, ndata );
      old_x->copy( *this->x );
      this->ctx.kstatus = this->kctx.load( *msg );
    }
    return this->ctx.kstatus == KEY_OK;
  }
};

}
}
#endif
