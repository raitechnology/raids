#ifndef __rai_raids__redis_transaction_h__
#define __rai_raids__redis_transaction_h__

#include <raikv/work.h>
#include <raikv/dlinklist.h>
#include <raikv/ev_key.h>

namespace rai {
namespace ds {

struct RedisMsg;

struct RedisMultiMsg {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMultiMsg * next,
                * back;
  RedisMsg      * msg;  /* pending multi exec msg */
  kv::EvKeyCtx ** keys;
  kv::KeyCtx   ** kptr;
  kv::KeyCtx    * karr;
  size_t          key_count,
                  n;
  RedisCmd        cmd;

  RedisMultiMsg( RedisMsg *m,  size_t kc,  RedisCmd c )
    : next( 0 ), back( 0 ), msg( m ), keys( 0 ), kptr( 0 ), karr( 0 ),
      key_count( kc ), n( 0 ), cmd( c ) {}
};

struct RedisWatchList {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisWatchList * next,
                 * back;
  uint64_t         serial, /* last acquire serial */
                   pos;    /* the location of the watch */
  kv::EvKeyCtx     key;

  RedisWatchList( uint64_t sn,  uint64_t p,  kv::EvKeyCtx &ctx )
    : next( 0 ), back( 0 ), serial( sn ), pos( p ), key( ctx ) {}
};

struct RedisMultiExec {
  static RedisMultiExec *make( void ) {
     void * p = kv::aligned_malloc( sizeof( RedisMultiExec ) );
    if ( p == NULL )
      return NULL;
    return new ( p ) RedisMultiExec();
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { kv::aligned_free( ptr ); }
  kv::WorkAllocT< 8192 >        wrk;          /* space to use for msgs below */
  RedisMultiMsg                 watch_msg;
  kv::DLinkList<RedisMultiMsg>  msg_list;     /* msgs in the multi section */
  kv::DLinkList<RedisWatchList> watch_list;   /* watched keys */
  RedisMultiMsg               * mm_iter,      /* currently executing msg */
                              * mm_lock;      /* locked msgs */
  kv::KeyCtx                  * wr_kctx;      /* if writing to key */
  size_t                        msg_count,    /* how many msgs queued */
                                watch_count;  /* how many watches */
  uint64_t                      filter[ 4 ];  /* filter to identify dup keys */
  bool                          multi_start,  /* if in MULTI before EXEC */
                                multi_abort;  /* if in MULTI cmd has error */

  RedisMultiExec() : watch_msg( NULL, 0, WATCH_CMD ), mm_iter( 0 ),
                     mm_lock( 0 ), wr_kctx( 0 ), msg_count( 0 ),
                     watch_count( 0 ), multi_start( false ),
                     multi_abort( false ) {
    this->zero_filter();
  }
  void zero_filter( void ) {
    for ( size_t i = 0; i < 4; i++ )
      this->filter[ i ] = 0;
  }
  bool test_set_filter( uint64_t h ) {
    uint64_t mask = (uint64_t) 1 << ( h % 64 ),
             off  = ( h / 64 ) % 4;
    bool     is_set = ( this->filter[ off ] & mask ) != 0;
    this->filter[ off ] |= mask;
    return is_set;
  }
  kv::KeyCtx   * get_dup_kctx( kv::EvKeyCtx &ctx ) const noexcept;
  kv::EvKeyCtx * get_dup_key( kv::EvKeyCtx &ctx,
                              bool post_exec ) const noexcept;
};

}
}

#endif
