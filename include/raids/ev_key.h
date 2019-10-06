#ifndef __rai_raids__ev_key_h__
#define __rai_raids__ev_key_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/prio_queue.h>

namespace rai {
namespace ds {

struct EvSocket;

struct EvKeyTempResult {
  size_t  mem_size, /* alloc size */
          size;     /* data size */
  uint8_t type;     /* type of data */
  char * data( size_t x ) const {
    return &((char *) (void *) &this[ 1 ])[ x ];
  }
};

enum EvKeyState { /* status for exec_key_continue(), memcached_key_continue() */
  EK_SUCCESS  = 20, /* statement finished */
  EK_DEPENDS  = 21, /* key depends on another */
  EK_CONTINUE = 22  /* more keys to process */
};

enum EvKeyFlags { /* bits for EvKeyCtx::flags */
  EKF_IS_READ_ONLY   = 1, /* key is read only (no write access) */
  EKF_IS_NEW         = 2, /* key doesn't exist */
  EKF_KEYSPACE_FWD   = 4, /* operation resulted in a keyspace modification */
  EKF_KEYEVENT_FWD   = 8, /* operation resulted in a keyevent modification */
  EKF_KEYSPACE_EVENT = 12, /* both the above bits together */
  EKF_KEYSPACE_DEL   = 16, /* keyspace event (pop, srem, etc) caused a key del */

#define EKF_TYPE_SHIFT 5
  EKF_KEYSPACE_STRING = ( 1 << EKF_TYPE_SHIFT ), /* 32 */
  EKF_KEYSPACE_LIST   = ( 2 << EKF_TYPE_SHIFT ), /* 64 */
  EKF_KEYSPACE_HASH   = ( 3 << EKF_TYPE_SHIFT ), /* 96 */
  EKF_KEYSPACE_SET    = ( 4 << EKF_TYPE_SHIFT ), /* 128 */
  EKF_KEYSPACE_ZSET   = ( 5 << EKF_TYPE_SHIFT ), /* 160 */
  EKF_KEYSPACE_GEO    = ( 6 << EKF_TYPE_SHIFT ), /* 192 */
  EKF_KEYSPACE_HLL    = ( 7 << EKF_TYPE_SHIFT )  /* 224 */
};

struct EvKeyCtx {
  void * operator new( size_t, void *ptr ) { return ptr; }

  static bool is_greater( EvKeyCtx *ctx,  EvKeyCtx *ctx2 );
  kv::HashTab     & ht;
  EvSocket        * owner;  /* parent connection */
  uint64_t          hash1,  /* 128 bit hash of key */
                    hash2;
  int64_t           ival;   /* if it returns int */
  EvKeyTempResult * part;   /* saved data for key */
  const int         argn;   /* which arg number of command */
  int               status; /* result of exec for this key */
  kv::KeyStatus     kstatus;/* result of key lookup */
  uint8_t           dep,    /* depends on another key */
                    type,   /* value type, string, list, hash, etc */
                    flags;  /* is new, is read only */
  kv::KeyFragment   kbuf;   /* key material, extends past structure */

  EvKeyCtx( kv::HashTab &h,  EvSocket *own,  const char *key,  size_t keylen,
            const int n,  const uint64_t seed,  const uint64_t seed2 )
     : ht( h ), owner( own ), hash1( seed ), hash2( seed2 ), ival( 0 ),
       part( 0 ), argn( n ), status( 0 ), kstatus( KEY_OK ), dep( 0 ),
       type( 0 ), flags( EKF_IS_READ_ONLY ) {
    uint16_t * p = (uint16_t *) (void *) this->kbuf.u.buf,
             * k = (uint16_t *) (void *) key,
             * e = (uint16_t *) (void *) &key[ keylen ];
    do {
      *p++ = *k++;
    } while ( k < e );
    this->kbuf.u.buf[ keylen ] = '\0'; /* string keys terminate with nul char */
    this->kbuf.keylen = keylen + 1;
    this->kbuf.hash( this->hash1, this->hash2 );
  }
  static size_t size( size_t keylen ) {
    return sizeof( EvKeyCtx ) + keylen; /* alloc size of *this */
  }
  const char *get_type_str( void ) const;
  EvKeyCtx *prefetch( kv::KeyCtx &kctx ) {
    kctx.set_key( this->kbuf );
    kctx.set_hash( this->hash1, this->hash2 );
    kctx.prefetch( 2 );
    return this;
  }
  bool is_new( void ) const {
    return ( this->flags & EKF_IS_NEW ) != 0;
  }
  bool is_read_only( void ) const {
    return ( this->flags & EKF_IS_READ_ONLY ) != 0;
  }
};

inline bool
EvKeyCtx::is_greater( EvKeyCtx *ctx,  EvKeyCtx *ctx2 )
{
  if ( ctx->dep > ctx2->dep ) /* fetch other keys first if depends on another */
    return true;
  if ( ctx->dep == ctx2->dep ) {
    return ctx->ht.hdr.ht_mod( ctx->hash1 ) > /* if ht position greater */
           ctx->ht.hdr.ht_mod( ctx2->hash1 );
  }
  return false;
}

struct EvPrefetchQueue :
    public kv::PrioQueue<EvKeyCtx *, EvKeyCtx::is_greater> {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  static EvPrefetchQueue *create( void ) {
    void *p = ::malloc( sizeof( EvPrefetchQueue ) );
    return new ( p ) EvPrefetchQueue();
  }
};

}
}

#endif
