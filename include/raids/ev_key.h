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
  char * data( size_t x ) const {
    return &((char *) (void *) &this[ 1 ])[ x ];
  }
};

enum EvKeyState { /* status for exec_key_continue(), memcached_key_continue() */
  EK_SUCCESS  = 20, /* statement finished */
  EK_DEPENDS  = 21, /* key depends on another */
  EK_CONTINUE = 22  /* more keys to process */
};
/* the key operations (ex. set, lpush, del, xadd, etc) set these flags --
 * when something is subscribed to a keyspace event, it causes a publish to
 * occur for the event if the bits subscribed & flags != 0 */
enum EvKeyFlags { /* bits for EvKeyCtx::flags */
  EKF_IS_READ_ONLY    = 1,     /* key is read only (no write access) */
  EKF_IS_NEW          = 2,     /* key doesn't exist */
  EKF_IS_SAVED_CONT   = 4,     /* key has saved data on continuation */
  EKF_IS_EXPIRED      = 8,     /* a key read oper caused expired data */
  EKF_KEYSPACE_FWD    = 0x10,  /* operation resulted in a keyspace mod */
  EKF_KEYEVENT_FWD    = 0x20,  /* operation resulted in a keyevent mod */
  EKF_KEYSPACE_EVENT  = 0x30,  /* when a key is updated, this is set */
  EKF_KEYSPACE_DEL    = 0x40,  /* keyspace event (pop, srem, etc) -> key del */
  EKF_KEYSPACE_TRIM   = 0x80,  /* xadd with maxcnt caused a trim event */
  EKF_LISTBLKD_NOT    = 0x100, /* notify a blocked list an element available */
  EKF_ZSETBLKD_NOT    = 0x200, /* notify a blocked zset */
  EKF_STRMBLKD_NOT    = 0x400, /* notify a blocked stream */
  EKF_MONITOR         = 0x800, /* command monitor is enabled */

#define EKF_TYPE_SHIFT 12
  /* the type of the key that event occurred */
  EKF_KEYSPACE_STRING = ( 1 << EKF_TYPE_SHIFT ), /* 4092 */
  EKF_KEYSPACE_LIST   = ( 2 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_HASH   = ( 3 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_SET    = ( 4 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_ZSET   = ( 5 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_GEO    = ( 6 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_HLL    = ( 7 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_STREAM = ( 8 << EKF_TYPE_SHIFT )  /* 32768 */
  /* must fit into 16 bits */
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
  uint16_t          flags;  /* is new, is read only */
  kv::KeyStatus     kstatus;/* result of key lookup */
  uint8_t           dep,    /* depends on another key */
                    type;   /* value type, string, list, hash, etc */
  kv::KeyFragment   kbuf;   /* key material, extends past structure */

  EvKeyCtx( kv::HashTab &h,  EvSocket *own,  const char *key,  size_t keylen,
            const int n,  const uint64_t seed,  const uint64_t seed2 )
     : ht( h ), owner( own ), hash1( seed ), hash2( seed2 ), ival( 0 ),
       part( 0 ), argn( n ), status( 0 ), flags( EKF_IS_READ_ONLY ),
       kstatus( KEY_OK ), dep( 0 ), type( 0 ) {
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
  EvKeyCtx *set( kv::KeyCtx &kctx ) {
    kctx.set_key( this->kbuf );
    kctx.set_hash( this->hash1, this->hash2 );
    return this;
  }
  void prefetch( kv::HashTab &ht,  bool for_read ) {
    ht.prefetch( this->hash1, for_read );
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
