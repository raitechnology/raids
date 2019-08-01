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
  uint8_t           dep,    /* depends on another key */
                    type;   /* value type, string, list, hash, etc */
  int               status; /* result of exec for this key */
  kv::KeyStatus     kstatus;/* result of key lookup */
  bool              is_new, /* if the key does not exist */
                    is_read;/* if the key is read only */
  kv::KeyFragment   kbuf;   /* key material, extends past structure */

  EvKeyCtx( kv::HashTab &h,  EvSocket *own,  const char *key,  size_t keylen,
            const int n,  const uint64_t seed,  const uint64_t seed2 )
     : ht( h ), owner( own ), hash1( seed ), hash2( seed2 ), ival( 0 ),
       part( 0 ), argn( n ), dep( 0 ), type( 0 ), status( 0 ),
       kstatus( KEY_OK ), is_new( false ), is_read( true ) {
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
    kctx.prefetch( 1 );
    return this;
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
