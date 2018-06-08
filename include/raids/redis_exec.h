#ifndef __rai_raids__redis_exec_h__
#define __rai_raids__redis_exec_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/prio_queue.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/stream_buf.h>

namespace rai {
namespace ds {

enum ExecStatus {
  EXEC_OK               = 0,
  EXEC_SETUP_OK         = 1, /* key setup */
  EXEC_SEND_OK          = 2, /* send +OK */
  EXEC_SEND_NIL         = 3, /* send $-1 */
  EXEC_SEND_INT         = 4, /* send :100 */
  EXEC_SUCCESS          = 5,
  EXEC_KV_STATUS        = 6, /* kstatus != ok */
  EXEC_MSG_STATUS       = 7, /* mstatus != ok */
  EXEC_BAD_ARGS         = 8,
  EXEC_BAD_CMD          = 9,
  EXEC_QUIT             = 10, /* quit/shutdown command */
  EXEC_ALLOC_FAIL       = 11,
  EXEC_KEY_EXISTS       = 12,
  EXEC_KEY_DOESNT_EXIST = 13
};

struct EvService;
struct RedisExec;

struct RedisKeyCtx {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  bool operator>( const RedisKeyCtx *ctx2 ) const {
    return ( this->hash1 > ctx2->hash1 );
  }
  uint64_t        hash1,
                  hash2;
  RedisExec     & exec;
  EvService     * owner;
  int64_t         ival;        /* if it returns int */
  ExecStatus      status;
  kv::KeyStatus   kstatus;
  kv::KeyFragment kbuf;

  RedisKeyCtx( RedisExec &ex,  EvService *own,  const char *key,  size_t keylen,
               const uint64_t seed,  const uint64_t seed2 )
     : hash1( seed ), hash2( seed2 ), exec( ex ), owner( own ), ival( 0 ),
       status( EXEC_SETUP_OK ), kstatus( KEY_OK ) {
    uint16_t * p = (uint16_t *) (void *) this->kbuf.u.buf,
             * k = (uint16_t *) (void *) key,
             * e = (uint16_t *) (void *) &key[ keylen ];
    do {
      *p++ = *k++;
    } while ( k < e );
    this->kbuf.u.buf[ keylen ] = '\0';
    this->kbuf.keylen = keylen + 1;
    this->kbuf.hash( this->hash1, this->hash2 );
  }
  static size_t size( size_t keylen ) {
    return sizeof( RedisKeyCtx ) + keylen;
  }
  void prefetch( void );
  bool run( EvService *&own );
};

struct EvPrefetchQueue : public kv::PrioQueue<RedisKeyCtx *> {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  static EvPrefetchQueue *create( void ) {
    void *p = ::malloc( sizeof( EvPrefetchQueue ) );
    return new ( p ) EvPrefetchQueue();
  }
};

struct RedisExec {
  const uint64_t seed, seed2; /* kv map hash seeds, set when map is created */
  kv::KeyCtx     kctx;        /* current key context */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffers */
  StreamBuf    & strm;
  RedisMsg       msg;         /* current command msg */
  RedisKeyCtx  * key,
              ** keys;
  uint32_t       key_cnt, key_done;
  RedisCmd       cmd;         /* current command (GET_CMD) */
  RedisMsgStatus mstatus;
  uint16_t       flags;       /* command flags (CMD_READONLY_FLAG) */
  int            arity,       /* number of command args */
                 first,       /* first key in args */
                 last,        /* last key in args */
                 step;        /* incr between keys */
  size_t         argc;        /* count of args in cmd msg */

  RedisExec( kv::HashTab &map,  uint32_t ctx_id,  StreamBuf &s,  bool single ) :
      seed( map.hdr.hash_key_seed ), seed2( map.hdr.hash_key_seed2 ),
      kctx( map, ctx_id, NULL ), strm( s ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ) {
    if ( single ) this->kctx.set( kv::KEYCTX_IS_SINGLE_THREAD );
  }

  void release( void ) {
    this->wrk.reset();
    this->wrk.release_all();
  }
  ExecStatus exec( EvService *own,  EvPrefetchQueue *q );
  ExecStatus exec_key_setup( EvService *own,  EvPrefetchQueue *q,
                             RedisKeyCtx *&ctx,  int n );
  bool exec_key_continue( RedisKeyCtx &ctx );

  void exec_key_prefetch( RedisKeyCtx &ctx ) {
    this->key = &ctx;
    this->kctx.set_key( ctx.kbuf );
    this->kctx.set_hash( ctx.hash1, ctx.hash2 );
    this->kctx.prefetch( 2 );
  }

  /* redis_exec_string */
  ExecStatus exec_append( RedisKeyCtx &ctx );
  ExecStatus exec_bitcount( RedisKeyCtx &ctx );
  ExecStatus exec_bitfield( RedisKeyCtx &ctx );
  ExecStatus exec_bitop( RedisKeyCtx &ctx );
  ExecStatus exec_bitpos( RedisKeyCtx &ctx );
  ExecStatus exec_decr( RedisKeyCtx &ctx );
  ExecStatus exec_decrby( RedisKeyCtx &ctx );
  ExecStatus exec_get( RedisKeyCtx &ctx );
  ExecStatus exec_getbit( RedisKeyCtx &ctx );
  ExecStatus exec_getrange( RedisKeyCtx &ctx );
  ExecStatus exec_getset( RedisKeyCtx &ctx );
  ExecStatus exec_incr( RedisKeyCtx &ctx );
  ExecStatus exec_incrby( RedisKeyCtx &ctx );
  ExecStatus exec_incrbyfloat( RedisKeyCtx &ctx );
  ExecStatus exec_mget( RedisKeyCtx &ctx );
  ExecStatus exec_mset( RedisKeyCtx &ctx );
  ExecStatus exec_msetnx( RedisKeyCtx &ctx );
  ExecStatus exec_psetex( RedisKeyCtx &ctx );
  ExecStatus exec_set( RedisKeyCtx &ctx );
  ExecStatus exec_setbit( RedisKeyCtx &ctx );
  ExecStatus exec_setex( RedisKeyCtx &ctx );
  ExecStatus exec_setnx( RedisKeyCtx &ctx );
  ExecStatus exec_setrange( RedisKeyCtx &ctx );
  ExecStatus exec_strlen( RedisKeyCtx &ctx );

  ExecStatus exec_set_value( RedisKeyCtx &ctx,  int n );
  ExecStatus exec_debug( EvService *own );
  ExecStatus exec_command( void );
  ExecStatus exec_ping( void );
  ExecStatus exec_quit( void );

  void send_err( ExecStatus status,  kv::KeyStatus kstatus = KEY_OK );
  void send_ok( void );
  void send_nil( void );
  void send_int( void );
  void send_err_bad_args( void );
  void send_err_kv( kv::KeyStatus kstatus );
  void send_err_bad_cmd( void );
  void send_err_msg( RedisMsgStatus mstatus );
  void send_err_alloc_fail( void );
  void send_err_key_exists( void );
  void send_err_key_doesnt_exist( void );
};

inline void
RedisKeyCtx::prefetch( void )
{
  this->exec.exec_key_prefetch( *this );
}

inline bool
RedisKeyCtx::run( EvService *&own )
{
  own = this->owner;
  return this->exec.exec_key_continue( *this );
}

static inline void
str_to_upper( const char *in,  char *out,  size_t len )
{
  /* take away the 0x20 bits */
  for ( size_t i = 0; i < len; i += 4 )
    *(uint32_t *) &out[ i ] = *((uint32_t *) &in[ i ]) & 0xdfdfdfdfU;
}

static inline RedisCmd
get_upper_cmd( const char *name,  size_t len )
{
  if ( len < 32 ) {
    char tmp[ 32 ];
    str_to_upper( name, tmp, len ); 
    return get_redis_cmd( tmp, len );
  }   
  return NO_CMD;
}

}
}

#endif
