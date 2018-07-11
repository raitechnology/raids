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
  EXEC_OK = 0,
  EXEC_SETUP_OK,         /* key setup */
  EXEC_SEND_OK,          /* send +OK */
  EXEC_SEND_NIL,         /* send $-1 */
  EXEC_SEND_INT,         /* send :100 */
  EXEC_SEND_ZERO,        /* send :0 */
  EXEC_SEND_ONE,         /* send :1 */
  EXEC_SEND_NEG_ONE,     /* send :-1 */
  EXEC_SEND_ZERO_STRING, /* send $0 */
  EXEC_SUCCESS,          /* <= success = good */
  EXEC_DEPENDS,          /* key depends (dest) on another key arg (src) */
  EXEC_CONTINUE,         /* continue working, more keys */
  EXEC_QUIT,             /* quit/shutdown command */
  EXEC_DEBUG,            /* debug command */
  /* errors v v v / ok ^ ^ ^ */
  EXEC_KV_STATUS,        /* kstatus != ok */
  EXEC_MSG_STATUS,       /* mstatus != ok */
  EXEC_BAD_ARGS,         /* argument mismatch or malformed command */
  EXEC_BAD_CMD,          /* command unknown or not implmented */
  EXEC_BAD_TYPE,         /* data type not compatible with operator */
  EXEC_ALLOC_FAIL,       /* alloc returned NULL */
  EXEC_KEY_EXISTS,       /* when set with NX operator */
  EXEC_KEY_DOESNT_EXIST, /* when set with XX operator */
  EXEC_ABORT_SEND_ZERO   /* abort multiple key operation and return 0 */
};

inline static bool exec_status_success( ExecStatus status ) {
  return (int) status <= EXEC_SUCCESS;
}

inline static bool exec_status_fail( ExecStatus status ) {
  return (int) status > EXEC_SUCCESS;
}

struct EvSocket;
struct RedisExec;

struct RedisKeyRes {
  size_t mem_size, /* alloc size */
         size;     /* data size */
  char   data[ 0 ];/* data start */
};

struct RedisKeyCtx {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  bool operator>( const RedisKeyCtx *ctx2 ) const;
  uint64_t        hash1,  /* 128 bit hash of key */
                  hash2;
  RedisExec     & exec;   /* parent context */
  EvSocket      * owner;  /* parent connection */
  int64_t         ival;   /* if it returns int */
  RedisKeyRes   * part;   /* saved data for key */
  const int       argn;   /* which arg number of command */
  uint8_t         dep,    /* depends on another key */
                  type;   /* value type, string, list, hash, etc */
  ExecStatus      status; /* result of exec for this key */
  kv::KeyStatus   kstatus;/* result of key lookup */
  bool            is_new, /* if the key does not exist */
                  is_read;/* if the key is read only */
  kv::KeyFragment kbuf;   /* key material, extends past structure */

  RedisKeyCtx( RedisExec &ex,  EvSocket *own,  const char *key,  size_t keylen,
               const int n,  const uint64_t seed,  const uint64_t seed2 )
     : hash1( seed ), hash2( seed2 ), exec( ex ), owner( own ), ival( 0 ),
       part( 0 ), argn( n ), dep( 0 ), type( 0 ), status( EXEC_SETUP_OK ),
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
    return sizeof( RedisKeyCtx ) + keylen; /* alloc size of *this */
  }
  void prefetch( void );             /* prefetch the key */
  ExecStatus run( EvSocket *&svc ); /* execute key operation */
  const char *get_type_str( void ) const;
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
  uint64_t seed, seed2;       /* kv map hash seeds, different for each db */
  kv::KeyCtx     kctx;        /* key context used for every key in command */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffer, reset before each key lookup */
  StreamBuf    & strm;        /* output buffer, result of command execution */
  RedisMsg       msg;         /* current command msg */
  RedisKeyCtx  * key,         /* currently executing key */
              ** keys;        /* all of the keys in command */
  uint32_t       key_cnt,     /* total keys[] size */
                 key_done;    /* number of keys processed */
  RedisCmd       cmd;         /* current command (GET_CMD) */
  RedisMsgStatus mstatus;     /* command message parse status */
  uint16_t       flags;       /* command flags (CMD_READONLY_FLAG) */
  int            arity,       /* number of command args */
                 first,       /* first key in args */
                 last,        /* last key in args */
                 step;        /* incr between keys */
  size_t         argc;        /* count of args in cmd msg */

  RedisExec( kv::HashTab &map,  uint32_t ctx_id,  StreamBuf &s,  bool single ) :
      kctx( map, ctx_id, NULL ), strm( s ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ) {
    if ( single ) this->kctx.set( kv::KEYCTX_IS_SINGLE_THREAD );
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->seed,
                                     this->seed2 );
    this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  }

  void release( void ) {
    this->wrk.reset();
    this->wrk.release_all();
  }
  /* set up a single key, there may be multiple in a command */
  ExecStatus exec_key_setup( EvSocket *svc,  EvPrefetchQueue *q,
                             RedisKeyCtx *&ctx,  int n );
  void exec_run_to_completion( void );
  /* parse set up a command */
  ExecStatus exec( EvSocket *svc,  EvPrefetchQueue *q );
  /* execute a key operation */
  ExecStatus exec_key_continue( RedisKeyCtx &ctx );
  /* compute the hash and prefetch the ht[] location */
  void exec_key_prefetch( RedisKeyCtx &ctx ) {
    this->key = &ctx;
    this->kctx.set_key( ctx.kbuf );
    this->kctx.set_hash( ctx.hash1, ctx.hash2 );
    this->kctx.prefetch( 1 );
  }
  /* fetch a read key value or acquire it for write */
  kv::KeyStatus exec_key_fetch( RedisKeyCtx &ctx,  bool force_read = false );

  /* CLUSTER */
  ExecStatus exec_cluster( void );
  ExecStatus exec_readonly( void );
  ExecStatus exec_readwrite( void );
  /* CONNECTION */
  ExecStatus exec_auth( void );
  ExecStatus exec_echo( void );
  ExecStatus exec_ping( void );
  ExecStatus exec_quit( void );
  ExecStatus exec_select( void );
  ExecStatus exec_swapdb( void );
  /* GEO */
  ExecStatus exec_geoadd( RedisKeyCtx &ctx );
  ExecStatus exec_geohash( RedisKeyCtx &ctx );
  ExecStatus exec_geopos( RedisKeyCtx &ctx );
  ExecStatus exec_geodist( RedisKeyCtx &ctx );
  ExecStatus exec_georadius( RedisKeyCtx &ctx );
  ExecStatus exec_georadiusbymember( RedisKeyCtx &ctx );
  /* HASH */
  ExecStatus exec_hdel( RedisKeyCtx &ctx );
  ExecStatus exec_hexists( RedisKeyCtx &ctx );
  ExecStatus exec_hget( RedisKeyCtx &ctx );
  ExecStatus exec_hgetall( RedisKeyCtx &ctx );
  ExecStatus exec_hincrby( RedisKeyCtx &ctx );
  ExecStatus exec_hincrbyfloat( RedisKeyCtx &ctx );
  ExecStatus exec_hkeys( RedisKeyCtx &ctx );
  ExecStatus exec_hlen( RedisKeyCtx &ctx );
  ExecStatus exec_hmget( RedisKeyCtx &ctx );
  ExecStatus exec_hmset( RedisKeyCtx &ctx );
  ExecStatus exec_hset( RedisKeyCtx &ctx );
  ExecStatus exec_hsetnx( RedisKeyCtx &ctx );
  ExecStatus exec_hstrlen( RedisKeyCtx &ctx );
  ExecStatus exec_hvals( RedisKeyCtx &ctx );
  ExecStatus exec_hscan( RedisKeyCtx &ctx );
  /* HYPERLOGLOG */
  ExecStatus exec_pfadd( RedisKeyCtx &ctx );
  ExecStatus exec_pfcount( RedisKeyCtx &ctx );
  ExecStatus exec_pfmerge( RedisKeyCtx &ctx );
  /* KEY */
  ExecStatus exec_del( RedisKeyCtx &ctx );
  ExecStatus exec_dump( RedisKeyCtx &ctx );
  ExecStatus exec_exists( RedisKeyCtx &ctx );
  ExecStatus exec_expire( RedisKeyCtx &ctx );
  ExecStatus exec_expireat( RedisKeyCtx &ctx );
  ExecStatus exec_keys( void );
  ExecStatus scan_keys( uint64_t pos,  int64_t maxcnt,  const char *pattern,
                        size_t patlen );
  ExecStatus exec_migrate( RedisKeyCtx &ctx );
  ExecStatus exec_move( RedisKeyCtx &ctx );
  ExecStatus exec_object( RedisKeyCtx &ctx );
  ExecStatus exec_persist( RedisKeyCtx &ctx );
  ExecStatus exec_pexpire( RedisKeyCtx &ctx );
  ExecStatus exec_pexpireat( RedisKeyCtx &ctx );
  ExecStatus do_pexpire( RedisKeyCtx &ctx,  uint64_t units );
  ExecStatus do_pexpireat( RedisKeyCtx &ctx,  uint64_t units );
  ExecStatus exec_pttl( RedisKeyCtx &ctx );
  ExecStatus do_pttl( RedisKeyCtx &ctx,  int64_t units );
  ExecStatus exec_randomkey( void );
  ExecStatus exec_rename( RedisKeyCtx &ctx );
  ExecStatus exec_renamenx( RedisKeyCtx &ctx );
  ExecStatus exec_restore( RedisKeyCtx &ctx );
  ExecStatus exec_sort( RedisKeyCtx &ctx );
  ExecStatus exec_touch( RedisKeyCtx &ctx );
  ExecStatus exec_ttl( RedisKeyCtx &ctx );
  ExecStatus exec_type( RedisKeyCtx &ctx );
  ExecStatus exec_unlink( RedisKeyCtx &ctx );
  ExecStatus exec_wait( void );
  ExecStatus exec_scan( void );
  /* LIST */
  ExecStatus exec_blpop( RedisKeyCtx &ctx );
  ExecStatus exec_brpop( RedisKeyCtx &ctx );
  ExecStatus exec_brpoplpush( RedisKeyCtx &ctx );
  ExecStatus exec_lindex( RedisKeyCtx &ctx );
  ExecStatus exec_linsert( RedisKeyCtx &ctx );
  ExecStatus exec_llen( RedisKeyCtx &ctx );
  ExecStatus exec_lpop( RedisKeyCtx &ctx );
  ExecStatus exec_lpush( RedisKeyCtx &ctx );
  ExecStatus exec_lpushx( RedisKeyCtx &ctx );
  ExecStatus exec_lrange( RedisKeyCtx &ctx );
  ExecStatus exec_lrem( RedisKeyCtx &ctx );
  ExecStatus exec_lset( RedisKeyCtx &ctx );
  ExecStatus exec_ltrim( RedisKeyCtx &ctx );
  ExecStatus exec_rpop( RedisKeyCtx &ctx );
  ExecStatus exec_rpoplpush( RedisKeyCtx &ctx );
  ExecStatus exec_rpush( RedisKeyCtx &ctx );
  ExecStatus exec_rpushx( RedisKeyCtx &ctx );
  /* PUBSUB */
  ExecStatus exec_psubscribe( RedisKeyCtx &ctx );
  ExecStatus exec_pubsub( RedisKeyCtx &ctx );
  ExecStatus exec_publish( RedisKeyCtx &ctx );
  ExecStatus exec_punsubscribe( RedisKeyCtx &ctx );
  ExecStatus exec_subscribe( RedisKeyCtx &ctx );
  ExecStatus exec_unsubscribe( RedisKeyCtx &ctx );
  /* SCRIPT */
  ExecStatus exec_eval( RedisKeyCtx &ctx );
  ExecStatus exec_evalsha( RedisKeyCtx &ctx );
  ExecStatus exec_script( RedisKeyCtx &ctx );
  /* SERVER */
  ExecStatus exec_bgrewriteaof( void );
  ExecStatus exec_bgsave( void );
  ExecStatus exec_client( void );
  ExecStatus exec_command( void );
  ExecStatus exec_config( void );
  ExecStatus exec_dbsize( void );
  ExecStatus exec_debug( void );
  ExecStatus exec_flushall( void );
  ExecStatus exec_flushdb( void );
  ExecStatus exec_info( void );
  ExecStatus exec_lastsave( void );
  ExecStatus exec_memory( void );
  ExecStatus exec_monitor( void );
  ExecStatus exec_role( void );
  ExecStatus exec_save( void );
  ExecStatus exec_shutdown( void );
  ExecStatus exec_slaveof( void );
  ExecStatus exec_slowlog( void );
  ExecStatus exec_sync( void );
  ExecStatus exec_time( void );
  /* SET */
  ExecStatus exec_sadd( RedisKeyCtx &ctx );
  ExecStatus exec_scard( RedisKeyCtx &ctx );
  ExecStatus exec_sdiff( RedisKeyCtx &ctx );
  ExecStatus exec_sdiffstore( RedisKeyCtx &ctx );
  ExecStatus exec_sinter( RedisKeyCtx &ctx );
  ExecStatus exec_sinterstore( RedisKeyCtx &ctx );
  ExecStatus exec_sismember( RedisKeyCtx &ctx );
  ExecStatus exec_smembers( RedisKeyCtx &ctx );
  ExecStatus exec_smove( RedisKeyCtx &ctx );
  ExecStatus exec_spop( RedisKeyCtx &ctx );
  ExecStatus exec_srandmember( RedisKeyCtx &ctx );
  ExecStatus exec_srem( RedisKeyCtx &ctx );
  ExecStatus exec_sunion( RedisKeyCtx &ctx );
  ExecStatus exec_sunionstore( RedisKeyCtx &ctx );
  ExecStatus exec_sscan( RedisKeyCtx &ctx );
  /* SORTED_SET */
  ExecStatus exec_zadd( RedisKeyCtx &ctx );
  ExecStatus exec_zcard( RedisKeyCtx &ctx );
  ExecStatus exec_zcount( RedisKeyCtx &ctx );
  ExecStatus exec_zincrby( RedisKeyCtx &ctx );
  ExecStatus exec_zinterstore( RedisKeyCtx &ctx );
  ExecStatus exec_zlexcount( RedisKeyCtx &ctx );
  ExecStatus exec_zrange( RedisKeyCtx &ctx );
  ExecStatus exec_zrangebylex( RedisKeyCtx &ctx );
  ExecStatus exec_zrevrangebylex( RedisKeyCtx &ctx );
  ExecStatus exec_zrangebyscore( RedisKeyCtx &ctx );
  ExecStatus exec_zrank( RedisKeyCtx &ctx );
  ExecStatus exec_zrem( RedisKeyCtx &ctx );
  ExecStatus exec_zremrangebylex( RedisKeyCtx &ctx );
  ExecStatus exec_zremrangebyrank( RedisKeyCtx &ctx );
  ExecStatus exec_zremrangebyscore( RedisKeyCtx &ctx );
  ExecStatus exec_zrevrange( RedisKeyCtx &ctx );
  ExecStatus exec_zrevrangebyscore( RedisKeyCtx &ctx );
  ExecStatus exec_zrevrank( RedisKeyCtx &ctx );
  ExecStatus exec_zscore( RedisKeyCtx &ctx );
  ExecStatus exec_zunionstore( RedisKeyCtx &ctx );
  ExecStatus exec_zscan( RedisKeyCtx &ctx );
  /* STRING */
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
  /* TRANSACTION */
  ExecStatus exec_discard( RedisKeyCtx &ctx );
  ExecStatus exec_exec( RedisKeyCtx &ctx );
  ExecStatus exec_multi( RedisKeyCtx &ctx );
  ExecStatus exec_unwatch( RedisKeyCtx &ctx );
  ExecStatus exec_watch( RedisKeyCtx &ctx );
  /* STREAM */
  ExecStatus exec_xadd( RedisKeyCtx &ctx );
  ExecStatus exec_xlen( RedisKeyCtx &ctx );
  ExecStatus exec_xrange( RedisKeyCtx &ctx );
  ExecStatus exec_xrevrange( RedisKeyCtx &ctx );
  ExecStatus exec_xread( RedisKeyCtx &ctx );
  ExecStatus exec_xreadgroup( RedisKeyCtx &ctx );
  ExecStatus exec_xgroup( RedisKeyCtx &ctx );
  ExecStatus exec_xack( RedisKeyCtx &ctx );
  ExecStatus exec_xpending( RedisKeyCtx &ctx );
  ExecStatus exec_xclaim( RedisKeyCtx &ctx );
  ExecStatus exec_xinfo( RedisKeyCtx &ctx );
  ExecStatus exec_xdel( RedisKeyCtx &ctx );

  /* string extras */
  ExecStatus exec_add( RedisKeyCtx &ctx,  int64_t incr );
  ExecStatus exec_set_value( RedisKeyCtx &ctx,  int n,  int flags );
  ExecStatus exec_set_value_expire( RedisKeyCtx &ctx,  int n,  uint64_t ns,
                                    int flags );
  /* result senders */
  void send_err( ExecStatus status,  kv::KeyStatus kstatus = KEY_OK );
  void send_ok( void );
  void send_nil( void );
  void send_msg( const RedisMsg &m );
  void send_int( void );
  void send_int( int64_t ival );
  void send_zero( void );
  void send_one( void );
  void send_neg_one( void );
  void send_zero_string( void );
  size_t send_string( void *data,  size_t size );
  void send_err_bad_args( void );
  void send_err_kv( kv::KeyStatus kstatus );
  void send_err_bad_cmd( void );
  void send_err_bad_type( void );
  void send_err_msg( RedisMsgStatus mstatus );
  void send_err_alloc_fail( void );
  void send_err_key_exists( void );
  void send_err_key_doesnt_exist( void );

  bool save_string_result( RedisKeyCtx &ctx,  void *data,  size_t size );
  bool save_data( RedisKeyCtx &ctx,  void *data,  size_t size );
  void array_string_result( void );
};

inline void
RedisKeyCtx::prefetch( void )
{
  this->exec.exec_key_prefetch( *this );
}

inline ExecStatus
RedisKeyCtx::run( EvSocket *&svc )
{
  svc = this->owner;
  return this->exec.exec_key_continue( *this );
}

inline bool
RedisKeyCtx::operator>( const RedisKeyCtx *ctx2 ) const
{
  if ( this->dep > ctx2->dep )
    return true;
  if ( this->dep == ctx2->dep ) {
    kv::HashTab &ht = this->exec.kctx.ht;
    return ht.hdr.ht_mod( this->hash1 ) > ht.hdr.ht_mod( ctx2->hash1 );
  }
  return false;
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

template <class Int> static inline Int max( Int i,  Int j ) { return i>j?i:j; }
template <class Int> static inline Int min( Int i,  Int j ) { return i<j?i:j; }

}
}

#endif
