#ifndef __rai_raids__redis_exec_h__
#define __rai_raids__redis_exec_h__

#include <raids/ev_key.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/stream_buf.h>
#include <raids/redis_pubsub.h>

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

namespace rai {
namespace ds {

enum ExecStatus {
  EXEC_OK = 0,
  EXEC_SETUP_OK,         /* key setup */
  EXEC_SEND_OK,          /* send +OK */
  EXEC_SEND_NIL,         /* send $-1 */
  EXEC_SEND_NULL,        /* send *-1 */
  EXEC_SEND_INT,         /* send :100 */
  EXEC_SEND_ZERO,        /* send :0 */
  EXEC_SEND_ZEROARR,     /* send *0 */
  EXEC_SEND_ONE,         /* send :1 */
  EXEC_SEND_NEG_ONE,     /* send :-1 */
  EXEC_SEND_ZERO_STRING, /* send $0 */
  EXEC_SUCCESS  = EK_SUCCESS,  /* <= success = good */
  EXEC_DEPENDS  = EK_DEPENDS,  /* key depends (dest) on another key arg (src) */
  EXEC_CONTINUE = EK_CONTINUE, /* continue working, more keys */
  EXEC_QUEUED,           /* cmd queued for multi transaction */
  EXEC_BLOCKED,          /* cmd blocked waiting for elements */
  EXEC_QUIT,             /* quit/shutdown command */
  EXEC_DEBUG,            /* debug command */
  EXEC_SKIP,             /* client skip set */
  EXEC_ABORT_SEND_ZERO,  /* abort multiple key operation and return 0 */
  EXEC_ABORT_SEND_NIL,   /* abort multiple key operation and return nil */
  EXEC_SEND_DATA,        /* multiple key, first to succeed wins */
  /* errors v v v / ok ^ ^ ^ */
  ERR_KV_STATUS,        /* kstatus != ok */
  ERR_MSG_STATUS,       /* mstatus != ok */
  ERR_BAD_CMD,          /* command unknown or not implmented */
  ERR_BAD_ARGS,         /* argument mismatch or malformed command */
  ERR_BAD_TYPE,         /* data type not compatible with operator */
  ERR_BAD_RANGE,        /* index out of range */
  ERR_NO_GROUP,         /* group not found */
  ERR_GROUP_EXISTS,     /* group exists */
  ERR_STREAM_ID,        /* stream id invalid */
  ERR_ALLOC_FAIL,       /* alloc returned NULL */
  ERR_KEY_EXISTS,       /* when set with NX operator */
  ERR_KEY_DOESNT_EXIST, /* when set with XX operator */
  ERR_BAD_MULTI,        /* nested multi transaction */
  ERR_BAD_EXEC,         /* no transaction active to exec */
  ERR_BAD_DISCARD       /* no transaction active to discard */
};

inline static bool exec_status_success( int status ) {
  return status <= EXEC_SUCCESS; /* the OK and SEND_xxx status */
}

inline static bool exec_status_fail( int status ) {
  return status > EXEC_SUCCESS;  /* the bad status */
}

struct EvSocket;
struct EvPublish;
struct RedisExec;
struct RouteDB;
struct PeerData;
struct PeerMatchArgs;
struct KvPubSub;
struct ExecStreamCtx;

struct ScanArgs {
  int64_t pos,    /* position argument, the position where scan starts */
          maxcnt; /* COUNT argument, the maximum number of elements    */
  pcre2_real_code_8       * re; /* pcre regex compiled */
  pcre2_real_match_data_8 * md; /* pcre match context  */
  ScanArgs() : pos( 0 ), maxcnt( 10 ), re( 0 ), md( 0 ) {}
};

struct RedisMsgList {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMsgList * next,
               * back;
  RedisMsg     * msg;  /* pending multi exec msg */

  RedisMsgList() : next( 0 ), back( 0 ), msg( 0 ) {}
};

struct RedisWatchList {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisWatchList * next,
                 * back;
  uint64_t hash1, hash2, serial, pos; /* the location of the watch */

  RedisWatchList( uint64_t h1,  uint64_t h2,  uint64_t sn,  uint64_t p )
    : next( 0 ), back( 0 ), hash1( h1 ), hash2( h2 ), serial( sn ), pos( p ) {}
};

struct RedisMultiExec {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  kv::WorkAllocT< 1024 >        wrk;         /* space to use for msgs below */
  kv::DLinkList<RedisMsgList>   msg_list;    /* msgs in the multi section */
  kv::DLinkList<RedisWatchList> watch_list;  /* watched keys */
  size_t                        msg_count,   /* how many msgs queued */
                                watch_count; /* how many watches */
  bool                          multi_start; /* whether in MULTI before EXEC */

  RedisMultiExec() : msg_count( 0 ), watch_count( 0 ), multi_start( false ) {}

  bool append_msg( RedisMsg &msg );

  bool append_watch( uint64_t h1,  uint64_t h2,  uint64_t sn,  uint64_t pos );
};

enum ExecCmdState { /* cmd_state flags */
  CMD_STATE_NORMAL            = 0, /* no flags */
  CMD_STATE_MONITOR           = 1, /* monitor cmd on */
  CMD_STATE_CLIENT_REPLY_SKIP = 2, /* skip output of next cmd */
  CMD_STATE_CLIENT_REPLY_OFF  = 4  /* mute output until client reply on again */
};

struct RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  uint64_t seed,   seed2;     /* kv map hash seeds, different for each db */
  kv::KeyCtx       kctx;      /* key context used for every key in command */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffer, reset before each key lookup */
  kv::DLinkList<RedisContinueMsg> cont_list, /* continuations ready to run */
                                  wait_list; /* these are waiting on a timer */
  StreamBuf      & strm;      /* output buffer, result of command execution */
  size_t           strm_start;/* output offset before command starts */
  RedisMsg         msg;       /* current command msg */
  EvKeyCtx       * key,       /* currently executing key */
                ** keys;      /* all of the keys in command */
  uint32_t         key_cnt,   /* total keys[] size */
                   key_done;  /* number of keys processed */
  RedisMultiExec * multi;     /* MULTI .. EXEC block */
  RedisCmd         cmd;       /* current command (GET_CMD) */
  RedisMsgStatus   mstatus;   /* command message parse status */
  uint8_t          blk_state, /* if blocking cmd timed out (RBLK_CMD_TIMEOUT) */
                   cmd_state; /* if monitor is active or skipping */
  uint16_t         cmd_flags, /* command flags (CMD_READONLY_FLAG) */
                   key_flags; /* EvKeyFlags, if a key has a keyspace event */
  int16_t          arity,     /* number of command args */
                   first,     /* first key in args */
                   last,      /* last key in args */
                   step;      /* incr between keys */
  uint64_t         step_mask; /* step key mask */
  size_t           argc;      /* count of args in cmd msg */
  RedisSubMap      sub_tab;   /* pub/sub subscription table */
  RedisPatternMap  pat_tab;   /* pub/sub pattern sub table */
  RedisContinueMap continue_tab; /* blocked continuations */
  RouteDB        & sub_route; /* map subject to sub_id */
  PeerData       & peer;      /* name and address of this peer */
  uint32_t         sub_id,    /* fd, set this after accept() */
                   next_event_id; /* next event id for timers */
  uint64_t         timer_id;  /* timer id of this service */

  RedisExec( kv::HashTab &map,  uint32_t ctx_id,  StreamBuf &s,
             RouteDB &rdb,  PeerData &pd ) :
      kctx( map, ctx_id, NULL ), strm( s ), strm_start( s.pending() ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ), multi( 0 ),
      cmd( NO_CMD ), blk_state( 0 ), cmd_state( 0 ), key_flags( 0 ),
      sub_route( rdb ), peer( pd ), sub_id( ~0U ), next_event_id( 0 ),
      timer_id( 0 ) {
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->seed,
                                     this->seed2 );
    this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  }
  /* stop a continuation and send null */
  bool continue_expire( uint64_t event_id,  RedisContinueMsg *&cm );
  /* remove and unsubscribe contiuation subjects from continue_tab */
  void pop_continue_tab( RedisContinueMsg *cm );
  /* restart continuation */
  void push_continue_list( RedisContinueMsg *cm );
  /* release anything allocated */
  void release( void );
  /* unsubscribe anything subscribed */
  void rem_all_sub( void );
  /* handle the keys are cmd specific */
  bool locate_movablekeys( void );
  /* fetch next key arg[ i ] after current i, return false if none */
  bool next_key( int &i );
  /* return number of keys in cmd */
  size_t calc_key_count( void );
  /* set up a single key, there may be multiple in a command */
  ExecStatus exec_key_setup( EvSocket *svc,  EvPrefetchQueue *q,
                             EvKeyCtx *&ctx,  int n );
  void exec_run_to_completion( void );
  /* parse set up a command */
  ExecStatus exec( EvSocket *svc,  EvPrefetchQueue *q );
  /* run cmd that doesn't have keys */
  ExecStatus exec_nokeys( void );
  /* execute a key operation */
  ExecStatus exec_key_continue( EvKeyCtx &ctx );
  /* subscribe to keyspace subjects and wait for publish to continue */
  ExecStatus save_blocked_cmd( int64_t timeout_val );
  /* execute the saved commands after signaled */
  void drain_continuations( EvSocket *svc );
  /* publish keyspace events */
  void pub_keyspace_events( void );
  /* set the hash */
  void exec_key_set( EvKeyCtx &ctx ) {
    this->key = ctx.set( this->kctx );
  }
  /* compute the hash and prefetch the ht[] location */
  void exec_key_prefetch( EvKeyCtx &ctx ) {
    ctx.prefetch( this->kctx.ht,
      test_cmd_mask( this->cmd_flags, CMD_READONLY_FLAG ) ? true : false );
  }
  /* fetch key for write and check type matches or is not set */
  kv::KeyStatus get_key_write( EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, false );
    if ( status == KEY_OK && ctx.type != type ) {
      if ( ctx.type == 0 ) {
        ctx.flags |= EKF_IS_NEW;
        return KEY_IS_NEW;
      }
      return KEY_NO_VALUE;
    }
    return status;
  }
  /* fetch key for read and check type matches or is not set */
  kv::KeyStatus get_key_read( EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, true );
    if ( status == KEY_OK && ctx.type != type )
      return ( ctx.type == 0 ) ? KEY_NOT_FOUND : KEY_NO_VALUE;
    return status;
  }
  /* fetch a read key value or acquire it for write */
  kv::KeyStatus exec_key_fetch( EvKeyCtx &ctx,  bool force_read = false );

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
  ExecStatus exec_geoadd( EvKeyCtx &ctx );
  ExecStatus exec_geohash( EvKeyCtx &ctx );
  ExecStatus exec_geopos( EvKeyCtx &ctx );
  ExecStatus exec_geodist( EvKeyCtx &ctx );
  ExecStatus exec_georadius( EvKeyCtx &ctx );
  ExecStatus exec_georadiusbymember( EvKeyCtx &ctx );
  ExecStatus do_gread( EvKeyCtx &ctx,  int flags );
  ExecStatus do_gradius( EvKeyCtx &ctx,  int flags );
  ExecStatus do_gradius_store( EvKeyCtx &ctx );
  /* HASH */
  ExecStatus exec_happend( EvKeyCtx &ctx );
  ExecStatus exec_hdel( EvKeyCtx &ctx );
  ExecStatus exec_hdiff( EvKeyCtx &ctx );
  ExecStatus exec_hdiffstore( EvKeyCtx &ctx );
  ExecStatus exec_hexists( EvKeyCtx &ctx );
  ExecStatus exec_hget( EvKeyCtx &ctx );
  ExecStatus exec_hgetall( EvKeyCtx &ctx );
  ExecStatus exec_hincrby( EvKeyCtx &ctx );
  ExecStatus exec_hincrbyfloat( EvKeyCtx &ctx );
  ExecStatus exec_hinter( EvKeyCtx &ctx );
  ExecStatus exec_hinterstore( EvKeyCtx &ctx );
  ExecStatus exec_hkeys( EvKeyCtx &ctx );
  ExecStatus exec_hlen( EvKeyCtx &ctx );
  ExecStatus exec_hmget( EvKeyCtx &ctx );
  ExecStatus exec_hmset( EvKeyCtx &ctx );
  ExecStatus exec_hset( EvKeyCtx &ctx );
  ExecStatus exec_hsetnx( EvKeyCtx &ctx );
  ExecStatus exec_hstrlen( EvKeyCtx &ctx );
  ExecStatus exec_hvals( EvKeyCtx &ctx );
  ExecStatus exec_hscan( EvKeyCtx &ctx );
  ExecStatus exec_hunion( EvKeyCtx &ctx );
  ExecStatus exec_hunionstore( EvKeyCtx &ctx );
  ExecStatus do_hmultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *hs );
  ExecStatus do_hread( EvKeyCtx &ctx,  int flags );
  ExecStatus do_hwrite( EvKeyCtx &ctx,  int flags );
  /* HYPERLOGLOG */
  ExecStatus exec_pfadd( EvKeyCtx &ctx );
  ExecStatus exec_pfcount( EvKeyCtx &ctx );
  ExecStatus exec_pfmerge( EvKeyCtx &ctx );
  /* KEY */
  ExecStatus exec_del( EvKeyCtx &ctx );
  ExecStatus exec_dump( EvKeyCtx &ctx );
  ExecStatus exec_exists( EvKeyCtx &ctx );
  ExecStatus exec_expire( EvKeyCtx &ctx );
  ExecStatus exec_expireat( EvKeyCtx &ctx );
  ExecStatus exec_keys( void );
  ExecStatus exec_migrate( EvKeyCtx &ctx );
  ExecStatus exec_move( EvKeyCtx &ctx );
  ExecStatus exec_object( EvKeyCtx &ctx );
  ExecStatus exec_persist( EvKeyCtx &ctx );
  ExecStatus exec_pexpire( EvKeyCtx &ctx );
  ExecStatus exec_pexpireat( EvKeyCtx &ctx );
  ExecStatus do_pexpire( EvKeyCtx &ctx,  uint64_t units );
  ExecStatus do_pexpireat( EvKeyCtx &ctx,  uint64_t units );
  ExecStatus exec_pttl( EvKeyCtx &ctx );
  ExecStatus do_pttl( EvKeyCtx &ctx,  int64_t units );
  ExecStatus exec_randomkey( void );
  ExecStatus exec_rename( EvKeyCtx &ctx );
  ExecStatus exec_renamenx( EvKeyCtx &ctx );
  ExecStatus exec_restore( EvKeyCtx &ctx );
  ExecStatus exec_sort( EvKeyCtx &ctx );
  ExecStatus exec_touch( EvKeyCtx &ctx );
  ExecStatus exec_ttl( EvKeyCtx &ctx );
  ExecStatus exec_type( EvKeyCtx &ctx );
  ExecStatus exec_unlink( EvKeyCtx &ctx );
  ExecStatus exec_wait( void );
  ExecStatus exec_scan( void );
  ExecStatus match_scan_args( ScanArgs &sa,  size_t i );
  void release_scan_args( ScanArgs &sa );
  ExecStatus scan_keys( ScanArgs &sa );
  /* LIST */
  ExecStatus exec_blpop( EvKeyCtx &ctx );
  ExecStatus exec_brpop( EvKeyCtx &ctx );
  ExecStatus exec_brpoplpush( EvKeyCtx &ctx );
  ExecStatus exec_lindex( EvKeyCtx &ctx );
  ExecStatus exec_linsert( EvKeyCtx &ctx );
  ExecStatus exec_llen( EvKeyCtx &ctx );
  ExecStatus exec_lpop( EvKeyCtx &ctx );
  ExecStatus exec_lpush( EvKeyCtx &ctx );
  ExecStatus exec_lpushx( EvKeyCtx &ctx );
  ExecStatus exec_lrange( EvKeyCtx &ctx );
  ExecStatus exec_lrem( EvKeyCtx &ctx );
  ExecStatus exec_lset( EvKeyCtx &ctx );
  ExecStatus exec_ltrim( EvKeyCtx &ctx );
  ExecStatus exec_rpop( EvKeyCtx &ctx );
  ExecStatus exec_rpoplpush( EvKeyCtx &ctx );
  ExecStatus exec_rpush( EvKeyCtx &ctx );
  ExecStatus exec_rpushx( EvKeyCtx &ctx );
  ExecStatus do_push( EvKeyCtx &ctx,  int flags,  const char *value = NULL,
                      size_t valuelen = 0 );
  ExecStatus do_pop( EvKeyCtx &ctx,  int flags );
  /* PUBSUB */
  ExecStatus exec_psubscribe( void );
  ExecStatus exec_pubsub( void );
  ExecStatus exec_publish( void );
  ExecStatus exec_punsubscribe( void );
  ExecStatus exec_subscribe( void );
  ExecStatus exec_unsubscribe( void );
  ExecStatus do_subscribe( const char *sub,  size_t len );
  ExecStatus do_unsubscribe( const char *sub,  size_t len );
  ExecStatus do_psubscribe( const char *sub,  size_t len );
  ExecStatus do_punsubscribe( const char *sub,  size_t len );
  ExecStatus do_sub( int flags );
  bool pub_message( EvPublish &pub,  RedisPatternRoute *rt );
  int do_pub( EvPublish &pub,  RedisContinueMsg *&cm );
  bool do_hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  /* SCRIPT */
  ExecStatus exec_eval( EvKeyCtx &ctx );
  ExecStatus exec_evalsha( EvKeyCtx &ctx );
  ExecStatus exec_script( EvKeyCtx &ctx );
  /* SERVER */
  ExecStatus exec_bgrewriteaof( void );
  ExecStatus exec_bgsave( void );
  bool get_peer_match_args( PeerMatchArgs &ka );
  ExecStatus exec_client( void );
  int client_list( char *buf,  size_t buflen );
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
  ExecStatus exec_sadd( EvKeyCtx &ctx );
  ExecStatus exec_scard( EvKeyCtx &ctx );
  ExecStatus exec_sdiff( EvKeyCtx &ctx );
  ExecStatus exec_sdiffstore( EvKeyCtx &ctx );
  ExecStatus exec_sinter( EvKeyCtx &ctx );
  ExecStatus exec_sinterstore( EvKeyCtx &ctx );
  ExecStatus exec_sismember( EvKeyCtx &ctx );
  ExecStatus exec_smembers( EvKeyCtx &ctx );
  ExecStatus exec_smove( EvKeyCtx &ctx );
  ExecStatus exec_spop( EvKeyCtx &ctx );
  ExecStatus exec_srandmember( EvKeyCtx &ctx );
  ExecStatus exec_srem( EvKeyCtx &ctx );
  ExecStatus exec_sunion( EvKeyCtx &ctx );
  ExecStatus exec_sunionstore( EvKeyCtx &ctx );
  ExecStatus exec_sscan( EvKeyCtx &ctx );
  ExecStatus do_sread( EvKeyCtx &ctx,  int flags );
  ExecStatus do_swrite( EvKeyCtx &ctx,  int flags );
  ExecStatus do_smultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa );
  ExecStatus do_ssetop( EvKeyCtx &ctx,  int flags );
  /* SORTED_SET */
  ExecStatus exec_zadd( EvKeyCtx &ctx );
  ExecStatus exec_zcard( EvKeyCtx &ctx );
  ExecStatus exec_zcount( EvKeyCtx &ctx );
  ExecStatus exec_zincrby( EvKeyCtx &ctx );
  ExecStatus exec_zinterstore( EvKeyCtx &ctx );
  ExecStatus exec_zlexcount( EvKeyCtx &ctx );
  ExecStatus exec_zrange( EvKeyCtx &ctx );
  ExecStatus exec_zrangebylex( EvKeyCtx &ctx );
  ExecStatus exec_zrevrangebylex( EvKeyCtx &ctx );
  ExecStatus exec_zrangebyscore( EvKeyCtx &ctx );
  ExecStatus exec_zrank( EvKeyCtx &ctx );
  ExecStatus exec_zrem( EvKeyCtx &ctx );
  ExecStatus exec_zremrangebylex( EvKeyCtx &ctx );
  ExecStatus exec_zremrangebyrank( EvKeyCtx &ctx );
  ExecStatus exec_zremrangebyscore( EvKeyCtx &ctx );
  ExecStatus exec_zrevrange( EvKeyCtx &ctx );
  ExecStatus exec_zrevrangebyscore( EvKeyCtx &ctx );
  ExecStatus exec_zrevrank( EvKeyCtx &ctx );
  ExecStatus exec_zscore( EvKeyCtx &ctx );
  ExecStatus exec_zunionstore( EvKeyCtx &ctx );
  ExecStatus exec_zscan( EvKeyCtx &ctx );
  ExecStatus exec_zpopmin( EvKeyCtx &ctx );
  ExecStatus exec_zpopmax( EvKeyCtx &ctx );
  ExecStatus exec_bzpopmin( EvKeyCtx &ctx );
  ExecStatus exec_bzpopmax( EvKeyCtx &ctx );
  ExecStatus do_zread( EvKeyCtx &ctx,  int flags );
  ExecStatus do_zwrite( EvKeyCtx &ctx,  int flags );
  ExecStatus do_zmultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa );
  ExecStatus do_zremrange( EvKeyCtx &ctx,  int flags );
  ExecStatus do_zsetop( EvKeyCtx &ctx,  int flags );
  ExecStatus do_zsetop_store( EvKeyCtx &ctx,  int flags );
  /* STRING */
  ExecStatus exec_append( EvKeyCtx &ctx );
  ExecStatus exec_bitcount( EvKeyCtx &ctx );
  ExecStatus exec_bitfield( EvKeyCtx &ctx );
  ExecStatus exec_bitop( EvKeyCtx &ctx );
  ExecStatus exec_bitpos( EvKeyCtx &ctx );
  ExecStatus exec_decr( EvKeyCtx &ctx );
  ExecStatus exec_decrby( EvKeyCtx &ctx );
  ExecStatus exec_get( EvKeyCtx &ctx );
  ExecStatus exec_getbit( EvKeyCtx &ctx );
  ExecStatus exec_getrange( EvKeyCtx &ctx );
  ExecStatus exec_getset( EvKeyCtx &ctx );
  ExecStatus exec_incr( EvKeyCtx &ctx );
  ExecStatus exec_incrby( EvKeyCtx &ctx );
  ExecStatus exec_incrbyfloat( EvKeyCtx &ctx );
  ExecStatus exec_mget( EvKeyCtx &ctx );
  ExecStatus exec_mset( EvKeyCtx &ctx );
  ExecStatus exec_msetnx( EvKeyCtx &ctx );
  ExecStatus exec_psetex( EvKeyCtx &ctx );
  ExecStatus exec_set( EvKeyCtx &ctx );
  ExecStatus exec_setbit( EvKeyCtx &ctx );
  ExecStatus exec_setex( EvKeyCtx &ctx );
  ExecStatus exec_setnx( EvKeyCtx &ctx );
  ExecStatus exec_setrange( EvKeyCtx &ctx );
  ExecStatus exec_strlen( EvKeyCtx &ctx );
  /* string extras */
  ExecStatus do_add( EvKeyCtx &ctx,  int64_t incr );
  ExecStatus do_set_value( EvKeyCtx &ctx,  int n,  int flags );
  ExecStatus do_set_value_expire( EvKeyCtx &ctx,  int n,  uint64_t ns,
                                  int flags );
  /* TRANSACTION */
  bool make_multi( void );
  void discard_multi( void );
  ExecStatus exec_discard( void );
  ExecStatus exec_exec( void );
  ExecStatus exec_multi( void );
  ExecStatus exec_unwatch( void );
  ExecStatus exec_watch( EvKeyCtx &ctx );
  /* STREAM */
  ExecStatus exec_xinfo( EvKeyCtx &ctx );
  ExecStatus xinfo_consumers( ExecStreamCtx &stream );
  ExecStatus xinfo_groups( ExecStreamCtx &stream );
  ExecStatus xinfo_streams( ExecStreamCtx &stream );

  ExecStatus exec_xadd( EvKeyCtx &ctx );
  ExecStatus exec_xtrim( EvKeyCtx &ctx );
  ExecStatus exec_xdel( EvKeyCtx &ctx );
  bool construct_xfield_output( ExecStreamCtx &stream,  size_t idx,
                                StreamBuf::BufQueue &q );
  ExecStatus exec_xrange( EvKeyCtx &ctx );
  ExecStatus exec_xrevrange( EvKeyCtx &ctx );
  ExecStatus exec_xlen( EvKeyCtx &ctx );
  ExecStatus exec_xread( EvKeyCtx &ctx );
  ExecStatus finish_xread( EvKeyCtx &ctx,  StreamBuf::BufQueue &q );
  ExecStatus exec_xreadgroup( EvKeyCtx &ctx );
  ExecStatus exec_xgroup( EvKeyCtx &ctx );
  ExecStatus exec_xack( EvKeyCtx &ctx );
  ExecStatus exec_xclaim( EvKeyCtx &ctx );
  ExecStatus exec_xpending( EvKeyCtx &ctx );
  ExecStatus exec_xsetid( EvKeyCtx &ctx );

  /* result senders */
  void send_err( int status,  kv::KeyStatus kstatus = KEY_OK );
  void send_ok( void );
  void send_nil( void );
  void send_null( void );
  void send_msg( const RedisMsg &m );
  void send_int( void );
  void send_int( int64_t ival );
  void send_zero( void );
  void send_zeroarr( void );
  void send_one( void );
  void send_neg_one( void );
  void send_zero_string( void );
  void send_queued( void );

  size_t send_string( const void *data,  size_t size );
  size_t send_simple_string( const void *data,  size_t size );
  size_t send_concat_string( const void *data,  size_t size,
                             const void *data2,  size_t size2 );

  bool save_string_result( EvKeyCtx &ctx,  const void *data,  size_t size );
  bool save_data( EvKeyCtx &ctx,  const void *data,  size_t size );
  void *save_data2( EvKeyCtx &ctx,  const void *data,  size_t size,
                                    const void *data2,  size_t size2 );
  void array_string_result( void );

  void send_err_string( const char *s,  size_t slen );
  void send_err_kv( kv::KeyStatus kstatus );
  void send_err_msg( RedisMsgStatus mstatus );
  void send_err_bad_cmd( void );
  void send_err_fmt( int status );
};

enum RedisDoPubState {
  RPUB_FORWARD_MSG  = 1, /* forward message to client */
  RPUB_CONTINUE_MSG = 2  /* trigger continuation message */
};

enum RedisBlockState {
  RBLK_CMD_TIMEOUT    = 1, /* a timer expired caused retry */
  RBLK_CMD_KEY_CHANGE = 2, /* key change caused retry */
  RBLK_CMD_COMPLETE   = 4  /* command continuation success */
};

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
