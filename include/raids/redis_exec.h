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
  ERR_BAD_DISCARD,      /* no transaction active to discard */
  ERR_ABORT_TRANS,      /* transaction aborted due to error */
  ERR_SAVE,             /* save failed */
  ERR_LOAD              /* load failed */
};

inline static bool exec_status_success( int status ) {
  return status <= EXEC_SUCCESS; /* the OK and SEND_xxx status */
}

inline static bool exec_status_fail( int status ) {
  return status > EXEC_SUCCESS;  /* the bad status */
}

/* time used to determine whether a timestamp or not */
static const uint64_t TEN_YEARS_NS = (uint64_t) ( 10 * 12 ) *
                                     (uint64_t) ( 30 * 24 * 60 * 60 ) *
                                     (uint64_t) ( 1000 * 1000 * 1000 );

struct EvSocket;
struct EvPublish;
struct RedisExec;
struct RouteDB;
struct PeerData;
struct PeerMatchArgs;
struct KvPubSub;
struct ExecStreamCtx;
struct RedisMultiExec;

struct ScanArgs {
  int64_t pos,    /* position argument, the position where scan starts */
          maxcnt; /* COUNT argument, the maximum number of elements    */
  pcre2_real_code_8       * re; /* pcre regex compiled */
  pcre2_real_match_data_8 * md; /* pcre match context  */
  ScanArgs() : pos( 0 ), maxcnt( 10 ), re( 0 ), md( 0 ) {}
};

enum ExecCmdState { /* cmd_state flags */
  CMD_STATE_NORMAL            = 0, /* no flags */
  CMD_STATE_MONITOR           = 1, /* monitor cmd on */
  CMD_STATE_CLIENT_REPLY_SKIP = 2, /* skip output of next cmd */
  CMD_STATE_CLIENT_REPLY_OFF  = 4, /* mute output until client reply on again */
  CMD_STATE_MULTI_QUEUED      = 8, /* MULTI started, queue cmds */
  CMD_STATE_EXEC_MULTI        = 16,/* EXEC transaction running */
  CMD_STATE_SAVE              = 32,/* SAVE is running */
  CMD_STATE_LOAD              = 64 /* LOAD is running */
};

struct RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  kv::HashSeed     hs;        /* kv map hash seeds, different for each db */
  kv::KeyCtx       kctx;      /* key context used for every key in command */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffer, reset before each key lookup */
  kv::DLinkList<RedisContinueMsg> cont_list, /* continuations ready to run */
                                  wait_list; /* these are waiting on a timer */
  StreamBuf       & strm;      /* output buffer, result of command execution */
  size_t            strm_start;/* output offset before command starts */
  RedisMsg          msg;       /* current command msg */
  EvKeyCtx        * key,       /* currently executing key */
                 ** keys;      /* all of the keys in command */
  uint32_t          key_cnt,   /* total keys[] size */
                    key_done;  /* number of keys processed */
  RedisMultiExec  * multi;     /* MULTI .. EXEC block */
  RedisCmd          cmd;       /* current command (GET_CMD) */
  RedisCatg         catg;      /* current command (GET_CMD) */
  RedisMsgStatus    mstatus;   /* command message parse status */
  uint8_t           blk_state, /* if blocking cmd timed out (RBLK_CMD_TIMEOUT)*/
                    cmd_state; /* if monitor is active or skipping */
  uint16_t          cmd_flags, /* command flags (CMD_READ_FLAG) */
                    key_flags; /* EvKeyFlags, if a key has a keyspace event */
  int16_t           arity,     /* number of command args */
                    first,     /* first key in args */
                    last,      /* last key in args */
                    step;      /* incr between keys */
  uint64_t          step_mask; /* step key mask */
  size_t            argc;      /* count of args in cmd msg */
  RedisSubMap       sub_tab;   /* pub/sub subscription table */
  RedisPatternMap   pat_tab;   /* pub/sub pattern sub table */
  RedisContinueMap  continue_tab; /* blocked continuations */
  RouteDB         & sub_route; /* map subject to sub_id */
  PeerData        & peer;      /* name and address of this peer */
  uint64_t          timer_id;  /* timer id of this service */
  kv::KeyFragment * save_key;  /* if key is being saved */
  uint64_t          msg_route_cnt; /* count of msgs forwarded */
  uint32_t          sub_id,    /* fd, set this after accept() */
                    next_event_id; /* next event id for timers */

  RedisExec( kv::HashTab &map,  uint32_t ,  uint32_t dbx_id,
             StreamBuf &s,  RouteDB &rdb,  PeerData &pd ) :
      kctx( map, dbx_id, NULL ), strm( s ), strm_start( s.pending() ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ), multi( 0 ),
      cmd( NO_CMD ), catg( NO_CATG ), blk_state( 0 ), cmd_state( 0 ),
      key_flags( 0 ), sub_route( rdb ), peer( pd ), timer_id( 0 ),
      save_key( 0 ), msg_route_cnt( 0 ), sub_id( ~0U ), next_event_id( 0 ) {
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->hs );
    this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  }
  /* different for each endpoint */
  void setup_ids( uint32_t sid,  uint64_t tid ) {
    this->sub_id        = sid;
    this->timer_id      = tid;
    this->msg_route_cnt = 0;
  }
  void setup_cmd( const RedisCmdData &c ) {
    this->catg      = (RedisCatg) c.catg;
    this->arity     = c.arity;
    this->first     = c.first;
    this->last      = c.last;
    this->step      = c.step;
    this->cmd_flags = c.flags;
    this->key_flags = EKF_MONITOR; /* monitor always published if subscribed */
    this->step_mask = 0;
  }
  /* stop a continuation and send null */
  bool continue_expire( uint64_t event_id,  RedisContinueMsg *&cm ) noexcept;
  /* remove and unsubscribe contiuation subjects from continue_tab */
  void pop_continue_tab( RedisContinueMsg *cm ) noexcept;
  /* restart continuation */
  void push_continue_list( RedisContinueMsg *cm ) noexcept;
  /* release anything allocated */
  void release( void ) noexcept;
  /* unsubscribe anything subscribed */
  void rem_all_sub( void ) noexcept;
  /* handle the keys are cmd specific */
  bool locate_movablekeys( void ) noexcept;
  /* fetch next key arg[ i ] after current i, return false if none */
  bool next_key( int &i ) noexcept;
  /* return number of keys in cmd */
  size_t calc_key_count( void ) noexcept;
  /* set up a single key, there may be multiple in a command */
  ExecStatus exec_key_setup( EvSocket *svc,  EvPrefetchQueue *q,
                             EvKeyCtx *&ctx,  int n,  uint32_t idx ) noexcept;
  void exec_run_to_completion( void ) noexcept;
  /* resolve and setup command */
  ExecStatus prepare_exec_command( void ) noexcept;
  /* parse set up a command */
  ExecStatus exec( EvSocket *svc,  EvPrefetchQueue *q ) noexcept;
  /* run cmd that doesn't have keys */
  ExecStatus exec_nokeys( void ) noexcept;
  /* execute a key operation */
  ExecStatus exec_key_continue( EvKeyCtx &ctx ) noexcept;
  /* subscribe to keyspace subjects and wait for publish to continue */
  ExecStatus save_blocked_cmd( int64_t timeout_val ) noexcept;
  /* execute the saved commands after signaled */
  void drain_continuations( EvSocket *svc ) noexcept;
  /* publish keyspace events */
  void pub_keyspace_events( void ) noexcept;
  /* set the hash */
  void exec_key_set( EvKeyCtx &ctx ) {
    this->key = ctx.set( this->kctx );
  }
  /* compute the hash and prefetch the ht[] location */
  void exec_key_prefetch( EvKeyCtx &ctx ) {
    ctx.prefetch( this->kctx.ht, ( this->cmd_flags & CMD_READ_FLAG ) != 0 );
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
  kv::KeyStatus exec_key_fetch( EvKeyCtx &ctx,
                                bool force_read = false ) noexcept;
  /* CLUSTER */
  ExecStatus exec_cluster( void ) noexcept;
  ExecStatus exec_readonly( void ) noexcept;
  ExecStatus exec_readwrite( void ) noexcept;
  /* CONNECTION */
  ExecStatus exec_auth( void ) noexcept;
  ExecStatus exec_echo( void ) noexcept;
  ExecStatus exec_ping( void ) noexcept;
  ExecStatus exec_quit( void ) noexcept;
  ExecStatus exec_select( void ) noexcept;
  ExecStatus exec_swapdb( void ) noexcept;
  /* GEO */
  ExecStatus exec_geoadd( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geohash( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geopos( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geodist( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_georadius( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_georadiusbymember( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_gread( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_gradius( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_gradius_store( EvKeyCtx &ctx ) noexcept;
  /* HASH */
  ExecStatus exec_happend( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdel( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdiff( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdiffstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hexists( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hget( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hgetall( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hincrby( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hincrbyfloat( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hinter( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hinterstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hkeys( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hlen( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hmget( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hmset( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hset( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hsetnx( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hstrlen( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hvals( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hscan( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hunion( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hunionstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_hmultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *hs ) noexcept;
  ExecStatus do_hread( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_hwrite( EvKeyCtx &ctx,  int flags ) noexcept;
  /* HYPERLOGLOG */
  ExecStatus exec_pfadd( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pfcount( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pfmerge( EvKeyCtx &ctx ) noexcept;
  /* KEY */
  ExecStatus exec_del( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_dump( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_string( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_list( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_hash( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_set( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_zset( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_geo( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_hll( EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_stream( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_exists( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_expire( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_expireat( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_keys( void ) noexcept;
  ExecStatus exec_migrate( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_move( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_object( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_persist( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pexpire( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pexpireat( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_pexpire( EvKeyCtx &ctx,  uint64_t units ) noexcept;
  ExecStatus do_pexpireat( EvKeyCtx &ctx,  uint64_t units ) noexcept;
  ExecStatus exec_pttl( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_pttl( EvKeyCtx &ctx,  int64_t units ) noexcept;
  ExecStatus exec_randomkey( void ) noexcept;
  ExecStatus exec_rename( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_renamenx( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_restore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sort( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_touch( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_ttl( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_type( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_unlink( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_wait( void ) noexcept;
  ExecStatus exec_scan( void ) noexcept;
  ExecStatus match_scan_args( ScanArgs &sa,  size_t i ) noexcept;
  void release_scan_args( ScanArgs &sa ) noexcept;
  ExecStatus scan_keys( ScanArgs &sa ) noexcept;
  /* LIST */
  ExecStatus exec_blpop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_brpop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_brpoplpush( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lindex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_linsert( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_llen( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpush( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpushx( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lrem( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lset( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_ltrim( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpoplpush( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpush( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpushx( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_push( EvKeyCtx &ctx,  int flags,  const char *value = NULL,
                      size_t valuelen = 0 ) noexcept;
  ExecStatus do_pop( EvKeyCtx &ctx,  int flags ) noexcept;
  /* PUBSUB */
  ExecStatus exec_psubscribe( void ) noexcept;
  ExecStatus exec_pubsub( void ) noexcept;
  ExecStatus exec_publish( void ) noexcept;
  ExecStatus exec_punsubscribe( void ) noexcept;
  ExecStatus exec_subscribe( void ) noexcept;
  ExecStatus exec_unsubscribe( void ) noexcept;
  ExecStatus do_subscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_unsubscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_psubscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_punsubscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_sub( int flags ) noexcept;
  bool pub_message( EvPublish &pub,  RedisPatternRoute *rt ) noexcept;
  int do_pub( EvPublish &pub,  RedisContinueMsg *&cm ) noexcept;
  bool do_hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept;
  /* SCRIPT */
  ExecStatus exec_eval( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_evalsha( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_script( EvKeyCtx &ctx ) noexcept;
  /* SERVER */
  ExecStatus exec_bgrewriteaof( void ) noexcept;
  ExecStatus exec_bgsave( void ) noexcept;
  bool get_peer_match_args( PeerMatchArgs &ka ) noexcept;
  ExecStatus exec_client( void ) noexcept;
  int client_list( char *buf,  size_t buflen ) noexcept;
  ExecStatus exec_command( void ) noexcept;
  ExecStatus exec_config( void ) noexcept;
  ExecStatus exec_dbsize( void ) noexcept;
  ExecStatus debug_object( void ) noexcept;
  ExecStatus debug_htstats( void ) noexcept;
  ExecStatus exec_debug( void ) noexcept;
  ExecStatus exec_flushall( void ) noexcept;
  ExecStatus exec_flushdb( void ) noexcept;
  void flushdb( uint8_t db_num ) noexcept;
  ExecStatus exec_info( void ) noexcept;
  ExecStatus exec_lastsave( void ) noexcept;
  ExecStatus exec_memory( void ) noexcept;
  ExecStatus exec_monitor( void ) noexcept;
  ExecStatus exec_role( void ) noexcept;
  ExecStatus exec_save( void ) noexcept;
  ExecStatus exec_load( void ) noexcept;
  ExecStatus exec_shutdown( void ) noexcept;
  ExecStatus exec_slaveof( void ) noexcept;
  ExecStatus exec_slowlog( void ) noexcept;
  ExecStatus exec_sync( void ) noexcept;
  ExecStatus exec_time( void ) noexcept;
  /* SET */
  ExecStatus exec_sadd( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_scard( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sdiff( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sdiffstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sinter( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sinterstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sismember( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_smembers( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_smove( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_spop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_srandmember( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_srem( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sunion( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sunionstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sscan( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_sread( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_swrite( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_smultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa ) noexcept;
  ExecStatus do_ssetop( EvKeyCtx &ctx,  int flags ) noexcept;
  /* SORTED_SET */
  ExecStatus exec_zadd( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zcard( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zcount( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zincrby( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zinterstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zlexcount( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrangebylex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrangebylex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrangebyscore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrank( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrem( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebylex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebyrank( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebyscore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrangebyscore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrank( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zscore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zunionstore( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zscan( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zpopmin( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zpopmax( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bzpopmin( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bzpopmax( EvKeyCtx &ctx ) noexcept;
  ExecStatus do_zread( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zwrite( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zmultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa ) noexcept;
  ExecStatus do_zremrange( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zsetop( EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zsetop_store( EvKeyCtx &ctx,  int flags ) noexcept;
  /* STRING */
  ExecStatus exec_append( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitcount( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitfield( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitop( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitpos( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_decr( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_decrby( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_get( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getbit( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getset( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incr( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incrby( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incrbyfloat( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_mget( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_mset( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_msetnx( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_psetex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_set( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setbit( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setex( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setnx( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_strlen( EvKeyCtx &ctx ) noexcept;
  /* string extras */
  ExecStatus do_add( EvKeyCtx &ctx,  int64_t incr ) noexcept;
  ExecStatus do_set_value( EvKeyCtx &ctx,  int n,  int flags ) noexcept;
  ExecStatus do_set_value_expire( EvKeyCtx &ctx,  int n,  uint64_t ns,
                                  int flags ) noexcept;
  /* TRANSACTION */
  bool make_multi( void ) noexcept;
  void discard_multi( void ) noexcept;
  void multi_key_fetch( EvKeyCtx &ctx,  bool force_read ) noexcept;
  bool multi_try_lock( void ) noexcept;
  void multi_release_lock( void ) noexcept;
  ExecStatus multi_queued( EvSocket *svc ) noexcept;
  ExecStatus exec_discard( void ) noexcept;
  ExecStatus exec_exec( void ) noexcept;
  ExecStatus exec_multi( void ) noexcept;
  ExecStatus exec_unwatch( void ) noexcept;
  ExecStatus exec_watch( EvKeyCtx &ctx ) noexcept;
  /* STREAM */
  ExecStatus exec_xinfo( EvKeyCtx &ctx ) noexcept;
  ExecStatus xinfo_consumers( ExecStreamCtx &stream ) noexcept;
  ExecStatus xinfo_groups( ExecStreamCtx &stream ) noexcept;
  ExecStatus xinfo_streams( ExecStreamCtx &stream ) noexcept;

  ExecStatus exec_xadd( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xtrim( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xdel( EvKeyCtx &ctx ) noexcept;
  bool construct_xfield_output( ExecStreamCtx &stream,  size_t idx,
                                StreamBuf::BufQueue &q ) noexcept;
  ExecStatus exec_xrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xrevrange( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xlen( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xread( EvKeyCtx &ctx ) noexcept;
  ExecStatus finish_xread( EvKeyCtx &ctx,  StreamBuf::BufQueue &q ) noexcept;
  ExecStatus exec_xreadgroup( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xgroup( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xack( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xclaim( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xpending( EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xsetid( EvKeyCtx &ctx ) noexcept;

  /* result senders */
  void send_status( ExecStatus stat,  kv::KeyStatus kstat ) noexcept;
  void send_err_string( ExecStatus err,  kv::KeyStatus kstat ) noexcept;
  void send_ok( void ) noexcept;
  void send_nil( void ) noexcept;
  void send_null( void ) noexcept;
  void send_msg( const RedisMsg &m ) noexcept;
  void send_int( void ) noexcept;
  void send_int( int64_t ival ) noexcept;
  void send_zero( void ) noexcept;
  void send_zeroarr( void ) noexcept;
  void send_one( void ) noexcept;
  void send_neg_one( void ) noexcept;
  void send_zero_string( void ) noexcept;
  void send_queued( void ) noexcept;

  size_t send_string( const void *data,  size_t size ) noexcept;
  size_t send_simple_string( const void *data,  size_t size ) noexcept;
  size_t send_concat_string( const void *data,  size_t size,
                             const void *data2,  size_t size2 ) noexcept;

  bool save_string_result( EvKeyCtx &ctx,  const void *data,
                           size_t size ) noexcept;
  bool save_data( EvKeyCtx &ctx,  const void *data,  size_t size ) noexcept;
  void *save_data2( EvKeyCtx &ctx,  const void *data,  size_t size,
                                    const void *data2,  size_t size2 ) noexcept;
  void array_string_result( void ) noexcept;
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

}
}

#endif
