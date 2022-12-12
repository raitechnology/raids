#ifndef __rai_raids__redis_exec_h__
#define __rai_raids__redis_exec_h__

#include <raikv/ev_key.h>
#include <raikv/stream_buf.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_pubsub.h>

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

namespace rai {

namespace kv {
  struct RoutePublish;
  struct TimerQueue;
  struct EvSocket;
  struct PeerMatchArgs;
  struct EvSocket;
  struct EvPublish;
  struct KvPubSub;
  struct EvPrefetchQueue;
  struct EvPoll;
  struct NotifySub;
  struct NotifyPattern;
}

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
  EXEC_SUCCESS  = kv::EK_SUCCESS,  /* <= success = good */
  EXEC_DEPENDS  = kv::EK_DEPENDS,  /* key depends (dest) on another key arg (src) */
  EXEC_CONTINUE = kv::EK_CONTINUE, /* continue working, more keys */
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

struct RedisExec;
struct ExecStreamCtx;
struct RedisMultiExec;
struct RedisMsgTransform;

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

struct RedisBufQueue : public kv::StreamBuf::BufQueue {
  RedisBufQueue( kv::StreamBuf &s ) : kv::StreamBuf::BufQueue( s, 48, 928 ) {}
  /* a nil string: $-1\r\n or *-1\r\n if is_null true */
  size_t append_nil( bool is_null = false ) noexcept;
  /* a zero length array: *0\r\n */
  size_t append_zero_array( void ) noexcept;
  /* one string item, appended with decorations: $<strlen>\r\n<string>\r\n */
  size_t append_string( const void *str,  size_t len,  const void *str2=0,
                        size_t len2=0 ) noexcept;
  /* an int, appended with decorations: :<val>\r\n */
  size_t append_uint( uint64_t val ) noexcept;
  /* make array: [item1, item2, ...], prepends decorations *<arraylen>\r\n */
  void prepend_array( size_t nitems ) noexcept;
  /* cursor is two arrays: [cursor,[items]] */
  void prepend_cursor_array( size_t curs,  size_t nitems ) noexcept;
};

struct RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  kv::HashSeed     hs;        /* kv map hash seeds, different for each db */
  kv::KeyCtx       kctx;      /* key context used for every key in command */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffer, reset before each key lookup */
  kv::DLinkList<RedisContinueMsg> cont_list, /* continuations ready to run */
                                  wait_list; /* these are waiting on a timer */
  kv::StreamBuf   & strm;      /* output buffer, result of command execution */
  size_t            strm_start;/* output offset before command starts */
  RedisMsg          msg;       /* current command msg */
  kv::EvKeyCtx    * key,       /* currently executing key */
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
  kv::RoutePublish& sub_route; /* map subject to sub_id */
  kv::TimerQueue  & timer;
  kv::EvSocket    & peer;      /* name and address of this peer */
  uint64_t          timer_id;  /* timer id of this service */
  kv::KeyFragment * save_key;  /* if key is being saved */
  uint64_t          msg_route_cnt; /* count of msgs forwarded */
  uint32_t          sub_id,    /* fd, set this after accept() */
                    next_event_id; /* next event id for timers */
  RedisSubMap       sub_tab;   /* pub/sub subscription table */
  RedisPatternMap   pat_tab;   /* pub/sub pattern sub table */
  uint64_t          stamp;
  uint16_t          prefix_len,/* pubsub prefix */
                    session_len;
  char              prefix[ 16 ],
                    session[ 64 /*MAX_SESSION_LEN*/ ];

  RedisExec( kv::HashTab &map,  uint32_t ,  uint32_t dbx_id,
             kv::StreamBuf &s,  kv::RoutePublish &rdb,  kv::EvSocket &pd,
             kv::TimerQueue &tq ) noexcept;
#if 0
      kctx( map, dbx_id, NULL ), strm( s ), strm_start( s.pending() ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ), multi( 0 ),
      cmd( NO_CMD ), catg( NO_CATG ), blk_state( 0 ), cmd_state( 0 ),
      key_flags( 0 ), prefix_len( 0 ), sub_route( rdb ), timer( tq ),
      peer( pd ), timer_id( 0 ), save_key( 0 ), msg_route_cnt( 0 ),
      sub_id( ~0U ), next_event_id( 0 ) {
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->hs );
    this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  }
#endif
  /* different for each endpoint */
  void setup_ids( uint32_t sid,  uint64_t tid ) {
    this->sub_id        = sid;
    this->timer_id      = tid;
    this->msg_route_cnt = 0;
    this->stamp         = 0;
  }
  void setup_cmd( const RedisCmdData &c ) {
    this->catg      = (RedisCatg) c.catg;
    this->arity     = c.arity;
    this->first     = c.first;
    this->last      = c.last;
    this->step      = c.step;
    this->cmd_flags = c.flags;
    this->key_flags = kv::EKF_MONITOR; /* monitor always pub if subscribed */
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
  ExecStatus exec_key_setup( kv::EvSocket *svc,  kv::EvPrefetchQueue *q,
                             kv::EvKeyCtx *&ctx,  int n,  uint32_t idx ) noexcept;
  void exec_run_to_completion( void ) noexcept;
  /* resolve and setup command */
  ExecStatus prepare_exec_command( void ) noexcept;
  /* parse set up a command */
  ExecStatus exec( kv::EvSocket *svc,  kv::EvPrefetchQueue *q ) noexcept;
  /* run cmd that doesn't have keys */
  ExecStatus exec_nokeys( void ) noexcept;
  /* execute a key operation */
  ExecStatus exec_key_continue( kv::EvKeyCtx &ctx ) noexcept;
  /* subscribe to keyspace subjects and wait for publish to continue */
  ExecStatus save_blocked_cmd( int64_t timeout_val ) noexcept;
  /* execute the saved commands after signaled */
  void drain_continuations( kv::EvSocket *svc ) noexcept;
  /* publish keyspace events */
  void pub_keyspace_events( void ) noexcept;
  /* set the hash */
  void exec_key_set( kv::EvKeyCtx &ctx ) {
    this->key = ctx.set( this->kctx );
  }
  /* compute the hash and prefetch the ht[] location */
  void exec_key_prefetch( kv::EvKeyCtx &ctx ) {
    ctx.prefetch( this->kctx.ht, ( this->cmd_flags & CMD_READ_FLAG ) != 0 );
  }
  /* fetch key for write and check type matches or is not set */
  kv::KeyStatus get_key_write( kv::EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, false );
    if ( status == KEY_OK && ctx.type != type ) {
      if ( ctx.type == 0 ) {
        ctx.flags |= kv::EKF_IS_NEW;
        return KEY_IS_NEW;
      }
      return KEY_NO_VALUE;
    }
    return status;
  }
  /* fetch key for read and check type matches or is not set */
  kv::KeyStatus get_key_read( kv::EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, true );
    if ( status == KEY_OK && ctx.type != type )
      return ( ctx.type == 0 ) ? KEY_NOT_FOUND : KEY_NO_VALUE;
    return status;
  }
  /* fetch a read key value or acquire it for write */
  kv::KeyStatus exec_key_fetch( kv::EvKeyCtx &ctx,
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
  ExecStatus exec_geoadd( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geohash( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geopos( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_geodist( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_georadius( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_georadiusbymember( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_gread( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_gradius( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_gradius_store( kv::EvKeyCtx &ctx ) noexcept;
  /* HASH */
  ExecStatus exec_happend( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdel( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdiff( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hdiffstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hexists( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hget( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hgetall( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hincrby( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hincrbyfloat( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hinter( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hinterstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hkeys( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hlen( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hmget( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hmset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hsetnx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hstrlen( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hvals( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hscan( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hunion( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_hunionstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_hmultiscan( kv::EvKeyCtx &ctx,  int flags,  ScanArgs *hs ) noexcept;
  ExecStatus do_hread( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_hwrite( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  /* HYPERLOGLOG */
  ExecStatus exec_pfadd( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pfcount( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pfmerge( kv::EvKeyCtx &ctx ) noexcept;
  /* KEY */
  ExecStatus exec_del( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_dump( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_string( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_list( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_hash( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_set( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_zset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_geo( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_hll( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus dump_stream( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_exists( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_expire( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_expireat( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_keys( void ) noexcept;
  ExecStatus exec_migrate( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_move( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_object( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_persist( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pexpire( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_pexpireat( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_pexpire( kv::EvKeyCtx &ctx,  uint64_t units ) noexcept;
  ExecStatus do_pexpireat( kv::EvKeyCtx &ctx,  uint64_t units ) noexcept;
  ExecStatus exec_pttl( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_pttl( kv::EvKeyCtx &ctx,  int64_t units ) noexcept;
  ExecStatus exec_randomkey( void ) noexcept;
  ExecStatus exec_rename( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_renamenx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_restore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sort( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_touch( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_ttl( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_type( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_unlink( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_wait( void ) noexcept;
  ExecStatus exec_scan( void ) noexcept;
  ExecStatus match_scan_args( ScanArgs &sa,  size_t i ) noexcept;
  void release_scan_args( ScanArgs &sa ) noexcept;
  ExecStatus scan_keys( ScanArgs &sa ) noexcept;
  /* LIST */
  ExecStatus exec_blpop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_brpop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_brpoplpush( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lindex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_linsert( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_llen( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpush( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lpushx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lrem( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_lset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_ltrim( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpoplpush( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpush( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_rpushx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_push( kv::EvKeyCtx &ctx,  int flags,  const char *value = NULL,
                      size_t valuelen = 0 ) noexcept;
  ExecStatus do_pop( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  /* PUBSUB */
  ExecStatus exec_psubscribe( void ) noexcept;
  ExecStatus exec_pubsub( void ) noexcept;
  ExecStatus exec_publish( void ) noexcept;
  ExecStatus exec_punsubscribe( void ) noexcept;
  ExecStatus exec_subscribe( void ) noexcept;
  ExecStatus exec_unsubscribe( void ) noexcept;
  ExecStatus do_unsubscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_subscribe_cb( const char *sub,  size_t len,
                             ds_on_msg_t cb = NULL,  void *cl = NULL ) noexcept;
  ExecStatus do_psubscribe_cb( const char *sub,  size_t len,
                             ds_on_msg_t cb = NULL,  void *cl = NULL ) noexcept;
  ExecStatus do_punsubscribe( const char *sub,  size_t len ) noexcept;
  ExecStatus do_sub( int flags ) noexcept;
  bool pub_message( kv::EvPublish &pub,  RedisMsgTransform &xf,
                    RedisWildMatch *m ) noexcept;
  int do_pub( kv::EvPublish &pub,  RedisContinueMsg *&cm ) noexcept;
  int on_inbox_reply( kv::EvPublish &pub,  RedisContinueMsg *&cm ) noexcept;
  uint8_t test_subscribed( const kv::NotifySub &sub ) noexcept;
  uint8_t test_psubscribed( const kv::NotifyPattern &pat ) noexcept;
  size_t do_get_subscriptions( kv::SubRouteDB &subs, kv::SubRouteDB &pats,
                               int &pattern_fmt ) noexcept;
  bool do_hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept;
  void set_session( const char *sess,  size_t sess_len ) noexcept;
  /* SCRIPT */
  ExecStatus exec_eval( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_evalsha( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_script( kv::EvKeyCtx &ctx ) noexcept;
  /* SERVER */
  ExecStatus exec_bgrewriteaof( void ) noexcept;
  ExecStatus exec_bgsave( void ) noexcept;
  bool get_peer_match_args( kv::PeerMatchArgs &ka ) noexcept;
  ExecStatus exec_client( void ) noexcept;
  int exec_client_list( char *buf,  size_t buflen ) noexcept;
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
  ExecStatus exec_sadd( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_scard( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sdiff( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sdiffstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sinter( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sinterstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sismember( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_smembers( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_smove( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_spop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_srandmember( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_srem( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sunion( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sunionstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_sscan( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_sread( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_swrite( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_smultiscan( kv::EvKeyCtx &ctx,  int flags,  ScanArgs *sa ) noexcept;
  ExecStatus do_ssetop( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  /* SORTED_SET */
  ExecStatus exec_zadd( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zcard( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zcount( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zincrby( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zinterstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zlexcount( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrangebylex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrangebylex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrangebyscore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrank( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrem( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebylex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebyrank( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zremrangebyscore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrangebyscore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zrevrank( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zscore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zunionstore( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zscan( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zpopmin( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_zpopmax( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bzpopmin( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bzpopmax( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus do_zread( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zwrite( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zmultiscan( kv::EvKeyCtx &ctx,  int flags,  ScanArgs *sa ) noexcept;
  ExecStatus do_zremrange( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zsetop( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  ExecStatus do_zsetop_store( kv::EvKeyCtx &ctx,  int flags ) noexcept;
  /* STRING */
  ExecStatus exec_append( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitcount( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitfield( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitop( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_bitpos( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_decr( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_decrby( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_get( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getbit( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_getset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incr( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incrby( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_incrbyfloat( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_mget( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_mset( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_msetnx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_psetex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_set( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setbit( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setex( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setnx( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_setrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_strlen( kv::EvKeyCtx &ctx ) noexcept;
  /* string extras */
  ExecStatus do_add( kv::EvKeyCtx &ctx,  int64_t incr ) noexcept;
  ExecStatus do_set_value( kv::EvKeyCtx &ctx,  int n,  int flags ) noexcept;
  ExecStatus do_set_value_expire( kv::EvKeyCtx &ctx,  int n,  uint64_t ns,
                                  int flags ) noexcept;
  /* TRANSACTION */
  bool make_multi( void ) noexcept;
  void discard_multi( void ) noexcept;
  void multi_key_fetch( kv::EvKeyCtx &ctx,  bool force_read ) noexcept;
  bool multi_try_lock( void ) noexcept;
  void multi_release_lock( void ) noexcept;
  ExecStatus multi_queued( kv::EvSocket *svc ) noexcept;
  ExecStatus exec_discard( void ) noexcept;
  ExecStatus exec_exec( void ) noexcept;
  ExecStatus exec_multi( void ) noexcept;
  ExecStatus exec_unwatch( void ) noexcept;
  ExecStatus exec_watch( kv::EvKeyCtx &ctx ) noexcept;
  /* STREAM */
  ExecStatus exec_xinfo( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus xinfo_consumers( ExecStreamCtx &stream ) noexcept;
  ExecStatus xinfo_groups( ExecStreamCtx &stream ) noexcept;
  ExecStatus xinfo_streams( ExecStreamCtx &stream ) noexcept;

  ExecStatus exec_xadd( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xtrim( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xdel( kv::EvKeyCtx &ctx ) noexcept;
  bool construct_xfield_output( ExecStreamCtx &stream,  size_t idx,
                                RedisBufQueue &q ) noexcept;
  ExecStatus exec_xrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xrevrange( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xlen( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xread( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus finish_xread( kv::EvKeyCtx &ctx,  RedisBufQueue &q ) noexcept;
  ExecStatus exec_xreadgroup( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xgroup( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xack( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xclaim( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xpending( kv::EvKeyCtx &ctx ) noexcept;
  ExecStatus exec_xsetid( kv::EvKeyCtx &ctx ) noexcept;

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

  bool save_string_result( kv::EvKeyCtx &ctx,  const void *data,
                           size_t size ) noexcept;
  bool save_data( kv::EvKeyCtx &ctx,  const void *data,  size_t size ) noexcept;
  void *save_data2( kv::EvKeyCtx &ctx,  const void *data,  size_t size,
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
