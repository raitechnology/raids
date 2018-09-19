#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/time.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>
#include <raids/md_type.h>

using namespace rai;
using namespace ds;
using namespace kv;

static char ok[]      = "+OK\r\n";
static char nil[]     = "$-1\r\n";
static char zero[]    = ":0\r\n";
static char one[]     = ":1\r\n";
static char neg_one[] = ":-1\r\n";
static char mt[]      = "$0\r\n\r\n"; /* zero length string */
static const size_t ok_sz      = sizeof( ok ) - 1,
                    nil_sz     = sizeof( nil ) - 1,
                    zero_sz    = sizeof( zero ) - 1,
                    one_sz     = sizeof( one ) - 1,
                    neg_one_sz = sizeof( neg_one ) - 1,
                    mt_sz      = sizeof( mt ) - 1;
void RedisExec::send_ok( void ) { this->strm.append( ok, ok_sz ); }
void RedisExec::send_nil( void ) { this->strm.append( nil, nil_sz ); }
void RedisExec::send_zero( void ) { this->strm.append( zero, zero_sz ); }
void RedisExec::send_one( void ) { this->strm.append( one, one_sz ); }
void RedisExec::send_neg_one( void ) { this->strm.append( neg_one, neg_one_sz);}
void RedisExec::send_zero_string( void ) { this->strm.append( mt, mt_sz ); }

size_t
RedisExec::send_string( const void *data,  size_t size )
{
  size_t sz  = 32 + size;
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return 0;
  str[ 0 ] = '$';
  sz = 1 + RedisMsg::uint_to_str( size, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  return crlf( str, sz + size );
}

size_t
RedisExec::send_concat_string( const void *data,  size_t size,
                               const void *data2,  size_t size2 )
{
  size_t sz  = 32 + size + size2;
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return 0;
  str[ 0 ] = '$';
  sz = 1 + RedisMsg::uint_to_str( size + size2, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  if ( size2 > 0 )
    ::memcpy( &str[ sz + size ], data2, size2 );
  return crlf( str, sz + size + size2 );
}

bool
RedisExec::save_string_result( RedisKeyCtx &ctx,  const void *data,
                               size_t size )
{
  size_t msz = sizeof( RedisKeyTempResult ) + size + 24;
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    RedisKeyTempResult *part;
    part = (RedisKeyTempResult *) this->strm.alloc_temp( msz );
    if ( part != NULL ) {
      part->mem_size = msz;
      part->type = 0; /* no type */
      ctx.part = part;
    }
    else {
      return false;
    }
  }
  char *str = ctx.part->data( 0 );
  size_t sz;
  str[ 0 ] = '$';
  sz = 1 + RedisMsg::uint_to_str( size, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  ctx.part->size = crlf( str, sz + size );
  return true;
}

bool
RedisExec::save_data( RedisKeyCtx &ctx,  const void *data,  size_t size,
                      uint8_t type )
{
  size_t msz = sizeof( RedisKeyTempResult ) + size;
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    RedisKeyTempResult *part;
    part = (RedisKeyTempResult *) this->strm.alloc_temp( msz );
    if ( part != NULL ) {
      part->mem_size = msz;
      ctx.part = part;
    }
    else {
      return false;
    }
  }
  ::memcpy( ctx.part->data( 0 ), data, size );
  ctx.part->size = size;
  ctx.part->type = type;
  return true;
}

void
RedisExec::array_string_result( void )
{
  char               * str = this->strm.alloc( 32 );
  RedisKeyTempResult * part;
  size_t               sz;
  if ( str == NULL )
    return;
  str[ 0 ] = '*';
  sz = 1 + RedisMsg::uint_to_str( this->key_cnt, &str[ 1 ] );
  this->strm.sz += crlf( str, sz );

  if ( this->key_cnt == 1 ) /* only one part, no keys[] array */
    part = this->key->part;
  else
    part = this->keys[ 0 ]->part;
  for ( uint32_t i = 0; ; ) {
    if ( part != NULL ) {
      if ( part->size < 256 )
        this->strm.append( part->data( 0 ), part->size );
      else
        this->strm.append_iov( part->data( 0 ), part->size );
    }
    else
      this->strm.append( nil, nil_sz );
    if ( ++i == this->key_cnt )
      break;
    part = this->keys[ i ]->part;
  }
}

ExecStatus
RedisExec::exec_key_setup( EvSocket *own,  EvPrefetchQueue *q,
                           RedisKeyCtx *&ctx,  int n )
{
  const char * key;
  size_t       keylen;
  if ( ! this->msg.get_arg( n, key, keylen ) )
    return ERR_BAD_ARGS;
  void *p = this->strm.alloc_temp( RedisKeyCtx::size( keylen ) );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;
  ctx = new ( p ) RedisKeyCtx( *this, own, key, keylen, n,
                               this->seed, this->seed2 );
  if ( q != NULL && ! q->push( ctx ) )
    return ERR_ALLOC_FAIL;
  ctx->status = EXEC_CONTINUE;
  return EXEC_SETUP_OK;
}

void
RedisExec::exec_run_to_completion( void )
{
  if ( this->key_cnt == 1 ) { /* only one key */
    while ( this->key->status == EXEC_CONTINUE ||
            this->key->status == EXEC_DEPENDS )
      if ( this->exec_key_continue( *this->key ) == EXEC_SUCCESS )
        break;
  }
  else {
    /* cycle through keys */
    uint32_t j = 0;
    for ( uint32_t i = 0; ; ) {
      if ( this->keys[ i ]->status == EXEC_CONTINUE ||
           this->keys[ i ]->status == EXEC_DEPENDS ) {
        if ( this->exec_key_continue( *this->keys[ i ] ) == EXEC_SUCCESS )
          break;
        j = 0;
      }
      else if ( ++j == this->key_cnt )
        break;
      if ( ++i == this->key_cnt )
        i = 0;
    }
  }
}

bool
RedisExec::locate_movablekeys( void )
{
  int64_t i;
  this->step_mask = 0;
  this->first     = 0;
  this->last      = 0;
  this->step      = 0;
  switch ( this->cmd ) {  /* these commands do not follow regular rules */
    /* GEORADIUS key long lat mem rad unit [WITHCOORD] [WITHDIST]
     * [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key] */
    /* GEORADIUSBYMEMBER key mem ... */
    case GEORADIUS_CMD:
    case GEORADIUSBYMEMBER_CMD:
      this->first = this->last = 1; /* source key */
      this->step  = 1;
      this->step_mask = 1 << this->first;
      if ( this->argc > 2 &&
           this->msg.match_arg( this->argc - 2,
                                "STORE", 5, "STOREDIST", 9, NULL ) != 0 ) {
        this->last = this->argc - 1;
        this->step_mask = 1 << this->last;
        this->step = this->last - this->first;
      }
      return true;

    case MIGRATE_CMD: break;
    case SORT_CMD: break;
    case EVAL_CMD: break;
    case EVALSHA_CMD: break;

    case ZINTERSTORE_CMD:
    case ZUNIONSTORE_CMD:
      /* ZINTERSTORE dest nkeys key key [WEIGHTS w1 w2] [AGGREGATE ...] */
      if ( ! this->msg.get_arg( 2, i ) ) /* num keys */
        return false;
      /* mask which args are keys */
      this->step_mask  = ( ( (uint64_t) 1 << i ) - 1 ) << 3;
      this->first      = 1;
      this->step_mask |= 1 << this->first; /* the dest key */
      this->last       = 2 + i; /* the last key = 2 + nkeys */
      this->step       = 1;     /* step through step_mask 1 bit at a time */
      if ( (size_t) ( 3 + i ) > this->argc ) /* if args above fit into argc */
        return false;
      return true;
    case XREADGROUP_CMD: break;
    default: break;
  }
  return false;
}

bool
RedisExec::next_key( int &i )
{
  /* step through argc until last key */
  i += this->step;
  if ( this->last < 0 && (size_t) i < this->argc )
    return true;
  if ( test_cmd_mask( this->flags, CMD_MOVABLEKEYS_FLAG ) ) {
    while ( ( ( (uint64_t) 1 << i ) & this->step_mask ) == 0 ) {
      i += this->step;
      if ( i > this->last )
        return false;
    }
    return true;
  }
  return ( i <= this->last );
}

size_t
RedisExec::calc_key_count( void )
{
  /* how many keys are in the command */
  if ( test_cmd_mask( this->flags, CMD_MOVABLEKEYS_FLAG ) )
    return __builtin_popcountl( this->step_mask );
  if ( this->last > 0 )
    return ( this->last + 1 - this->first ) / this->step;
  if ( this->last < 0 )
    return ( this->argc - this->first ) / this->step;
  return 0;
}

ExecStatus
RedisExec::exec( EvSocket *svc,  EvPrefetchQueue *q )
{
  const char * arg0;
  size_t       arg0len;
  char         upper_cmd[ 32 ];

  arg0 = this->msg.command( arg0len, this->argc );
  /* max command len is 17 (GEORADIUSBYMEMBER) */
  if ( arg0len >= 32 )
    return ERR_BAD_CMD;

  str_to_upper( arg0, upper_cmd, arg0len );
  if ( (this->cmd = get_redis_cmd( upper_cmd, arg0len )) == NO_CMD )
    return ERR_BAD_CMD;

  get_cmd_arity( this->cmd, this->arity, this->first, this->last,
		 this->step );
  if ( this->arity > 0 ) {
    if ( (size_t) this->arity != this->argc )
      return ERR_BAD_ARGS;
  }
  else if ( (size_t) -this->arity > this->argc )
    return ERR_BAD_ARGS;
  this->flags = get_cmd_flag_mask( this->cmd );

  if ( test_cmd_mask( this->flags, CMD_MOVABLEKEYS_FLAG ) )
    if ( ! this->locate_movablekeys() )
      return ERR_BAD_ARGS;
  /* if there are keys, setup a keyctx for each one */
  if ( this->first > 0 ) {
    int i = this->first;
    ExecStatus status;

    this->key_cnt  = 1;
    this->key_done = 0;

    this->key  = NULL;
    this->keys = NULL;
    /* setup first key */
    status = this->exec_key_setup( svc, q, this->key, i );
    if ( status == EXEC_SETUP_OK ) {
      /* setup rest of keys, if any */
      if ( this->next_key( i ) ) {
        size_t key_count = this->calc_key_count();
        if ( key_count == 0 )
          return ERR_BAD_ARGS;
        this->keys = (RedisKeyCtx **)
          this->strm.alloc_temp( sizeof( this->keys[ 0 ] ) * key_count );
        if ( this->keys == NULL )
          status = ERR_ALLOC_FAIL;
        else {
          this->keys[ 0 ] = this->key;
          do {
            status = this->exec_key_setup( svc, q,
                                           this->keys[ this->key_cnt++ ], i );
          } while ( status == EXEC_SETUP_OK && this->next_key( i ) );
        }
      }
    }
    return status;
  }
  /* has no key when first == 0 */
  switch ( this->cmd ) {
    /* CLUSTER */
    case CLUSTER_CMD:      return this->exec_cluster();
    case READONLY_CMD:     return this->exec_readonly();
    case READWRITE_CMD:    return this->exec_readwrite();
    /* CONNECTION */
    case AUTH_CMD:         return this->exec_auth();
    case ECHO_CMD:         /* same as ping */
    case PING_CMD:         return this->exec_ping();
    case QUIT_CMD:         return this->exec_quit(); //EXEC_QUIT;
    case SELECT_CMD:       return this->exec_select();
    case SWAPDB_CMD:       return this->exec_swapdb();
    /* SERVER */
    case BGREWRITEAOF_CMD: return this->exec_bgrewriteaof();
    case BGSAVE_CMD:       return this->exec_bgsave();
    case CLIENT_CMD:       return this->exec_client();
    case COMMAND_CMD:      return this->exec_command();
    case CONFIG_CMD:       return this->exec_config();
    case DBSIZE_CMD:       return this->exec_dbsize();
    case DEBUG_CMD:        return this->exec_debug();
    case FLUSHALL_CMD:     return this->exec_flushall();
    case FLUSHDB_CMD:      return this->exec_flushdb();
    case INFO_CMD:         return this->exec_info();
    case LASTSAVE_CMD:     return this->exec_lastsave();
    case MEMORY_CMD:       return this->exec_memory();
    case MONITOR_CMD:      return this->exec_monitor();
    case ROLE_CMD:         return this->exec_role();
    case SAVE_CMD:         return this->exec_save();
    case SHUTDOWN_CMD:     return this->exec_shutdown();
    case SLAVEOF_CMD:      return this->exec_slaveof();
    case SLOWLOG_CMD:      return this->exec_slowlog();
    case SYNC_CMD:         return this->exec_sync();
    case TIME_CMD:         return this->exec_time();
    /* KEYS */
    case KEYS_CMD:         return this->exec_keys();
    case RANDOMKEY_CMD:    return this->exec_randomkey();
    case WAIT_CMD:         return this->exec_wait();
    case SCAN_CMD:         return this->exec_scan();
    default:               return ERR_BAD_CMD;
  }
}

kv::KeyStatus
RedisExec::exec_key_fetch( RedisKeyCtx &ctx,  bool force_read )
{
  if ( test_cmd_mask( this->flags, CMD_READONLY_FLAG ) || force_read ) {
    ctx.kstatus = this->kctx.find( &this->wrk );
    ctx.is_read = true;
  }
  else if ( test_cmd_mask( this->flags, CMD_WRITE_FLAG ) ) {
    ctx.kstatus = this->kctx.acquire( &this->wrk );
    ctx.is_new = ( ctx.kstatus == KEY_IS_NEW );
    ctx.is_read = false;
  }
  else {
    ctx.kstatus = KEY_NO_VALUE;
    ctx.status  = ERR_BAD_CMD;
    ctx.is_read = true;
  }
  if ( ctx.kstatus == KEY_OK ) /* not new and is found */
    ctx.type = this->kctx.get_type();
  return ctx.kstatus;
}

ExecStatus
RedisExec::exec_key_continue( RedisKeyCtx &ctx )
{
  if ( ctx.status != EXEC_CONTINUE && ctx.status != EXEC_DEPENDS ) {
    if ( ++this->key_done < this->key_cnt )
      return EXEC_CONTINUE;
    return EXEC_SUCCESS;
  }
  if ( this->kctx.kbuf != &ctx.kbuf ||
       this->kctx.key != ctx.hash1 || this->kctx.key2 != ctx.hash2 )
    this->exec_key_prefetch( ctx );
  for (;;) {
    switch ( this->cmd ) {
      /* CLUSTER */
      case CLUSTER_CMD:  /* these exist so that the compiler errors when a */
      case READONLY_CMD: /* command is not handled by the switch() */
      case READWRITE_CMD: ctx.status = ERR_BAD_CMD; break;
      /* CONNECTION */
      case AUTH_CMD: case ECHO_CMD: case PING_CMD: case QUIT_CMD:
      case SELECT_CMD: case SWAPDB_CMD:
                          ctx.status = ERR_BAD_CMD; break; /* in exec() */
      /* GEO */
      case GEOADD_CMD:    ctx.status = this->exec_geoadd( ctx ); break;
      case GEOHASH_CMD:   ctx.status = this->exec_geohash( ctx ); break;
      case GEOPOS_CMD:    ctx.status = this->exec_geopos( ctx ); break;
      case GEODIST_CMD:   ctx.status = this->exec_geodist( ctx ); break;
      case GEORADIUS_CMD: ctx.status = this->exec_georadius( ctx ); break;
      case GEORADIUSBYMEMBER_CMD:
                        ctx.status = this->exec_georadiusbymember( ctx ); break;
      /* HASH */
      case HAPPEND_CMD:   ctx.status = this->exec_happend( ctx ); break;
      case HDEL_CMD:      ctx.status = this->exec_hdel( ctx ); break;
      case HDIFF_CMD:     ctx.status = this->exec_hdiff( ctx ); break;
      case HDIFFSTORE_CMD: ctx.status = this->exec_hdiffstore( ctx ); break;
      case HEXISTS_CMD:   ctx.status = this->exec_hexists( ctx ); break;
      case HGET_CMD:      ctx.status = this->exec_hget( ctx ); break;
      case HGETALL_CMD:   ctx.status = this->exec_hgetall( ctx ); break;
      case HINCRBY_CMD:   ctx.status = this->exec_hincrby( ctx ); break;
      case HINCRBYFLOAT_CMD: ctx.status = this->exec_hincrbyfloat( ctx ); break;
      case HINTER_CMD:    ctx.status = this->exec_hinter( ctx ); break;
      case HINTERSTORE_CMD: ctx.status = this->exec_hinterstore( ctx ); break;
      case HKEYS_CMD:     ctx.status = this->exec_hkeys( ctx ); break;
      case HLEN_CMD:      ctx.status = this->exec_hlen( ctx ); break;
      case HMGET_CMD:     ctx.status = this->exec_hmget( ctx ); break;
      case HMSET_CMD:     ctx.status = this->exec_hmset( ctx ); break;
      case HSET_CMD:      ctx.status = this->exec_hset( ctx ); break;
      case HSETNX_CMD:    ctx.status = this->exec_hsetnx( ctx ); break;
      case HSTRLEN_CMD:   ctx.status = this->exec_hstrlen( ctx ); break;
      case HVALS_CMD:     ctx.status = this->exec_hvals( ctx ); break;
      case HSCAN_CMD:     ctx.status = this->exec_hscan( ctx ); break;
      case HUNION_CMD:    ctx.status = this->exec_hunion( ctx ); break;
      case HUNIONSTORE_CMD: ctx.status = this->exec_hunionstore( ctx ); break;
      /* HYPERLOGLOG */
      case PFADD_CMD:     ctx.status = this->exec_pfadd( ctx ); break;
      case PFCOUNT_CMD:   ctx.status = this->exec_pfcount( ctx ); break;
      case PFMERGE_CMD:   ctx.status = this->exec_pfmerge( ctx ); break;
      /* KEY */
      case DEL_CMD:       ctx.status = this->exec_del( ctx ); break;
      case DUMP_CMD:      ctx.status = this->exec_dump( ctx ); break;
      case EXISTS_CMD:    ctx.status = this->exec_exists( ctx ); break;
      case EXPIRE_CMD:    ctx.status = this->exec_expire( ctx ); break;
      case EXPIREAT_CMD:  ctx.status = this->exec_expireat( ctx ); break;
      case KEYS_CMD:      ctx.status = ERR_BAD_CMD; break; /* in exec() */
      case MIGRATE_CMD:   ctx.status = this->exec_migrate( ctx ); break;
      case MOVE_CMD:      ctx.status = this->exec_move( ctx ); break;
      case OBJECT_CMD:    ctx.status = this->exec_object( ctx ); break;
      case PERSIST_CMD:   ctx.status = this->exec_persist( ctx ); break;
      case PEXPIRE_CMD:   ctx.status = this->exec_pexpire( ctx ); break;
      case PEXPIREAT_CMD: ctx.status = this->exec_pexpireat( ctx ); break;
      case PTTL_CMD:      ctx.status = this->exec_pttl( ctx ); break;
      case RANDOMKEY_CMD: ctx.status = ERR_BAD_CMD; break; /* in exec() */
      case RENAME_CMD:    ctx.status = this->exec_rename( ctx ); break;
      case RENAMENX_CMD:  ctx.status = this->exec_renamenx( ctx ); break;
      case RESTORE_CMD:   ctx.status = this->exec_restore( ctx ); break;
      case SORT_CMD:      ctx.status = this->exec_sort( ctx ); break;
      case TOUCH_CMD:     ctx.status = this->exec_touch( ctx ); break;
      case TTL_CMD:       ctx.status = this->exec_ttl( ctx ); break;
      case TYPE_CMD:      ctx.status = this->exec_type( ctx ); break;
      case UNLINK_CMD:    ctx.status = this->exec_unlink( ctx ); break;
      case WAIT_CMD: 
      case SCAN_CMD:      ctx.status = ERR_BAD_CMD; break; /* in exec() */
      /* LIST */
      case BLPOP_CMD:     ctx.status = this->exec_blpop( ctx ); break;
      case BRPOP_CMD:     ctx.status = this->exec_brpop( ctx ); break;
      case BRPOPLPUSH_CMD: ctx.status = this->exec_brpoplpush( ctx ); break;
      case LINDEX_CMD:    ctx.status = this->exec_lindex( ctx ); break;
      case LINSERT_CMD:   ctx.status = this->exec_linsert( ctx ); break;
      case LLEN_CMD:      ctx.status = this->exec_llen( ctx ); break;
      case LPOP_CMD:      ctx.status = this->exec_lpop( ctx ); break;
      case LPUSH_CMD:     ctx.status = this->exec_lpush( ctx ); break;
      case LPUSHX_CMD:    ctx.status = this->exec_lpushx( ctx ); break;
      case LRANGE_CMD:    ctx.status = this->exec_lrange( ctx ); break;
      case LREM_CMD:      ctx.status = this->exec_lrem( ctx ); break;
      case LSET_CMD:      ctx.status = this->exec_lset( ctx ); break;
      case LTRIM_CMD:     ctx.status = this->exec_ltrim( ctx ); break;
      case RPOP_CMD:      ctx.status = this->exec_rpop( ctx ); break;
      case RPOPLPUSH_CMD: ctx.status = this->exec_rpoplpush( ctx ); break;
      case RPUSH_CMD:     ctx.status = this->exec_rpush( ctx ); break;
      case RPUSHX_CMD:    ctx.status = this->exec_rpushx( ctx ); break;
      /* PUBSUB */
      case PSUBSCRIBE_CMD: ctx.status = this->exec_psubscribe( ctx ); break;
      case PUBSUB_CMD:    ctx.status = this->exec_pubsub( ctx ); break;
      case PUBLISH_CMD:   ctx.status = this->exec_publish( ctx ); break;
      case PUNSUBSCRIBE_CMD: ctx.status = this->exec_punsubscribe( ctx ); break;
      case SUBSCRIBE_CMD: ctx.status = this->exec_subscribe( ctx ); break;
      case UNSUBSCRIBE_CMD: ctx.status = this->exec_unsubscribe( ctx ); break;
      /* SCRIPT */
      case EVAL_CMD:      ctx.status = this->exec_eval( ctx ); break;
      case EVALSHA_CMD:   ctx.status = this->exec_evalsha( ctx ); break;
      case SCRIPT_CMD:    ctx.status = this->exec_script( ctx ); break;
      /* SERVER */
      case BGREWRITEAOF_CMD: case BGSAVE_CMD: case CLIENT_CMD: case COMMAND_CMD:
      case CONFIG_CMD: case DBSIZE_CMD: case DEBUG_CMD: case FLUSHALL_CMD:
      case FLUSHDB_CMD: case INFO_CMD: case LASTSAVE_CMD: case MEMORY_CMD:
      case MONITOR_CMD: case ROLE_CMD: case SAVE_CMD: case SHUTDOWN_CMD:
      case SLAVEOF_CMD: case SLOWLOG_CMD: case SYNC_CMD:
      case TIME_CMD:      ctx.status = ERR_BAD_CMD; break; /* in exec() */
      /* SET */
      case SADD_CMD:      ctx.status = this->exec_sadd( ctx ); break;
      case SCARD_CMD:     ctx.status = this->exec_scard( ctx ); break;
      case SDIFF_CMD:     ctx.status = this->exec_sdiff( ctx ); break;
      case SDIFFSTORE_CMD: ctx.status = this->exec_sdiffstore( ctx ); break;
      case SINTER_CMD:    ctx.status = this->exec_sinter( ctx ); break;
      case SINTERSTORE_CMD: ctx.status = this->exec_sinterstore( ctx ); break;
      case SISMEMBER_CMD: ctx.status = this->exec_sismember( ctx ); break;
      case SMEMBERS_CMD:  ctx.status = this->exec_smembers( ctx ); break;
      case SMOVE_CMD:     ctx.status = this->exec_smove( ctx ); break;
      case SPOP_CMD:      ctx.status = this->exec_spop( ctx ); break;
      case SRANDMEMBER_CMD: ctx.status = this->exec_srandmember( ctx ); break;
      case SREM_CMD:      ctx.status = this->exec_srem( ctx ); break;
      case SUNION_CMD:    ctx.status = this->exec_sunion( ctx ); break;
      case SUNIONSTORE_CMD: ctx.status = this->exec_sunionstore( ctx ); break;
      case SSCAN_CMD:     ctx.status = this->exec_sscan( ctx ); break;
      /* SORTED_SET */
      case ZADD_CMD:      ctx.status = this->exec_zadd( ctx ); break;
      case ZCARD_CMD:     ctx.status = this->exec_zcard( ctx ); break;
      case ZCOUNT_CMD:    ctx.status = this->exec_zcount( ctx ); break;
      case ZINCRBY_CMD:   ctx.status = this->exec_zincrby( ctx ); break;
      case ZINTERSTORE_CMD: ctx.status = this->exec_zinterstore( ctx ); break;
      case ZLEXCOUNT_CMD: ctx.status = this->exec_zlexcount( ctx ); break;
      case ZRANGE_CMD:    ctx.status = this->exec_zrange( ctx ); break;
      case ZRANGEBYLEX_CMD: ctx.status = this->exec_zrangebylex( ctx ); break;
      case ZREVRANGEBYLEX_CMD:
                          ctx.status = this->exec_zrevrangebylex( ctx ); break;
      case ZRANGEBYSCORE_CMD:
                          ctx.status = this->exec_zrangebyscore( ctx ); break;
      case ZRANK_CMD:     ctx.status = this->exec_zrank( ctx ); break;
      case ZREM_CMD:      ctx.status = this->exec_zrem( ctx ); break;
      case ZREMRANGEBYLEX_CMD:
                          ctx.status = this->exec_zremrangebylex( ctx ); break;
      case ZREMRANGEBYRANK_CMD:
                          ctx.status = this->exec_zremrangebyrank( ctx ); break;
      case ZREMRANGEBYSCORE_CMD:
                         ctx.status = this->exec_zremrangebyscore( ctx ); break;
      case ZREVRANGE_CMD: ctx.status = this->exec_zrevrange( ctx ); break;
      case ZREVRANGEBYSCORE_CMD:
                         ctx.status = this->exec_zrevrangebyscore( ctx ); break;
      case ZREVRANK_CMD:  ctx.status = this->exec_zrevrank( ctx ); break;
      case ZSCORE_CMD:    ctx.status = this->exec_zscore( ctx ); break;
      case ZUNIONSTORE_CMD: ctx.status = this->exec_zunionstore( ctx ); break;
      case ZSCAN_CMD:     ctx.status = this->exec_zscan( ctx ); break;
      /* STRING */
      case APPEND_CMD:    ctx.status = this->exec_append( ctx ); break;
      case BITCOUNT_CMD:  ctx.status = this->exec_bitcount( ctx ); break;
      case BITFIELD_CMD:  ctx.status = this->exec_bitfield( ctx ); break;
      case BITOP_CMD:     ctx.status = this->exec_bitop( ctx ); break;
      case BITPOS_CMD:    ctx.status = this->exec_bitpos( ctx ); break;
      case DECR_CMD:      ctx.status = this->exec_decr( ctx ); break;
      case DECRBY_CMD:    ctx.status = this->exec_decrby( ctx ); break;
      case GET_CMD:       ctx.status = this->exec_get( ctx ); break;
      case GETBIT_CMD:    ctx.status = this->exec_getbit( ctx ); break;
      case GETRANGE_CMD:  ctx.status = this->exec_getrange( ctx ); break;
      case GETSET_CMD:    ctx.status = this->exec_getset( ctx ); break;
      case INCR_CMD:      ctx.status = this->exec_incr( ctx ); break;
      case INCRBY_CMD:    ctx.status = this->exec_incrby( ctx ); break;
      case INCRBYFLOAT_CMD: ctx.status = this->exec_incrbyfloat( ctx ); break;
      case MGET_CMD:      ctx.status = this->exec_mget( ctx ); break;
      case MSET_CMD:      ctx.status = this->exec_mset( ctx ); break;
      case MSETNX_CMD:    ctx.status = this->exec_msetnx( ctx ); break;
      case PSETEX_CMD:    ctx.status = this->exec_psetex( ctx ); break;
      case SET_CMD:       ctx.status = this->exec_set( ctx ); break;
      case SETBIT_CMD:    ctx.status = this->exec_setbit( ctx ); break;
      case SETEX_CMD:     ctx.status = this->exec_setex( ctx ); break;
      case SETNX_CMD:     ctx.status = this->exec_setnx( ctx ); break;
      case SETRANGE_CMD:  ctx.status = this->exec_setrange( ctx ); break;
      case STRLEN_CMD:    ctx.status = this->exec_strlen( ctx ); break;
      /* TRANSACTION */
      case DISCARD_CMD:   ctx.status = this->exec_discard( ctx ); break;
      case EXEC_CMD:      ctx.status = this->exec_exec( ctx ); break;
      case MULTI_CMD:     ctx.status = this->exec_multi( ctx ); break;
      case UNWATCH_CMD:   ctx.status = this->exec_unwatch( ctx ); break;
      case WATCH_CMD:     ctx.status = this->exec_watch( ctx ); break;
      /* STREAM */
      case XADD_CMD:      ctx.status = this->exec_xadd( ctx ); break;
      case XLEN_CMD:      ctx.status = this->exec_xlen( ctx ); break;
      case XRANGE_CMD:    ctx.status = this->exec_xrange( ctx ); break;
      case XREVRANGE_CMD: ctx.status = this->exec_xrevrange( ctx ); break;
      case XREAD_CMD:     ctx.status = this->exec_xread( ctx ); break;
      case XREADGROUP_CMD: ctx.status = this->exec_xreadgroup( ctx ); break;
      case XGROUP_CMD:    ctx.status = this->exec_xgroup( ctx ); break;
      case XACK_CMD:      ctx.status = this->exec_xack( ctx ); break;
      case XPENDING_CMD:  ctx.status = this->exec_xpending( ctx ); break;
      case XCLAIM_CMD:    ctx.status = this->exec_xclaim( ctx ); break;
      case XINFO_CMD:     ctx.status = this->exec_xinfo( ctx ); break;
      case XDEL_CMD:      ctx.status = this->exec_xdel( ctx ); break;

      case NO_CMD:        ctx.status = ERR_BAD_CMD; break;
    }
    /* set the type when key is new */
    if ( ! ctx.is_read ) {
      if ( ctx.is_new && exec_status_success( ctx.status ) ) {
        uint8_t type;
        if ( (type = ctx.type) == MD_NODATA ) {
          switch ( get_cmd_category( this->cmd ) ) {
            default:               type = MD_NODATA;      break;
            case GEO_CATG:         type = MD_GEO;         break;
            case HASH_CATG:        type = MD_HASH;        break;
            case HYPERLOGLOG_CATG: type = MD_HYPERLOGLOG; break;
            case LIST_CATG:        type = MD_LIST;        break;
            case PUBSUB_CATG:      type = MD_PUBSUB;      break;
            case SCRIPT_CATG:      type = MD_SCRIPT;      break;
            case SET_CATG:         type = MD_SET;         break;
            case SORTED_SET_CATG:  type = MD_SORTEDSET;   break;
            case STRING_CATG:      type = MD_STRING;      break;
            case TRANSACTION_CATG: type = MD_TRANSACTION; break;
            case STREAM_CATG:      type = MD_STREAM;      break;
          }
        }
        if ( type != MD_NODATA )
          this->kctx.set_type( type );
      }
      this->kctx.release();
    }
    /* if key depends on other keys */
    if ( ctx.status == EXEC_DEPENDS ) {
      ctx.dep++;
      return EXEC_DEPENDS;
    }
    /* continue if read key mutated while running */
    if ( ctx.status != ERR_KV_STATUS || ctx.kstatus != KEY_MUTATED )
      break;
  }
  if ( ++this->key_done < this->key_cnt ) {
    if ( exec_status_success( ctx.status ) )
      return EXEC_CONTINUE;
    for ( uint32_t i = 0; i < this->key_cnt; i++ )
      this->keys[ i ]->status = ctx.status;
  }
  else if ( test_cmd_mask( this->flags, CMD_MULTI_KEY_ARRAY_FLAG ) ) {
    if ( exec_status_success( ctx.status ) ) {
      this->array_string_result();
      return EXEC_SUCCESS;
    }
  }
  switch ( ctx.status ) {
    case EXEC_OK:           break;
    case EXEC_SEND_OK:      this->strm.append( ok, ok_sz );            break;
    case EXEC_SEND_NIL:     this->strm.append( nil, nil_sz );          break;
    case EXEC_ABORT_SEND_ZERO:
    case EXEC_SEND_ZERO:    this->strm.append( zero, zero_sz );        break;
    case EXEC_SEND_ONE:     this->strm.append( one, one_sz );          break;
    case EXEC_SEND_NEG_ONE: this->strm.append( neg_one, neg_one_sz );  break;
    case EXEC_SEND_ZERO_STRING: this->strm.append( mt, mt_sz );        break;
    case EXEC_SEND_INT:     this->send_int();                          break;
    default:                this->send_err( ctx.status, ctx.kstatus ); break;
  }
  if ( this->key_done < this->key_cnt )
    return EXEC_CONTINUE;
  return EXEC_SUCCESS;
}

/* CLUSTER */
ExecStatus
RedisExec::exec_cluster( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_readonly( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_readwrite( void )
{
  return ERR_BAD_CMD;
}

/* CONNECTION */
ExecStatus
RedisExec::exec_auth( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_echo( void )
{
  return this->exec_ping();
}

ExecStatus
RedisExec::exec_ping( void )
{
  if ( this->argc > 1 ) {
    this->send_msg( this->msg.array[ 1 ] );
  }
  else {
    static char pong[] = "+PONG\r\n";
    this->strm.append( pong, sizeof( pong ) - 1 );
  }
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_quit( void )
{
  return EXEC_QUIT;
}

ExecStatus
RedisExec::exec_select( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_swapdb( void )
{
  return ERR_BAD_CMD;
}

/* SERVER */
ExecStatus
RedisExec::exec_bgrewriteaof( void )
{
  /* start a AOF */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_bgsave( void )
{
  /* save in the bg */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus RedisExec::exec_client( void )
{
  switch ( this->msg.match_arg( 1, "getname", 7,
                                   "kill", 4,
                                   "list", 4,
                                   "pause", 5,
                                   "reply", 5,
                                   "setname", 7, NULL ) ) {
    default: return ERR_BAD_ARGS;
    case 1: /* getname */
      this->send_nil();  /* get my name */
      return EXEC_OK;
    case 2: /* kill (ip) (ID id) (TYPE norm|mast|slav|pubsub)
                    (ADDR ip) (SKIPME y/n) */
      this->send_zero(); /* number of clients killed */
      return EXEC_OK;
    case 3: /* list */
      /* list: 'id=1082 addr=[::1]:43362 fd=8 name= age=1 idle=0 flags=N db=0
       * sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0
       * events=r cmd=client\n' id=unique id, addr=peer addr, fd=sock, age=time
       * conn, idle=time idle, flags=mode, db=cur db, sub=channel subs,
       * psub=pattern subs, multi=cmds qbuf=query buf size, qbuf-free=free
       * qbuf, obl=output buf len, oll=outut list len, omem=output mem usage,
       * events=sock rd/wr, cmd=last cmd issued */
    case 4: /* pause (ms) pause clients for ms time*/
    case 5: /* reply (on/off/skip) en/disable replies */
    case 6: /* setname (name) set the name of this conn */
      return ERR_BAD_ARGS;
  }
}

ExecStatus
RedisExec::exec_command( void )
{
  RedisMsg     m;
  const char * name;
  size_t       j = 0, len;
  RedisCmd     cmd;

  this->mstatus = REDIS_MSG_OK;
  switch ( this->msg.match_arg( 1, "info",    4,
                                   "getkeys", 7,
                                   "count",   5,
                                   "help",    4, NULL ) ) {
    case 0: { /* no args */
      if ( ! m.alloc_array( this->strm.tmp, REDIS_CMD_COUNT - 1 ) )
        return ERR_ALLOC_FAIL;
      for ( size_t i = 1; i < REDIS_CMD_COUNT; i++ ) {
        this->mstatus = m.array[ j++ ].unpack_json( cmd_db[ i ].attr,
                                                    this->strm.tmp );
        if ( this->mstatus != REDIS_MSG_OK )
          break;
      }
      m.len = j;
      break;
    }
    case 1: { /* info */
      if ( ! m.alloc_array( this->strm.tmp, this->msg.len - 2 ) )
        return ERR_ALLOC_FAIL;
      if ( m.len > 0 ) {
        for ( int i = 2; this->msg.get_arg( i, name, len ); i++ ) {
          cmd = get_upper_cmd( name, len );
          m.array[ j++ ].unpack_json( cmd_db[ cmd ].attr, this->strm.tmp );
        }
        m.len = j;
      }
      break;
    }
    case 2: /* getkeys */
      return ERR_BAD_ARGS;
    case 3: /* count */
      m.set_int( REDIS_CMD_COUNT - 1 );
      break;
    case 4: { /* help */
      if ( ! m.alloc_array( this->strm.tmp, this->msg.len * 2 ) )
        return ERR_ALLOC_FAIL;
      for ( int i = 2; this->msg.get_arg( i, name, len ); i++ ) {
        cmd = get_upper_cmd( name, len );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ cmd ].name );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ cmd ].descr );
      }
      if ( j == 0 ) {
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ COMMAND_CMD ].name );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ COMMAND_CMD ].descr);
      }
      m.len = j;
      break;
    }
    default:
      return ERR_BAD_ARGS;
  }
  if ( this->mstatus == REDIS_MSG_OK ) {
    size_t sz  = 16 * 1024;
    void * buf = this->strm.alloc( sz );
    if ( buf == NULL )
      return ERR_ALLOC_FAIL;
    this->strm.append_iov( buf, m.pack( buf ) );
  }
  if ( this->mstatus != REDIS_MSG_OK )
    return ERR_MSG_STATUS;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_config( void )
{
  switch ( this->msg.match_arg( 1, "get",       3,
                                   "resetstat", 9,
                                   "rewrite",   7,
                                   "set",       3, NULL ) ) {
    default: return ERR_BAD_ARGS;
    case 1: /* get */
    case 2: /* resetstat */
    case 3: /* rewrite */
    case 4: /* set */
      return ERR_BAD_CMD;
  }
}

ExecStatus
RedisExec::exec_dbsize( void )
{
  this->send_int( this->kctx.ht.hdr.last_entry_count );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_debug( void )
{
  return EXEC_DEBUG;
}

ExecStatus
RedisExec::exec_flushall( void )
{
  /* delete all keys */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_flushdb( void )
{
  /* delete current db */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_info( void )
{
  size_t len = 256;
  char * buf = (char *) this->strm.tmp.alloc( len );
  if ( buf != NULL ) {
    int n = ::snprintf( &buf[ 32 ], len-32,
      "# Server\r\n"
      "redis_version:4.0\r\n"
      "raids_version:%s\r\n"
      "gcc_version:%d.%d.%d\r\n"
      "process_id:%d\r\n",
      kv_stringify( DS_VER ),
      __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__,
      ::getpid() );
    size_t dig = RedisMsg::uint_digits( n ),
           off = 32 - ( dig + 3 );

    buf[ off ] = '$';
    RedisMsg::uint_to_str( n, &buf[ off + 1 ], dig );
    crlf( buf, off + 1 + dig );
    crlf( buf, 32 + n );
    this->strm.append_iov( &buf[ off ], n + dig + 3 + 2 );
    return EXEC_OK;
  }
  return ERR_ALLOC_FAIL;
}

ExecStatus
RedisExec::exec_lastsave( void )
{
  this->send_int( this->kctx.ht.hdr.create_stamp / 1000000000 );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_memory( void )
{
  switch ( this->msg.match_arg( 1, "doctor",       6,
                                   "help",         4,
                                   "malloc-stats", 12,
                                   "purge",        5,
                                   "stats",        5,
                                   "usage",        5, NULL ) ) {
    default: return ERR_BAD_ARGS;
    case 1: /* doctor */
    case 2: /* help */
    case 3: /* malloc-stats */
    case 4: /* purge */
    case 5: /* stats */
    case 6: /* usage */
      return ERR_BAD_CMD;
  }
}

ExecStatus
RedisExec::exec_monitor( void )
{
  /* monitor commands:
   * 1339518083.107412 [0 127.0.0.1:60866] "keys" "*"
   * 1339518087.877697 [0 127.0.0.1:60866] "dbsize" */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_role( void )
{
  /* master/slave
   * replication offset
   * slaves connected */
  RedisMsg m;
  if ( m.alloc_array( this->strm.tmp, 3 ) ) {
    static char master[] = "master";
    m.array[ 0 ].set_bulk_string( master, sizeof( master ) - 1 );
    m.array[ 1 ].set_int( 0 );
    m.array[ 2 ].set_mt_array();
    this->send_msg( m );
    return EXEC_OK;
  }
  return ERR_ALLOC_FAIL;
}

ExecStatus
RedisExec::exec_save( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_shutdown( void )
{
  return EXEC_QUIT;
}

ExecStatus
RedisExec::exec_slaveof( void )
{
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_slowlog( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sync( void )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_time( void )
{
  RedisMsg m;
  char     sb[ 32 ], ub[ 32 ];
  struct timeval tv;
  ::gettimeofday( &tv, 0 );
  if ( m.string_array( this->strm.tmp, 2,
                  RedisMsg::uint_to_str( tv.tv_sec, sb ), sb,
                  RedisMsg::uint_to_str( tv.tv_usec, ub ), ub ) ) {
    this->send_msg( m );
    return EXEC_OK;
  }
  return ERR_ALLOC_FAIL;
}

void
RedisExec::send_msg( const RedisMsg &m )
{
  char * buf = this->strm.alloc( m.pack_size() );
  if ( buf != NULL )
    this->strm.sz += m.pack( buf );
}

void
RedisExec::send_int( void )
{
  if ( this->key_cnt == 1 ) {
    this->send_int( this->key->ival );
  }
  else if ( this->key_cnt > 1 ) {
    int64_t ival = 0;
    for ( uint32_t i = 0; i < this->key_cnt; i++ )
      ival += this->keys[ i ]->ival;
    this->send_int( ival );
  }
  else {
    this->send_int( -1 );
  }
}

void
RedisExec::send_int( int64_t ival )
{
  if ( ival >= 0 && ival < 10 ) {
    char  * buf = this->strm.alloc( 4 );
    if ( buf != NULL ) {
      buf[ 0 ] = ':';
      buf[ 1 ] = (char) ival + '0';
      this->strm.sz += crlf( buf, 2 );
    }
  }
  else {
    size_t ilen = RedisMsg::int_digits( ival );
    char  * buf = this->strm.alloc( ilen + 3 );
    if ( buf != NULL ) {
      buf[ 0 ] = ':';
      ilen = 1 + RedisMsg::int_to_str( ival, &buf[ 1 ], ilen );
      this->strm.sz += crlf( buf, ilen );
    }
  }
}

void
RedisExec::send_err( ExecStatus status,  KeyStatus kstatus )
{
  switch ( status ) {
    case EXEC_OK:               break;
    case EXEC_SETUP_OK:         break;
    case EXEC_SEND_OK:          this->send_ok(); break;
    case EXEC_SEND_NIL:         this->send_nil(); break;
    case EXEC_SEND_INT:         this->send_int(); break;
    case EXEC_ABORT_SEND_ZERO:
    case EXEC_SEND_ZERO:        this->send_zero(); break;
    case EXEC_SEND_ONE:         this->send_one(); break;
    case EXEC_SEND_NEG_ONE:     this->send_neg_one(); break;
    case EXEC_SEND_ZERO_STRING: this->send_zero_string(); break;
    case EXEC_SUCCESS:          break;
    case EXEC_DEPENDS:          break;
    case EXEC_CONTINUE:         break;
    case ERR_KV_STATUS:         this->send_err_kv( kstatus ); break;
    case ERR_MSG_STATUS:        this->send_err_msg( this->mstatus ); break;
    case ERR_BAD_ARGS:          this->send_err_bad_args(); break;
    case ERR_BAD_CMD:           this->send_err_bad_cmd(); break;
    case ERR_BAD_TYPE:          this->send_err_bad_type(); break;
    case ERR_BAD_RANGE:         this->send_err_bad_range(); break;
    case EXEC_QUIT:
    case EXEC_DEBUG:            this->send_ok(); break;
    case ERR_ALLOC_FAIL:        this->send_err_alloc_fail(); break;
    case ERR_KEY_EXISTS:        this->send_err_key_exists(); break;
    case ERR_KEY_DOESNT_EXIST:  this->send_err_key_doesnt_exist(); break;
  }
}

void
RedisExec::send_err_bad_args( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz,
              "-ERR wrong number of arguments for '%.*s' command\r\n",
              (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_kv( KeyStatus kstatus )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 256;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': KeyCtx %d/%s %s\r\n",
                     (int) arg0len, arg0,
                     kstatus, kv_key_status_string( (KeyStatus) kstatus ),
                     kv_key_status_description( (KeyStatus) kstatus ) );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_msg( RedisMsgStatus mstatus )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 256;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': RedisMsg %d/%s %s\r\n",
                   (int) arg0len, arg0,
                   mstatus, redis_msg_status_string( (RedisMsgStatus) mstatus ),
                   redis_msg_status_description( (RedisMsgStatus) mstatus ) );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_bad_cmd( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR unknown command: '%.*s'\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
  {
    char tmpbuf[ 1024 ];
    if ( this->msg.to_almost_json_size() < sizeof( tmpbuf ) ) {
      size_t sz = this->msg.to_almost_json( tmpbuf );
      fprintf( stderr, "Bad command: %.*s\n", (int) sz, tmpbuf );
    }
  }
}

void
RedisExec::send_err_bad_type( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz,
                      "-ERR value type bad for command: '%.*s'\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_bad_range( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz,
                      "-ERR index out of range for command: '%.*s'\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_alloc_fail( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': allocation failure\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_key_exists( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': key exists\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
RedisExec::send_err_key_doesnt_exist( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  void       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': key does not exist\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

const char *
RedisKeyCtx::get_type_str( void ) const
{
  switch ( this->type ) {
    default:
    case MD_NODATA: return "nodata";
    case MD_MESSAGE: return "message";
    case MD_STRING: return "string";
    case MD_OPAQUE: return "opaque";
    case MD_BOOLEAN: return "boolean";
    case MD_INT: return "int";
    case MD_UINT: return "uint";
    case MD_REAL: return "real";
    case MD_ARRAY: return "array";
    case MD_PARTIAL: return "partial";
    case MD_IPDATA: return "ipdata";
    case MD_SUBJECT: return "subject";
    case MD_ENUM: return "enum";
    case MD_TIME: return "time";
    case MD_DATE: return "date";
    case MD_DATETIME: return "datetime";
    case MD_STAMP: return "stamp";
    case MD_DECIMAL: return "decimal";
    case MD_LIST: return "list";
    case MD_HASH: return "hash";
    case MD_SET: return "set";
    case MD_SORTEDSET: return "sortedset";
    case MD_STREAM: return "stream";
    case MD_GEO: return "geo";
    case MD_HYPERLOGLOG: return "hyperloglog";
    case MD_PUBSUB: return "pubsub";
    case MD_SCRIPT: return "script";
    case MD_TRANSACTION: return "transaction";
  }
}

