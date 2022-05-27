#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/route_db.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>
#include <raids/redis_keyspace.h>
#include <raids/redis_transaction.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

static char ok[]      = "+OK\r\n";
static char nil[]     = "$-1\r\n";
static char null[]    = "*-1\r\n";
static char zeroarr[] = "*0\r\n";
static char zero[]    = ":0\r\n";
static char one[]     = ":1\r\n";
static char neg_one[] = ":-1\r\n";
static char mt[]      = "$0\r\n\r\n"; /* zero length string */
static char queued[]  = "+QUEUED\r\n";
static const size_t ok_sz      = sizeof( ok ) - 1,
                    nil_sz     = sizeof( nil ) - 1,
                    null_sz    = sizeof( null ) - 1,
                    zeroarr_sz = sizeof( zeroarr ) - 1,
                    zero_sz    = sizeof( zero ) - 1,
                    one_sz     = sizeof( one ) - 1,
                    neg_one_sz = sizeof( neg_one ) - 1,
                    mt_sz      = sizeof( mt ) - 1,
                    queued_sz  = sizeof( queued ) - 1;
void RedisExec::send_ok( void ) noexcept
{ this->strm.append( ok, ok_sz ); }
void RedisExec::send_nil( void ) noexcept
{ this->strm.append( nil, nil_sz ); }
void RedisExec::send_null( void ) noexcept
{ this->strm.append( null, null_sz ); }
void RedisExec::send_zeroarr( void ) noexcept
{ this->strm.append( zeroarr,zeroarr_sz );}
void RedisExec::send_zero( void ) noexcept
{ this->strm.append( zero, zero_sz ); }
void RedisExec::send_one( void ) noexcept
{ this->strm.append( one, one_sz ); }
void RedisExec::send_neg_one( void ) noexcept
{ this->strm.append( neg_one, neg_one_sz);}
void RedisExec::send_zero_string( void ) noexcept
{ this->strm.append( mt, mt_sz ); }
void RedisExec::send_queued( void ) noexcept
{ this->strm.append( queued, queued_sz ); }

void
RedisExec::release( void ) noexcept
{
  if ( this->multi != NULL )
    this->discard_multi();
  if ( ! this->sub_tab.is_null() || ! this->pat_tab.is_null() ) {
    this->rem_all_sub();
    this->sub_tab.release();
    this->pat_tab.release();
  }
  this->wrk.release_all();
  this->cmd = NO_CMD;
  this->blk_state = 0;
  this->key_flags = 0;
  this->cmd_state = CMD_STATE_NORMAL;
}

size_t
RedisExec::send_string( const void *data,  size_t size ) noexcept
{
  size_t sz  = 32 + size;
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return 0;
  str[ 0 ] = '$';
  sz = 1 + uint64_to_string( size, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  return crlf( str, sz + size );
}

size_t
RedisExec::send_simple_string( const void *data,  size_t size ) noexcept
{
  char * str = this->strm.alloc( 3 + size );
  if ( str == NULL )
    return 0;
  str[ 0 ] = '+';
  ::memcpy( &str[ 1 ], data, size );
  return crlf( str, 1 + size );
}

size_t
RedisExec::send_concat_string( const void *data,  size_t size,
                               const void *data2,  size_t size2 ) noexcept
{
  size_t sz  = 32 + size + size2;
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return 0;
  str[ 0 ] = '$';
  sz = 1 + uint64_to_string( size + size2, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  if ( size2 > 0 )
    ::memcpy( &str[ sz + size ], data2, size2 );
  return crlf( str, sz + size + size2 );
}

bool
RedisExec::save_string_result( EvKeyCtx &ctx,  const void *data,
                               size_t size ) noexcept
{
  size_t msz = sizeof( EvKeyTempResult ) + size + 24;
  /* this result may be set multiple times if key read is unsuccessful */
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    EvKeyTempResult *part;
    part = (EvKeyTempResult *) this->strm.alloc_temp( msz );
    if ( part != NULL ) {
      part->mem_size = msz;
      ctx.part = part;
    }
    else {
      return false;
    }
  }
  char *str = ctx.part->data( 0 );
  size_t sz;
  str[ 0 ] = '$';
  sz = 1 + uint64_to_string( size, &str[ 1 ] );
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  ctx.part->size = crlf( str, sz + size );
  return true;
}

bool
RedisExec::save_data( EvKeyCtx &ctx,  const void *data,  size_t size ) noexcept
{
  size_t msz = sizeof( EvKeyTempResult ) + size;
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    EvKeyTempResult *part;
    part = (EvKeyTempResult *) this->strm.alloc_temp( msz );
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
  return true;
}

void *
RedisExec::save_data2( EvKeyCtx &ctx,  const void *data,  size_t size,
                       const void *data2,  size_t size2 ) noexcept
{
  size_t msz = sizeof( EvKeyTempResult ) + size + size2;
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    EvKeyTempResult *part;
    part = (EvKeyTempResult *) this->strm.alloc_temp( msz );
    if ( part != NULL ) {
      part->mem_size = msz;
      ctx.part = part;
    }
    else {
      return NULL;
    }
  }
  uint8_t * ptr = (uint8_t *) ctx.part->data( 0 );
  ::memcpy( ptr, data, size );
  ::memcpy( &ptr[ size ], data2, size2 );
  ctx.part->size = size + size2;
  return ptr;
}

void
RedisExec::array_string_result( void ) noexcept
{
  char            * str = this->strm.alloc( 32 );
  EvKeyTempResult * part;
  size_t            sz;
  if ( str == NULL )
    return;
  str[ 0 ] = '*';
  sz = 1 + uint64_to_string( this->key_cnt, &str[ 1 ] );
  this->strm.sz += crlf( str, sz );

  if ( this->key_cnt > 0 ) {
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
}

ExecStatus
RedisExec::exec_key_setup( EvSocket *own,  EvPrefetchQueue *q,
                           EvKeyCtx *&ctx,  int n,  uint32_t idx ) noexcept
{
  const char * key;
  size_t       keylen;
  if ( ! this->msg.get_arg( n, key, keylen ) )
    return ERR_BAD_ARGS;
  void *p = this->strm.alloc_temp( EvKeyCtx::size( keylen ) );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;
  ctx = new ( p ) EvKeyCtx( this->kctx.ht, own, key, keylen, n, idx, this->hs );
  if ( q != NULL && ! q->push( ctx ) )
    return ERR_ALLOC_FAIL;
  ctx->status = EXEC_CONTINUE;
  return EXEC_SETUP_OK;
}

void
RedisExec::exec_run_to_completion( void ) noexcept
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
RedisExec::locate_movablekeys( void ) noexcept
{
  int64_t i;
  this->first = 0;
  this->last  = 0;
  this->step  = 0;
  switch ( this->cmd ) {  /* these commands do not follow regular rules */
    /* GEORADIUS key long lat mem rad unit [WITHCOORD] [WITHDIST]
     * [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key] */
    /* GEORADIUSBYMEMBER key mem ... */
    case GEORADIUS_CMD:
    case GEORADIUSBYMEMBER_CMD:
      this->first = this->last = 1; /* source key */
      this->step  = 1;
      this->step_mask = (uint64_t) 1 << this->first;
      if ( this->argc > 2 &&
           this->msg.match_arg( this->argc - 2,
                                MARG( "STORE" ),
                                MARG( "STOREDIST" ), NULL ) != 0 ) {
        this->last = (uint16_t) ( this->argc - 1 );
        this->step_mask = (uint64_t) 1 << this->last;
        this->step = this->last - this->first;
      }
      return true;
#if 0
    case MIGRATE_CMD: break;
    case SORT_CMD: break;
    case EVAL_CMD: break;
    case EVALSHA_CMD: break;
#endif
    case ZINTERSTORE_CMD:
    case ZUNIONSTORE_CMD:
      /* ZINTERSTORE dest nkeys key key [WEIGHTS w1 w2] [AGGREGATE ...] */
      if ( ! this->msg.get_arg( 2, i ) ) /* num keys */
        return false;
      /* mask which args are keys */
      this->step_mask  = ( ( (uint64_t) 1 << i ) - 1 ) << 3;
      this->first      = 1;
      this->step_mask |= (uint64_t) 1 << this->first; /* the dest key */
      this->last       = (uint16_t) ( 2 + i ); /* the last key = 2 + nkeys */
      this->step       = 1;     /* step through step_mask 1 bit at a time */
      if ( (size_t) ( 3 + i ) > this->argc ) /* if args above fit into argc */
        return false;
      return true;
    case XREAD_CMD:
      /* XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...] */
      for ( i = 1; (size_t) i < this->argc; ) {
        switch ( this->msg.match_arg( i, MARG( "count" ),
                                         MARG( "block" ),
                                         MARG( "streams" ), NULL ) ) {
          default:
            return false;
          case 1: /* count N */
            i += 2;
            break;
          case 2: /* block millseconds */
            i += 2;
            break;
          case 3: /* streams */
            if ( (size_t) ++i < this->argc ) {
              this->first = (uint16_t) i;
              this->last  = (uint16_t) ( i + ( this->argc - i ) / 2 - 1 );
              this->step  = 1;
              if ( (size_t) ( this->first +
                              ( this->last - this->first + 1 ) * 2 ) == 
                   this->argc )
                return true;
            }
            return false;
        }
      }
      return false;
    case XREADGROUP_CMD:
      /* XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK]
       * STREAMS key [key ...] id [id ...] */
      for ( i = 1; (size_t) i < this->argc; ) {
        switch ( msg.match_arg( i, MARG( "group" ),
                                   MARG( "count" ),
                                   MARG( "block" ),
                                   MARG( "noack" ),
                                   MARG( "streams" ), NULL ) ) {
          default:
            return false;
          case 1: /* group */
            i += 3;
            break;
          case 2: /* count N */
            i += 2;
            break;
          case 3: /* block millseconds */
            i += 2;
            break;
          case 4: /* noack */
            i += 1;
            break;
          case 5: /* streams */
            if ( (size_t) ++i < this->argc ) {
              this->first = (uint16_t) i;
              this->last  = (uint16_t) ( i + ( this->argc - i ) / 2 - 1 );
              this->step  = 1;
              if ( (size_t) ( this->first +
                              ( this->last - this->first + 1 ) * 2 ) == 
                   this->argc )
                return true;
            }
            return false;
        }
      }
      return false;
    default: break;
  }
  return false;
}

bool
RedisExec::next_key( int &i ) noexcept
{
  /* step through argc until last key */
  i += this->step;
  if ( this->last < 0 && i < (int) this->argc + (int) this->last + 1 )
    return true;
  if ( this->step_mask != 0 ) {
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
RedisExec::calc_key_count( void ) noexcept
{
  /* how many keys are in the command */
  if ( this->step_mask != 0 )
    return kv_popcountl( this->step_mask );
  if ( this->last > 0 )
    return ( this->last + 1 - this->first ) / this->step;
  if ( this->last < 0 )
    return ( this->argc - this->first ) / this->step;
  return 0;
}

inline ExecStatus
RedisExec::prepare_exec_command( void ) noexcept
{
  RedisMsg * arg0 = this->msg.get_arg( 0 );

  /* get command string, hash it, check cmd args geometry */
  if ( arg0 == NULL )
    return ERR_BAD_CMD;
  this->argc = ( this->msg.type == DS_BULK_ARRAY ? this->msg.len : 1 );
  if ( arg0->type == DS_BULK_STRING || arg0->type == DS_SIMPLE_STRING ) {
    /* max command len is 17 (GEORADIUSBYMEMBER) */
    if ( arg0->len <= 0 || (size_t) arg0->len >= MAX_CMD_LEN )
      return ERR_BAD_CMD;

    uint32_t h = get_redis_cmd_hash( arg0->strval, arg0->len );
    this->cmd  = get_redis_cmd( h );
    const RedisCmdData &c = cmd_db[ this->cmd ];
    this->setup_cmd( c );
    if ( c.hash != h )
      return ERR_BAD_CMD;
  }
  else if ( arg0->type == DS_INTEGER_VALUE ) {
    if ( arg0->ival <= 0 || arg0->ival > (int64_t) REDIS_CMD_DB_SIZE )
      return ERR_BAD_CMD;
    this->cmd = (RedisCmd) arg0->ival;
    this->setup_cmd( cmd_db[ this->cmd ] );
  }
  else
    return ERR_BAD_CMD;

  if ( this->arity > 0 ) {
    if ( (size_t) this->arity != this->argc )
      return ERR_BAD_ARGS;
  }
  else if ( (size_t) -this->arity > this->argc )
    return ERR_BAD_ARGS;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec( EvSocket *svc,  EvPrefetchQueue *q ) noexcept
{
  ExecStatus status;
  /* determine command */
  status = this->prepare_exec_command();
  if ( status == EXEC_OK ) {
    if ( ( this->cmd_flags & CMD_MOVABLE_FLAG ) != 0 ) {
      if ( ! this->locate_movablekeys() )
        status = ERR_BAD_ARGS;
    }
  }
  /* save output state, may need to rewind */
  this->strm_start = this->strm.pending();
  /* if queueing cmds for transaction */
  if ( ( this->cmd_state & CMD_STATE_MULTI_QUEUED ) != 0 ) {
    if ( status != EXEC_OK ) {
      this->multi->multi_abort = true;
      return status;
    }
    switch ( this->cmd ) {
      case EXEC_CMD:
      case MULTI_CMD:
      case DISCARD_CMD:
        break;
      default:
        if ( (status = this->multi_queued( svc )) != EXEC_OK )
          return status;
        return EXEC_QUEUED;
    }
  }
  else {
    if ( status != EXEC_OK )
      return status;
  }
  /* if there are keys, setup a keyctx for each one */
  if ( this->first > 0 ) {
    int i = this->first;

    this->key_cnt  = 1;
    this->key_done = 0;

    this->key  = NULL;
    this->keys = &this->key;
    this->kctx.msg = NULL;
    /* setup first key */
    status = this->exec_key_setup( svc, q, this->key, i, 0 );
    if ( status == EXEC_SETUP_OK ) {
      /* setup rest of keys, if any */
      if ( this->next_key( i ) ) {
        size_t key_count = this->calc_key_count();
        if ( key_count == 0 )
          return ERR_BAD_ARGS;
        this->keys = (EvKeyCtx **)
          this->strm.alloc_temp( sizeof( this->keys[ 0 ] ) * key_count );
        if ( this->keys == NULL )
          status = ERR_ALLOC_FAIL;
        else {
          this->keys[ 0 ] = this->key;
          do {
            status = this->exec_key_setup( svc, q, this->keys[ this->key_cnt ],
                                           i, this->key_cnt );
            this->key_cnt++;
          } while ( status == EXEC_SETUP_OK && this->next_key( i ) );
        }
      }
    }
    return status; /* cmds with keys return setup ok */
  }
  /* cmd has no key when first == 0 */
  status = this->exec_nokeys();
  /* does this go here for EKF_MONITOR? */
  if ( ( this->key_flags & this->sub_route.key_flags ) != 0 )
    RedisKeyspace::pub_keyspace_events( *this );
  /* if "client reply skip" cmd run, this eats the result and turns of skip */
  if ( status != EXEC_SKIP &&
       ( this->cmd_state & ( CMD_STATE_CLIENT_REPLY_OFF |
                             CMD_STATE_CLIENT_REPLY_SKIP ) ) != 0 ) {
    this->cmd_state &= ~CMD_STATE_CLIENT_REPLY_SKIP;
    this->strm.truncate( this->strm_start );
    return EXEC_OK;
  }
  return status;
}

ExecStatus
RedisExec::exec_nokeys( void ) noexcept
{
  switch ( this->cmd ) {
#if 0
    /* CLUSTER */
    case CLUSTER_CMD:      return this->exec_cluster();
    case READONLY_CMD:     return this->exec_readonly();
    case READWRITE_CMD:    return this->exec_readwrite();
#endif
    /* CONNECTION */
#if 0
    case AUTH_CMD:         return this->exec_auth();
#endif
    case ECHO_CMD:         return this->exec_echo();
    case PING_CMD:         return this->exec_ping();
    case QUIT_CMD:         return this->exec_quit(); //EXEC_QUIT;
    case SELECT_CMD:       return this->exec_select();
#if 0
    case SWAPDB_CMD:       return this->exec_swapdb();
#endif
    /* SERVER */
#if 0
    case BGREWRITEAOF_CMD: return this->exec_bgrewriteaof();
    case BGSAVE_CMD:       return this->exec_bgsave();
#endif
    case CLIENT_CMD:       return this->exec_client();
    case COMMAND_CMD:      return this->exec_command();
    case CONFIG_CMD:       return this->exec_config();
    case DBSIZE_CMD:       return this->exec_dbsize();
    case DEBUG_CMD:        return this->exec_debug();
    case FLUSHALL_CMD:     return this->exec_flushall();
    case FLUSHDB_CMD:      return this->exec_flushdb();
    case INFO_CMD:         return this->exec_info();
#if 0
    case LASTSAVE_CMD:     return this->exec_lastsave();
    case MEMORY_CMD:       return this->exec_memory();
#endif
    case MONITOR_CMD:      return this->exec_monitor();
#if 0
    case ROLE_CMD:         return this->exec_role();
#endif
    case SAVE_CMD:         return this->exec_save();
    case LOAD_CMD:         return this->exec_load();
    case SHUTDOWN_CMD:     return this->exec_shutdown();
#if 0
    case SLAVEOF_CMD:      return this->exec_slaveof();
    case SLOWLOG_CMD:      return this->exec_slowlog();
    case SYNC_CMD:         return this->exec_sync();
#endif
    case TIME_CMD:         return this->exec_time();
    /* KEYS */
    case KEYS_CMD:         return this->exec_keys();
    case RANDOMKEY_CMD:    return this->exec_randomkey();
#if 0
    case WAIT_CMD:         return this->exec_wait();
#endif
    case SCAN_CMD:         return this->exec_scan();
    /* PUBSUB */
    case PSUBSCRIBE_CMD:   return this->exec_psubscribe();
    case PUBSUB_CMD:       return this->exec_pubsub();
    case PUBLISH_CMD:      return this->exec_publish();
    case PUNSUBSCRIBE_CMD: return this->exec_punsubscribe();
    case SUBSCRIBE_CMD:    return this->exec_subscribe();
    case UNSUBSCRIBE_CMD:  return this->exec_unsubscribe();
    /* TRANSACTION */
    case DISCARD_CMD:      return this->exec_discard();
    case EXEC_CMD:         return this->exec_exec();
    case MULTI_CMD:        return this->exec_multi();
    case UNWATCH_CMD:      return this->exec_unwatch();
    default:               return ERR_BAD_CMD;
  }
}

kv::KeyStatus
RedisExec::exec_key_fetch( EvKeyCtx &ctx,  bool force_read ) noexcept
{
  /* if not executing multi transaction or save */
  if ( ( this->cmd_state & ( CMD_STATE_EXEC_MULTI | CMD_STATE_SAVE ) ) == 0 ) {
    this->exec_key_set( ctx );
    if ( ( this->cmd_flags & CMD_READ_FLAG ) != 0 || force_read ) {
      for (;;) {
        ctx.kstatus = this->kctx.find( &this->wrk );
        ctx.flags  |= EKF_IS_READ_ONLY;
        /* check if key expired */
        if ( ctx.kstatus != KEY_OK || ! this->kctx.is_expired() )
          break;
        /* if expired, release entry and find it as new element */
        ctx.kstatus = this->kctx.acquire( &this->wrk );
        if ( ctx.kstatus == KEY_OK && this->kctx.is_expired() ) {
          this->kctx.expire();
          ctx.flags |= EKF_IS_EXPIRED | EKF_KEYSPACE_EVENT;
        }
        this->kctx.release();
      }
    }
    else if ( ( this->cmd_flags & CMD_WRITE_FLAG ) != 0 ||
              ( this->cmd_state & CMD_STATE_LOAD ) != 0 ) {
      ctx.kstatus = this->kctx.acquire( &this->wrk );
      if ( ctx.kstatus == KEY_OK && this->kctx.is_expired() ) {
        this->kctx.expire();
        ctx.flags  |= EKF_IS_EXPIRED | EKF_KEYSPACE_EVENT | EKF_IS_NEW;
        ctx.kstatus = KEY_IS_NEW;
      }
      else {
        ctx.flags |= ( ( ctx.kstatus == KEY_IS_NEW ) ? EKF_IS_NEW : 0 );
      }
      ctx.flags &= ~EKF_IS_READ_ONLY;
    }
    else {
      ctx.kstatus = KEY_NO_VALUE;
      ctx.status  = ERR_BAD_CMD;
      ctx.flags  |= EKF_IS_READ_ONLY;
    }
  }
  else {
    if ( ( this->cmd_state & CMD_STATE_EXEC_MULTI ) != 0 )
      this->multi_key_fetch( ctx, force_read );
    else { /* CMD_STATE_SAVE */
      ctx.kstatus = KEY_OK;
      ctx.flags  |= EKF_IS_READ_ONLY;
    }
  }
  if ( ctx.kstatus == KEY_OK ) /* not new and is found */
    ctx.type = this->kctx.get_type();
  return ctx.kstatus;
}

ExecStatus
RedisExec::exec_key_continue( EvKeyCtx &ctx ) noexcept
{
  if ( ctx.status != EXEC_CONTINUE && ctx.status != EXEC_DEPENDS ) {
    if ( ++this->key_done < this->key_cnt )
      return EXEC_CONTINUE;
    goto success;
  }
#if 0
  if ( ( this->cmd_flags & CMD_MULTI_EXEC_FLAG ) == 0 )
    this->exec_key_set( ctx );
#endif
  /*if ( this->kctx.kbuf != &ctx.kbuf ||
       this->kctx.key != ctx.hash1 || this->kctx.key2 != ctx.hash2 )
    this->exec_key_prefetch( ctx );*/
  for (;;) {
    switch ( this->cmd ) {
#if 0
      /* CLUSTER */
      case CLUSTER_CMD:  /* these exist so that the compiler errors when a */
      case READONLY_CMD: /* command is not handled by the switch() */
      case READWRITE_CMD: ctx.status = ERR_BAD_CMD; break;
#endif
      /* CONNECTION */
#if 0
      case AUTH_CMD:
#endif
      case ECHO_CMD: case PING_CMD: case QUIT_CMD: case SELECT_CMD:
#if 0
      case SWAPDB_CMD:
#endif
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
      case HEXISTS_CMD:   ctx.status = this->exec_hexists( ctx ); break;
      case HGET_CMD:      ctx.status = this->exec_hget( ctx ); break;
      case HGETALL_CMD:   ctx.status = this->exec_hgetall( ctx ); break;
      case HINCRBY_CMD:   ctx.status = this->exec_hincrby( ctx ); break;
      case HINCRBYFLOAT_CMD: ctx.status = this->exec_hincrbyfloat( ctx ); break;
      case HKEYS_CMD:     ctx.status = this->exec_hkeys( ctx ); break;
      case HLEN_CMD:      ctx.status = this->exec_hlen( ctx ); break;
      case HMGET_CMD:     ctx.status = this->exec_hmget( ctx ); break;
      case HMSET_CMD:     ctx.status = this->exec_hmset( ctx ); break;
      case HSET_CMD:      ctx.status = this->exec_hset( ctx ); break;
      case HSETNX_CMD:    ctx.status = this->exec_hsetnx( ctx ); break;
      case HSTRLEN_CMD:   ctx.status = this->exec_hstrlen( ctx ); break;
      case HVALS_CMD:     ctx.status = this->exec_hvals( ctx ); break;
      case HSCAN_CMD:     ctx.status = this->exec_hscan( ctx ); break;
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
#if 0
      case MIGRATE_CMD:   ctx.status = this->exec_migrate( ctx ); break;
      case MOVE_CMD:      ctx.status = this->exec_move( ctx ); break;
#endif
      case OBJECT_CMD:    ctx.status = this->exec_object( ctx ); break;
      case PERSIST_CMD:   ctx.status = this->exec_persist( ctx ); break;
      case PEXPIRE_CMD:   ctx.status = this->exec_pexpire( ctx ); break;
      case PEXPIREAT_CMD: ctx.status = this->exec_pexpireat( ctx ); break;
      case PTTL_CMD:      ctx.status = this->exec_pttl( ctx ); break;
      case RANDOMKEY_CMD: ctx.status = ERR_BAD_CMD; break; /* in exec() */
      case RENAME_CMD:    ctx.status = this->exec_rename( ctx ); break;
      case RENAMENX_CMD:  ctx.status = this->exec_renamenx( ctx ); break;
      case RESTORE_CMD:   ctx.status = this->exec_restore( ctx ); break;
#if 0
      case SORT_CMD:      ctx.status = this->exec_sort( ctx ); break;
#endif
      case TOUCH_CMD:     ctx.status = this->exec_touch( ctx ); break;
      case TTL_CMD:       ctx.status = this->exec_ttl( ctx ); break;
      case TYPE_CMD:      ctx.status = this->exec_type( ctx ); break;
      case UNLINK_CMD:    ctx.status = this->exec_unlink( ctx ); break;
#if 0
      case WAIT_CMD: 
#endif
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
      case PSUBSCRIBE_CMD:   case PUBSUB_CMD:    case PUBLISH_CMD:
      case PUNSUBSCRIBE_CMD: case SUBSCRIBE_CMD:
      case UNSUBSCRIBE_CMD:  ctx.status = ERR_BAD_CMD; break; /* in exec() */
#if 0
      /* SCRIPT */
      case EVAL_CMD:      ctx.status = this->exec_eval( ctx ); break;
      case EVALSHA_CMD:   ctx.status = this->exec_evalsha( ctx ); break;
      case SCRIPT_CMD:    ctx.status = this->exec_script( ctx ); break;
#endif
      /* SERVER */
#if 0
      case BGREWRITEAOF_CMD: case BGSAVE_CMD:
#endif
      case CLIENT_CMD:
      case COMMAND_CMD:
      case CONFIG_CMD:
      case DBSIZE_CMD:
      case DEBUG_CMD:
      case FLUSHALL_CMD:
      case FLUSHDB_CMD:
      case INFO_CMD:
#if 0
      case LASTSAVE_CMD:
      case MEMORY_CMD:
#endif
      case MONITOR_CMD:
#if 0
      case ROLE_CMD:
#endif
      case SAVE_CMD:
      case LOAD_CMD:
      case SHUTDOWN_CMD:
#if 0
      case SLAVEOF_CMD:
      case SLOWLOG_CMD:
      case SYNC_CMD:
#endif
      case TIME_CMD:      ctx.status = ERR_BAD_CMD; break;
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
      case ZPOPMIN_CMD:   ctx.status = this->exec_zpopmin( ctx ); break;
      case ZPOPMAX_CMD:   ctx.status = this->exec_zpopmax( ctx ); break;
      case BZPOPMIN_CMD:  ctx.status = this->exec_bzpopmin( ctx ); break;
      case BZPOPMAX_CMD:  ctx.status = this->exec_bzpopmax( ctx ); break;
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
      case DISCARD_CMD:   
      case EXEC_CMD:      
      case MULTI_CMD:     
      case UNWATCH_CMD:   ctx.status = ERR_BAD_CMD; break; /* no keys */
      case WATCH_CMD:     ctx.status = this->exec_watch( ctx ); break;
      /* STREAM */
      case XINFO_CMD:     ctx.status = this->exec_xinfo( ctx ); break;
      case XADD_CMD:      ctx.status = this->exec_xadd( ctx ); break;
      case XTRIM_CMD:     ctx.status = this->exec_xtrim( ctx ); break;
      case XDEL_CMD:      ctx.status = this->exec_xdel( ctx ); break;
      case XRANGE_CMD:    ctx.status = this->exec_xrange( ctx ); break;
      case XREVRANGE_CMD: ctx.status = this->exec_xrevrange( ctx ); break;
      case XLEN_CMD:      ctx.status = this->exec_xlen( ctx ); break;
      case XREAD_CMD:     ctx.status = this->exec_xread( ctx ); break;
      case XGROUP_CMD:    ctx.status = this->exec_xgroup( ctx ); break;
      case XREADGROUP_CMD: ctx.status = this->exec_xreadgroup( ctx ); break;
      case XACK_CMD:      ctx.status = this->exec_xack( ctx ); break;
      case XCLAIM_CMD:    ctx.status = this->exec_xclaim( ctx ); break;
      case XPENDING_CMD:  ctx.status = this->exec_xpending( ctx ); break;
      case XSETID_CMD:    ctx.status = this->exec_xsetid( ctx ); break;

      case NO_CMD:        ctx.status = ERR_BAD_CMD; break;
    }
    /* set the type when key is new */
    if ( ! ctx.is_read_only() ) {
      if ( exec_status_success( ctx.status ) ) {
        if ( ( ctx.flags & EKF_KEYSPACE_DEL ) == 0 ) {
          if ( ctx.is_new() ) {
            uint8_t type;
            if ( (type = ctx.type) == MD_NODATA ) {
              switch ( this->catg ) {
                default:               type = MD_NODATA;      break;
                case GEO_CATG:         type = MD_GEO;         break;
                case HASH_CATG:        type = MD_HASH;        break;
                case HYPERLOGLOG_CATG: type = MD_HYPERLOGLOG; break;
                case LIST_CATG:        type = MD_LIST;        break;
                case PUBSUB_CATG:      type = MD_NODATA;      break;
    #if 0
                case SCRIPT_CATG:      type = MD_NODATA; /*MD_SCRIPT;*/  break;
    #endif
                case SET_CATG:         type = MD_SET;         break;
                case SORTED_SET_CATG:  type = MD_ZSET;        break;
                case STRING_CATG:      type = MD_STRING;      break;
                case TRANSACTION_CATG: type = MD_NODATA;      break;
                case STREAM_CATG:      type = MD_STREAM;      break;
              }
            }
            if ( type != MD_NODATA ) {
              this->kctx.set_type( type );
              ctx.type = type;
            }
            this->kctx.set_val( 0 );
          }
          if ( ( ctx.state & EKS_NO_UPDATE ) == 0 )
            this->kctx.update_stamps( 0, this->kctx.ht.hdr.current_stamp );
        }
        this->key_flags |= ctx.flags;
      }
      /* if not executing multi transaction */
      if ( ( this->cmd_state & CMD_STATE_EXEC_MULTI ) == 0 )
        this->kctx.release();
    }
    /* read only */
    else {
      /* in case a key expired with fetch read only */
      this->key_flags |= ctx.flags;
      /* check if mutated */
      if ( ctx.status > ERR_KV_STATUS ) {
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_MUTATED )
          ctx.status = ERR_KV_STATUS;
      }
      /* if key depends on other keys */
      else if ( ctx.status == EXEC_DEPENDS ) {
        ctx.dep++;
        return EXEC_DEPENDS;
      }
    }
    /* continue if read key mutated while running */
    if ( ctx.status != ERR_KV_STATUS || ctx.kstatus != KEY_MUTATED ||
         ( ctx.flags & EKF_IS_READ_ONLY ) == 0 )
      break;
  } /* for(;;) - keep trying key */

  /* key is done, check if command is done */
  if ( ++this->key_done == this->key_cnt ) {
    /* if all keys are blocked, waiting for some event */
    if ( ctx.status == EXEC_BLOCKED ) {
      /* if no pre-existing state created, create it */
      if ( this->blk_state == 0 ) {
        ctx.status = this->save_blocked_cmd( ctx.ival /* timeout */ );
        if ( ctx.status == EXEC_OK )
          return EXEC_SUCCESS;
        return (ExecStatus) ctx.status;
      }
      else if ( ( this->blk_state & RBLK_CMD_TIMEOUT ) != 0 ) {
        /* blocked cmd timeout */
        ctx.status = EXEC_SEND_NULL;
        this->blk_state |= RBLK_CMD_COMPLETE;
      }
    }
    else if ( this->blk_state != 0 ) {
      /* did not block, so blocked command is complete */
      this->blk_state |= RBLK_CMD_COMPLETE;
    }
    /* all keys complete, mget is special */
    if ( this->cmd == MGET_CMD ) {
      if ( exec_status_success( ctx.status ) ) {
        this->array_string_result();
        goto success;
      }
    }
  }
  /* more keys left, command continues */
  else {
    /* if status is still good, continue to process next key */
    if ( ctx.status <= EXEC_BLOCKED )
      return EXEC_CONTINUE;
    /* other status values finish the command (ERR, ABORT, SEND_DATA) */
    for ( uint32_t i = 0; i < this->key_cnt; i++ )
      /* if keys are in a prefetch queue, set status to prevent exec */
      this->keys[ i ]->status = ctx.status;
  }
  /* if ctx.status outputs data */
  switch ( ctx.status ) {
    case EXEC_SEND_DATA: /* have multiple keys and only one key triggers */
      if ( this->blk_state != 0 )
        this->blk_state |= RBLK_CMD_COMPLETE;
      goto success;
    case EXEC_OK:      break;
    case EXEC_BLOCKED: break;
    default:
      this->send_status( (ExecStatus) ctx.status, ctx.kstatus );
      break;
  }
  /* wait for the last key to be run */
  if ( this->key_done < this->key_cnt )
    return EXEC_CONTINUE;
success:;
  /* publish keyspace events (maybe "client reply skip" mutes monitor?) */
  if ( ( this->key_flags & this->sub_route.key_flags ) != 0 )
    RedisKeyspace::pub_keyspace_events( *this );

  /* if "client reply skip" cmd run, this eats the result and turns of skip */
  if ( ( this->cmd_state & ( CMD_STATE_CLIENT_REPLY_OFF |
                             CMD_STATE_CLIENT_REPLY_SKIP ) ) != 0 ) {
    this->cmd_state &= ~CMD_STATE_CLIENT_REPLY_SKIP;
    this->strm.truncate( this->strm_start );
    ctx.status = EXEC_OK;
  }
  return EXEC_SUCCESS;
}

void
RedisExec::send_msg( const RedisMsg &m ) noexcept
{
  char * buf = this->strm.alloc( m.pack_size() );
  if ( buf != NULL )
    this->strm.sz += m.pack( buf );
}

void
RedisExec::send_int( void ) noexcept
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
RedisExec::send_int( int64_t ival ) noexcept
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
    size_t ilen = int64_digits( ival );
    char  * buf = this->strm.alloc( ilen + 3 );
    if ( buf != NULL ) {
      buf[ 0 ] = ':';
      ilen = 1 + int64_to_string( ival, &buf[ 1 ], ilen );
      this->strm.sz += crlf( buf, ilen );
    }
  }
}

void
RedisExec::send_status( ExecStatus stat,  KeyStatus kstat ) noexcept
{
  switch ( stat ) {
    /* these already sent data or don't need to */
    case EXEC_OK:
    case EXEC_SETUP_OK:
    case EXEC_SEND_DATA: break;

    /* status constants */
    case EXEC_SEND_OK:          this->send_ok(); break;
    case EXEC_ABORT_SEND_NIL:
    case EXEC_SEND_NIL:         this->send_nil(); break;
    case EXEC_SEND_NULL:        this->send_null(); break;
    case EXEC_SEND_INT:         this->send_int(); break;
    case EXEC_ABORT_SEND_ZERO:
    case EXEC_SEND_ZEROARR:     this->send_zeroarr(); break;
    case EXEC_SEND_ZERO:        this->send_zero(); break;
    case EXEC_SEND_ONE:         this->send_one(); break;
    case EXEC_SEND_NEG_ONE:     this->send_neg_one(); break;
    case EXEC_SEND_ZERO_STRING: this->send_zero_string(); break;
    case EXEC_QUEUED:           this->send_queued(); break;

    /* states of execution, shouldn't be here */
    case EXEC_BLOCKED:
    case EXEC_SUCCESS:
    case EXEC_DEPENDS:
    case EXEC_CONTINUE: break;

    /* errors */
    case ERR_KV_STATUS:
    case ERR_MSG_STATUS:
    case ERR_BAD_CMD:
    case ERR_BAD_ARGS:
    case ERR_BAD_TYPE:
    case ERR_BAD_RANGE:
    case ERR_NO_GROUP:
    case ERR_GROUP_EXISTS:
    case ERR_STREAM_ID:
    case ERR_ALLOC_FAIL:
    case ERR_KEY_EXISTS:
    case ERR_KEY_DOESNT_EXIST:
    case ERR_BAD_MULTI:
    case ERR_BAD_EXEC:
    case ERR_BAD_DISCARD:
    case ERR_ABORT_TRANS:
    case ERR_SAVE:
    case ERR_LOAD:        this->send_err_string( stat, kstat ); break;

    /* ok status */
    case EXEC_QUIT:
    case EXEC_DEBUG: this->send_ok(); break;
    case EXEC_SKIP:  break;
  }
}

void
RedisExec::send_err_string( ExecStatus stat,  KeyStatus kstat ) noexcept
{
  const char *str = NULL;
  switch ( stat ) {
    case ERR_KV_STATUS:        break;
    case ERR_MSG_STATUS:       break;
    case ERR_BAD_CMD:          str = "-ERR unknown"; break;
    case ERR_BAD_ARGS:         str = "-ERR arguments format"; break;
    case ERR_BAD_TYPE:         str = "-ERR type invalid"; break;
    case ERR_BAD_RANGE:        str = "-ERR index invalid"; break;
    case ERR_NO_GROUP:         str = "-ERR group missing"; break;
    case ERR_GROUP_EXISTS:     str = "-ERR group exists"; break;
    case ERR_STREAM_ID:        str = "-ERR stream id invalid"; break;
    case ERR_ALLOC_FAIL:       str = "-ERR alloc failed"; break;
    case ERR_KEY_EXISTS:       str = "-ERR key exists"; break;
    case ERR_KEY_DOESNT_EXIST: str = "-ERR key missing"; break;
    case ERR_BAD_MULTI:        str = "-ERR bad multi, no nesting"; break;
    case ERR_BAD_EXEC:         str = "-ERR bad exec, no multi"; break;
    case ERR_BAD_DISCARD:      str = "-ERR bad discard, no multi"; break;
    case ERR_ABORT_TRANS:      str = "-ERR transaction aborted, error"; break;
    case ERR_SAVE:             str = "-ERR save failed, file error"; break;
    case ERR_LOAD:             str = "-ERR load failed, file error"; break;
    default:                   str = "-ERR"; break;
  }

  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 256 + MAX_CMD_LEN;
  char       * buf  = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    if ( arg0len > MAX_CMD_LEN ) {
      arg0len = MAX_CMD_LEN;
    }
    else if ( arg0len == 0 ) {
      arg0len = 3;
      arg0    = "???";
    }
    switch ( (ExecStatus) stat ) {
      default:
        ::strcpy( buf, str );
        bsz = ::strlen( buf );
        break;
      /* problem in the key store */
      case ERR_KV_STATUS:
        bsz = ::snprintf( buf, bsz, "-ERR kv %d/%s %s",
                         kstat, kv_key_status_string( (KeyStatus) kstat ),
                         kv_key_status_description( (KeyStatus) kstat ) );
        break;
      /* problem parsing the message */
      case ERR_MSG_STATUS: {
        RedisMsgStatus mstat = this->mstatus;
        bsz  = ::snprintf( buf, bsz, "-ERR message %d/%s %s",
                           mstat, ds_msg_status_string( mstat ),
                           ds_msg_status_description( mstat ) );
        break;
      }
    }
    /* append " cmd: '%s'\r\n" */
    ::memcpy( &buf[ bsz ], " cmd: ", 6 );
    bsz += 6;
    buf[ bsz++ ] = '\'';
    ::memcpy( &buf[ bsz ], arg0, arg0len );
    bsz += arg0len;
    buf[ bsz++ ] = '\'';

    strm.sz += crlf( buf, bsz );
  }
  if ( stat == ERR_BAD_CMD ) {
    char tmpbuf[ 1024 ];
    if ( this->msg.to_almost_json_size() < sizeof( tmpbuf ) ) {
      size_t sz = this->msg.to_almost_json( tmpbuf );
      fprintf( stderr, "Bad command: %.*s\n", (int) sz, tmpbuf );
    }
  }
}

const char *
EvKeyCtx::get_type_str( void ) const noexcept
{
  return md_type_str( (MDType) this->type, 0 );
}

size_t
RedisBufQueue::append_string( const void *str,  size_t len,
                              const void *str2,  size_t len2 ) noexcept
{
  size_t itemlen = len + len2,
         d       = uint64_digits( itemlen );
  StreamBuf::BufList * p = this->get_buf( itemlen + d + 5 );

  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used++ ] = '$';
  p->used += uint64_to_string( itemlen, &bufp[ p->used ], d );
  p->used = crlf( bufp, p->used );
  ::memcpy( &bufp[ p->used ], str, len );
  if ( len2 > 0 )
    ::memcpy( &bufp[ p->used + len ], str2, len2 );
  p->used = crlf( bufp, p->used + len + len2 );

  return p->used;
}

size_t
RedisBufQueue::append_nil( bool is_null ) noexcept
{
  StreamBuf::BufList * p = this->get_buf( 5 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used ]   = ( is_null ? '*' : '$' );
  bufp[ p->used+1 ] = '-';
  bufp[ p->used+2 ] = '1';
  p->used = crlf( bufp, p->used + 3 );

  return p->used;
}

size_t
RedisBufQueue::append_zero_array( void ) noexcept
{
  StreamBuf::BufList * p = this->get_buf( 5 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used ]   = '*';
  bufp[ p->used+1 ] = '0';
  p->used = crlf( bufp, p->used + 2 );

  return p->used;
}

size_t
RedisBufQueue::append_uint( uint64_t val ) noexcept
{
  size_t d = uint64_digits( val );
  StreamBuf::BufList * p = this->get_buf( d + 3 );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  bufp[ p->used++ ] = ':';
  p->used += uint64_to_string( val, &bufp[ p->used ], d );
  p->used = crlf( bufp, p->used );
  return p->used;
}

void
RedisBufQueue::prepend_array( size_t nitems ) noexcept
{
  size_t    itemlen = uint64_digits( nitems ),
                 /*  '*'   4      '\r\n' (nitems = 1234) */
            len     = 1 + itemlen + 2;
  char * bufp = this->prepend_buf( len );
  bufp[ 0 ] = '*';
  uint64_to_string( nitems, &bufp[ 1 ], itemlen );
  crlf( bufp, len - 2 );
}

void
RedisBufQueue::prepend_cursor_array( size_t curs,  size_t nitems ) noexcept
{
  size_t    curslen = uint64_digits( curs ),
            clenlen = uint64_digits( curslen ),
            itemlen = uint64_digits( nitems ),
            len     = /* '*2\r\n$'    1    '\r\n'  0     '\r\n' (curs=0) */
                           5      + clenlen + 2 + curslen + 2 +
                      /* '*'    4     '\r\n'  (nitems = 1234) */
                          1 + itemlen + 2,
            i;
  char * bufp = this->prepend_buf( len );

  bufp[ 0 ] = '*';
  bufp[ 1 ] = '2';
  crlf( bufp, 2 );
  bufp[ 4 ] = '$';
  i  = 5 + uint64_to_string( curslen, &bufp[ 5 ], clenlen );
  i  = crlf( bufp, i );
  i += uint64_to_string( curs, &bufp[ i ], curslen );
  i  = crlf( bufp, i );
  bufp[ i ] = '*';
  i += 1 + uint64_to_string( nitems, &bufp[ 1 + i ], itemlen );
  crlf( bufp, i );
}
