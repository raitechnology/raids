#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>

using namespace rai;
using namespace ds;
using namespace kv;

ExecStatus
RedisExec::exec_key_setup( EvService *own,  EvPrefetchQueue *q,
                           RedisKeyCtx *&ctx,  int n )
{
  const char * key;
  size_t       keylen;
  if ( ! this->msg.get_arg( n, key, keylen ) )
    return EXEC_BAD_ARGS;
  void *p = this->strm.tmp.alloc( RedisKeyCtx::size( keylen ) );
  if ( p == NULL )
    return EXEC_ALLOC_FAIL;
  ctx = new ( p ) RedisKeyCtx( *this, own, key, keylen,
                               this->seed, this->seed2 );
  if ( q != NULL && ! q->push( ctx ) )
    return EXEC_ALLOC_FAIL;
  return EXEC_SETUP_OK;
}

ExecStatus
RedisExec::exec( EvService *own,  EvPrefetchQueue *q )
{
  if ( this->first > 0 ) {
    int i, end;
    ExecStatus status;

    this->key_cnt  = 1;
    this->key_done = 0;
    if ( (end = this->last) < 0 )
      end = this->argc - 1;

    i = this->first;
    status = this->exec_key_setup( own, q, this->key, i );
    if ( status == EXEC_SETUP_OK ) {
      if ( i < end ) {
        this->keys = (RedisKeyCtx **)
                     this->strm.tmp.alloc( sizeof( this->keys[ 0 ] ) * 
                                           ( ( end + 1 ) - this->first ) );
        if ( this->keys == NULL )
          status = EXEC_ALLOC_FAIL;
        else {
          i += this->step;
          this->keys[ 0 ] = this->key;
          do {
            status = this->exec_key_setup( own, q,
                                           this->keys[ this->key_cnt++ ], i );
          } while ( status == EXEC_SETUP_OK && (i += this->step) <= end );
        }
      }
    }
    return status;
  }
  switch ( this->cmd ) {
    case COMMAND_CMD:  return this->exec_command();
    case ECHO_CMD:     
    case PING_CMD:     return this->exec_ping();
    case DEBUG_CMD:    return this->exec_debug( own );
    case SHUTDOWN_CMD:
    case QUIT_CMD:     return EXEC_QUIT;
    default:           return EXEC_BAD_CMD;
  }
}

bool
RedisExec::exec_key_continue( RedisKeyCtx &ctx )
{
  if ( this->kctx.key != ctx.hash1 || this->kctx.key2 != ctx.hash2 )
    this->exec_key_prefetch( ctx );
  for (;;) {
    if ( test_cmd_mask( this->flags, CMD_READONLY_FLAG ) )
      ctx.kstatus = this->kctx.find( &this->wrk );
    else if ( test_cmd_mask( this->flags, CMD_WRITE_FLAG ) )
      ctx.kstatus = this->kctx.acquire( &this->wrk );
    else {
      ctx.status = EXEC_BAD_CMD;
      break;
    }
    switch ( this->cmd ) {
      /* string group */
      case APPEND_CMD:      ctx.status = this->exec_append( ctx );      break;
      case BITCOUNT_CMD:    ctx.status = this->exec_bitcount( ctx );    break;
      case BITFIELD_CMD:    ctx.status = this->exec_bitfield( ctx );    break;
      case BITOP_CMD:       ctx.status = this->exec_bitop( ctx );       break;
      case BITPOS_CMD:      ctx.status = this->exec_bitpos( ctx );      break;
      case DECR_CMD:        ctx.status = this->exec_decr( ctx );        break;
      case DECRBY_CMD:      ctx.status = this->exec_decrby( ctx );      break;
      case GET_CMD:         ctx.status = this->exec_get( ctx );         break;
      case GETBIT_CMD:      ctx.status = this->exec_getbit( ctx );      break;
      case GETRANGE_CMD:    ctx.status = this->exec_getrange( ctx );    break;
      case GETSET_CMD:      ctx.status = this->exec_getset( ctx );      break;
      case INCR_CMD:        ctx.status = this->exec_incr( ctx );        break;
      case INCRBY_CMD:      ctx.status = this->exec_incrby( ctx );      break;
      case INCRBYFLOAT_CMD: ctx.status = this->exec_incrbyfloat( ctx ); break;
      case MGET_CMD:        ctx.status = this->exec_mget( ctx );        break;
      case MSET_CMD:        ctx.status = this->exec_mset( ctx );        break;
      case MSETNX_CMD:      ctx.status = this->exec_msetnx( ctx );      break;
      case PSETEX_CMD:      ctx.status = this->exec_psetex( ctx );      break;
      case SET_CMD:         ctx.status = this->exec_set( ctx );         break;
      case SETBIT_CMD:      ctx.status = this->exec_setbit( ctx );      break;
      case SETEX_CMD:       ctx.status = this->exec_setex( ctx );       break;
      case SETNX_CMD:       ctx.status = this->exec_setnx( ctx );       break;
      case SETRANGE_CMD:    ctx.status = this->exec_setrange( ctx );    break;
      case STRLEN_CMD:      ctx.status = this->exec_strlen( ctx );      break;
      default:              ctx.status = EXEC_BAD_CMD;                  break;
    }
    if ( test_cmd_mask( this->flags, CMD_WRITE_FLAG ) ) {
      if ( (int) ctx.status <= EXEC_SUCCESS ) {
        uint8_t type;
        switch ( cmd_category( this->cmd ) ) {
          default:               type = 0;  break;
          case GEO_CATG:         type = 23; break;
          case HASH_CATG:        type = 19; break;
          case HYPERLOGLOG_CATG: type = 24; break;
          case LIST_CATG:        type = 18; break;
          case PUBSUB_CATG:      type = 25; break;
          case SCRIPT_CATG:      type = 26; break;
          case SERVER_CATG:      type = 27; break;
          case SET_CATG:         type = 20; break;
          case SORTED_SET_CATG:  type = 21; break;
          case STRING_CATG:      type = 2;  break;
          case TRANSACTION_CATG: type = 28; break;
          case STREAM_CATG:      type = 22; break;
        }
        this->kctx.set_type( type );
      }
      this->kctx.release();
    }
    if ( ctx.status != EXEC_KV_STATUS || ctx.kstatus != KEY_MUTATED )
      break;
  }
  if ( ++this->key_done < this->key_cnt )
    return false;
  switch ( ctx.status ) {
    case EXEC_OK:
      break;
    case EXEC_SEND_OK: {
      static char ok[] = "+OK\r\n";
      this->strm.append( ok, 5 );
      break;
    }
    case EXEC_SEND_NIL: {
      static char nil[] = "$-1\r\n";
      this->strm.append( nil, 5 );
      break;
    }
    case EXEC_SEND_INT: {
      char   buf[ 32 ];
      size_t len     = 1 + int64_to_string( ctx.ival, &buf[ 1 ] );
      buf[ 0 ]       = ':';
      buf[ len ]     = '\r';
      buf[ len + 1 ] = '\n';
      this->strm.append( buf, len + 2 );
      break;
    }
    default:
      this->send_err( ctx.status, ctx.kstatus );
      break;
  }
  return true;
}

ExecStatus
RedisExec::exec_set_value( RedisKeyCtx &ctx,  int n )
{
  switch ( ctx.kstatus ) {
    case KEY_OK:
    case KEY_IS_NEW: {
      const char * value;
      size_t       valuelen;
      void       * data;
      if ( ! this->msg.get_arg( n, value, valuelen ) )
        return EXEC_BAD_ARGS;
      ctx.kstatus = this->kctx.resize( &data, valuelen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, value, valuelen );
        return EXEC_SEND_OK;
      }
    }
    /* fall through */
    default:
      return EXEC_KV_STATUS;
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
        return EXEC_ALLOC_FAIL;
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
        return EXEC_ALLOC_FAIL;
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
      return EXEC_BAD_ARGS;
    case 3: /* count */
      m.set_int( REDIS_CMD_COUNT - 1 );
      break;
    case 4: { /* help */
      if ( ! m.alloc_array( this->strm.tmp, this->msg.len * 2 ) )
        return EXEC_ALLOC_FAIL;
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
      return EXEC_BAD_ARGS;
  }
  if ( this->mstatus == REDIS_MSG_OK ) {
    size_t sz  = 16 * 1024;
    void * buf = this->strm.alloc( sz );
    if ( buf == NULL )
      return EXEC_ALLOC_FAIL;
    this->mstatus = m.pack( buf, sz );
    if ( this->mstatus == REDIS_MSG_OK )
      this->strm.append_iov( buf, sz );
  }
  if ( this->mstatus != REDIS_MSG_OK )
    return EXEC_MSG_STATUS;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_ping( void )
{
  if ( this->argc > 1 ) {
    RedisMsg &sub = this->msg.array[ 1 ];
    size_t sz  = sub.len + 32;
    void * buf = this->strm.alloc( sz );
    if ( buf != NULL ) {
      sub.pack( buf, sz );
      this->strm.sz += sz;
    }
  }
  else {
    static char pong[] = "+PONG\r\n";
    this->strm.append( pong, 7 );
  }
  return EXEC_OK;
}

void
RedisExec::send_ok( void )
{
  static char ok[] = "+OK\r\n";
  this->strm.append( ok, 5 );
}

void
RedisExec::send_nil( void )
{
  static char nil[] = "$-1\r\n";
  this->strm.append( nil, 5 );
}

void
RedisExec::send_int( void )
{
  int64_t ival = ( ( this->key != NULL ) ? this->key->ival : -1 );
  char    buf[ 32 ];
  size_t  len    = 1 + int64_to_string( ival, &buf[ 1 ] );
  buf[ 0 ]       = ':';
  buf[ len ]     = '\r';
  buf[ len + 1 ] = '\n';
  this->strm.append( buf, len + 2 );
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
    case EXEC_KV_STATUS:        this->send_err_kv( kstatus ); break;
    case EXEC_MSG_STATUS:       this->send_err_msg( this->mstatus ); break;
    case EXEC_BAD_ARGS:         this->send_err_bad_args(); break;
    case EXEC_BAD_CMD:          this->send_err_bad_cmd(); break;
    case EXEC_QUIT:             this->send_ok(); break;
    case EXEC_ALLOC_FAIL:       this->send_err_alloc_fail(); break;
    case EXEC_KEY_EXISTS:       this->send_err_key_exists(); break;
    case EXEC_KEY_DOESNT_EXIST: this->send_err_key_doesnt_exist(); break;
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
    size_t tmpsz = sizeof( tmpbuf );
    if ( this->msg.to_json( tmpbuf, tmpsz ) == REDIS_MSG_OK )
      fprintf( stderr, "Bad command: %s\n", tmpbuf );
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

