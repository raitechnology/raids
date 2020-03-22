#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/resource.h>
#define SPRINTF_RELA_TIME
#include <raikv/rela_ts.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>
#include <raids/route_db.h>
#include <raids/redis_transaction.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

static void xnprintf( char *&b,  size_t &sz,  const char *format, ... )
  __attribute__((format(printf,3,4)));

static void
xnprintf( char *&b,  size_t &sz,  const char *format, ... )
{
  va_list args;
  size_t  x;
  if ( sz > 0 ) {
    va_start( args, format );
    /* I don't trust vsnprintf return value, printf is a plugable fmt system */
    /*x =*/ vsnprintf( b, sz, format, args );
    va_end( args );
    x   = ::strnlen( b, sz );
    b   = &b[ x ];
    sz -= x;
  }
}

/* CLUSTER */
ExecStatus
RedisExec::exec_cluster( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_readonly( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_readwrite( void ) noexcept
{
  return ERR_BAD_CMD;
}

/* CONNECTION */
ExecStatus
RedisExec::exec_auth( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_echo( void ) noexcept
{
  this->send_msg( this->msg.array[ 1 ] );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_ping( void ) noexcept
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
RedisExec::exec_quit( void ) noexcept
{
  return EXEC_QUIT;
}

ExecStatus
RedisExec::exec_select( void ) noexcept
{
  int64_t db;
  if ( ! this->msg.get_arg( 1, db ) || db < 0 || db >= (int64_t) DB_COUNT )
    return ERR_BAD_RANGE;

  uint32_t ctx_id = this->kctx.ctx_id,
           dbx_id = this->kctx.ht.attach_db( ctx_id, (uint8_t) db );
  
  if ( dbx_id == MAX_STAT_ID )
    return ERR_BAD_RANGE;

  this->kctx.set_db( dbx_id );
  this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->hs );
  this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_swapdb( void ) noexcept
{
  return ERR_BAD_CMD;
}

/* SERVER */
ExecStatus
RedisExec::exec_bgrewriteaof( void ) noexcept
{
  /* start a AOF */
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_bgsave( void ) noexcept
{
  /* save in the bg */
  this->send_ok();
  return EXEC_OK;
}

bool
RedisExec::get_peer_match_args( PeerMatchArgs &ka ) noexcept
{
  /* (ip) (ID id) (TYPE norm|mast|slav|pubsub) (ADDR ip) (SKIPME y/n) */
  for ( size_t i = 2; i < this->argc; ) {
    switch ( this->msg.match_arg( i, MARG( "addr" ),
                                     MARG( "id" ),
                                     MARG( "type" ),
                                     MARG( "skipme" ), NULL ) ) {
      default: /* last arg can be ip addr */
        if ( i + 1 == this->argc ) {
          if ( ! this->msg.get_arg( i, ka.ip, ka.ip_len ) )
            return false;
          i += 1;
          break;
        }
        return false;
      case 1: /* addr */
        if ( ! this->msg.get_arg( i + 1, ka.ip, ka.ip_len ) )
          return false;
        i += 2;
        break;
      case 2: /* id */
        if ( ! this->msg.get_arg( i + 1, ka.id ) )
          return false;
        i += 2;
        break;
      case 3: /* type */
        if ( ! this->msg.get_arg( i + 1, ka.type, ka.type_len ) )
          return false;
        i += 2;
        break;
      case 4: /* skipme */
        ka.skipme = true;
        i += 1;
        if ( i < this->argc ) {
          switch ( this->msg.match_arg( i, MARG( "y" ),
                                           MARG( "yes" ),
                                           MARG( "n" ),
                                           MARG( "no" ), NULL ) ) {
            case 0: break;
            case 1: case 2: i += 1; break; /* already true */
            case 3: case 4: i += 1; ka.skipme = false; break;
          }
        }
        break;
    }
  }
  return true;
}

PeerData *
PeerMatchIter::first( void ) noexcept
{
  /* go to the front */
  for ( this->p = &this->me; this->p->back != NULL; this->p = this->p->back )
    ;
  return this->next(); /* match the next */
}

PeerData *
PeerMatchIter::next( void ) noexcept
{
  while ( this->p != NULL ) { /* while peers to match */
    PeerData & x = *this->p;
    this->p = x.next;
    if ( ( &x == &this->me && this->ka.skipme ) || /* match peer */
         ! x.op.match( x, this->ka ) )
      continue;
    return &x;
  }
  return NULL;
}

ExecStatus
RedisExec::exec_client( void ) noexcept
{
  char        * s;
  const char  * nm;
  PeerData    * p;
  size_t        d, i, off, sz;
  PeerMatchArgs ka;
  PeerMatchIter iter( this->peer, ka );

  switch ( this->msg.match_arg( 1, MARG( "getname" ),
                                   MARG( "id" ),
                                   MARG( "kill" ),
                                   MARG( "list" ),
                                   MARG( "pause" ),
                                   MARG( "reply" ),
                                   MARG( "setname" ), NULL ) ) {
    default: return ERR_BAD_ARGS;
    case 1: /* getname - get my name */
      if ( (i = this->peer.get_name_strlen()) == 0 )
        this->send_nil();
      else
        this->strm.sz += this->send_string( this->peer.name, i );
      return EXEC_OK;
    case 2: /* id - get the id associated with this connection */
      this->send_int( this->peer.id );
      return EXEC_OK;
    case 3: { /* kill (ip) (ID id) (TYPE norm|mast|slav|pubsub)
                      (ADDR ip) (SKIPME y/n) */
      if ( ! this->get_peer_match_args( ka ) )
        return ERR_BAD_ARGS;
      i = 0;
      for ( p = iter.first(); p != NULL; p = iter.next() ) {
        if ( p->op.client_kill( *p ) )
          i++;
      }
      this->send_int( i ); /* number of clients killed */
      return EXEC_OK;
    }
    case 4: { /* list (ip) (ID id) (TYPE norm|mast|slav|pubsub)
                      (ADDR ip) (SKIPME y/n) */
      if ( this->argc > 2 && ! this->get_peer_match_args( ka ) )
        return ERR_BAD_ARGS;
      if ( this->strm.sz > 0 )
        this->strm.flush();
      off = this->strm.pending(); /* record location at start of list */
      i   = this->strm.idx;
      for ( p = iter.first(); p != NULL; p = iter.next() ) {
        char buf[ 8 * 1024 ];
        int  sz = p->op.client_list( *p, buf, sizeof( buf ) );
        if ( sz > 0 ) {
          char *str = this->strm.alloc( sz );
          ::memcpy( str, buf, sz );
          str[ sz - 1 ] = '\n';
          this->strm.sz += sz;
        }
      }
      s  = this->strm.alloc( 2 );
      sz = this->strm.pending();     /* size of payload */
      this->strm.sz += crlf( s, 0 ); /* terminate bulk string with \r\n */
      this->strm.flush();
      /* prepend the $count for bulk string */
      d = uint_digits( sz - off );
      s = this->strm.alloc( 1 + d + 2 ); /* $<d>\r\n */
      s[ 0 ] = '$';
      uint_to_str( sz - off, &s[ 1 ], d );
      this->strm.sz = crlf( s, 1 + d );
      this->strm.prepend_flush( i );
      return EXEC_OK;
    }
    case 5: /* pause (ms) pause clients for ms time */
      return ERR_BAD_ARGS;
    case 6: /* reply (on/off/skip) en/disable replies */
      switch ( this->msg.match_arg( 2, MARG( "on" ),
                                       MARG( "off" ),
                                       MARG( "skip" ), NULL ) ) {
        default: return ERR_BAD_ARGS;
        case 1: /* on */
          this->cmd_state &= ~( CMD_STATE_CLIENT_REPLY_OFF |
                                CMD_STATE_CLIENT_REPLY_SKIP );
          return EXEC_SEND_OK;
        case 2: /* off */
          this->cmd_state |= CMD_STATE_CLIENT_REPLY_OFF;
          this->cmd_state &= ~CMD_STATE_CLIENT_REPLY_SKIP;
          return EXEC_OK;
        case 3: /* skip */
          this->cmd_state |= CMD_STATE_CLIENT_REPLY_SKIP;
          this->cmd_state &= ~CMD_STATE_CLIENT_REPLY_OFF;
          return EXEC_SKIP;
      }
    case 7: /* setname (name) set the name of this conn */
      if ( ! this->msg.get_arg( 2, nm, sz ) ) {
        nm = NULL;
        sz = 0;
      }
      this->peer.set_name( nm, sz );
      return EXEC_SEND_OK;
  }
}

int
RedisExec::client_list( char *buf,  size_t buflen ) noexcept
{
  /* id=unique id, addr=peer addr, fd=sock, age=time connected
   * idle=time idle, flags=what, db=cur db, sub=channel subs,
   * psub=pattern subs, multi=cmds qbuf=query buf size, qbuf-free=free
   * qbuf, obl=output buf len, oll=outut list len, omem=output mem usage,
   * events=sock rd/wr, cmd=last cmd issued */
  char flags[ 8 ];
  int i = 0;
  /* A: connection to be closed ASAP
     b: the client is waiting in a blocking operation            x <- does it
     c: connection to be closed after writing entire reply
     d: a watched keys has been modified - EXEC will fail
     i: the client is waiting for a VM I/O (deprecated)
     M: the client is a master
     N: no specific flag set                                     x
     O: the client is a client in MONITOR mode                   x
     P: the client is a Pub/Sub subscriber                       x
     r: the client is in readonly mode against a cluster node
     S: the client is a replica node connection to this instance
     u: the client is unblocked
     U: the client is connected via a Unix domain socket
     x: the client is in a MULTI/EXEC context                    x  */
  if ( this->continue_tab.continue_count() > 0 )
    flags[ i++ ] = 'b'; /* blocking operation in progress */
  if ( ( this->cmd_state & CMD_STATE_MONITOR ) != 0 )
    flags[ i++ ] = 'O'; /* subscribed to monitor */
  if ( this->sub_tab.sub_count() + this->pat_tab.sub_count() != 0 )
    flags[ i++ ] = 'P'; /* subscribed to something */
  if ( this->multi != NULL )
    flags[ i++ ] = 'x'; /* has a multi */
  if ( i == 0 )
    flags[ i++ ] = 'N'; /* no specific flags */
  flags[ i ] = '\0';
  return ::snprintf( buf, buflen,
    "flags=%s db=%u sub=%lu psub=%lu multi=%d cmd=%s ",
    flags,
    this->kctx.db_num,
    this->sub_tab.sub_count(),
    this->pat_tab.sub_count(),
    ( this->multi == NULL ? -1 : (int) this->multi->msg_count ),
    cmd_db[ this->cmd ].name );
}

static void
get_cmd_info( RedisCmd cmd,  char *tmp,  size_t tmplen )
{
  const RedisCmdData &db = cmd_db[ cmd ];
  char flbuf[ 64 ];
  size_t j;
  flbuf[ 0 ] = '[';
  j = 1;
  if ( ( db.flags & CMD_READ_FLAG ) != 0 ) {
    ::strcpy( &flbuf[ j ], "\'readonly\'" );
    j += 10;
  }
  else if ( ( db.flags & CMD_WRITE_FLAG ) != 0 ) {
    ::strcpy( &flbuf[ j ], "\'write\'" );
    j += 7;
  }
  if ( ( db.flags & CMD_ADMIN_FLAG ) != 0 ) {
    if ( j > 1 ) flbuf[ j++ ] = ',';
    ::strcpy( &flbuf[ j ], "\'admin\'" );
    j += 7;
  }
  if ( ( db.flags & CMD_MOVABLE_FLAG ) != 0 ) {
    if ( j > 1 ) flbuf[ j++ ] = ',';
    ::strcpy( &flbuf[ j ], "\'movablekeys\'" );
    j += 13;
  }
  flbuf[ j++ ] = ']';
  flbuf[ j ] = '\0';

  ::snprintf( tmp, tmplen, "[\"%s\",%d,%s,%d,%d,%d]",
              db.name, db.arity, flbuf, db.first, db.last, db.step );
}

static void
get_cmd_usage( RedisCmd cmd,  char *tmp,  size_t tmplen )
{
  const RedisCmdExtra *ex = cmd_db[ cmd ].get_extra( XTRA_USAGE );
  if ( ex == NULL ) {
    ::strcpy( tmp, "none" );
    return;
  }

  size_t off = 0;
  const char * s = ex->text;
  for (;;) {
    if ( off == tmplen - 1 )
      break;
    if ( (tmp[ off ] = *s) == '\0' )
      break;
    if ( *s != '\n' )
      off++;
    s++;
  }
  tmp[ off ] = '\0';
}

ExecStatus
RedisExec::exec_command( void ) noexcept
{
  RedisMsg     m;
  const char * name;
  size_t       j = 0, len;
  char         tmp[ 8 * 80 ];

  this->mstatus = REDIS_MSG_OK;
  switch ( this->msg.match_arg( 1, MARG( "info" ),
                                   MARG( "getkeys" ),
                                   MARG( "count" ),
                                   MARG( "help" ), NULL ) ) {
    case 0: { /* no args */
      if ( this->argc > 1 )
        return ERR_BAD_ARGS;
      if ( ! m.alloc_array( this->strm.tmp, REDIS_CMD_DB_SIZE - 1 ) )
        return ERR_ALLOC_FAIL;
      for ( size_t i = 1; i < REDIS_CMD_DB_SIZE; i++ ) {
        get_cmd_info( (RedisCmd) i, tmp, sizeof( tmp ) );
        this->mstatus = m.array[ j++ ].unpack_json( tmp, this->strm.tmp );
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
          get_cmd_info( get_redis_cmd( get_redis_cmd_hash( name, len ) ),
                        tmp, sizeof( tmp ) );
          m.array[ j++ ].unpack_json( tmp, this->strm.tmp );
        }
        m.len = j;
      }
      break;
    }
    case 2: /* getkeys cmd ... show the keys of cmd */
      return ERR_BAD_ARGS;
    case 3: /* count */
      m.set_int( REDIS_CMD_DB_SIZE - 1 );
      break;
    case 4: { /* help */
      if ( ! m.alloc_array( this->strm.tmp, this->msg.len * 2 ) )
        return ERR_ALLOC_FAIL;
      for ( int i = 2; this->msg.get_arg( i, name, len ); i++ ) {
        RedisCmd cmd = get_redis_cmd( get_redis_cmd_hash( name, len ) );
        get_cmd_usage( cmd, tmp, sizeof( tmp ) );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ cmd ].name );
        m.array[ j++ ].set_simple_string( tmp );
      }
      if ( j == 0 ) {
        get_cmd_usage( COMMAND_CMD, tmp, sizeof( tmp ) );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ COMMAND_CMD ].name );
        m.array[ j++ ].set_simple_string( tmp );
      }
      m.len = j;
      break;
    }
    default:
      return ERR_BAD_ARGS;
  }
  if ( this->mstatus == REDIS_MSG_OK ) {
    size_t sz  = m.pack_size();
    void * buf = this->strm.alloc_temp( sz );
    if ( buf == NULL )
      return ERR_ALLOC_FAIL;
    this->strm.append_iov( buf, m.pack( buf ) );
  }
  if ( this->mstatus != REDIS_MSG_OK )
    return ERR_MSG_STATUS;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_config( void ) noexcept
{
  RedisMsg m;
  size_t   sz;
  void   * buf;
  switch ( this->msg.match_arg( 1, MARG( "get" ),
                                   MARG( "resetstat" ),
                                   MARG( "rewrite" ),
                                   MARG( "set" ), NULL ) ) {
    default: return ERR_BAD_ARGS;
    case 1: /* get */
      switch ( this->msg.match_arg( 2, MARG( "appendonly" ),
                                       MARG( "save" ), NULL ) ) {
        default: return ERR_BAD_ARGS;
        case 1:
          if ( ! m.alloc_array( this->strm.tmp, 2 ) )
            return ERR_ALLOC_FAIL;
          m.array[ 0 ].set_bulk_string( (char *) MARG( "appendonly" ) );
          m.array[ 1 ].set_bulk_string( (char *) MARG( "no" ) );
          break;
        case 2:
          if ( ! m.alloc_array( this->strm.tmp, 2 ) )
            return ERR_ALLOC_FAIL;
          m.array[ 0 ].set_bulk_string( (char *) MARG( "save" ) );
          m.array[ 1 ].set_bulk_string( (char *) MARG( "" ) );
          break;
      }
      sz  = m.pack_size();
      buf = this->strm.alloc_temp( sz );
      if ( buf == NULL )
        return ERR_ALLOC_FAIL;
      this->strm.append_iov( buf, m.pack( buf ) );
      return EXEC_OK;
    case 2: /* resetstat */
    case 3: /* rewrite */
    case 4: /* set */
      break;
  }
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_dbsize( void ) noexcept
{
  HashCounters tot;
  this->kctx.ht.get_db_stats( tot, this->kctx.db_num );
  this->send_int( (int64_t) ( tot.add - tot.drop ) );
  return EXEC_OK;
}

static char *
copy_fl( char *buf,  const char *s )
{
  while ( ( *buf = *s ) != '\0' )
    buf++, s++;
  return buf;
}

static char *
flags_string( uint16_t fl,  uint8_t type,  char *buf )
{
  char *s = buf;
  /**buf++ = (char) ( fl & FL_ALIGNMENT ) + '0';*/
  buf = copy_fl( buf, md_type_str( (MDType) type ) );
  if ( ( fl & FL_SEQNO ) != 0 )
    buf = copy_fl( buf, "-Sno" );
  if ( ( fl & FL_MSG_LIST ) != 0 )
    buf = copy_fl( buf, "-Mls" );
  if ( ( fl & FL_SEGMENT_VALUE ) != 0 )
    buf = copy_fl( buf, "-Seg" );
  if ( ( fl & FL_UPDATED ) != 0 )
    buf = copy_fl( buf, "-Upd" );
  if ( ( fl & FL_IMMEDIATE_VALUE ) != 0 )
    buf = copy_fl( buf, "-Ival" );
  if ( ( fl & FL_IMMEDIATE_KEY ) != 0 )
    buf = copy_fl( buf, "-Key" );
  if ( ( fl & FL_PART_KEY ) != 0 )
    buf = copy_fl( buf, "-Part" );
  if ( ( fl & FL_DROPPED ) != 0 )
    buf = copy_fl( buf, "-Drop" );
  if ( ( fl & FL_EXPIRE_STAMP ) != 0 )
    buf = copy_fl( buf, "-Exp" );
  if ( ( fl & FL_UPDATE_STAMP ) != 0 )
    buf = copy_fl( buf, "-Stmp" );
  if ( ( fl & FL_MOVED ) != 0 )
    buf = copy_fl( buf, "-Mved" );
  if ( ( fl & FL_BUSY ) != 0 )
    buf = copy_fl( buf, "-Busy" );
  *buf = '\0';
  return s;
}

ExecStatus
RedisExec::debug_object( void ) noexcept
{
  const char * key;
  size_t       keylen;
  char         buf[ 1024 ],
             * b  = buf,
             * s;
  size_t       n  = sizeof( buf ),
               len;
  uint64_t     natural_pos,
               pos_off = 0;

  if ( ! this->msg.get_arg( 2, key, keylen ) )
    return ERR_BAD_ARGS;
  void *p = this->strm.alloc_temp( EvKeyCtx::size( keylen ) );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  EvKeyCtx * ctx = new ( p ) EvKeyCtx( this->kctx.ht, NULL, key, keylen, 0,
                                       0, this->hs );
  this->exec_key_set( *ctx );
  ctx->kstatus = this->kctx.find( &this->wrk );

  if ( keylen > 32 ) {
    b = this->strm.alloc_temp( keylen + 1024 );
    n = keylen + 1024;
  }
  s = b;
  len = n;

  xnprintf( b, n, "key:         \"%.*s\"\r\n", (int) keylen, key );
  xnprintf( b, n, "hash:        %08lx:%08lx\r\n",
                  ctx->hash1, ctx->hash2 );
  this->kctx.get_pos_info( natural_pos, pos_off );
  xnprintf( b, n, "pos:         [%lu]+%u.%lu\r\n",
       this->kctx.pos, this->kctx.inc, pos_off );

  if ( ctx->kstatus != KEY_OK ) {
    xnprintf( b, n, "status:      %d/%s %s\r\n",
                    ctx->kstatus, kv_key_status_string( ctx->kstatus ),
                    kv_key_status_description( ctx->kstatus ) );
  }
  else {
    uint64_t sno, exp_ns, upd_ns, cur = 0;
    char buf[ 128 ];
    if ( this->kctx.get_stamps( exp_ns, upd_ns ) == KEY_OK ) {
      if ( upd_ns != 0 ) {
        if ( cur == 0 )
          cur = current_realtime_ns();
        xnprintf( b, n, "update_time: %s\r\n",
                sprintf_rela_time( upd_ns, cur, NULL, buf, sizeof( buf ) ) );
      }
      if ( exp_ns != 0 ) {
        if ( cur == 0 )
          cur = current_realtime_ns();
        xnprintf( b, n, "expire_time: %s\r\n",
                sprintf_rela_time( exp_ns, cur, NULL, buf, sizeof( buf ) ) );
      }
    }

    xnprintf( b, n, "flags:       %s\r\n",
         flags_string( this->kctx.entry->flags, this->kctx.get_type(), buf ) );

    if ( this->kctx.entry->test( FL_SEQNO ) )
      sno = this->kctx.entry->seqno( this->kctx.hash_entry_size );
    else
      sno = this->kctx.serial - ( this->kctx.key & ValueCtr::SERIAL_MASK );

    xnprintf( b, n, "db:          %u\r\n"
                    "val:         %u\r\n"
                    "seqno:       %lu\r\n",
                    this->kctx.get_db(), this->kctx.get_val(), sno );

    if ( this->kctx.entry->test( FL_SEGMENT_VALUE ) ) {
      ValueGeom geom;
      this->kctx.entry->get_value_geom( this->kctx.hash_entry_size, geom,
                                        this->kctx.ht.hdr.seg_align_shift );
      xnprintf( b, n, "segment:     %u\r\n"
                      "size:        %lu\r\n"
                      "offset:      %lu\r\n",
                      geom.segment, geom.size, geom.offset );
    }
    else if ( this->kctx.entry->test( FL_IMMEDIATE_VALUE ) ) {
      uint64_t sz = 0;
      this->kctx.get_size( sz );
      xnprintf( b, n, "size:        %lu\r\n", sz );
    }
  }
  this->strm.sz += this->send_string( s, len - n );

  return EXEC_OK;
}

ExecStatus
RedisExec::debug_htstats( void ) noexcept
{
  char         buf[ 1024 ],
             * b  = buf,
             * s;
  size_t       n  = sizeof( buf ),
               len;
  s = b;
  len = n;

  HashCounters tot, & sta = this->kctx.stat;
  this->kctx.ht.get_db_stats( tot, this->kctx.db_num );
  xnprintf( b, n, "db_num:  %u\r\n", this->kctx.db_num );
  xnprintf( b, n, "\r\n-= totals =-\r\n" );
  xnprintf( b, n, "read:    %lu\r\n", tot.rd );
  xnprintf( b, n, "write:   %lu\r\n", tot.wr );
  xnprintf( b, n, "spins:   %lu\r\n", tot.spins );
  xnprintf( b, n, "chains:  %lu\r\n", tot.chains );
  xnprintf( b, n, "add:     %lu\r\n", tot.add );
  xnprintf( b, n, "drop:    %lu\r\n", tot.drop );
  xnprintf( b, n, "expire:  %lu\r\n", tot.expire );
  xnprintf( b, n, "htevict: %lu\r\n", tot.htevict );
  xnprintf( b, n, "afail:   %lu\r\n", tot.afail );
  xnprintf( b, n, "hit:     %lu\r\n", tot.hit );
  xnprintf( b, n, "miss:    %lu\r\n", tot.miss );
  xnprintf( b, n, "cuckacq: %lu\r\n", tot.cuckacq );
  xnprintf( b, n, "cuckfet: %lu\r\n", tot.cuckfet );
  xnprintf( b, n, "cuckmov: %lu\r\n", tot.cuckmov );
  xnprintf( b, n, "cuckret: %lu\r\n", tot.cuckret );
  xnprintf( b, n, "cuckmax: %lu\r\n", tot.cuckmax );
  xnprintf( b, n, "\r\n-= self =-\r\n" );
  xnprintf( b, n, "read:    %lu\r\n", sta.rd );
  xnprintf( b, n, "write:   %lu\r\n", sta.wr );
  xnprintf( b, n, "spins:   %lu\r\n", sta.spins );
  xnprintf( b, n, "chains:  %lu\r\n", sta.chains );
  xnprintf( b, n, "add:     %lu\r\n", sta.add );
  xnprintf( b, n, "drop:    %lu\r\n", sta.drop );
  xnprintf( b, n, "expire:  %lu\r\n", sta.expire );
  xnprintf( b, n, "htevict: %lu\r\n", sta.htevict );
  xnprintf( b, n, "afail:   %lu\r\n", sta.afail );
  xnprintf( b, n, "hit:     %lu\r\n", sta.hit );
  xnprintf( b, n, "miss:    %lu\r\n", sta.miss );
  xnprintf( b, n, "cuckacq: %lu\r\n", sta.cuckacq );
  xnprintf( b, n, "cuckfet: %lu\r\n", sta.cuckfet );
  xnprintf( b, n, "cuckmov: %lu\r\n", sta.cuckmov );
  xnprintf( b, n, "cuckret: %lu\r\n", sta.cuckret );
  xnprintf( b, n, "cuckmax: %lu\r\n", sta.cuckmax );

  this->strm.sz += this->send_string( s, len - n );

  return EXEC_OK;
}

ExecStatus
RedisExec::exec_debug( void ) noexcept
{
  switch ( this->msg.match_arg( 1, MARG( "segfault" ),
                              /*2*/MARG( "panic" ),
                              /*3*/MARG( "restart" ),
                              /*4*/MARG( "crash-and-recovery" ),
                              /*5*/MARG( "assert" ),
                              /*6*/MARG( "reload" ),
                              /*7*/MARG( "loadaof" ),
                              /*8*/MARG( "object" ),
                              /*9*/MARG( "sdslen" ),
                             /*10*/MARG( "ziplist" ),
                             /*11*/MARG( "populate" ),
                             /*12*/MARG( "digest" ),
                             /*13*/MARG( "sleep" ),
                             /*14*/MARG( "set-active-expire" ),
                             /*15*/MARG( "lua-always-replicate-commands" ),
                             /*16*/MARG( "error" ),
                             /*17*/MARG( "structsize" ),
                             /*18*/MARG( "htstats" ),
                             /*19*/MARG( "change-repl-id" ),
                                   NULL ) ) {
    default: return EXEC_DEBUG;
    case 1: /* segfault -- Crash the server with sigsegv */
    case 2: /* panic -- Crash the server simulating a panic */
    case 3: /* restart  -- Graceful restart: save config, db, restart */
    case 4: /* crash-and-recovery <ms> -- Hard crash and restart after delay */
    case 5: /* assert   -- Crash by assertion failed */
    case 6: /* reload   -- Save the RDB on disk and reload it back in memory */
    case 7: /* loadaof  -- Flush the AOF buf on disk and reload AOF in memory */
      return ERR_BAD_CMD;
    case 8: /* object <key> -- Show low level info about key and value */
      return this->debug_object();
    case 9: /* sdslen <key> -- Show low level SDS string info key and value */
    case 10: /* ziplist <key> -- Show low level info about ziplist encoding */
    case 11: /* populate <count> [prefix] [size] -- Create <count> string keys
                named key:<num>. If a prefix is specified is used instead of 
                the \'key\' prefix. */
    case 12: /* digest   -- Outputs an hex signature of the current DB */
    case 13: /* sleep <seconds> -- Stop the server for <seconds> */
    case 14: /* set-active-expire (0|1) -- Setting it to 0 disables expiring
                keys in background when they are not accessed (otherwise the
                Redis behavior). Setting it to 1 reenables back the default */
    case 15: /* lua-always-replicate-commands (0|1) -- Setting it to 1 makes
                Lua replication defaulting to replicating single commands,
                without the script having to enable effects replication */
    case 16: /* error <string> -- Return a Redis protocol error with <string>
                as message */
    case 17: /* structsize -- Return the size of Redis core C structures */
      return ERR_BAD_CMD;
    case 18: /* htstats <dbid> -- Return hash table statistics of the
                specified Redis database. */
      return this->debug_htstats();
    case 19: /* change-repl-id -- Change the replication IDs of the instance;
                Dangerous, should be used only for testing the replication
                subsystem */
      return ERR_BAD_CMD;
  }
}

ExecStatus
RedisExec::exec_flushall( void ) noexcept
{
  /* delete all keys */
  HashCounters tot;
  for ( uint8_t db_num = 0; db_num < 254; db_num++ ) {
    if ( this->kctx.ht.hdr.test_db_opened( db_num ) ) {
      this->kctx.ht.get_db_stats( tot, db_num );
      if ( tot.add > tot.drop )
        this->flushdb( db_num );
    }
  }
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_flushdb( void ) noexcept
{
  this->flushdb( this->kctx.db_num );
  return EXEC_SEND_OK;
}

void
RedisExec::flushdb( uint8_t db_num ) noexcept
{
  KeyCtx    scan_kctx( this->kctx );
  uint64_t  ht_size = this->kctx.ht.hdr.ht_size;
  uint32_t  ctx_id = scan_kctx.ctx_id,
            dbx_id = scan_kctx.ht.attach_db( ctx_id, db_num );
  KeyStatus status;
  
  if ( dbx_id == MAX_STAT_ID )
    return;

  scan_kctx.set_db( dbx_id );
  for ( uint64_t pos = 0; pos < ht_size; pos++ ) {
    status = scan_kctx.fetch( &this->wrk, pos );
    if ( status != KEY_OK ) /* could be NOT_FOUND */
      continue;
    if ( scan_kctx.entry->test( FL_DROPPED ) != 0 ||
         scan_kctx.get_db() != db_num )
      continue;
    for (;;) {
      status = scan_kctx.try_acquire_position( pos );
      if ( status != KEY_BUSY )
        break;
    }
    if ( status <= KEY_IS_NEW ) { /* ok or is new */
      if ( scan_kctx.entry->test( FL_DROPPED ) == 0 &&
           scan_kctx.get_db() == db_num ) {
        scan_kctx.tombstone();
      }
      scan_kctx.release();
    }
  }
}

static int
get_proc_status_size( const char *s,  size_t *ival,  ... )
{
  /* find a set of strings in status and parse the ints */
  int fd = ::open( "/proc/self/status", O_RDONLY );
  if ( fd < 0 )
    return 0;
  char buf[ 4096 ];
  ssize_t n = ::read( fd, buf, sizeof( buf ) - 1 );
  ::close( fd );
  if ( n < 0 )
    return 0;
  buf[ n ] = '\0';
  char * b = buf,
       * p;
  int    i, j, cnt = 0;

  va_list args;
  va_start( args, ival );
  for (;;) {
    /* find the string: VmPeak: */
    if ( (p = (char *) ::memchr( b, s[ 0 ], n )) == NULL )
      break;
    n -= ( p - b );
    b = p;
    for ( i = 1; s[ i ] != '\0'; i++ )
      if ( b[ i ] != s[ i ] )
        break;
    /* if string matches */
    if ( s[ i ] == '\0' && b[ i ] == '\t' ) {
      while ( b[ ++i ] == ' ' )
        ;
      if ( b[ i ] >= '0' && b[ i ] <= '9' ) {
        for ( j = i + 1; b[ j ] >= '0' && b[ j ] <= '9'; j++ )
          ;
        /* parse the integer after the string */
        string_to_uint( &b[ i ], j - i, *ival );
        i = j;
        cnt++;
        /* match next string */
        s = va_arg( args, const char * );
        if ( s == NULL )
          break;
        ival = va_arg( args, size_t * );
      }
    }
    n -= i;
    b = &b[ i ];
  }
  va_end( args );
  return cnt;
}

static char *
mstring( double f,  char *buf,  int64_t k )
{
  return mem_to_string( (int64_t) ceil( f ), buf, k );
}

ExecStatus
RedisExec::exec_info( void ) noexcept
{
  enum { INFO_SERVER = 1, INFO_CLIENTS = 2, INFO_MEMORY = 4, INFO_PERSIST = 8,
         INFO_STATS  = 16, INFO_REPLIC = 32, INFO_CPU = 64, INFO_CMDSTATS = 128,
         INFO_CLUSTER = 256, INFO_KEYSPACE = 512 };
  PeerMatchArgs ka;
  PeerMatchIter iter( this->peer, ka );
  PeerStats     stats;
  PeerData    * p;
  size_t        len = 1024 * 8;
  char        * buf = (char *) this->strm.tmp.alloc( len ),
              * b   = &buf[ 32 ];
  size_t        sz  = len - 32;
  HashTab     & map = this->kctx.ht;
  char          tmp[ 64 ];
  int           info;

  if ( buf == NULL )
    return ERR_ALLOC_FAIL;

  switch ( this->msg.match_arg( 1, MARG( "server" ),
                                   MARG( "clients" ),
                                   MARG( "memory" ),
                                   MARG( "persistence" ),
                                   MARG( "stats" ),
                                   MARG( "replication" ),
                                   MARG( "cpu" ),
                                   MARG( "commandstats" ),
                                   MARG( "cluster" ),
                                   MARG( "keyspace" ), NULL ) ) {
    default: info = 0xffff;        break; /* all */
    case 1:  info = INFO_SERVER;   break; /* server */
    case 2:  info = INFO_CLIENTS;  break; /* clients */
    case 3:  info = INFO_MEMORY;   break; /* memory */
    case 4:  info = INFO_PERSIST;  break; /* persistence  XXX <- no output */
    case 5:  info = INFO_STATS;    break; /* stats */
    case 6:  info = INFO_REPLIC;   break; /* replication  XXX */
    case 7:  info = INFO_CPU;      break; /* cpu */
    case 8:  info = INFO_CMDSTATS; break; /* commandstats XXX */
    case 9:  info = INFO_CLUSTER;  break; /* cluster      XXX */
    case 10: info = INFO_KEYSPACE; break; /* keyspace     XXX */
  }

  if ( ( info & INFO_SERVER ) != 0 ) {
    static utsname name;
    if ( name.sysname[ 0 ] == '\0' )
      uname( &name );

    xnprintf( b, sz, "raids_version:        %s\r\n", kv_stringify( DS_VER ) );
    xnprintf( b, sz, "raids_git:            %s\r\n", kv_stringify( GIT_HEAD ) );
    xnprintf( b, sz, "gcc_version:          %d.%d.%d\r\n",
      __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__ );
    xnprintf( b, sz, "process_id:           %d\r\n", ::getpid() );
    xnprintf( b, sz, "os:                   %s %s %s\r\n",
      name.sysname, name.release, name.machine );

    char    path[ 256 ];
    ssize_t lsz = ::readlink( "/proc/self/exe", path, sizeof( path ) );
    if ( lsz > 0 )
      xnprintf( b, sz, "executable:           %.*s\r\n", (int) lsz, path );

    /* the ports open */
    ka.set_type( MARG( "listen" ) );
    for ( p = iter.first(); p != NULL; p = iter.next() ) {
      xnprintf( b, sz, "%s: %*s%s\r\n", p->kind,
                20 - (int) ::strlen( p->kind ), "", p->peer_address );
    }
  }

  if ( ( info & INFO_CLIENTS ) != 0 ) {
    ka.set_type( MARG( "redis" ) );
    xnprintf( b, sz, "redis_clients:        %lu\r\n", iter.length() );
    ka.set_type( MARG( "pubsub" ) );
    xnprintf( b, sz, "pubsub_clients:       %lu\r\n", iter.length() );
  }

  if ( ( info & INFO_SERVER ) != 0 ) {
    print_map_geom( &map, this->kctx.ctx_id, b, sz );
    size_t x = ::strlen( b );
    b   = &b[ x ];
    sz -= x;
  }
  if ( ( info & INFO_STATS ) != 0 ) {
    HashTabStats *hts = HashTabStats::create( map );
    if ( hts != NULL ) {
      hts->fetch();
      HashCounters & ops  = hts->hops,
                   & tot  = hts->htot;
      MemCounters  & chg  = hts->mops;

      double op, ch;
      if ( ops.rd + ops.wr == 0 ) {
        op = 0;
        ch = 0;
      }
      else {
        op = (double) ( ops.rd + ops.wr );
        ch = 1.0 + ( (double) ops.chains / (double) ( ops.rd + ops.wr ) );
      }
      xnprintf( b, sz, "ht_operations:        %s\r\n",
                mstring( op, tmp, 1000 ) );
      xnprintf( b, sz, "ht_chains:            %.1f\r\n", ch );
      xnprintf( b, sz, "ht_read:              %s\r\n",
                mstring( (double) ops.rd, tmp, 1000 ) );
      xnprintf( b, sz, "ht_write:             %s\r\n",
                mstring( (double) ops.wr, tmp, 1000 ) );
      xnprintf( b, sz, "ht_spins:             %s\r\n",
                mstring( (double) ops.spins, tmp, 1000 ) );
      xnprintf( b, sz, "ht_map_load:          %u\r\n",
                (uint32_t) ( map.hdr.ht_load * 100.0 + 0.5 ) );
      xnprintf( b, sz, "ht_value_load:        %u\r\n",
                (uint32_t) ( map.hdr.value_load * 100.0 + 0.5 ) );
      xnprintf( b, sz, "ht_entries:           %s\r\n",
                mstring( tot.add - tot.drop, tmp, 1000 ) );
      xnprintf( b, sz, "ht_gc:                %s\r\n",
                mstring( (double) chg.move_msgs, tmp, 1000 ) );
      xnprintf( b, sz, "ht_drop:              %s\r\n",
                mstring( (double) ops.drop, tmp, 1000 ) );
      xnprintf( b, sz, "ht_hit:               %s\r\n",
                mstring( (double) ops.hit, tmp, 1000 ) );
      xnprintf( b, sz, "ht_miss:              %s\r\n",
                mstring( (double) ops.miss, tmp, 1000 ) );

      delete hts;
    }
  }
  if ( ( info & INFO_CPU ) != 0 ) {
    struct rusage usage;
    if ( ::getrusage( RUSAGE_SELF, &usage ) == 0 ) {
      xnprintf( b, sz, "used_cpu_sys:         %lu.%06lu\r\n", 
        usage.ru_stime.tv_sec, usage.ru_stime.tv_usec );
      xnprintf( b, sz, "used_cpu_user:        %lu.%06lu\r\n", 
        usage.ru_utime.tv_sec, usage.ru_utime.tv_usec );

      uint64_t sec  = usage.ru_stime.tv_sec + usage.ru_utime.tv_sec,
               usec = usage.ru_stime.tv_usec + usage.ru_utime.tv_usec;
      if ( usec >= 1000000 ) {
        sec++;
        usec -= 1000000;
      }
      xnprintf( b, sz, "used_cpu_total:       %lu.%06lu\r\n", sec, usec );
      xnprintf( b, sz, "minor_page_fault:     %lu\r\n", usage.ru_minflt );
      xnprintf( b, sz, "major_page_fault:     %lu\r\n", usage.ru_majflt );
      xnprintf( b, sz, "voluntary_cswitch:    %lu\r\n", usage.ru_nvcsw );
      xnprintf( b, sz, "involuntary_cswitch:  %lu\r\n", usage.ru_nivcsw );
    }
  }
  if ( ( info & INFO_MEMORY ) != 0 ) {
    size_t vm_peak, vm_size, vm_hwm, vm_rss, vm_pte;
    if ( get_proc_status_size( "VmPeak:", &vm_peak,
                               "VmSize:", &vm_size,
                               "VmHWM:", &vm_hwm,
                               "VmRSS:", &vm_rss,
                               "VmPTE:", &vm_pte, NULL ) == 5 ) {
      vm_peak *= 1024;
      xnprintf( b, sz, "vm_peak:              %s\r\n",
                mstring( vm_peak, tmp, 1024 ) );
      vm_size *= 1024;
      xnprintf( b, sz, "vm_size:              %s\r\n",
                mstring( vm_size, tmp, 1024 ) );
      vm_hwm *= 1024;
      xnprintf( b, sz, "vm_hwm:               %s\r\n",
                mstring( vm_hwm, tmp, 1024 ) );
      vm_rss *= 1024;
      xnprintf( b, sz, "vm_rss:               %s\r\n",
                mstring( vm_rss, tmp, 1024 ) );
      vm_pte *= 1024;
      xnprintf( b, sz, "vm_pte:               %s\r\n",
                mstring( vm_pte, tmp, 1024 ) );
    }
  }
  if ( ( info & INFO_STATS ) != 0 ) {
    ka.set_type( NULL, 0 );
    this->peer.op.retired_stats( this->peer, stats );
    for ( p = iter.first(); p != NULL; p = iter.next() )
      p->op.client_stats( *p, stats );

    xnprintf( b, sz, "net_bytes_recv:       %s\r\n",
              mstring( stats.bytes_recv, tmp, 1024 ) );
    xnprintf( b, sz, "net_bytes_sent:       %s\r\n",
              mstring( stats.bytes_sent, tmp, 1024 ) );
    xnprintf( b, sz, "net_msgs_recv:        %s\r\n",
              mstring( stats.msgs_recv, tmp, 1024 ) );
    xnprintf( b, sz, "net_msgs_sent:        %s\r\n",
              mstring( stats.msgs_sent, tmp, 1024 ) );
    xnprintf( b, sz, "net_accept_count:     %s\r\n",
              mstring( stats.accept_cnt, tmp, 1024 ) );
  }
  size_t n   = len - 32 - sz,
         dig = uint_digits( n ),
         off = 32 - ( dig + 3 );

  buf[ off ] = '$';
  uint_to_str( n, &buf[ off + 1 ], dig );
  crlf( buf, off + 1 + dig );
  crlf( b, 0 );
  this->strm.append_iov( &buf[ off ], n + dig + 3 + 2 );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_lastsave( void ) noexcept
{
  this->send_int( this->kctx.ht.hdr.create_stamp / 1000000000 );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_memory( void ) noexcept
{
  switch ( this->msg.match_arg( 1, MARG( "doctor" ),
                                   MARG( "help" ),
                                   MARG( "malloc-stats" ),
                                   MARG( "purge" ),
                                   MARG( "stats" ),
                                   MARG( "usage" ), NULL ) ) {
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
RedisExec::exec_monitor( void ) noexcept
{
  /* monitor commands:
   * 1339518083.107412 [0 127.0.0.1:60866] "keys" "*"
   * 1339518087.877697 [0 127.0.0.1:60866] "dbsize" */
  if ( ( this->cmd_state & CMD_STATE_MONITOR ) == 0 ) {
    this->cmd_state |= CMD_STATE_MONITOR;
    return this->do_psubscribe( "__monitor_@*", 12 );
  }
  this->cmd_state &= ~CMD_STATE_MONITOR;
  return this->do_punsubscribe( "__monitor_@*", 12 );
}

ExecStatus
RedisExec::exec_role( void ) noexcept
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
RedisExec::exec_save( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_shutdown( void ) noexcept
{
  return EXEC_QUIT;
}

ExecStatus
RedisExec::exec_slaveof( void ) noexcept
{
  this->send_ok();
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_slowlog( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sync( void ) noexcept
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_time( void ) noexcept
{
  RedisMsg m;
  char     sb[ 32 ], ub[ 32 ];
  uint64_t x    = this->kctx.ht.hdr.current_stamp,
           sec  = x / 1000000000,
           usec = ( x % 1000000000 ) / 1000;

  if ( m.string_array( this->strm.tmp, 2,
                  uint_to_str( sec, sb ), sb,
                  uint_to_str( usec, ub ), ub ) ) {
    this->send_msg( m );
    return EXEC_OK;
  }
  return ERR_ALLOC_FAIL;
}

