#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>
#include <raids/route_db.h>

using namespace rai;
using namespace ds;
using namespace kv;

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
  this->send_msg( this->msg.array[ 1 ] );
  return EXEC_OK;
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

bool
RedisExec::get_peer_match_args( PeerMatchArgs &ka )
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

ExecStatus
RedisExec::exec_client( void )
{
  char        * s;
  const char  * nm;
  PeerData    * p;
  size_t        d, i, off, sz;
  PeerMatchArgs ka;

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
    case 3: /* kill (ip) (ID id) (TYPE norm|mast|slav|pubsub)
                    (ADDR ip) (SKIPME y/n) */
      if ( ! this->get_peer_match_args( ka ) )
        return ERR_BAD_ARGS;
      i = 0;
      for ( p = &this->peer; p->back != NULL; p = p->back )
        ;
      for ( ; p != NULL; p = p->next ) {
        if ( ka.skipme && p == &this->peer )
          continue;
        if ( p->op.client_kill( *p, ka ) )
          i++;
      }
      this->send_int( i ); /* number of clients killed */
      return EXEC_OK;
    case 4: /* list (ip) (ID id) (TYPE norm|mast|slav|pubsub)
                    (ADDR ip) (SKIPME y/n) */
      if ( this->argc > 2 && ! this->get_peer_match_args( ka ) )
        return ERR_BAD_ARGS;
      if ( this->strm.sz > 0 )
        this->strm.flush();
      off = this->strm.pending(); /* record location at start of list */
      i   = this->strm.idx;
      for ( p = &this->peer; p->back != NULL; p = p->back )
        ;
      /* this iterates all the fds in the system and calls the client_list
       * virtual function, which is different based on the protocol type,
       * a redis protocol type will use the client_list() defined below */
      /* XXX needs to hook into pubsub for multi-process listing */
      for ( ; p != NULL; p = p->next ) {
        if ( ka.skipme && p == &this->peer )
          continue;
        char buf[ 8 * 1024 ];
        int  sz = p->op.client_list( *p, ka, buf, sizeof( buf ) );
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
RedisExec::client_list( char *buf,  size_t buflen )
{
  /* id=unique id, addr=peer addr, fd=sock, age=time connected
   * idle=time idle, flags=what, db=cur db, sub=channel subs,
   * psub=pattern subs, multi=cmds qbuf=query buf size, qbuf-free=free
   * qbuf, obl=output buf len, oll=outut list len, omem=output mem usage,
   * events=sock rd/wr, cmd=last cmd issued */
  char cbuf[ 64 ], flags[ 8 ];
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
    cmd_tolower( this->cmd, cbuf ) );
}

ExecStatus
RedisExec::exec_command( void )
{
  RedisMsg     m;
  const char * name;
  size_t       j = 0, len;
  RedisCmd     cmd;

  this->mstatus = REDIS_MSG_OK;
  switch ( this->msg.match_arg( 1, MARG( "info" ),
                                   MARG( "getkeys" ),
                                   MARG( "count" ),
                                   MARG( "help" ), NULL ) ) {
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
    case 2: /* getkeys cmd ... show the keys of cmd */
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
RedisExec::exec_config( void )
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
    size_t dig = uint_digits( n ),
           off = 32 - ( dig + 3 );

    buf[ off ] = '$';
    uint_to_str( n, &buf[ off + 1 ], dig );
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
RedisExec::exec_monitor( void )
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
                  uint_to_str( tv.tv_sec, sb ), sb,
                  uint_to_str( tv.tv_usec, ub ), ub ) ) {
    this->send_msg( m );
    return EXEC_OK;
  }
  return ERR_ALLOC_FAIL;
}

