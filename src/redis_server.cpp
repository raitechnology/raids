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

PeerData *
PeerMatchIter::first( void )
{
  /* go to the front */
  for ( this->p = &this->me; this->p->back != NULL; this->p = this->p->back )
    ;
  return this->next(); /* match the next */
}

PeerData *
PeerMatchIter::next( void )
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
RedisExec::exec_client( void )
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

static int xnprintf( char *&b,  size_t &sz,  const char *format, ... )
  __attribute__((format(printf,3,4)));

static int
xnprintf( char *&b,  size_t &sz,  const char *format, ... )
{
#ifndef va_copy
#define va_copy( dst, src ) memcpy( &( dst ), &( src ), sizeof( va_list ) )
#endif
  va_list args;
  size_t  x;
  va_start( args, format );
  x = vsnprintf( b, sz, format, args );
  va_end( args );
  b   = &b[ x ];
  sz -= x;
  return x;
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

char *
mstring( double f,  char *buf,  int64_t k )
{
  return mem_to_string( (int64_t) ceil( f ), buf, k );
}

ExecStatus
RedisExec::exec_info( void )
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
    print_map_geom( &map, this->kctx.thr_ctx.ctx_id, b, sz );
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
      xnprintf( b, sz, "ht_entries            %s\r\n",
                mstring( tot.add - tot.drop, tmp, 1000 ) );
      xnprintf( b, sz, "ht_gc                 %s\r\n",
                mstring( (double) chg.move_msgs, tmp, 1000 ) );
      xnprintf( b, sz, "ht_drop               %s\r\n",
                mstring( (double) ops.drop, tmp, 1000 ) );
      xnprintf( b, sz, "ht_hit                %s\r\n",
                mstring( (double) ops.hit, tmp, 1000 ) );
      xnprintf( b, sz, "ht_miss               %s\r\n",
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

