#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/redis_exec.h>
#include <raids/redis_keyspace.h>
#include <raikv/ev_publish.h>
#include <raikv/ev_net.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

/* alloc temporary space for subject */
inline bool
RedisKeyspace::alloc_subj( size_t subj_len ) noexcept
{
  if ( this->alloc_len < subj_len ) {
    size_t len = 20 + this->exec.prefix_len + this->keylen + this->evtlen;
    char * tmp = this->exec.strm.alloc_temp( len + 1 );
    if ( tmp == NULL )
      return false;
    this->subj = tmp;
    if ( this->exec.prefix_len > 0 )
      ::memcpy( tmp, this->exec.prefix, this->exec.prefix_len );
    this->ptr = &tmp[ this->exec.prefix_len ];
    this->alloc_len = len;
  }
  return true;
}

inline size_t
RedisKeyspace::db_str( size_t off ) noexcept
{
  size_t i = 0;
  if ( this->db[ 0 ] == 0 ) {
    uint8_t x = this->exec.kctx.db_num;
    if ( x < 10 ) {
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + x;
    }
    else if ( x < 100 ) {
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + ( x / 10 );
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + ( x % 10 );
    }
    else {
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + ( x / 100 );
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + ( ( x / 10 ) % 10 );
      this->ptr[ off++ ] = this->db[ i++ ] = '0' + ( x % 10 );
    }
    this->db[ i ] = 0;
  }
  else {
    for ( ; this->db[ i ] != 0; i++ )
      this->ptr[ off++ ] = this->db[ i ];
  }
  return off;
}
/* append "@db__:" to subject */
inline size_t
RedisKeyspace::db_to_subj( size_t off ) noexcept
{
  this->ptr[ off++ ] = '@';
  off = this->db_str( off );
  this->ptr[ off ] = '_';
  this->ptr[ off + 1 ] = '_';
  this->ptr[ off + 2 ] = ':';
  return off + 3;
}

size_t
RedisKeyspace::make_bsubj( const char *blk ) noexcept
{
  size_t subj_len = 20 + this->keylen;
  if ( ! this->alloc_subj( subj_len ) )
    return 0;
  ::memcpy( this->ptr, blk, 10 );
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->ptr[ subj_len ], this->key, this->keylen );
  subj_len += this->keylen;
  this->ptr[ subj_len ] = '\0';
  return this->exec.prefix_len + subj_len;
}
/* blk = blocking subject (listblkd, zsetblkd, strmblkd), or keyspace */
bool
RedisKeyspace::fwd_bsubj( const char *blk ) noexcept
{
  size_t subj_len = this->make_bsubj( blk );
  uint32_t rcount;
  bool b;
  if ( subj_len == 0 )
    return false;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->evt, this->evtlen,
                 this->exec.sub_route, this->exec.sub_id,
                 kv_crc_c( this->subj, subj_len, 0 ), MD_STRING, ':' );
  /*printf( "%s <- %s\n", this->subj, this->evt );*/
  b = this->exec.sub_route.forward_with_cnt( pub, rcount );
  this->exec.msg_route_cnt += rcount;
  return b;
}
/* publish __keyevent@N__:event <- key */
bool
RedisKeyspace::fwd_keyevent( void ) noexcept
{
  size_t subj_len = 20 + this->evtlen;
  uint32_t rcount;
  bool b;
  if ( ! this->alloc_subj( subj_len ) )
    return false;
  ::memcpy( this->ptr, "__keyevent", 10 );
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->ptr[ subj_len ], this->evt, this->evtlen );
  subj_len += this->evtlen;
  this->ptr[ subj_len ] = '\0';
  subj_len += this->exec.prefix_len;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->key, this->keylen,
                 this->exec.sub_route, this->exec.sub_id,
                 kv_crc_c( this->subj, subj_len, 0 ), MD_STRING, ';' );
  /*printf( "%s <- %s\n", this->subj, this->key );*/
  b = this->exec.sub_route.forward_with_cnt( pub, rcount );
  this->exec.msg_route_cnt += rcount;
  return b;
}
/* publish __monitor_@N__:peer_address <- [ msg, result, time ] */
bool
RedisKeyspace::fwd_monitor( void ) noexcept
{
  size_t addr_len = this->exec.peer.get_peer_address_strlen();
  if ( ! this->alloc_subj( 20 + addr_len ) )
    return false;
  ::memcpy( this->ptr, "__monitor_", 10 );

  char  * timestamp,
        * result;
  StreamBuf & strm  = this->exec.strm;
  size_t  subj_len  = this->db_to_subj( 10 ),
          pack_sz   = this->exec.msg.pack_size(),
          start     = this->exec.strm_start,
          result_sz = strm.pending() - start,
          msg_sz    = /* *3 \r\n */
                       4 +
                      pack_sz +
                      ( result_sz != 0 ? result_sz : 5 ) +
                      /* $17 \r\n */
                       3  + 2 +
                      /* 1578737366.890420 \r\n*/
                       17 + 2;
  char  * msg = strm.alloc_temp( msg_sz );
  uint32_t rcount;
  bool b;

  if ( msg == NULL )
    return false;
  if ( addr_len > 0 ) {
    ::memcpy( &this->ptr[ subj_len ], this->exec.peer.peer_address.buf,
              addr_len );
    subj_len += addr_len;
    this->ptr[ subj_len ] = '\0';
  }
  ::memcpy( msg, "*3\r\n", 4 );
  this->exec.msg.pack( &msg[ 4 ] );

  result = &msg[ 4 + pack_sz ];
  if ( result_sz == 0 ) {
    ::memcpy( result, "*-1\r\n", 5 ); /* null */
    result_sz = 5;
  }
  else {
    if ( strm.sz > 0 )
      strm.flush();
    size_t off = 0;
    for ( size_t i = 0; i < strm.idx; i++ ) {
      char * base = (char *) strm.iov[ i ].iov_base;
      size_t len  = strm.iov[ i ].iov_len;
      if ( off >= start )
        ::memcpy( &result[ off - start ], base, len );
      else if ( off + len > start ) {
        size_t j = ( off + len ) - start;
        ::memcpy( &result[ 0 ], &base[ i ], len - j );
      }
      off += len;
    }
  }

  timestamp = &msg[ 4 + pack_sz + result_sz ];
  ::memcpy( timestamp, "$17\r\n", 5 );
  uint64_t us = current_realtime_us();
  uint64_to_string( us / 1000000, &timestamp[ 5 ], 10 ); /* in year 2288 == 11 */
  us = ( us % 1000000 ) + 1000000; /* make all usec digits show */
  uint64_to_string( us, &timestamp[ 15 ], 7 );
  timestamp[ 15 ] = '.';
  crlf( timestamp, 17 + 5 );
  subj_len += this->exec.prefix_len;

  EvPublish pub( this->subj, subj_len, NULL, 0, msg, msg_sz,
                 this->exec.sub_route, this->exec.sub_id,
                 kv_crc_c( this->subj, subj_len, 0 ), MD_MESSAGE, '<' );
  b = this->exec.sub_route.forward_with_cnt( pub, rcount );
  this->exec.msg_route_cnt += rcount;
  return b;
}
/* given a command and keys, publish keyspace events */
bool
RedisKeyspace::pub_keyspace_events( RedisExec &exec ) noexcept
{
  /* translate cmd into an event */
  RedisKeyspace ev( exec );
  const char  * e      = NULL;
  size_t        elen   = 0;
  uint32_t      i;
  uint16_t      key_fl = exec.sub_route.key_flags |
                         EKF_KEYSPACE_DEL | EKF_KEYSPACE_TRIM | EKF_IS_EXPIRED;
  bool          b      = true;
  /* take care of expired keys */
  if ( ( exec.key_flags & EKF_IS_EXPIRED ) != 0 ) {
    ev.evt    = "expired"; 
    ev.evtlen = 7;
    for ( i = 0; i < exec.key_cnt; i++ ) {
      uint16_t fl = exec.keys[ i ]->flags & key_fl;
      if ( ( fl & ( EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD ) ) != 0 ) {
        if ( ( fl & EKF_IS_EXPIRED ) != 0 ) {
          ev.key    = (const char *) exec.keys[ i ]->kbuf.u.buf;
          ev.keylen = exec.keys[ i ]->kbuf.keylen - 1;
          if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
            b &= ev.fwd_keyspace();
          if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
            b &= ev.fwd_keyevent();
        }
      }
    }
  }
#define EVT( STR ) e = STR; elen = sizeof( STR ) - 1
  /*printf( "key_fl %u\n", key_fl );*/
  switch ( exec.cmd ) {
    default:                   break;
    case DEL_CMD:              EVT( "del" ); break;
    case BRPOPLPUSH_CMD:       /* rpop, lpush */
    case RPOPLPUSH_CMD:
    case RENAME_CMD:           break; /* rename_from, rename_to */
    case EXPIRE_CMD:           EVT( "expire" ); break;
    case BITFIELD_CMD:         EVT( "setbit" ); break;
    case BITOP_CMD:
    case GETSET_CMD:
    case MSET_CMD:
    case MSETNX_CMD:
    case PSETEX_CMD:
    case SETEX_CMD:
    case SETNX_CMD:
    case SET_CMD:              EVT( "set" ); break;
    case SETRANGE_CMD:         EVT( "setrange" ); break;
    case DECR_CMD:
    case DECRBY_CMD:
    case INCR_CMD:
    case INCRBY_CMD:           EVT( "incrby" ); break;
    case INCRBYFLOAT_CMD:      EVT( "incrbyfloat" ); break;
    case APPEND_CMD:           EVT( "append" ); break;
    case LPUSH_CMD:
    case LPUSHX_CMD:           EVT( "lpush" ); break;
    case BLPOP_CMD:
    case LPOP_CMD:             EVT( "lpop" ); break;
    case RPUSH_CMD:
    case RPUSHX_CMD:           EVT( "rpush" ); break;
    case BRPOP_CMD:
    case RPOP_CMD:             EVT( "rpop" ); break;
    case LINSERT_CMD:          EVT( "linsert" ); break;
    case LSET_CMD:             EVT( "lset" ); break;
    case LTRIM_CMD:            EVT( "ltrim" ); break;
    case HSETNX_CMD:
    case HMSET_CMD:
    case HSET_CMD:             EVT( "hset" ); break;
    case HINCRBY_CMD:          EVT( "hincrby" ); break;
    case HINCRBYFLOAT_CMD:     EVT( "hincrbyfloat" ); break;
    case HDEL_CMD:             EVT( "hdel" ); break;
    case SMOVE_CMD:
    case SADD_CMD:             EVT( "sadd" ); break;
    case SREM_CMD:             EVT( "srem" ); break;
    case SPOP_CMD:             EVT( "spop" ); break;
    case SINTERSTORE_CMD:      EVT( "sinterstore" ); break;
    case SUNIONSTORE_CMD:      EVT( "sunionstore" ); break;
    case SDIFFSTORE_CMD:       EVT( "sdiffstore" ); break;
    case ZINCRBY_CMD:          EVT( "zincrby" ); break;
    case GEOADD_CMD:
    case ZADD_CMD:             EVT( "zadd" ); break;
    case ZREM_CMD:             EVT( "zrem" ); break;
    case ZREMRANGEBYLEX_CMD:   EVT( "zremrangebylex" ); break;
    case ZREMRANGEBYRANK_CMD:  EVT( "zremrangebyrank" ); break;
    case ZREMRANGEBYSCORE_CMD: EVT( "zremrangebyscore" ); break;
    case ZINTERSTORE_CMD:      EVT( "zinterstore" ); break;
    case ZUNIONSTORE_CMD:      EVT( "zunionstore" ); break;
    case BZPOPMIN_CMD:
    case ZPOPMIN_CMD:          EVT( "zpopmin" ); break;
    case BZPOPMAX_CMD:
    case ZPOPMAX_CMD:          EVT( "zpopmax" ); break;
    case XADD_CMD:             EVT( "xadd" ); break;
    case XDEL_CMD:             EVT( "xdel" ); break;
    case XGROUP_CMD:           break; /* xgroup-create, xgroup-delconsumer, */
                                      /* xgroup-destroy, xgroup-setid, */
    case XTRIM_CMD:            EVT( "xtrim" ); break;
    case XSETID_CMD:           EVT( "xsetid" ); break;
  }
#undef EVT
  /* if a cmd updates a key, a simple event is usually attached to it */
  if ( e != NULL ) {
    ev.evt    = e;
    ev.evtlen = elen;
    for ( i = 0; i < exec.key_cnt; i++ ) {
      uint16_t fl = exec.keys[ i ]->flags & key_fl;
      if ( ( fl & ( EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD |
                    EKF_LISTBLKD_NOT | EKF_ZSETBLKD_NOT |
                    EKF_STRMBLKD_NOT ) ) != 0 ) {
        ev.key    = (const char *) exec.keys[ i ]->kbuf.u.buf;
        ev.keylen = exec.keys[ i ]->kbuf.keylen - 1;
        if ( ( fl & EKF_KEYSPACE_FWD ) != 0 ) /* __keyspace.. */
          b &= ev.fwd_keyspace();
        if ( ( fl & EKF_KEYEVENT_FWD ) != 0 ) /* __keyevent.. */
          b &= ev.fwd_keyevent();
        if ( ( fl & EKF_LISTBLKD_NOT ) != 0 ) /* __listblkd.. -> b(lr)pop */
          b &= ev.fwd_listblkd();
        if ( ( fl & EKF_ZSETBLKD_NOT ) != 0 ) /* __zsetblkd.. -> bzpop */
          b &= ev.fwd_zsetblkd();
        if ( ( fl & EKF_STRMBLKD_NOT ) != 0 ) /* __strmblkd.. -> str readers */
          b &= ev.fwd_strmblkd();
      }
    }
    /* if a pop or xadd maxcount caused other events */
    if ( ( exec.key_flags & ( EKF_KEYSPACE_DEL | EKF_KEYSPACE_TRIM ) ) != 0 ) {
      for ( i = 0; i < exec.key_cnt; i++ ) {
        uint16_t fl = exec.keys[ i ]->flags & key_fl;
        if ( ( fl & ( EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD |
                      EKF_KEYSPACE_DEL | EKF_KEYSPACE_TRIM ) ) != 0 ) {
          ev.key    = (const char *) exec.keys[ i ]->kbuf.u.buf;
          ev.keylen = exec.keys[ i ]->kbuf.keylen - 1;
          if ( ( fl & EKF_KEYSPACE_DEL ) != 0 ) {
            if ( exec.cmd != DEL_CMD ) { /* DEL already sent del event */
              ev.evt    = "del";
              ev.evtlen = 3;
              if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
                b &= ev.fwd_keyspace();
              if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
                b &= ev.fwd_keyevent();
            }
          }
          else if ( ( fl & EKF_KEYSPACE_TRIM ) != 0 ) {
            ev.evt    = "xtrim";
            ev.evtlen = 5;
            if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
              b &= ev.fwd_keyspace();
            if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
              b &= ev.fwd_keyevent();
          }
        }
      }
    }
  }
  else { /* cmds that cause irregular event patterns */
    const char * first,
               * second;
    size_t       firstlen,
                 secondlen;
    switch ( exec.cmd ) {
      case RENAME_CMD:
        first     = "rename_from";
        firstlen  = 11;
        second    = "rename_to";
        secondlen = 9;
        break;
      case BRPOPLPUSH_CMD:       /* rpop, lpush */
      case RPOPLPUSH_CMD:
        first     = "rpop";
        firstlen  = 4;
        second    = "lpush";
        secondlen = 5;
        break;
      case XGROUP_CMD:
        switch ( exec.msg.match_arg( 1, MARG( "create" ),
                                        MARG( "setid" ),
                                        MARG( "destroy" ),
                                        MARG( "delconsumer" ), NULL ) ) {
          default: first = "none";               firstlen = 4; break;
          case 1:  first = "xgroup-create";      firstlen = 13; break;
          case 2:  first = "xgroup-setid";       firstlen = 12; break;
          case 3:  first = "xgroup-destroy";     firstlen = 14; break;
          case 4:  first = "xgroup-delconsumer"; firstlen = 18; break;
        }
        second = NULL; secondlen = 0;
        break;
      default:
        first     = NULL;
        firstlen  = 0;
        second    = NULL;
        secondlen = 0;
        break;
    }
    if ( first != NULL ) {
      uint16_t fl = exec.keys[ 0 ]->flags & key_fl;
      ev.key    = (const char *) exec.keys[ 0 ]->kbuf.u.buf;
      ev.keylen = exec.keys[ 0 ]->kbuf.keylen - 1;
      ev.evt    = first;
      ev.evtlen = firstlen;
      if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
        b &= ev.fwd_keyspace();
      if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
        b &= ev.fwd_keyevent();

      if ( ( fl & EKF_KEYSPACE_DEL ) != 0 ) {
        ev.evt    = "del";
        ev.evtlen = 3;
        if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
          b &= ev.fwd_keyspace();
        if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
          b &= ev.fwd_keyevent();
      }

      if ( second != NULL ) {
        uint16_t fl = exec.keys[ 1 ]->flags & key_fl;
        ev.key    = (const char *) exec.keys[ 1 ]->kbuf.u.buf;
        ev.keylen = exec.keys[ 1 ]->kbuf.keylen - 1;
        ev.evt    = second;
        ev.evtlen = secondlen;
        if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
          b &= ev.fwd_keyspace();
        if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
          b &= ev.fwd_keyevent();
      }
    }
  }
  if ( ( exec.sub_route.key_flags & EKF_MONITOR ) != 0 )
    b &= ev.fwd_monitor();
  return b;
}

void
RedisKeyspace::init_keyspace_events( RedisExec &e ) noexcept
{
  if ( e.sub_route.keyspace == NULL ) {
    void * p = ::malloc( sizeof( RedisKeyspaceNotify ) );
    if ( p == NULL ) {
      perror( "malloc" );
      return;
    }
    RedisKeyspaceNotify * notify = new ( p ) RedisKeyspaceNotify( e.sub_route );
    e.sub_route.keyspace = notify;
    e.sub_route.add_route_notify( *notify );
  }
}
/* modify keyspace route */
void
RedisKeyspaceNotify::update_keyspace_route( uint32_t &val,  uint16_t bit,
                                            int add,  uint32_t fd ) noexcept
{
  RouteRef rte( this->sub_route.zip, 0 );
  uint32_t rcnt = 0, xcnt;
  uint16_t & key_flags = this->sub_route.key_flags;

  /* if bit is set, then val has routes */
  if ( ( key_flags & bit ) != 0 )
    rcnt = rte.decompress( val, add > 0 );
  /* if unsub or sub */
  if ( add < 0 )
    xcnt = rte.remove( fd );
  else
    xcnt = rte.insert( fd );
  /* if route changed */
  if ( xcnt != rcnt ) {
    /* if route deleted */
    if ( xcnt == 0 ) {
      val = 0;
      key_flags &= ~bit;
    }
    /* otherwise added */
    else {
      key_flags |= bit;
      val = rte.compress();
    }
    rte.deref_coderef();
  }
}
/* track number of subscribes to keyspace subjects to enable them */
void
RedisKeyspaceNotify::update_keyspace_count( const char *sub,  size_t len,
                                            int add,  uint32_t fd ) noexcept
{
  /* keyspace subjects are special, since subscribing to them can create
   * some overhead */
  static const char kspc[] = "__keyspace@",
                    kevt[] = "__keyevent@",
                    lblk[] = "__listblkd@",
                    zblk[] = "__zsetblkd@",
                    sblk[] = "__strmblkd@",
                    moni[] = "__monitor_@";
  if ( len > 3 ) {
    if ( sub[ 0 ] != '_' )
      return;
    if ( sub[ 1 ] != '_' ) {
      const char *p = (const char *) ::memchr( sub, '.', len );
      if ( p == NULL )
        return;
      len -= ( &p[ 1 ] - sub );
      sub  = &p[ 1 ];
      if ( len > 0 && sub[ 0 ] != '_' )
        return;
    }
  }
  if ( ::memcmp( kspc, sub, len ) == 0 ) /* len <= 11, could match multiple */
    this->update_keyspace_route( this->keyspace, EKF_KEYSPACE_FWD, add, fd );
  if ( ::memcmp( kevt, sub, len ) == 0 )
    this->update_keyspace_route( this->keyevent, EKF_KEYEVENT_FWD, add, fd );
  if ( ::memcmp( lblk, sub, len ) == 0 )
    this->update_keyspace_route( this->listblkd, EKF_LISTBLKD_NOT, add, fd );
  if ( ::memcmp( zblk, sub, len ) == 0 )
    this->update_keyspace_route( this->zsetblkd, EKF_ZSETBLKD_NOT, add, fd );
  if ( ::memcmp( sblk, sub, len ) == 0 )
    this->update_keyspace_route( this->strmblkd, EKF_STRMBLKD_NOT, add, fd );
  if ( ::memcmp( moni, sub, len ) == 0 )
    this->update_keyspace_route( this->monitor , EKF_MONITOR     , add, fd );

  /*printf( "%.*s %d key_flags %x\n", (int) len, sub, add, this->key_flags );*/
}
/* client subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_sub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, 1, sub.src.fd );
}

void
RedisKeyspaceNotify::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, -1, sub.src.fd );
}
/* client pattern subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_psub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, 1, pat.src.fd );
}

void
RedisKeyspaceNotify::on_punsub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, -1, pat.src.fd );
}

void
RedisKeyspaceNotify::on_reassert( uint32_t ,  RouteVec<RouteSub> &,
                                  RouteVec<RouteSub> & ) noexcept
{
}
