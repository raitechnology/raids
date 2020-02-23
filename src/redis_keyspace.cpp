#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <raids/redis_exec.h>
#include <raids/redis_keyspace.h>
#include <raids/ev_publish.h>
#include <raids/route_db.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace md;

/* alloc temporary space for subject */
inline bool
RedisKeyspace::alloc_subj( size_t subj_len ) noexcept
{
  if ( this->alloc_len < subj_len ) {
    size_t len = 20 + this->keylen + this->evtlen;
    char * tmp = this->exec.strm.alloc_temp( len + 1 );
    if ( tmp == NULL )
      return false;
    this->subj = tmp;
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
      this->subj[ off++ ] = this->db[ i++ ] = '0' + x;
    }
    else if ( x < 100 ) {
      this->subj[ off++ ] = this->db[ i++ ] = '0' + ( x / 10 );
      this->subj[ off++ ] = this->db[ i++ ] = '0' + ( x % 10 );
    }
    else {
      this->subj[ off++ ] = this->db[ i++ ] = '0' + ( x / 100 );
      this->subj[ off++ ] = this->db[ i++ ] = '0' + ( ( x / 10 ) % 10 );
      this->subj[ off++ ] = this->db[ i++ ] = '0' + ( x % 10 );
    }
    this->db[ i ] = 0;
  }
  else {
    for ( ; this->db[ i ] != 0; i++ )
      this->subj[ off++ ] = this->db[ i ];
  }
  return off;
}
/* append "@db__:" to subject */
inline size_t
RedisKeyspace::db_to_subj( size_t off ) noexcept
{
  this->subj[ off++ ] = '@';
  off = this->db_str( off );
  this->subj[ off ] = '_';
  this->subj[ off + 1 ] = '_';
  this->subj[ off + 2 ] = ':';
  return off + 3;
}

size_t
RedisKeyspace::make_bsubj( const char *blk ) noexcept
{
  size_t subj_len = 20 + this->keylen;
  if ( ! this->alloc_subj( subj_len ) )
    return 0;
  ::memcpy( this->subj, blk, 10 );
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->key, this->keylen );
  subj_len += this->keylen;
  this->subj[ subj_len ] = '\0';
  return subj_len;
}
/* blk = blocking subject (listblkd, zsetblkd, strmblkd), or keyspace */
bool
RedisKeyspace::fwd_bsubj( const char *blk ) noexcept
{
  size_t subj_len = this->make_bsubj( blk );
  if ( subj_len == 0 )
    return false;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->evt, this->evtlen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ':' );
  /*printf( "%s <- %s\n", this->subj, this->evt );*/
  return this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
}
/* publish __keyevent@N__:event <- key */
bool
RedisKeyspace::fwd_keyevent( void ) noexcept
{
  size_t subj_len = 20 + this->evtlen;
  if ( ! this->alloc_subj( subj_len ) )
    return false;
  ::memcpy( this->subj, "__keyevent", 10 );
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->evt, this->evtlen );
  subj_len += this->evtlen;
  this->subj[ subj_len ] = '\0';

  EvPublish pub( this->subj, subj_len, NULL, 0, this->key, this->keylen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ';' );
  /*printf( "%s <- %s\n", this->subj, this->key );*/
  return this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
}
/* publish __monitor_@N__:peer_address <- [ msg, result, time ] */
bool
RedisKeyspace::fwd_monitor( void ) noexcept
{
  size_t addr_len = this->exec.peer.get_peer_address_strlen();
  if ( ! this->alloc_subj( 20 + addr_len ) )
    return false;
  ::memcpy( this->subj, "__monitor_", 10 );

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
  timeval tv;
  char  * msg = strm.alloc_temp( msg_sz );

  if ( msg == NULL )
    return false;
  if ( addr_len > 0 ) {
    ::memcpy( &this->subj[ subj_len ], this->exec.peer.peer_address,
              addr_len );
    subj_len += addr_len;
    this->subj[ subj_len ] = '\0';
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
  ::gettimeofday( &tv, NULL );
  uint_to_str( tv.tv_sec, &timestamp[ 5 ], 10 ); /* in year 2288 == 11 */
  tv.tv_usec += 1000000; /* make all usec digits show */
  uint_to_str( tv.tv_usec, &timestamp[ 15 ], 7 );
  timestamp[ 15 ] = '.';
  crlf( timestamp, 17 + 5 );

  EvPublish pub( this->subj, subj_len, NULL, 0, msg, msg_sz,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_MESSAGE, '<' );
  return this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
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
  uint16_t      key_fl = exec.sub_route.rte.key_flags |
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
            ev.evt    = "del";
            ev.evtlen = 3;
            if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
              b &= ev.fwd_keyspace();
            if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
              b &= ev.fwd_keyevent();
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
  if ( ( exec.sub_route.rte.key_flags & EKF_MONITOR ) != 0 )
    b &= ev.fwd_monitor();
  return b;
}
