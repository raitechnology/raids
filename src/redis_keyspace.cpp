#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
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
RedisKeyspace::alloc_subj( size_t subj_len )
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
RedisKeyspace::db_str( size_t off )
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
RedisKeyspace::db_to_subj( size_t off )
{
  this->subj[ off++ ] = '@';
  off = this->db_str( off );
  this->subj[ off ] = '_';
  this->subj[ off + 1 ] = '_';
  this->subj[ off + 2 ] = ':';
  return off + 3;
}

size_t
RedisKeyspace::make_keyspace_subj( void )
{
  static const uint8_t kspc[ 8 ] = { '_', '_', 'k', 'e', 'y', 's', 'p', 'a' };
  size_t subj_len = 20 + this->keylen;
  if ( ! this->alloc_subj( subj_len ) )
    return 0;
  ::memcpy( this->subj, kspc, 8 );
  this->subj[ 8 ] = 'c';
  this->subj[ 9 ] = 'e';
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->key, this->keylen );
  subj_len += this->keylen;
  this->subj[ subj_len ] = '\0';
  return subj_len;
}

size_t
RedisKeyspace::make_listblkd_subj( void )
{
  static const uint8_t lblk[ 8 ] = { '_', '_', 'l', 'i', 's', 't', 'b', 'l' };
  size_t subj_len = 20 + this->keylen;
  if ( ! this->alloc_subj( subj_len ) )
    return 0;
  ::memcpy( this->subj, lblk, 8 );
  this->subj[ 8 ] = 'k';
  this->subj[ 9 ] = 'd';
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->key, this->keylen );
  subj_len += this->keylen;
  this->subj[ subj_len ] = '\0';
  return subj_len;
}

size_t
RedisKeyspace::make_zsetblkd_subj( void )
{
  static const uint8_t zblk[ 8 ] = { '_', '_', 'z', 's', 'e', 't', 'b', 'l' };
  size_t subj_len = 20 + this->keylen;
  if ( ! this->alloc_subj( subj_len ) )
    return 0;
  ::memcpy( this->subj, zblk, 8 );
  this->subj[ 8 ] = 'k';
  this->subj[ 9 ] = 'd';
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->key, this->keylen );
  subj_len += this->keylen;
  this->subj[ subj_len ] = '\0';
  return subj_len;
}

/* publish __keyspace@N__:key <- event */
bool
RedisKeyspace::fwd_keyspace( void )
{
  size_t subj_len = this->make_keyspace_subj();
  if ( subj_len == 0 )
    return false;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->evt, this->evtlen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ':' );
  /*printf( "%s <- %s\n", this->subj, this->evt );*/
  this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
  return true;
}

/* publish __listblkd@N__:key <- event */
bool
RedisKeyspace::fwd_listblkd( void )
{
  size_t subj_len = this->make_listblkd_subj();
  if ( subj_len == 0 )
    return false;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->evt, this->evtlen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ':' );
  /*printf( "%s <- %s\n", this->subj, this->evt );*/
  this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
  return true;
}

/* publish __zsetblkd@N__:key <- event */
bool
RedisKeyspace::fwd_zsetblkd( void )
{
  size_t subj_len = this->make_zsetblkd_subj();
  if ( subj_len == 0 )
    return false;

  EvPublish pub( this->subj, subj_len, NULL, 0, this->evt, this->evtlen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ':' );
  /*printf( "%s <- %s\n", this->subj, this->evt );*/
  this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
  return true;
}

/* publish __keyevent@N__:event <- key */
bool
RedisKeyspace::fwd_keyevent( void )
{
  static const uint8_t kevt[ 8 ] = { '_', '_', 'k', 'e', 'y', 'e', 'v', 'e' };
  size_t subj_len = 20 + this->evtlen;
  if ( ! this->alloc_subj( subj_len ) )
    return false;
  ::memcpy( this->subj, kevt, 8 );
  this->subj[ 8 ] = 'n';
  this->subj[ 9 ] = 't';
  subj_len = this->db_to_subj( 10 );

  ::memcpy( &this->subj[ subj_len ], this->evt, this->evtlen );
  subj_len += this->evtlen;
  this->subj[ subj_len ] = '\0';

  EvPublish pub( this->subj, subj_len, NULL, 0, this->key, this->keylen,
                 this->exec.sub_id, kv_crc_c( this->subj, subj_len, 0 ),
                 NULL, 0, MD_STRING, ';' );
  /*printf( "%s <- %s\n", this->subj, this->key );*/
  this->exec.sub_route.rte.forward_msg( pub, NULL, 0, NULL );
  return true;
}

/* given a command and keys, publish keyspace events */
bool
RedisKeyspace::pub_keyspace_events( RedisExec &exec )
{
  /* translate cmd into an event */
  const char * e      = NULL;
  size_t       elen   = 0;
  uint16_t     key_fl = exec.sub_route.rte.key_flags | EKF_KEYSPACE_DEL;
  bool         b      = true;
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
  }
#undef EVT
  if ( e != NULL ) {
    RedisKeyspace ev( exec );
    if ( exec.key_cnt == 1 ) {
      uint16_t fl = exec.key->flags & key_fl;
      ev.key    = (const char *) exec.key->kbuf.u.buf;
      ev.keylen = exec.key->kbuf.keylen - 1;
      ev.evt    = e;
      ev.evtlen = elen;
      if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
        b &= ev.fwd_keyspace();
      if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
        b &= ev.fwd_keyevent();
      if ( ( fl & EKF_LISTBLKD_NOT ) != 0 )
        b &= ev.fwd_listblkd();
      if ( ( fl & EKF_ZSETBLKD_NOT ) != 0 )
        b &= ev.fwd_zsetblkd();

      if ( ( fl & EKF_KEYSPACE_DEL ) != 0 ) {
        ev.evt    = "del";
        ev.evtlen = 3;
        if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
          b &= ev.fwd_keyspace();
        if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
          b &= ev.fwd_keyevent();
      }
    }
    else {
      uint32_t i;
      ev.evt    = e;
      ev.evtlen = elen;
      for ( i = 0; i < exec.key_cnt; i++ ) {
        uint16_t fl = exec.keys[ i ]->flags & key_fl;
        if ( ( fl & ( EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD |
                      EKF_LISTBLKD_NOT | EKF_ZSETBLKD_NOT ) ) != 0 ) {
          ev.key    = (const char *) exec.keys[ i ]->kbuf.u.buf;
          ev.keylen = exec.keys[ i ]->kbuf.keylen - 1;
          if ( ( fl & EKF_KEYSPACE_FWD ) != 0 )
            b &= ev.fwd_keyspace();
          if ( ( fl & EKF_KEYEVENT_FWD ) != 0 )
            b &= ev.fwd_keyevent();
          if ( ( fl & EKF_LISTBLKD_NOT ) != 0 )
            b &= ev.fwd_listblkd();
          if ( ( fl & EKF_ZSETBLKD_NOT ) != 0 )
            b &= ev.fwd_zsetblkd();
        }
      }
      if ( ( exec.key_flags & EKF_KEYSPACE_DEL ) != 0 ) {
        ev.evt    = "del";
        ev.evtlen = 3;
        for ( i = 0; i < exec.key_cnt; i++ ) {
          uint16_t fl = exec.keys[ i ]->flags & key_fl;
          if ( ( fl & ( EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD ) ) != 0 ) {
            if ( ( fl & EKF_KEYSPACE_DEL ) != 0 ) {
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
    }
  }
  else {
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
      default:
        first     = NULL;
        firstlen  = 0;
        second    = NULL;
        secondlen = 0;
        break;
    }
    if ( first != NULL ) {
      RedisKeyspace ev( exec );
      uint16_t fl = exec.key_flags & key_fl;
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
  return b;
}
