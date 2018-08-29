#define __STDC_WANT_DEC_FP__ 1
#include <stdio.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/msg_ctx.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_hash.h>
#include <raids/exec_list_ctx.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

enum {
  DO_HEXISTS      = 1<<0,
  DO_HGET         = 1<<1,
  DO_HGETALL      = 1<<2,
  DO_HKEYS        = 1<<3,
  DO_HLEN         = 1<<4,
  DO_HMGET        = 1<<5,
  DO_HVALS        = 1<<6,
  DO_HSTRLEN      = 1<<7,
  DO_HSCAN        = 1<<8,
  DO_HINCRBY      = 1<<9,
  DO_HINCRBYFLOAT = 1<<10,
  DO_HMSET        = 1<<11,
  DO_HSET         = 1<<12,
  DO_HSETNX       = 1<<13
};

ExecStatus
RedisExec::exec_hdel( RedisKeyCtx &ctx )
{
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );
  const char * arg;
  size_t       arglen, i = 2;
  HashPos      pos;
  HashStatus   hstat;

  /* HDEL key field [field ...] */
  if ( ! this->msg.get_arg( i, arg, arglen ) )
    return ERR_BAD_ARGS;
  pos.init( arg, arglen );

  switch ( hash.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:   return EXEC_SEND_ZERO;
    case KEY_OK:       break;
  }
  if ( ! hash.open() )
    return ERR_KV_STATUS;
  ctx.ival = 0;
  for (;;) {
    hstat = hash.x->hdel( arg, arglen, pos );
    if ( hstat == HASH_OK )
      ctx.ival++;
    if ( ++i == this->argc )
      break;
    if ( ! this->msg.get_arg( i, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_hexists( RedisKeyCtx &ctx )
{
  /* HEXISTS key field */
  return this->do_hread( ctx, DO_HEXISTS );
}

ExecStatus
RedisExec::exec_hget( RedisKeyCtx &ctx )
{
  /* HGET key field */
  return this->do_hread( ctx, DO_HGET );
}

ExecStatus
RedisExec::exec_hgetall( RedisKeyCtx &ctx )
{
  /* HGETALL key */
  return this->do_hmultiscan( ctx, DO_HGETALL, NULL );
}

ExecStatus
RedisExec::exec_hincrby( RedisKeyCtx &ctx )
{
  /* HINCRBY key field int */
  return this->do_hwrite( ctx, DO_HINCRBY );
}

ExecStatus
RedisExec::exec_hincrbyfloat( RedisKeyCtx &ctx )
{
  /* HINCRBYFLOAT key field float */
  return this->do_hwrite( ctx, DO_HINCRBYFLOAT );
}

ExecStatus
RedisExec::exec_hkeys( RedisKeyCtx &ctx )
{
  /* HKEYS key */
  return this->do_hmultiscan( ctx, DO_HKEYS, NULL );
}

ExecStatus
RedisExec::exec_hlen( RedisKeyCtx &ctx )
{
  /* HLEN key */
  return this->do_hread( ctx, DO_HLEN );
}

ExecStatus
RedisExec::exec_hmget( RedisKeyCtx &ctx )
{
  /* HMGET key field [field ...] */
  return this->do_hmultiscan( ctx, DO_HMGET, NULL );
}

ExecStatus
RedisExec::exec_hmset( RedisKeyCtx &ctx )
{
  /* HMSET key field val [field val ...] */
  return this->do_hwrite( ctx, DO_HMSET );
}

ExecStatus
RedisExec::exec_hset( RedisKeyCtx &ctx )
{
  /* HSET key field val */
  return this->do_hwrite( ctx, DO_HSET );
}

ExecStatus
RedisExec::exec_hsetnx( RedisKeyCtx &ctx )
{
  /* HSETNX key field val */
  return this->do_hwrite( ctx, DO_HSETNX );
}

ExecStatus
RedisExec::exec_hstrlen( RedisKeyCtx &ctx )
{
  /* HSTRLEN key field */
  return this->do_hread( ctx, DO_HSTRLEN );
}

ExecStatus
RedisExec::exec_hvals( RedisKeyCtx &ctx )
{
  /* HVALS key */
  return this->do_hmultiscan( ctx, DO_HVALS, NULL );
}

ExecStatus
RedisExec::exec_hscan( RedisKeyCtx &ctx )
{
  /* HSCAN key cursor [MATCH pat] */
  ScanArgs   sa;
  ExecStatus status;
  if ( (status = this->match_scan_args( sa, 2 )) != EXEC_OK )
    return status;
  status = this->do_hmultiscan( ctx, DO_HSCAN, &sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::do_hmultiscan( RedisKeyCtx &ctx,  int flags,  ScanArgs *sa )
{
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );
  StreamBuf::BufQueue q( this->strm );
  const char * key     = NULL;
  size_t       keylen  = 0;
  HashPos      pos;
  size_t       count   = 0,
               itemcnt = 0,
               i       = ( sa != NULL && sa->pos > 0 ? sa->pos : 1 ),
               maxcnt  = ( sa != NULL ? sa->maxcnt * 2 : 0 ),
               argi    = 3;
  HashVal      kv;
  HashStatus   hstatus;

  /* HMGET key value [value...] */
  if ( ( flags & DO_HMGET ) != 0 ) {
    if ( ! this->msg.get_arg( 2, key, keylen ) )
      return ERR_BAD_ARGS;
    pos.init( key, keylen );
  }
  /* HSCAN key cursor [MATCH pat] */
  /* HGETALL/HKEYS/HVALS key */
  switch ( hash.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: goto finished;
    case KEY_OK:        break;
  }
  if ( ! hash.open_readonly() )
    return ERR_KV_STATUS;
  count = hash.x->count();

  for (;;) {
    /* scan keys except for HMGET */
    if ( ( flags & DO_HMGET ) == 0 ) {
      if ( i >= count || ( maxcnt != 0 && itemcnt >= maxcnt ) )
        break;
      hstatus = hash.x->hindex( i++, kv );
      if ( hstatus != HASH_OK )
        break;
      if ( ( flags & DO_HSCAN ) != 0 ) {
        if ( sa->re != NULL ) {
          int rc = pcre2_match( sa->re, (PCRE2_SPTR8) kv.key, kv.keylen,
                                0, 0, sa->md, 0 );
          if ( rc < 1 )
            continue;
        }
      }
    }
    else { /* HMGET */
      hstatus = hash.x->hget( key, keylen, kv, pos );
    }
    /* append key for HGETALL, HKEYS, HSCAN */
    if ( ( flags & ( DO_HGETALL | DO_HKEYS | DO_HSCAN ) ) != 0 ) {
      if ( hstatus == HASH_OK )
        q.append_string( kv.key, kv.keylen );
      else
        q.append_nil();
      itemcnt++;
    }
    /* append value for HGETALL, HVALS, HMGET, HSCAN */
    if ( ( flags & ( DO_HGETALL | DO_HVALS | DO_HMGET |
                     DO_HSCAN ) ) != 0 ) {
      if ( hstatus == HASH_OK )
        q.append_string( kv.data, kv.sz, kv.data2, kv.sz2 );
      else
        q.append_nil();
      itemcnt++;
      /* next key to find for HMGET */
      if ( ( flags & DO_HMGET ) != 0 ) {
        if ( argi >= this->argc )
          break;
        if ( ! this->msg.get_arg( argi++, key, keylen ) )
          return ERR_BAD_ARGS;
        pos.init( key, keylen );
      }
    }
  }

finished:;
  q.finish_tail();
  if ( ( flags & DO_HSCAN ) != 0 )
    q.prepend_cursor_array( i == count ? 0 : i, itemcnt );
  else
    q.prepend_array( itemcnt );

  if ( ! hash.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::do_hread( RedisKeyCtx &ctx,  int flags )
{
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );
  const char * arg    = NULL;
  size_t       arglen = 0;
  HashPos      pos;
  ListVal      lv;
  size_t       sz = 0;
  HashStatus   hstat;
  ExecStatus   status;

  if ( ( flags & ( DO_HEXISTS | DO_HGET | DO_HSTRLEN ) ) != 0 ) {
    /* HEXISTS/HGET/HSTRLEN key field */
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }

  switch ( hash.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND:
      switch ( flags & ( DO_HEXISTS | DO_HGET | DO_HLEN | DO_HSTRLEN ) ) {
        default:
        case DO_HEXISTS: fallthrough;
        case DO_HLEN:    fallthrough;
        case DO_HSTRLEN: return EXEC_SEND_ZERO;
        case DO_HGET:    return EXEC_SEND_NIL;
      }
    case KEY_OK: break;
  }
  if ( ! hash.open_readonly() )
    return ERR_KV_STATUS;

  switch ( flags & ( DO_HEXISTS | DO_HGET | DO_HLEN | DO_HSTRLEN ) ) {
    case DO_HEXISTS:
      if ( hash.x->hexists( arg, arglen, pos ) == HASH_OK )
        status = EXEC_SEND_ONE;
      else
        status = EXEC_SEND_ZERO;
      break;
    case DO_HLEN:
      ctx.ival = hash.x->hcount();
      status = EXEC_SEND_INT;
      break;
    case DO_HSTRLEN:
    case DO_HGET:
      hstat = hash.x->hget( arg, arglen, lv, pos );
      if ( flags == DO_HGET ) {
        if ( hstat == HASH_OK ) {
          sz = this->send_concat_string( lv.data, lv.sz, lv.data2, lv.sz2 );
          status = EXEC_OK;
        }
        else {
          status = EXEC_SEND_NIL;
        }
      }
      else {
        if ( hstat == HASH_OK ) {
          ctx.ival = lv.sz + lv.sz2;
          status = EXEC_SEND_INT;
        }
        else {
          status = EXEC_SEND_ZERO;
        }
      }
      break;
    default:
      status = ERR_BAD_CMD;
      break;
  }
  if ( ! hash.validate_value() )
    return ERR_KV_STATUS;
  this->strm.sz += sz;
  return status;
}

ExecStatus
RedisExec::do_hwrite( RedisKeyCtx &ctx,  int flags )
{
  static char  DDfmt[5] = { '%', 'D', 'D', 'a', 0 };
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );
  const char * arg     = NULL;
  size_t       arglen  = 0;
  const char * val     = NULL;
  size_t       vallen  = 0;
  HashPos      pos;
  size_t       count,
               ndata,
               argi    = 4;
  ListVal      lv;
  const char * idata;
  char         ibuf[ 64 ],
             * str     = NULL;
  size_t       sz      = 0;
  int64_t      ival    = 0;
  HashStatus   hstatus = HASH_OK;

  if ( ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET | DO_HINCRBYFLOAT ) ) != 0 ) {
    /* HSET/HSETNX/HMSET/HINCRBYFLOAT key field value */
    if ( ! this->msg.get_arg( 2, arg, arglen ) ||
         ! this->msg.get_arg( 3, val, vallen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
  else if ( ( flags & DO_HINCRBY ) != 0 ) {
    /* HINCRBY key field ival */
    if ( ! this->msg.get_arg( 2, arg, arglen ) ||
         ! this->msg.get_arg( 3, ctx.ival ) )
      return ERR_BAD_ARGS;
    vallen = 4; /* in case of resize() */
    pos.init( arg, arglen );
  }

  switch ( hash.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      count = 2;
      ndata = arglen + vallen + 3;
      for ( size_t j = argi; j < this->argc; j += 2 ) {
        const char * tmparg, * tmpval;
        size_t       tmplen, tmplen2;
        if ( ! this->msg.get_arg( j, tmparg, tmplen ) ||
             ! this->msg.get_arg( j+1, tmpval, tmplen2 ) )
          return ERR_BAD_ARGS;
        count++;
        ndata += tmplen + tmplen2 + 3;
      }
      if ( ! hash.create( count, ndata ) )
        return ERR_KV_STATUS;
      break;
    case KEY_OK:
      if ( ! hash.open() )
        return ERR_KV_STATUS;
      break;
  }
  for (;;) {
    switch ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET |
                       DO_HINCRBYFLOAT | DO_HINCRBY ) ) {
      case DO_HSET:
      case DO_HMSET:
        hstatus = hash.x->hset( arg, arglen, val, vallen, pos );
        break;
      case DO_HSETNX:
        hstatus = hash.x->hsetnx( arg, arglen, val, vallen, pos );
        break;
      case DO_HINCRBY:
        hstatus = hash.x->hget( arg, arglen, lv, pos );
        ival = ctx.ival;
        if ( hstatus == HASH_OK ) { /* exists */
          sz = lv.sz + lv.sz2;
          if ( lv.sz2 == 0 )
            idata = (const char *) lv.data;
          else if ( lv.sz == 0 )
            idata = (const char *) lv.data2;
          else {
            sz = lv.concat( ibuf, sizeof( ibuf ) );
            idata = ibuf;
          }
          int64_t jval;
          if ( RedisMsg::str_to_int( idata, sz, jval ) == REDIS_MSG_OK )
            ival += jval;
        }
        str = this->strm.alloc( 32 );
        str[ 0 ] = ':';
        sz = 1 + RedisMsg::int_to_str( ival, &str[ 1 ] );
        sz = crlf( str, sz );
        hstatus = hash.x->hupdate( arg, arglen, &str[ 1 ], sz - 3, pos );
        break;
      case DO_HINCRBYFLOAT: {
        _Decimal128 fp;
        int fvallen;
        hstatus = hash.x->hget( arg, arglen, lv, pos );
        if ( hstatus == HASH_OK ) { /* exists */
          sz = lv.concat( ibuf, sizeof( ibuf ) - 1 );
          ibuf[ sz ] = '\0';
          fp = ::strtod128( ibuf, NULL );
        }
        else {
          fp = 0.0DL;
        }
        sz = min<size_t>( vallen, sizeof( ibuf ) - 1 );
        ::memcpy( ibuf, val, sz ); ibuf[ sz ] = '\0';
        fp += ::strtod128( ibuf, NULL );
        fvallen = ::snprintf( ibuf, sizeof( ibuf ), DDfmt, fp );
        sz = 32 + fvallen * 2;
        str = this->strm.alloc( sz );
        str[ 0 ] = '$';
        sz = 1 + RedisMsg::int_to_str( fvallen, &str[ 1 ] );
        sz = crlf( str, sz ); 
        ::memcpy( &str[ sz ], ibuf, fvallen );
        sz = crlf( str, sz + fvallen );
        hstatus = hash.x->hupdate( arg, arglen, ibuf, fvallen, pos );
        break;
      }
    }
    if ( hstatus == HASH_FULL ) {
      if ( ! hash.realloc( arglen + vallen + 3 ) )
        return ERR_KV_STATUS;
      continue;
    }
    switch ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET |
                       DO_HINCRBYFLOAT | DO_HINCRBY ) ) {
      case DO_HSET:
        if ( hstatus == HASH_OK )
          return EXEC_SEND_ONE; /* new item indicated with 1 */
        return EXEC_SEND_ZERO; /* HASH_UPDATED, replaced old item */
      case DO_HSETNX:
        if ( hstatus == HASH_EXISTS )
          return EXEC_SEND_ZERO; /* did not update, already exists */
        return EXEC_SEND_ONE; /* new item */
      case DO_HMSET:
        if ( this->argc > argi ) {
          if ( ! this->msg.get_arg( argi, arg, arglen ) ||
               ! this->msg.get_arg( argi+1, val, vallen ) )
            return ERR_BAD_ARGS;
          argi += 2;
          pos.init( arg, arglen );
          break;
        }
        return EXEC_SEND_OK; /* send OK status */
      case DO_HINCRBYFLOAT:
      case DO_HINCRBY:
        this->strm.sz += sz;
        return EXEC_OK;
    }
  }
}
