#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/msg_ctx.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_list.h>
#include <raids/exec_list_ctx.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

enum {
  DO_LPUSH     = 1<<0,
  DO_RPUSH     = 1<<1,
  DO_LINSERT   = 1<<2,
  DO_LSET      = 1<<3,
  DO_LPOP      = 1<<4,
  DO_RPOP      = 1<<5,
  DO_LREM      = 1<<6,
  DO_LTRIM     = 1<<7,
  L_MUST_EXIST = 1<<8
};

ExecStatus
RedisExec::exec_blpop( RedisKeyCtx &/*ctx*/ )
{
  /* BLPOP key [key...] timeout */
  //return this->do_pop( ctx, DO_LPOP );
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_brpop( RedisKeyCtx &/*ctx*/ )
{
  /* BRPOP key [key...] timeout */
  //return this->do_pop( ctx, DO_LPOP );
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_brpoplpush( RedisKeyCtx &/*ctx*/ )
{
  /* BRPOPLPUSH src dest */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_lindex( RedisKeyCtx &ctx )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  ListVal    lv;
  int64_t    idx;
  size_t     sz;
  ExecStatus status;
  ListStatus lstatus;

  /* LINDEX key idx */
  if ( ! this->msg.get_arg( 2, idx ) )
    return ERR_BAD_ARGS;

  switch ( list.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! list.open_readonly() )
    return ERR_KV_STATUS;
  lstatus = list.x->lindex( idx, lv );
  if ( lstatus != LIST_NOT_FOUND ) {
    sz     = this->send_concat_string( lv.data, lv.sz, lv.data2, lv.sz2 );
    status = EXEC_OK;
  }
  else {
    sz     = 0;
    status = EXEC_SEND_NIL;
  }
  if ( ! list.validate_value() )
    return ERR_KV_STATUS;
  this->strm.sz += sz;
  return status;
}

ExecStatus
RedisExec::exec_linsert( RedisKeyCtx &ctx )
{
  /* LINSERT key [before|after] piv val */
  return this->do_push( ctx, DO_LINSERT );
}

ExecStatus
RedisExec::exec_llen( RedisKeyCtx &ctx )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  /* LLEN key */
  switch ( list.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_OK:        break;
  }
  if ( ! list.open_readonly() )
    return ERR_KV_STATUS;
  ctx.ival = list.x->count();
  if ( ! list.validate_value() )
    return ERR_KV_STATUS;
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_lpop( RedisKeyCtx &ctx )
{
  /* LPOP key */
  return this->do_pop( ctx, DO_LPOP );
}

ExecStatus
RedisExec::exec_lpush( RedisKeyCtx &ctx )
{
  /* LPUSH key val [val..] */
  return this->do_push( ctx, DO_LPUSH );
}

ExecStatus
RedisExec::exec_lpushx( RedisKeyCtx &ctx )
{
  /* LPUSHX key val [val..] */
  return this->do_push( ctx, DO_LPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::exec_lrange( RedisKeyCtx &ctx )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  StreamBuf::BufQueue q( this->strm );
  ListVal    lv;
  int64_t    from, to;
  size_t     count,
             itemcnt;
  ListStatus lstatus;

  /* LRANGE key start stop */
  if ( ! this->msg.get_arg( 2, from ) || ! this->msg.get_arg( 3, to ) )
    return ERR_BAD_ARGS;

  itemcnt = 0;
  switch ( list.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: goto finished;
    case KEY_OK:        break;
  }
  if ( ! list.open_readonly() )
    return ERR_KV_STATUS;
  count = list.x->count();
  if ( count > 0 ) {
    if ( from < 0 )
      from = count + from;
    if ( to < 0 )
      to = count + to;
    from = min<int64_t>( count, max<int64_t>( 0, from ) );
    to   = min<int64_t>( count, max<int64_t>( 0, to + 1 ) );

    for ( ; from < to; from++ ) {
      lstatus = list.x->lindex( from, lv );
      if ( lstatus != LIST_OK )
        break;
      if ( q.append_string( lv.data, lv.sz, lv.data2, lv.sz2 ) == 0 )
        return ERR_ALLOC_FAIL;
      itemcnt++;
    }
  }
finished:;
  if ( ! list.validate_value() )
    return ERR_KV_STATUS;
  q.finish_tail();
  q.prepend_array( itemcnt );
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_lrem( RedisKeyCtx &ctx )
{
  /* LREM key count value */
  return this->do_pop( ctx, DO_LREM );
}

ExecStatus
RedisExec::exec_lset( RedisKeyCtx &ctx )
{
  /* LSET key idx value */
  return this->do_push( ctx, DO_LSET );
}

ExecStatus
RedisExec::exec_ltrim( RedisKeyCtx &ctx )
{
  /* LTRIM key start stop */
  return this->do_pop( ctx, DO_LTRIM );
}

ExecStatus
RedisExec::exec_rpop( RedisKeyCtx &ctx )
{
  /* RPOP key */
  return this->do_pop( ctx, DO_RPOP );
}

ExecStatus
RedisExec::exec_rpoplpush( RedisKeyCtx &/*ctx*/ )
{
  /* RPOPLPUSH src dest */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_rpush( RedisKeyCtx &ctx )
{
  /* RPUSH key [val..] */
  return this->do_push( ctx, DO_RPUSH );
}

ExecStatus
RedisExec::exec_rpushx( RedisKeyCtx &ctx )
{
  /* RPUSHX key [val..] */
  return this->do_push( ctx, DO_RPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::do_push( RedisKeyCtx &ctx,  int flags )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  const char * value    = NULL,
             * piv      = NULL;
  size_t       valuelen = 0,
               pivlen   = 0,
               argi     = 3;
  int64_t      pos      = 0;
  size_t       count,
               ndata;
  const char * tmparg;
  size_t       tmplen;
  ListStatus   lstatus  = LIST_OK;
  bool         after    = false;

  if ( ( flags & ( DO_LPUSH | DO_RPUSH ) ) != 0 ) {
    /* [LR]PUSH[X] key val [val..] */
    if ( ! this->msg.get_arg( 2, value, valuelen ) )
      return ERR_BAD_ARGS;
  }
  else if ( ( flags & DO_LINSERT ) != 0 ) {
    /* LINSERT key [before|after] piv val */
    switch ( this->msg.match_arg( 2, "before", 6,
                                     "after",  5, NULL ) ) {
      default: return ERR_BAD_ARGS;
      case 1:  after = false; break;
      case 2:  after = true;  break;
    }
    if ( ! this->msg.get_arg( 3, piv, pivlen ) ||
         ! this->msg.get_arg( 4, value, valuelen ) )
      return ERR_BAD_ARGS;
  }
  else if ( ( flags & DO_LSET ) != 0 ) {
    if ( ! this->msg.get_arg( 2, pos ) ||
         ! this->msg.get_arg( 3, value, valuelen ) )
      return ERR_BAD_ARGS;
  }

  switch ( list.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      count = this->argc;
      ndata = 1 + valuelen;
      for ( size_t j = argi; j < this->argc; j++ ) {
        if ( ! this->msg.get_arg( j, tmparg, tmplen ) )
          return ERR_BAD_ARGS;
        ndata += 1 + tmplen;
      }
      if ( ! list.create( count, ndata ) )
        return ERR_KV_STATUS;
      break;
    case KEY_OK:
      if ( ! list.open() )
        return ERR_KV_STATUS;
      break;
  }
  for (;;) {
    switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET ) ) {
      case DO_LPUSH:
        lstatus = list.x->lpush( value, valuelen );
        break;
      case DO_RPUSH:
        lstatus = list.x->rpush( value, valuelen );
        break;
      case DO_LINSERT:
        lstatus = list.x->linsert( piv, pivlen, value, valuelen, after );
        break;
      case DO_LSET:
        lstatus = list.x->lset( pos, value, valuelen );
        break;
    }
    if ( lstatus == LIST_FULL ) {
      if ( ! list.realloc( 1 + valuelen ) )
        return ERR_KV_STATUS;
      continue;
    }
    switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET ) ) {
      case DO_LPUSH:
      case DO_RPUSH:
        if ( this->argc > argi ) {
          if ( ! this->msg.get_arg( argi++, value, valuelen ) )
            return ERR_BAD_ARGS;
          break;
        }
        fallthrough;
      case DO_LINSERT:
        if ( lstatus == LIST_OK ) {
          ctx.ival = list.x->count();
          return EXEC_SEND_INT;
        }
        return EXEC_SEND_NIL;
      case DO_LSET:
        if ( lstatus == LIST_OK )
          return EXEC_SEND_OK;
        return ERR_BAD_RANGE;
    }
  }
}

ExecStatus
RedisExec::do_pop( RedisKeyCtx &ctx,  int flags )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  ListVal      lv;
  const char * arg      = NULL;
  size_t       arglen   = 0,
               cnt, pos;
  int64_t      ival     = 0,
               start    = 0,
               stop     = 0;
  ListStatus   lstatus  = LIST_OK;

  if ( ( flags & DO_LREM ) != 0 ) {
    /* LREM key count value */
    if ( ! this->msg.get_arg( 2, ival ) ||
         ! this->msg.get_arg( 3, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  else if ( ( flags & DO_LTRIM ) != 0 ) {
    /* LTRIM key start stop */
    if ( ! this->msg.get_arg( 2, start ) || ! this->msg.get_arg( 3, stop ) )
      return ERR_BAD_ARGS;
  }
  lv.zero();
  switch ( list.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      switch ( flags & ( DO_LPOP | DO_RPOP | DO_LREM | DO_LTRIM ) ) {
        case DO_LPOP:
        case DO_RPOP:  break;
        case DO_LREM:  return EXEC_SEND_ZERO;
        case DO_LTRIM: return EXEC_SEND_OK;
      }
      return EXEC_SEND_NIL;
    case KEY_OK:
      if ( ! list.open() )
        return ERR_KV_STATUS;
      break;
  }
  switch ( flags & ( DO_LPOP | DO_RPOP | DO_LREM | DO_LTRIM ) ) {
    case DO_LPOP: lstatus = list.x->lpop( lv ); break;
    case DO_RPOP: lstatus = list.x->rpop( lv ); break;
    case DO_LREM:
      cnt = 0;
      if ( ival < 0 ) {
        pos = list.x->count();
        do {
          if ( list.x->scan_rev( arg, arglen, pos ) == LIST_NOT_FOUND )
            break;
          list.x->lrem( pos );
          cnt++;
        } while ( ++ival != 0 );
      }
      else {
        pos = 0;
        do {
          if ( list.x->scan_fwd( arg, arglen, pos ) == LIST_NOT_FOUND )
            break;
          list.x->lrem( pos );
          cnt++;
        } while ( --ival != 0 );
      }
      ctx.ival = cnt;
      return EXEC_SEND_INT;

    case DO_LTRIM:
      cnt = list.x->count();
      if ( start < 0 )
        start = cnt + start;
      if ( stop < 0 )
        stop = cnt + stop;
      start = min<int64_t>( cnt, max<int64_t>( 0, start ) );
      stop = min<int64_t>( cnt, max<int64_t>( 0, stop + 1 ) );
      stop = cnt - stop;
      list.x->ltrim( start );
      list.x->rtrim( stop );
      return EXEC_SEND_OK;
  }
  /* lpop & rpop */
  if ( lstatus == LIST_NOT_FOUND ) /* success */
    return EXEC_SEND_NIL;
  this->strm.sz += this->send_concat_string( lv.data, lv.sz, lv.data2, lv.sz2 );
  return EXEC_OK;
}

