#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/msg_ctx.h>
#include <raids/redis_exec.h>
#include <raimd/md_types.h>
#include <raimd/md_list.h>
#include <raids/exec_list_ctx.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

enum {
  DO_LPUSH     = 1<<0,
  DO_RPUSH     = 1<<1,
  DO_LINSERT   = 1<<2,
  DO_LSET      = 1<<3,
  DO_LPOP      = 1<<4,
  DO_RPOP      = 1<<5,
  DO_RPOPLPUSH = 1<<6,
  DO_LREM      = 1<<7,
  DO_LTRIM     = 1<<8,
  L_MUST_EXIST = 1<<9
};

ExecStatus
RedisExec::exec_blpop( EvKeyCtx &ctx )
{
  /* BLPOP key [key...] timeout */
  ExecStatus status = this->do_pop( ctx, DO_LPOP );
  switch ( status ) {
    case EXEC_SEND_NIL: return EXEC_BLOCKED;
    case EXEC_OK:       return EXEC_SEND_DATA;
    default:            return status;
  }
}

ExecStatus
RedisExec::exec_brpop( EvKeyCtx &ctx )
{
  /* BRPOP key [key...] timeout */
  ExecStatus status = this->do_pop( ctx, DO_RPOP );
  switch ( status ) {
    case EXEC_SEND_NIL: return EXEC_BLOCKED;
    case EXEC_OK:       return EXEC_SEND_DATA;
    default:            return status;
  }
}

ExecStatus
RedisExec::exec_brpoplpush( EvKeyCtx &ctx )
{
  /* BRPOPLPUSH src dest timeout */
  if ( ctx.argn == 1 ) {
    ExecStatus status = this->do_pop( ctx, DO_RPOPLPUSH );
    switch ( status ) {
      case EXEC_ABORT_SEND_NIL: return EXEC_BLOCKED;
      case EXEC_OK:             return EXEC_OK;
      default:                  return status;
    }
  }
  if ( this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;
  if ( this->keys[ 0 ]->status == EXEC_BLOCKED )
    return EXEC_BLOCKED;
  return this->exec_rpoplpush( ctx );
}

ExecStatus
RedisExec::exec_lindex( EvKeyCtx &ctx )
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
RedisExec::exec_linsert( EvKeyCtx &ctx )
{
  /* LINSERT key [before|after] piv val */
  return this->do_push( ctx, DO_LINSERT );
}

ExecStatus
RedisExec::exec_llen( EvKeyCtx &ctx )
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
RedisExec::exec_lpop( EvKeyCtx &ctx )
{
  /* LPOP key */
  return this->do_pop( ctx, DO_LPOP );
}

ExecStatus
RedisExec::exec_lpush( EvKeyCtx &ctx )
{
  /* LPUSH key val [val..] */
  return this->do_push( ctx, DO_LPUSH );
}

ExecStatus
RedisExec::exec_lpushx( EvKeyCtx &ctx )
{
  /* LPUSHX key val [val..] */
  return this->do_push( ctx, DO_LPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::exec_lrange( EvKeyCtx &ctx )
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
RedisExec::exec_lrem( EvKeyCtx &ctx )
{
  /* LREM key count value */
  return this->do_pop( ctx, DO_LREM );
}

ExecStatus
RedisExec::exec_lset( EvKeyCtx &ctx )
{
  /* LSET key idx value */
  return this->do_push( ctx, DO_LSET );
}

ExecStatus
RedisExec::exec_ltrim( EvKeyCtx &ctx )
{
  /* LTRIM key start stop */
  return this->do_pop( ctx, DO_LTRIM );
}

ExecStatus
RedisExec::exec_rpop( EvKeyCtx &ctx )
{
  /* RPOP key */
  return this->do_pop( ctx, DO_RPOP );
}

ExecStatus
RedisExec::exec_rpoplpush( EvKeyCtx &ctx )
{
  /* RPOPLPUSH src dest */
  if ( ctx.argn == 1 )
    return this->do_pop( ctx, DO_RPOPLPUSH );
  if ( this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;

  const char * value    = this->strm.out_buf;
  size_t       i        = 0,
               valuelen = this->strm.sz;
  for ( i = 4; i + 2 < valuelen; i++ )
    if ( value[ i - 2 ] == '\r' )
      break;
  if ( i + 2 < valuelen ) {
    value    = &value[ i ];
    valuelen = valuelen - ( i + 2 );
    return this->do_push( ctx, DO_RPOPLPUSH, value, valuelen );
  }
  return EXEC_ABORT_SEND_NIL;
}

ExecStatus
RedisExec::exec_rpush( EvKeyCtx &ctx )
{
  /* RPUSH key [val..] */
  return this->do_push( ctx, DO_RPUSH );
}

ExecStatus
RedisExec::exec_rpushx( EvKeyCtx &ctx )
{
  /* RPUSHX key [val..] */
  return this->do_push( ctx, DO_RPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::do_push( EvKeyCtx &ctx,  int flags,
                    const char *value,  size_t valuelen )
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );
  const char * piv      = NULL;
  size_t       pivlen   = 0,
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
    switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET |
                       DO_RPOPLPUSH ) ) {
      case DO_LPUSH:
      case DO_RPOPLPUSH:
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
    switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET |
                       DO_RPOPLPUSH ) ) {
      case DO_LPUSH:
      case DO_RPUSH:
        if ( this->argc > argi ) {
          if ( ! this->msg.get_arg( argi++, value, valuelen ) )
            return ERR_BAD_ARGS;
          break;
        }
        /* FALLTHRU */
      case DO_LINSERT:
        if ( lstatus == LIST_OK ) {
          ctx.ival   = list.x->count();
          ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
          return EXEC_SEND_INT;
        }
        return EXEC_SEND_NIL;
      case DO_LSET:
        if ( lstatus == LIST_OK ) {
          ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
          return EXEC_SEND_OK;
        }
        return ERR_BAD_RANGE;
      case DO_RPOPLPUSH:
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
        return EXEC_OK;
    }
  }
}

ExecStatus
RedisExec::do_pop( EvKeyCtx &ctx,  int flags )
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
      switch ( flags & ( DO_LPOP | DO_RPOP | DO_RPOPLPUSH |
                         DO_LREM | DO_LTRIM ) ) {
        case DO_LPOP:
        case DO_RPOP:      break;
        case DO_RPOPLPUSH: return EXEC_ABORT_SEND_NIL;
        case DO_LREM:      return EXEC_SEND_ZERO;
        case DO_LTRIM:     return EXEC_SEND_OK;
      }
      return EXEC_SEND_NIL;
    case KEY_OK:
      if ( ! list.open() )
        return ERR_KV_STATUS;
      break;
  }
  /* XXX: need to delete list if empty */
  switch ( flags & ( DO_LPOP | DO_RPOP | DO_RPOPLPUSH | DO_LREM | DO_LTRIM ) ) {
    case DO_LPOP:      lstatus = list.x->lpop( lv ); break;
    case DO_RPOP:
    case DO_RPOPLPUSH: lstatus = list.x->rpop( lv ); break;
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
      if ( (ctx.ival = cnt) > 0 )
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
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
      /* gen event even when no elems are rm */
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
      if ( start > 0 || stop > 0 ) {
        if ( start > 0 )
          list.x->ltrim( start );
        if ( stop > 0 )
          list.x->rtrim( stop );
        if ( start + stop >= (int64_t) cnt ) {
          ctx.flags |= EKF_KEYSPACE_DEL;
          if ( ! list.tombstone() )
            return ERR_KV_STATUS;
        }
      }
      return EXEC_SEND_OK;
  }
  /* lpop & rpop */
  if ( lstatus == LIST_NOT_FOUND ) { /* success */
    if ( ( flags & DO_RPOPLPUSH ) != 0 )
      return EXEC_ABORT_SEND_NIL;
    return EXEC_SEND_NIL;
  }
  this->strm.sz += this->send_concat_string( lv.data, lv.sz, lv.data2, lv.sz2 );
  ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_LIST;
  if ( list.x->count() == 0 ) {
    ctx.flags |= EKF_KEYSPACE_DEL;
    if ( ! list.tombstone() )
      return ERR_KV_STATUS;
  }
  return EXEC_OK;
}

