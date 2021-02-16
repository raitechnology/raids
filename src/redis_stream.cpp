#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/exec_stream_ctx.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

ExecStatus
RedisExec::exec_xinfo( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  switch ( stream.get_key_read() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_NOT_FOUND:
      return EXEC_SEND_ZEROARR;
    case KEY_OK:
      if ( ! stream.open_readonly() )
        return ERR_KV_STATUS;
      break;
  }
  /* XINFO [CONSUMERS key groupname] [GROUPS key] [STREAM key] [HELP] */
  switch ( this->msg.match_arg( 1, MARG( "consumers" ),
                                   MARG( "groups" ),
                                   MARG( "stream" ), NULL ) ) {
    default:
      return ERR_BAD_ARGS;
    case 1: /* [CONSUMERS key groupname] */
      return this->xinfo_consumers( stream );
    case 2: /* [GROUPS key] */
      return this->xinfo_groups( stream );
    case 3: /* [STREAM key] */
      return this->xinfo_streams( stream );
  }
}

ExecStatus
RedisExec::xinfo_consumers( ExecStreamCtx &stream ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  MDMsgMem     tmp;
  const char * gname;
  size_t       glen,
               i, j, k,
               num_consumers = 0,
               max_consumers = 0,
             * pend_cnt = NULL;
  ListVal    * con_lv   = NULL;
  uint64_t   * pend_ns  = NULL,
               ns;
  StreamStatus xstat;

  if ( ! this->msg.get_arg( 3, gname, glen ) )
    return ERR_BAD_ARGS;

  j = stream.x->pending.count();
  /* pending is list of { id, group, consumer, ns, cnt } */
  for ( k = 0; k < j; k++ ) {
    ListData ld;
    ListVal  lv;
    xstat = stream.x->sindex( stream.x->pending, k, ld, tmp );
    if ( xstat != STRM_OK )
      break;
    /* only return list belonging to group */
    if ( ld.lindex_cmp_key( P_GRP, gname, glen ) != LIST_OK )
      continue;
    /* capture consumers */
    if ( ld.lindex( P_CON, lv ) == LIST_OK ) {
      for ( i = 0; i < num_consumers; i++ ) {
        if ( lv.cmp_key( con_lv[ i ] ) == 0 ) {
          pend_cnt[ i ]++;
          break;
        }
      }
      /* add new consumer to list */
      if ( i == num_consumers ) {
        if ( i == max_consumers ) {
          tmp.extend( sizeof( con_lv[ 0 ] ) * i,
                      sizeof( con_lv[ 0 ] ) * ( i + 8 ), &con_lv );
          tmp.extend( sizeof( pend_cnt[ 0 ] ) * i,
                      sizeof( pend_cnt[ 0 ] ) * ( i + 8 ), &pend_cnt );
          tmp.extend( sizeof( pend_ns[ 0 ] ) * i,
                      sizeof( pend_ns[ 0 ] ) * ( i + 8 ), &pend_ns );
          max_consumers = i + 8;
        }
        con_lv[ i ]   = lv;
        pend_cnt[ i ] = 1;
        pend_ns[ i ]  = 0;
        num_consumers++;
      }
      if ( ld.lindex( P_NS, lv ) != LIST_OK )
        break;
      ns = lv.u64();
      if ( pend_ns[ i ] < ns )
        pend_ns[ i ] = ns;
    }
  }
  /* construct array of these records:
   *  [ "name", consumer-name, "pending", count, "idle", milliseconds ] */
  ns = kv::current_realtime_coarse_ns();
  for ( k = 0; k < num_consumers; k++ ) {
    if ( pend_ns[ k ] < ns )
      pend_ns[ k ] = ( ns - pend_ns[ k ] ) / 1000000;
    else
      pend_ns[ k ] = 0;
    StreamBuf::BufQueue ar( this->strm );
    ar.append_string( "name", 4 );
    ar.append_string( con_lv[ k ].data, con_lv[ k ].sz,
                      con_lv[ k ].data2, con_lv[ k ].sz2 );
    ar.append_string( "pending", 7 );
    ar.append_uint( pend_cnt[ k ] );
    ar.append_string( "idle", 4 );
    ar.append_uint( pend_ns[ k ] );
    ar.prepend_array( 6 );
    q.append_list( ar );
  }
  q.prepend_array( num_consumers );
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );

  return EXEC_OK;
}

ExecStatus
RedisExec::xinfo_groups( ExecStreamCtx &stream ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  MDMsgMem     tmp;
  ListVal    * con_lv = NULL;
  size_t       i, j, k,
               gcnt,
               pcnt,
               pending_count,
               num_consumers,
               max_consumers = 0;
  StreamStatus xstat;

  gcnt = stream.x->group.count();
  pcnt = stream.x->pending.count();
  /* pending is list of { id, group, consumer, ns, cnt } */
  for ( k = 0; k < gcnt; k++ ) {
    ListData ld;
    ListVal  glv, lv;
    xstat = stream.x->sindex( stream.x->group, k, ld, tmp );
    if ( xstat != STRM_OK )
      break;
    if ( ld.lindex( 0, glv ) != LIST_OK )
      break;

    num_consumers = 0;
    pending_count = 0;

    /* count how many pending and how many consumers */
    for ( j = 0; j < pcnt; j++ ) {
      ListData ld;
      xstat = stream.x->sindex( stream.x->pending, j, ld, tmp );
      if ( xstat != STRM_OK )
        break;
      if ( ld.lindex_cmp_key( P_GRP, glv ) == LIST_OK ) {
        pending_count++;
        if ( ld.lindex( P_CON, lv ) != LIST_OK )
          break;
        for ( i = 0; i < num_consumers; i++ ) {
          if ( lv.cmp_key( con_lv[ i ] ) == 0 )
            break;
        }
        /* add new consumer to list */
        if ( i == num_consumers ) {
          if ( i == max_consumers ) {
            tmp.extend( sizeof( con_lv[ 0 ] ) * i,
                        sizeof( con_lv[ 0 ] ) * ( i + 8 ), &con_lv );
            max_consumers = i + 8;
          }
          con_lv[ i ] = lv;
          num_consumers++;
        }
      }
    }
    /* construct [ "name", group-name, "consumers", num_consumers, "pending",
     *             pending_count, "last-delivered-id", id ] */
    StreamBuf::BufQueue ar( this->strm );
    ar.append_string( "name", 4 );
    ar.append_string( glv.data, glv.sz, glv.data2, glv.sz2 );
    ar.append_string( "consumers", 9 );
    ar.append_uint( num_consumers );
    ar.append_string( "pending", 7 );
    ar.append_uint( pending_count );
    ar.append_string( "last-delivered-id", 17 );
    if ( ld.lindex( 1, lv ) != LIST_OK )
      break;
    ar.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
    ar.prepend_array( 8 );
    q.append_list( ar );
  }

  q.prepend_array( gcnt );
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );

  return EXEC_OK;
}

ExecStatus
RedisExec::xinfo_streams( ExecStreamCtx &stream ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  MDMsgMem     tmp;
  size_t       xcnt,
               gcnt;
  uint64_t     exp,
               upd;
  char         id[ 24 ];
  size_t       idlen;

  gcnt = stream.x->group.count();
  xcnt = stream.x->stream.count();

  this->kctx.get_stamps( exp, upd );
  if ( upd != 0 ) {
    uint64_t ms  = upd / 1000000,
             ser = ( upd % 1000000 );
    idlen = uint64_digits( ms );
    uint64_to_string( ms, id, idlen );
    id[ idlen++ ] = '-';
    if ( ser == 0 ) /* the first id used in this millisecond */
      id[ idlen++ ] = '0';
    else
      idlen += uint64_to_string( ser, &id[ idlen ] );
    id[ idlen ] = '\0';
  }
  else {
    idlen = 3;
    id[ 0 ] = '0';
    id[ 1 ] = '-';
    id[ 2 ] = '0';
  }
  q.append_string( "length", 6 );
  q.append_uint( xcnt );
  q.append_string( "groups", 6 );
  q.append_uint( gcnt );
  q.append_string( "last-generated-id", 17 );
  q.append_string( id, idlen );
  q.append_string( "first-entry", 11 );
  if ( xcnt == 0 || ! this->construct_xfield_output( stream, 0, q ) )
    q.append_nil();
  q.append_string( "last-entry", 10 );
  if ( xcnt == 0 || ! this->construct_xfield_output( stream, xcnt - 1, q ) )
    q.append_nil();
  q.prepend_array( 10 );

  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );

  return EXEC_OK;
}

static void
get_next_id( kv::KeyCtx &kctx,  char *id,  size_t &idlen )
{
  uint64_t ms = kv::current_realtime_coarse_ms();
  uint64_t exp, upd, ser = 0;

  /* use the update stamp of the entry to serialize the millisecond stamp */
  kctx.get_stamps( exp, upd );
  if ( upd != 0 ) {
    if ( upd / 1000000 >= ms ) {
      ms  = upd / 1000000;
      ser = ( upd % 1000000 ) + 1;
    }
  }
  /* save the last stamp used */
  kctx.update_stamps( exp, ( ms * 1000000 ) + ser );
  idlen = uint64_digits( ms );
  uint64_to_string( ms, id, idlen );
  id[ idlen++ ] = '-';
  if ( ser == 0 ) /* the first id used in this millisecond */
    id[ idlen++ ] = '0';
  else
    idlen += uint64_to_string( ser, &id[ idlen ] );
  id[ idlen ] = '\0';
}

ExecStatus
RedisExec::exec_xadd( EvKeyCtx &ctx ) noexcept
{
  /* XADD key [MAXLEN [~] N] id field string [field string ...] */
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  void        * mem,
              * p;
  ListData    * xl,
                ld;
  const char  * arg;
  size_t        arglen;
  uint64_t      maxlen = 0;
  size_t        j      = 2,
                count,
                ndata;
  const char  * tmparg;
  size_t        tmplen;
  const char  * idval;
  size_t        idlen;
  char          id[ 64 ];
  size_t        sz;

  /* id or MAXLEN */
  if ( ! this->msg.get_arg( j++, arg, arglen ) )
    return ERR_BAD_ARGS;
  if ( arglen == 6 && ::strncasecmp( arg, "MAXLEN", 6 ) == 0 ) {
    if ( ! this->msg.get_arg( j++, arg, arglen ) )
      return ERR_BAD_ARGS;
    /* if ~ is approximate */
    if ( arglen == 1 && arg[ 0 ] == '~' ) {
      if ( ! this->msg.get_arg( j++, arg, arglen ) )
        return ERR_BAD_ARGS;
    }
    /* cvt arg to maxlen */
    if ( string_to_uint( arg, arglen, maxlen ) != STR_CVT_OK )
      return ERR_BAD_ARGS;
    /* must be id */
    if ( ! this->msg.get_arg( j++, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  idval = arg;
  idlen = arglen;

  if ( (count = this->argc - j) == 0 )
    count = 1;
  ndata = 1 + arglen;
  for ( size_t k = j; k < this->argc; k++ ) {
    if ( ! this->msg.get_arg( k, tmparg, tmplen ) )
      return ERR_BAD_ARGS;
    ndata += 1 + tmplen;
  }
  /* constuct the stream if new */
  switch ( stream.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      if ( ! stream.create( count, ndata ) )
        return ERR_KV_STATUS;
      break;
    case KEY_OK:
      if ( ! stream.open() )
        return ERR_KV_STATUS;
      break;
  }
  /* convert '*' into a time-serial */
  if ( idlen == 1 && idval[ 0 ] == '*' ) {
    get_next_id( this->kctx, id, sz );
    idval = id;
    idlen = sz;
    ndata += sz - 1;
  }
  else {
    StreamId sid;
    if ( ! sid.str_to_id( idval, idlen ) )
      return ERR_STREAM_ID;
    size_t cnt = stream.x->stream.count();
    if ( cnt > 0 ) {
      ListVal lv;
      StreamStatus xstat;
      xstat = stream.x->sindex_id( stream.x->stream, cnt - 1, lv, tmp );
      if ( xstat != STRM_OK )
        return ERR_STREAM_ID;
      StreamId sid2;
      if ( ! sid2.str_to_id( lv, tmp ) || sid.compare( sid2 ) <= 0 )
        return ERR_STREAM_ID;
    }
  }
  sz = ListData::alloc_size( count, ndata );
  tmp.alloc( sizeof( ListData ) + sz, &mem );
  p   = &((char *) mem)[ sizeof( ListData ) ];
  xl  = new ( mem ) ListData( p, sz );
  xl->init( count, ndata );
  xl->rpush( idval, idlen );

  /* construct the list: [ id field value field value ] */
  for ( size_t k = j; k < this->argc; k++ ) {
    if ( this->msg.get_arg( k, tmparg, tmplen ) )
      xl->rpush( tmparg, tmplen );
  }
  /* push the list of values on to the stream */
  for (;;) {
    ListStatus lstat = stream.x->stream.rpush( xl->listp, xl->size );
    if ( lstat != LIST_FULL )
      break;
    if ( ! stream.realloc( xl->size, 0, 0 ) )
      return ERR_KV_STATUS;
  }
  /* trim the list if maxlen specified */
  if ( maxlen > 0 ) {
    size_t count = stream.x->stream.count();
    if ( count > maxlen ) {
      stream.x->stream.ltrim( count - maxlen );
      ctx.flags |= EKF_KEYSPACE_TRIM;
    }
  }
  /* the result is the list identifier */
  sz = this->send_string( idval, idlen );
  this->strm.sz += sz;
  ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM | EKF_STRMBLKD_NOT;

  return EXEC_OK;
}

ExecStatus
RedisExec::exec_xtrim( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  const char  * arg;
  size_t        arglen,
                j = 2,
                count;
  uint64_t      maxlen;

  /* XTRIM key MAXLEN [~] count */
  maxlen = 0;
  ctx.ival = 0;
  if ( this->msg.match_arg( j++, MARG( "maxlen" ), NULL ) == 0 )
    return ERR_BAD_ARGS;
  if ( ! this->msg.get_arg( j++, arg, arglen ) )
    return ERR_BAD_ARGS;
  if ( arglen == 1 && arg[ 0 ] == '~' ) {
    if ( ! this->msg.get_arg( j++, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  if ( string_to_uint( arg, arglen, maxlen ) != STR_CVT_OK )
    return ERR_BAD_ARGS;
  if ( maxlen > 0 ) {
    switch ( stream.get_key_write() ) {
      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
      case KEY_IS_NEW:   return EXEC_SEND_ZERO;
      case KEY_OK:
        if ( ! stream.open() )
          return ERR_KV_STATUS;
        break;
    }
    count = stream.x->stream.count();
    if ( count > maxlen ) {
      ctx.ival = count - maxlen;
      stream.x->stream.ltrim( ctx.ival );
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM;
    }
  }

  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_xdel( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  const char  * arg;
  size_t        arglen;
  ssize_t       off;
  bool          is_open = false;

  /* XDEL key id [id ...] */
  ctx.ival = 0;
  for ( size_t j = 2; j < this->argc; j++ ) {
    if ( ! this->msg.get_arg( j, arg, arglen ) )
      return ERR_BAD_ARGS;
    if ( ! is_open ) {
      switch ( stream.get_key_write() ) {
        default:           return ERR_KV_STATUS;
        case KEY_NO_VALUE: return ERR_BAD_TYPE;
        case KEY_IS_NEW:   return EXEC_SEND_ZERO;
        case KEY_OK:
          if ( ! stream.open() )
            return ERR_KV_STATUS;
          is_open = true;
          break;
      }
    }
    off = stream.x->bsearch_eq( stream.x->stream, arg, arglen, tmp );
    if ( off >= 0 ) {
      if ( stream.x->stream.lrem( off ) == LIST_OK ) {
        ctx.ival++;
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM;
      }
    }
  }
  return EXEC_SEND_INT;
}

bool
RedisExec::construct_xfield_output( ExecStreamCtx &stream,  size_t idx,
                                    StreamBuf::BufQueue &q ) noexcept
{
  MDMsgMem tmp;
  ListData ld;
  ListVal  lv;
  size_t   k;

  if ( stream.x->sindex( stream.x->stream, idx, ld, tmp ) == STRM_OK ) {
    StreamBuf::BufQueue item( this->strm );
    StreamBuf::BufQueue flds( this->strm );
    for ( k = 1; ld.lindex( k, lv ) == LIST_OK; k++ ) {
      flds.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
    }
    flds.prepend_array( k - 1 );
    if ( ld.lindex( 0, lv ) == LIST_OK ) {
      item.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
      item.append_list( flds );
      item.prepend_array( 2 );
      q.append_list( item );
      return true;
    }
  }
  return false;
}

ExecStatus
RedisExec::exec_xrange( EvKeyCtx &ctx ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  int64_t       maxcnt;
  const char  * arg,
              * end;
  size_t        arglen,
                endlen,
                i, j, x, y,
                cnt;
  ssize_t       off;

  /* XRANGE key start end [COUNT count] */
  if ( ! this->msg.get_arg( 2, arg, arglen ) ||
       ! this->msg.get_arg( 3, end, endlen ) )
    return ERR_BAD_ARGS;
  if ( this->cmd != XRANGE_CMD ) {
    const char * tmp = end;    end    = arg;    arg    = tmp;
    size_t       len = endlen; endlen = arglen; arglen = len;
  }

  maxcnt = 0;
  if ( this->msg.match_arg( 4, MARG( "count" ), NULL ) == 1 ) {
    if ( ! this->msg.get_arg( 5, maxcnt ) )
      return ERR_BAD_ARGS;
  }
  switch ( stream.get_key_read() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_NOT_FOUND:
      return EXEC_SEND_ZEROARR;
    case KEY_OK:
      if ( ! stream.open_readonly() )
        return ERR_KV_STATUS;
      break;
  }
  off = stream.x->bsearch_str( stream.x->stream, arg, arglen, false, tmp );
  if ( off < 0 )
    return ERR_STREAM_ID;
  x   = (size_t) off;
  off = stream.x->bsearch_str( stream.x->stream, end, endlen, true, tmp );
  if ( off < 0 )
    return ERR_STREAM_ID;
  y   = (size_t) off;
  i   = ( this->cmd == XRANGE_CMD ? x : y );
  j   = ( this->cmd == XRANGE_CMD ? y : x );
  cnt = 0;
  for (;;) {
    if ( this->cmd == XRANGE_CMD ) {
      if ( i >= j )
        break;
    }
    else {
      if ( i <= j )
        break;
      i = i - 1;
    }
    if ( this->construct_xfield_output( stream, i, q ) ) {
      cnt++;
      if ( maxcnt > 0 && --maxcnt == 0 )
        break;
    }
    if ( cmd == XRANGE_CMD )
      i = i + 1;
  }
  q.prepend_array( cnt );
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_xrevrange( EvKeyCtx &ctx ) noexcept
{
  /* XREVRANGE key start end [COUNT count] */
  return this->exec_xrange( ctx );
}

ExecStatus
RedisExec::exec_xlen( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  /* XLEN key */
  switch ( stream.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_OK:        break;
  }
  if ( ! stream.open_readonly() )
    return ERR_KV_STATUS;
  ctx.ival = stream.x->stream.count();
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_xread( EvKeyCtx &ctx ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  int64_t       maxcnt,
                blockms;
  const char  * arg;
  size_t        arglen;
  ssize_t       off;
  int           n;
  bool          blocking = false;

  /* XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...] */
  maxcnt = 0;
  blockms = 0;
  for ( n = 1; n + 1 < this->first; n += 2 ) {
    switch ( this->msg.match_arg( n, MARG( "count" ),
                                     MARG( "block" ), NULL ) ) {
      case 1:
        if ( ! this->msg.get_arg( n+1, maxcnt ) )
          return ERR_BAD_ARGS;
        break;
      case 2:
        if ( ! this->msg.get_arg( n+1, blockms ) )
          return ERR_BAD_ARGS;
        if ( (ctx.ival = blockms) < 0 )
          ctx.ival = 0;
        blocking = true;
        break;
      default:
        return ERR_BAD_ARGS;
    }
  }
  if ( ( ctx.state & EKS_IS_SAVED_CONT ) != 0 ) {
    arglen = ctx.part->size;
    arg    = ctx.part->data( 0 );
  }
  else {
    n = this->last + 1 + ctx.argn - this->first;
    if ( ! this->msg.get_arg( n, arg, arglen ) )
      return ERR_BAD_ARGS;
  }

  switch ( stream.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND:
      if ( blocking ) {
        if ( ( ctx.state & EKS_IS_SAVED_CONT ) == 0 ) {
          ctx.state |= EKS_IS_SAVED_CONT;
          this->save_data( ctx, "0", 1 );
        }
      }
      break;
    case KEY_OK:
      if ( ! stream.open_readonly() )
        return ERR_KV_STATUS;
      /* find the id (arg) in the stream */
      off = stream.x->bsearch_str( stream.x->stream, arg, arglen, true, tmp );
      if ( off < 0 )
        return ERR_STREAM_ID;
      size_t cnt = 0,
             i   = (size_t) off,
             j   = stream.x->stream.count();
      /* if no field lists available */
      if ( i == j ) {
        /* save the tail id if a wildcard($) is used */
        if ( blocking ) {
          if ( ( ctx.state & EKS_IS_SAVED_CONT ) == 0 ) {
            const char * id;
            size_t       idlen;
            /* various forms of the last element */
            if ( arglen == 1 &&
                 ( arg[ 0 ] == '$' || arg[ 0 ] == '>' || arg[ 0 ] == '+' ) &&
                 stream.x->sindex_id_last( stream.x->stream, id, idlen,
                                           tmp ) == STRM_OK ) {
              ctx.state |= EKS_IS_SAVED_CONT;
              this->save_data( ctx, id, idlen );
            }
          }
          ctx.kstatus = KEY_NOT_FOUND; /* don't use the saved data for output */
        }
      }
      /* i < j, at least one field list */
      else {
        do {
          if ( this->construct_xfield_output( stream, i, q ) ) {
            cnt++;
            if ( maxcnt > 0 && --maxcnt == 0 )
              break;
          }
        } while ( ++i != j );
      }
      if ( cnt > 0 ) {
        q.prepend_array( cnt );
      }
      break;
  }
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  ExecStatus status = this->finish_xread( ctx, q );
  if ( status == EXEC_SEND_NULL && blocking )
    return EXEC_BLOCKED;
  return status;
}

ExecStatus
RedisExec::exec_xreadgroup( EvKeyCtx &ctx ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  ExecStreamCtx    stream( *this, ctx );
  MDMsgMem         tmp;
  StreamArgs       sa;
  StreamGroupQuery grp;
  int64_t          maxcnt,
                   blockms;
  size_t           i, k;
  int              n;
  bool             blocking = false;
  ListStatus       lstat;
  StreamStatus     xstat;

  /* XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK]
   * STREAMS key [key ...] id [id ...] */
  maxcnt  = 0;
  blockms = 0;
  sa.ns   = kv::current_realtime_coarse_ns();
  for ( n = 1; n + 1 < this->first; ) {
    switch ( this->msg.match_arg( n, MARG( "group" ),
                                     MARG( "count" ),
                                     MARG( "block" ),
                                     MARG( "noack" ), NULL ) ) {
      default:
        return ERR_BAD_ARGS;
      case 1: /* group */
        if ( ! this->msg.get_arg( n+1, sa.gname, sa.glen ) ||
             ! this->msg.get_arg( n+2, sa.cname, sa.clen ) )
          return ERR_BAD_ARGS;
        n += 3;
        break;
      case 2: /* count N */
        if ( ! this->msg.get_arg( n+1, maxcnt ) )
          return ERR_BAD_ARGS;
        n += 2;
        break;
      case 3: /* block millseconds */
        if ( ! this->msg.get_arg( n+1, blockms ) )
          return ERR_BAD_ARGS;
        /* ctx.ival saves the blocking millisecs */
        if ( (ctx.ival = blockms) < 0 )
          ctx.ival = 0;
        blocking = true;
        n += 2;
        break;
      case 4: /* noack */
        n++;
        break;
    }
  }
  n = this->last + 1 + ctx.argn - this->first;
  if ( ! this->msg.get_arg( n, sa.idval, sa.idlen ) )
    return ERR_BAD_ARGS;
  /* is_id_next finds the last id consumed by group */
  sa.is_id_next = ( sa.idlen == 1 && ( sa.idval[ 0 ] == '>' ||
                    sa.idval[ 0 ] == '$' || sa.idval[ 0 ] == '+' ) );

  switch ( stream.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:   return ERR_NO_GROUP;
    case KEY_OK:
      if ( ! stream.open() )
        return ERR_KV_STATUS;
      break;
  }
  switch ( stream.x->group_query( sa, maxcnt, grp, tmp ) ) {
    default: break;
    case STRM_NOT_FOUND: return ERR_NO_GROUP;
    case STRM_BAD_ID:    return ERR_STREAM_ID;
  }

  size_t cnt = 0;
  /* if new data */
  if ( sa.is_id_next ) {
    /* construct results for this key */
    for ( i = 0; i < grp.count; i++ ) {
      ListData ld,
             * xl;
      ListVal  lv;
      xstat = stream.x->sindex( stream.x->stream, grp.first+i, ld, tmp );
      if ( xstat != STRM_OK )
        break;

      StreamBuf::BufQueue item( this->strm ); /* item[ identifier, [ flds ] ] */
      StreamBuf::BufQueue flds( this->strm );
      /* all of the fields for this id */
      for ( k = 1; ld.lindex( k, lv ) == LIST_OK; k++ ) {
        flds.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
      }
      flds.prepend_array( k - 1 );
      /* first is the identifier of the field data (time_ms-0) */
      if ( ld.lindex( 0, lv ) == LIST_OK ) {
        lv.unite( tmp );
        /* the identifier */
        item.append_string( lv.data, lv.sz );
        item.append_list( flds ); /* field data is constructed above */
        item.prepend_array( 2 );
        q.append_list( item );
        cnt++;

        /* use the identifier to add to the pending list */
        sa.idval = (const char *) lv.data;
        sa.idlen = lv.sz;
        xl = sa.construct_pending( tmp );
        for (;;) {
          lstat = stream.x->pending.rpush( xl->listp, xl->size );
          if ( lstat != LIST_FULL ) break;
          stream.realloc( 0, 0, xl->size );
        }
        /* update index, dup xl memory since resize above may invalidate lv */
        if ( i + 1 == grp.count ) {
          xl->lindex( 0, lv );
          sa.idlen = lv.dup( tmp, &sa.idval );
          xstat = stream.x->sindex( stream.x->group, grp.pos, ld, tmp );
          if ( xstat != STRM_OK ||
               ! stream.x->group.in_mem_region( ld.listp, ld.size ) ||
               ld.lset( 1, sa.idval, sa.idlen ) != LIST_OK ) {
            while ( stream.x->update_group( sa, tmp ) == STRM_FULL )
              stream.realloc( 0, sa.glen+sa.idlen+8, 0 );
          }
        }
      }
    }
    /* if any data */
    if ( cnt > 0 ) {
      q.prepend_array( cnt );
    }
  }
  /* otherwise, is returning a history of past data consumed by client */
  else {
    for ( i = 0; i < grp.count; i++ ) {
      if ( this->construct_xfield_output( stream, grp.idx[ i ], q ) )
        cnt++;
    }
    q.prepend_array( cnt );
  }
  /* construct results with all keys or save result until looked at all keys */
  ExecStatus status = this->finish_xread( ctx, q );
  if ( status == EXEC_SEND_NULL && blocking )
    return EXEC_BLOCKED;
  return status;
}

ExecStatus
RedisExec::finish_xread( EvKeyCtx &ctx,  StreamBuf::BufQueue &q ) noexcept
{
  /* the xread and xreadgroup format is ar[ key[ field data(q) ], ... ] */
  if ( ctx.kstatus != KEY_NOT_FOUND && q.hd != NULL ) {
    StreamBuf::BufQueue key( this->strm );
    key.append_string( ctx.kbuf.u.buf, ctx.kbuf.keylen - 1 );
    key.append_list( q );
    key.prepend_array( 2 );
    /* if only one key, construct return value without saving */
    if ( this->key_cnt == 1 ) {
      StreamBuf::BufQueue ar( this->strm );
      ar.append_list( key );
      ar.prepend_array( 1 );
      this->strm.append_iov( ar );
      return EXEC_OK;
    }
    /* save the data returned from this key, there are more keys */
    this->save_data( ctx, &key, sizeof( key ) );
  }
  /* if nothing saved, and only one key, no need to check anything else */
  else if ( this->key_cnt == 1 ) {
    return EXEC_SEND_NULL;
  }
  /* if no more keys left to read, make a result if there was some field data */
  if ( this->key_done + 1 == this->key_cnt ) {
    StreamBuf::BufQueue ar( this->strm );
    size_t cnt = 0;
    for ( size_t i = 0; i < this->key_cnt; i++ ) {
      /* either key not found or ident not found */
      if ( this->keys[ i ]->kstatus != KEY_NOT_FOUND ) {
        EvKeyTempResult * part = this->keys[ i ]->part;
        if ( part != NULL ) {
          ar.append_list( *(StreamBuf::BufQueue *) part->data( 0 ) );
          cnt++;
        }
      }
    }
    /* if no results cnt will be zero */
    if ( cnt == 0 )
      return EXEC_SEND_NULL;
    /* otherwise some key's field data were read */
    ar.prepend_array( cnt );
    this->strm.append_iov( ar );
  }
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_xsetid( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  StreamArgs    sa;
  StreamStatus  xstat = STRM_OK;

  /* XSETID key groupname id */
  switch ( stream.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:   return ERR_NO_GROUP;
    case KEY_OK:
      if ( ! stream.open() )
        return ERR_KV_STATUS;
      break;
  }
  if ( ! this->msg.get_arg( 2, sa.gname, sa.glen ) ||
       ! this->msg.get_arg( 3, sa.idval, sa.idlen ) )
    return ERR_BAD_ARGS;
  if ( sa.idlen == 1 && sa.idval[ 0 ] == '$' ) {
    xstat = stream.x->
            sindex_id_last( stream.x->stream, sa.idval, sa.idlen, tmp );
    if ( xstat != STRM_OK )
      return ERR_NO_GROUP;
  }
  if ( ! stream.x->group_exists( sa, tmp ) )
    return ERR_NO_GROUP;
  while ( (xstat = stream.x->update_group( sa, tmp )) == STRM_FULL )
    if ( ! stream.realloc( 0, sa.glen + sa.idlen + 8, 0 ) )
      return ERR_BAD_ARGS;
  ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM;
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_xgroup( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  StreamArgs    sa;
  int           subcmd;
  StreamStatus  xstat = STRM_OK;

  /* XGROUP [CREATE key groupname id] [SETID key groupname id]
   *        [DESTROY key groupname]   [DELCONSUMER key groupname consname] */
  subcmd = this->msg.match_arg( 1, MARG( "create" ),
                                   MARG( "setid" ),
                                   MARG( "destroy" ),
                                   MARG( "delconsumer" ), NULL );
  if ( subcmd == 0 || ! this->msg.get_arg( 3, sa.gname, sa.glen ) )
    return ERR_BAD_ARGS;

  switch ( stream.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      if ( subcmd != 1 ) {
        if ( subcmd == 2 )
          return ERR_NO_GROUP;
        return EXEC_SEND_OK; /* destroyed already */
      }
      else {
        if ( this->msg.match_arg( 5, MARG( "mkstream" ), NULL ) == 0 )
          return ERR_KEY_DOESNT_EXIST; /* must mkstream to create it */
      }
      if ( ! stream.create( 8, 64 ) )
        return ERR_KV_STATUS;
      break;
    case KEY_OK:
      if ( ! stream.open() )
        return ERR_KV_STATUS;
      break;
  }
  if ( subcmd <= 2 ) {
    if ( ! this->msg.get_arg( 4, sa.idval, sa.idlen ) )
      return ERR_BAD_ARGS;
    if ( sa.idlen == 1 && sa.idval[ 0 ] == '$' ) {
      xstat = stream.x->
              sindex_id_last( stream.x->stream, sa.idval, sa.idlen, tmp );
      if ( xstat != STRM_OK )
        return ERR_BAD_ARGS;
    }
  }
  if ( subcmd == 4 ) { /* must have consumer to delete */
    if ( ! this->msg.get_arg( 4, sa.cname, sa.clen ) )
      return ERR_BAD_ARGS;
  }
  switch ( subcmd ) {
    case 1: /* create key groupname id (group must not exist) */
      if ( stream.x->group_exists( sa, tmp ) )
        return ERR_GROUP_EXISTS;
       break;
    case 2: /* setid key groupname id (group must exist) */
      if ( ! stream.x->group_exists( sa, tmp ) )
        return ERR_NO_GROUP;
      break;
    default: /* destroy or delconsumer */
      break;
  }
  switch ( subcmd ) {
    default: /* create or setid */
      while ( (xstat = stream.x->update_group( sa, tmp )) == STRM_FULL )
        if ( ! stream.realloc( 0, sa.glen + sa.idlen + 8, 0 ) )
          return ERR_BAD_ARGS;
      break;
    case 3: /* destroy key groupname */
      xstat = stream.x->remove_group( sa, tmp );
      break;
    case 4: /* delconsumer key groupname consname */
      xstat = stream.x->remove_pending( sa, tmp );
      break;
  }

  if ( xstat == STRM_OK ) {
    if ( subcmd >= 3 ) {
      if ( sa.cnt > 0 ) /* if 0, nothing done */
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM;
      ctx.ival = sa.cnt;
      if ( ctx.ival > 1 && subcmd == 3 ) /* 1 or 0 */
        ctx.ival = 1;
      return EXEC_SEND_INT;
    } /* create or setid return "OK" */
    ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STREAM;
    return EXEC_SEND_OK;
  }
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xack( EvKeyCtx &ctx ) noexcept
{
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  StreamArgs    sa;
  StreamStatus  xstat;
  bool          is_open = false;

  /* XACK key group id */
  if ( ! this->msg.get_arg( 2, sa.gname, sa.glen ) )
    return ERR_BAD_ARGS;
  ctx.ival = 0;
  for ( size_t k = 3; k < this->argc; k++ ) {
    /* for each id, remove from group's pending list */
    if ( ! this->msg.get_arg( k, sa.idval, sa.idlen ) )
      return ERR_BAD_ARGS;
    if ( ! is_open ) {
      switch ( stream.get_key_write() ) {
        default:           return ERR_KV_STATUS;
        case KEY_NO_VALUE: return ERR_BAD_TYPE;
        case KEY_IS_NEW:   return EXEC_SEND_INT; /* ival = 0 */
        case KEY_OK:
          if ( ! stream.open() )
            return ERR_KV_STATUS;
          break;
      }
      is_open = true;
    }
    xstat = stream.x->ack( sa, tmp );
    if ( xstat == STRM_OK )
      ctx.ival++;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_xclaim( EvKeyCtx &ctx ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  StreamArgs    sa;
  StreamClaim   cl;
  const char ** idar;
  size_t      * idln;
  size_t        i, j,
                cnt;
  StreamStatus  xstat;
  ListStatus    lstat;

  /* XCLAIM key group consumer min-idle-time id [id ...]
   *       [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID] */
  cl.ns  = kv::current_realtime_coarse_ns();
  sa.ns  = cl.ns;
  tmp.alloc( ( this->argc - 4 ) * sizeof( char * ), &idar );
  tmp.alloc( ( this->argc - 4 ) * sizeof( size_t ), &idln );
  if ( ! this->msg.get_arg( 2, sa.gname, sa.glen ) ||
       ! this->msg.get_arg( 3, sa.cname, sa.clen ) ||
       ! this->msg.get_arg( 4, cl.min_idle ) ||
       ! this->msg.get_arg( 5, idar[ 0 ], idln[ 0 ] ) )
    return ERR_BAD_ARGS;
  j = 1;
  for ( i = 6; i < this->argc; ) {
    switch ( this->msg.match_arg( i, MARG( "idle" ),
                                     MARG( "time" ),
                                     MARG( "retrycount" ),
                                     MARG( "force" ),
                                     MARG( "justid" ), NULL ) ) {
      case 0: /* another id */
        this->msg.get_arg( i, idar[ j ], idln[ j ] );
        j++;
        i++;
        break;
      case 1: /* set idle to this value */
        this->msg.get_arg( i + 1, cl.idle );
        i += 2;
        break;
      case 2: /* set time to this value */
        this->msg.get_arg( i + 1, cl.time );
        i += 2;
        break;
      case 3: /* set delivery count to this */
        this->msg.get_arg( i + 1, cl.retrycount );
        i += 2;
        break;
      case 4: /* force the id into pending, if it exists in stream */
        i++;
        cl.force = true;
        break;
      case 5: /* only return the id, don't increment count */
        i++;
        cl.justid = true;
        break;
    }
  }
  switch ( stream.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:   return ERR_NO_GROUP;
    case KEY_OK:
      if ( ! stream.open() )
        return ERR_KV_STATUS;
      break;
  }
  cnt = 0;
  for ( i = 0; i < j; i++ ) {
    sa.idval = idar[ i ];
    sa.idlen = idln[ i ];
    for (;;) {
      /* claim the id */
      xstat = stream.x->claim( sa, cl, tmp );
      if ( xstat != STRM_FULL ) break;
      stream.realloc( 0, 0, sa.list_data().size );
    }
    /* if claimed or force is true */
    if ( xstat == STRM_OK || cl.force ) {
      ssize_t  off;
      /* find the stream entry */
      off = stream.x->bsearch_eq( stream.x->stream, sa.idval, sa.idlen, tmp );
      if ( off >= 0 ) {
        /* force it to be claimed */
        if ( cl.force && xstat == STRM_NOT_FOUND ) {
          ListData * xl = sa.construct_pending( tmp );
          for (;;) {
            lstat = stream.x->pending.rpush( xl->listp, xl->size );
            if ( lstat != LIST_FULL ) break;
            stream.realloc( 0, 0, xl->size );
          }
        }
        /* get the record */
        if ( ! cl.justid ) {
          if ( this->construct_xfield_output( stream, off, q ) )
            cnt++;
        }
        /* only ids */
        else {
          ListData ld;
          ListVal  lv;
          xstat = stream.x->sindex( stream.x->stream, off, ld, tmp );
          if ( xstat == STRM_OK ) {
            if ( ld.lindex( 0, lv ) == LIST_OK ) {
              q.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
              cnt++;
            }
          }
        }
      }
    }
  }
  if ( cnt == 0 )
    return EXEC_SEND_ZEROARR;
  q.prepend_array( cnt );
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_xpending( EvKeyCtx &ctx ) noexcept
{
  StreamBuf::BufQueue q( this->strm );
  ExecStreamCtx stream( *this, ctx );
  MDMsgMem      tmp;
  StreamArgs    sa;
  StreamId      sid,
                start_sid,
                end_sid;
  size_t        i, j, k,
                id_cnt,
              * pend_cnt,
                num_consumers,
                max_consumers;
  ListVal       last_id,
                first_id,
              * con_lv;
  const char  * start  = NULL,
              * end    = NULL,
              * filter = NULL;
  size_t        slen   = 0,
                elen   = 0,
                flen   = 0;
  int64_t       maxcnt = 0;
  uint64_t      ns     = kv::current_realtime_coarse_ns();
  bool          list_pending       = false, /* array of { id, con, idle, cnt }*/
                filter_consumer    = false, /* filter array by consumer */
                start_at_beginning = true,  /* no starting element */
                end_at_last        = true;  /* no ending element */
  StreamStatus  xstat;

  /* XPENDING key group [start end count] [consumer] */
  if ( ! this->msg.get_arg( 2, sa.gname, sa.glen ) )
    return ERR_BAD_ARGS;

  if ( this->msg.get_arg( 3, start, slen ) &&
       this->msg.get_arg( 4, end, elen ) &&
       this->msg.get_arg( 5, maxcnt ) ) {
    list_pending = true;
    if ( slen != 1 || start[ 0 ] != '-' ) {
      start_at_beginning = false;
      start_sid.str_to_id( start, slen );
    }
    if ( elen != 1 || end[ 0 ] != '+' ) {
      end_at_last = false;
      end_sid.str_to_id( end, elen );
    }
    if ( maxcnt <= 0 )
      return EXEC_SEND_ZEROARR;
    if ( this->msg.get_arg( 6, filter, flen ) ) {
      filter_consumer = true;
    }
  }

  switch ( stream.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return ERR_NO_GROUP;
    case KEY_OK:
      if ( ! stream.open_readonly() )
        return ERR_KV_STATUS;
      break;
  }

  last_id.zero();
  first_id.zero();
  con_lv        = NULL;
  id_cnt        = 0;
  max_consumers = 0;
  num_consumers = 0;

  k = 0;
  j = stream.x->pending.count();
  /* pending is list of { id, group, consumer, ns, cnt } */
  for ( ; k < j; k++ ) {
    ListData ld;
    ListVal  lv;
    xstat = stream.x->sindex( stream.x->pending, k, ld, tmp );
    if ( xstat != STRM_OK )
      break;
    /* only return list belonging to group */
    if ( ld.lindex_cmp_key( P_GRP, sa.gname, sa.glen ) != LIST_OK )
      continue;
    /* only return list with consumer */
    if ( filter_consumer ) {
      if ( ld.lindex_cmp_key( P_CON, filter, flen ) != LIST_OK )
        continue;
    }
    /* fetch the identifier */
    if ( ld.lindex( P_ID, lv ) != LIST_OK )
      break;
    /* for list, return array of pending items */
    if ( list_pending ) {
      /* check start and end ranges */
      if ( ! start_at_beginning || ! end_at_last ) {
        sid.str_to_id( lv, tmp );
        if ( ( ! start_at_beginning && sid.compare( start_sid ) < 0 ) ||
             ( ! end_at_last && sid.compare( end_sid ) > 0 ) )
          continue;
      }
      /* id string */
      StreamBuf::BufQueue ar( this->strm );
      ar.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
      if ( ld.lindex( P_CON, lv ) != LIST_OK )
        break;
      /* consumer string */
      ar.append_string( lv.data, lv.sz, lv.data2, lv.sz2 );
      if ( ld.lindex( P_NS, lv ) != LIST_OK )
        break;
      /* idle time in milliseconds */
      uint64_t diff = lv.u64();
      if ( diff <= ns )
        diff = ns - diff;
      else
        diff = 0;
      ar.append_uint( diff / 1000000 );
      if ( ld.lindex( P_CNT, lv ) != LIST_OK )
        break;
      /* number of times delivered */
      ar.append_uint( lv.u32() );
      ar.prepend_array( 4 );
      q.append_list( ar );
      id_cnt++;
      if ( id_cnt == (size_t) maxcnt )
        break;
    }
    else {
      /* capture first and last id */
      if ( id_cnt == 0 ) {
        first_id = lv;
        last_id = lv;
      }
      else {
        if ( lv.cmp_key( first_id ) > 0 )
          first_id = lv;
        else if ( lv.cmp_key( last_id ) < 0 )
          last_id = lv;
      }
      id_cnt++;
      /* capture consumers */
      if ( ld.lindex( P_CON, lv ) != LIST_OK )
        break;
      for ( i = 0; i < num_consumers; i++ ) {
        if ( lv.cmp_key( con_lv[ i ] ) == 0 ) {
          pend_cnt[ i ]++;
          break;
        }
      }
      /* add new consumer to list */
      if ( i == num_consumers ) {
        if ( i == max_consumers ) {
          tmp.extend( sizeof( con_lv[ 0 ] ) * i,
                      sizeof( con_lv[ 0 ] ) * ( i + 8 ), &con_lv );
          tmp.extend( sizeof( pend_cnt[ 0 ] ) * i,
                      sizeof( pend_cnt[ 0 ] ) * ( i + 8 ), &pend_cnt );
          max_consumers = i + 8;
        }
        con_lv[ i ] = lv;
        pend_cnt[ i ] = 1;
        num_consumers++;
      }
    }
  }
  /* pending already built */
  if ( list_pending ) {
    if ( id_cnt == 0 ) {
      if ( ! stream.x->group_exists( sa, tmp ) )
        return ERR_NO_GROUP;
      return EXEC_SEND_ZEROARR;
    }
    q.prepend_array( id_cnt );
  }
  /* count, first, last, array of consumers */
  else {
    if ( id_cnt == 0 ) {
      if ( ! stream.x->group_exists( sa, tmp ) )
        return ERR_NO_GROUP;
      q.append_uint( id_cnt );
      q.append_nil();
      q.append_nil();
      q.append_nil();
    }
    else {
      q.append_uint( id_cnt );
      q.append_string( first_id.data, first_id.sz, first_id.data2,
                       first_id.sz2 );
      q.append_string( last_id.data, last_id.sz, last_id.data2, last_id.sz2 );

      StreamBuf::BufQueue ar( this->strm );
      for ( i = 0; i < num_consumers; i++ ) {
        StreamBuf::BufQueue ids( this->strm );
        char s[ 16 ];
        size_t d = uint64_to_string( pend_cnt[ i ], s );
        ids.append_string( con_lv[ i ].data, con_lv[ i ].sz,
                           con_lv[ i ].data2, con_lv[ i ].sz2 );
        ids.append_string( s, d );
        ids.prepend_array( 2 );
        ar.append_list( ids );
      }
      ar.prepend_array( num_consumers );
      q.append_list( ar );
    }
    q.prepend_array( 4 );
  }
  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;
  this->strm.append_iov( q );
  return EXEC_OK;
}
