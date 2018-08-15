#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/msg_ctx.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_list.h>

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
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_brpop( RedisKeyCtx &/*ctx*/ )
{
  /* BRPOP key [key...] timeout */
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
  /* LINDEX key idx */
  void     * data;
  char     * itembuf;
  size_t     datalen,
             used;
  ListVal    lv;
  size_t     itemlen;
  int64_t    idx;
  uint8_t    lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t   llen;
  ListStatus lstatus;

  if ( ! this->msg.get_arg( 2, idx ) )
    return ERR_BAD_ARGS;

  switch ( this->exec_key_fetch( ctx ) ) {

    case KEY_NOT_FOUND:
      return EXEC_SEND_NIL;

    case KEY_OK:
      if ( ctx.type != MD_LIST ) {
        if ( ctx.type == MD_NODATA )
          return EXEC_SEND_NIL;
        return ERR_BAD_TYPE;
      }
      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        ListData list( data, datalen );
        list.open( lhdr, llen );
        lstatus = list.lindex( idx, lv );
        itemlen = lv.sz + lv.sz2;
        if ( lstatus == LIST_NOT_FOUND || itemlen > 30000 ) {
          if ( (ctx.kstatus = this->kctx.validate_value()) != KEY_OK )
            return ERR_KV_STATUS;
          if ( lstatus == LIST_NOT_FOUND )
            return EXEC_SEND_NIL;
        }
        itembuf = this->strm.alloc( itemlen + 32 );
        if ( itembuf == NULL )
          return ERR_ALLOC_FAIL;
        itembuf[ 0 ] = '$';
        used = 1 + RedisMsg::int_to_str( itemlen, &itembuf[ 1 ] );
        used = crlf( itembuf, used );
        ::memcpy( &itembuf[ used ], lv.data, lv.sz );
        if ( lv.sz2 > 0 )
          ::memcpy( &itembuf[ used + lv.sz ], lv.data2, lv.sz2 );
        used = crlf( itembuf, used + itemlen );
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
          this->strm.sz += used;
          return EXEC_OK;
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_linsert( RedisKeyCtx &ctx )
{
  /* LINSERT key [before|after] piv val */
  return this->exec_push( ctx, DO_LINSERT );
}

ExecStatus
RedisExec::exec_llen( RedisKeyCtx &ctx )
{
  /* LLEN key */
  void * data;
  size_t datalen;

  switch ( this->exec_key_fetch( ctx ) ) {

    case KEY_NOT_FOUND:
      return EXEC_SEND_ZERO;

    case KEY_OK:
      if ( ctx.type != MD_LIST ) {
        if ( ctx.type == MD_NODATA )
          return EXEC_SEND_ZERO;
        return ERR_BAD_TYPE;
      }
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        ListData list( data, datalen );
        list.open();
        ctx.ival = list.count();
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_SEND_INT;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_lpop( RedisKeyCtx &ctx )
{
  /* LPOP key */
  return this->exec_pop( ctx, DO_LPOP );
}

ExecStatus
RedisExec::exec_lpush( RedisKeyCtx &ctx )
{
  /* LPUSH key val [val..] */
  return this->exec_push( ctx, DO_LPUSH );
}

ExecStatus
RedisExec::exec_lpushx( RedisKeyCtx &ctx )
{
  /* LPUSHX key val [val..] */
  return this->exec_push( ctx, DO_LPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::exec_lrange( RedisKeyCtx &ctx )
{
  /* LRANGE key start stop */
  StreamBuf::BufQueue q( this->strm );
  void     * data;
  size_t     datalen,
             count,
             itemcnt;
  ListVal    lv;
  int64_t    from, to;
  uint8_t    lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t   llen;
  ListStatus lstatus;

  if ( ! this->msg.get_arg( 2, from ) || ! this->msg.get_arg( 3, to ) )
    return ERR_BAD_ARGS;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      return EXEC_SEND_ZERO;
    case KEY_OK:
      if ( ctx.type != MD_LIST ) {
        if ( ctx.type == MD_NODATA )
          return EXEC_SEND_ZERO;
        return ERR_BAD_TYPE;
      }
      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        ListData list( data, datalen );
        list.open( lhdr, llen );
        count = list.count();

        if ( from < 0 )
          from = count + from;
        if ( to < 0 )
          to = count + to;
        from = min<int64_t>( count, max<int64_t>( 0, from ) );
        to   = min<int64_t>( count, max<int64_t>( 0, to + 1 ) );

        for ( itemcnt = 0; from < to; from++ ) {
          lstatus = list.lindex( from, lv );
          if ( lstatus == LIST_NOT_FOUND )
            break;
          if ( q.append_string( lv.data, lv.sz, lv.data2, lv.sz2 ) == 0 )
            return ERR_ALLOC_FAIL;
          itemcnt++;
        }
        q.finish_tail();
        q.prepend_array( itemcnt );
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
          this->strm.append_iov( q );
          return EXEC_OK;
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_lrem( RedisKeyCtx &ctx )
{
  /* LREM key count value */
  return this->exec_pop( ctx, DO_LREM );
}

ExecStatus
RedisExec::exec_lset( RedisKeyCtx &ctx )
{
  /* LSET key idx value */
  return this->exec_push( ctx, DO_LSET );
}

ExecStatus
RedisExec::exec_ltrim( RedisKeyCtx &ctx )
{
  /* LTRIM key start stop */
  return this->exec_pop( ctx, DO_LTRIM );
}

ExecStatus
RedisExec::exec_rpop( RedisKeyCtx &ctx )
{
  /* RPOP key */
  return this->exec_pop( ctx, DO_RPOP );
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
  return this->exec_push( ctx, DO_RPUSH );
}

ExecStatus
RedisExec::exec_rpushx( RedisKeyCtx &ctx )
{
  /* RPUSHX key [val..] */
  return this->exec_push( ctx, DO_RPUSH | L_MUST_EXIST );
}

ExecStatus
RedisExec::exec_push( RedisKeyCtx &ctx,  int flags )
{
  void       * data;
  size_t       datalen;
  const char * value    = NULL,
             * piv      = NULL;
  size_t       valuelen = 0,
               pivlen   = 0,
               argi     = 3;
  int64_t      pos      = 0;
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
  size_t     count    = 2, /* set by alloc_size() */
             ndata    = valuelen,
             retry    = 0;
  ListData * old_list = NULL,
           * list     = NULL,
             tmp[ 2 ];
  MsgCtx   * msg      = NULL;
  MsgCtxBuf  tmpm;
  uint32_t   n        = 0;
  ListStatus lstatus  = LIST_OK;

  switch ( this->exec_key_fetch( ctx ) ) {

    case KEY_IS_NEW:
      if ( ( flags & L_MUST_EXIST ) != 0 )
        return EXEC_SEND_ZERO;
      datalen = ListData::alloc_size( count, ndata );
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        list = new ( &tmp[ n++%2 ]) ListData( data, datalen );
        list->init( count, ndata );
      }
      if ( 0 ) {
        fallthrough;

    case KEY_OK:
        if ( ctx.type != MD_LIST && ctx.type != MD_NODATA )
          return ERR_BAD_TYPE;
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus == KEY_OK ) {
          list = new ( &tmp[ n++%2 ] ) ListData( data, datalen );
          list->open();
        }
      }
      if ( list != NULL ) {
        for (;;) {
          if ( old_list != NULL ) {
            old_list->copy( *list );
            ctx.kstatus = this->kctx.load( *msg ); /* swap new and old */
            if ( ctx.kstatus != KEY_OK )
              break;
            old_list = NULL;
          }
        push_next_value:; /* lpush/rpush can have multiple values */
          switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET ) ) {
            case DO_LPUSH:
              lstatus = list->lpush( value, valuelen );
              break;
            case DO_RPUSH:
              lstatus = list->rpush( value, valuelen );
              break;
            case DO_LINSERT:
              lstatus = list->linsert( piv, pivlen, value, valuelen, after );
              break;
            case DO_LSET:
              lstatus = list->lset( pos, value, valuelen );
              break;
          }
          if ( lstatus != LIST_FULL ) { /* no realloc */
            switch ( flags & ( DO_LPUSH | DO_RPUSH | DO_LINSERT | DO_LSET ) ) {
              case DO_LPUSH:
              case DO_RPUSH:
                if ( this->argc > argi ) {
                  if ( ! this->msg.get_arg( argi++, value, valuelen ) )
                    return ERR_BAD_ARGS;
                  goto push_next_value;
                }
                fallthrough;
              case DO_LINSERT:
                if ( lstatus == LIST_OK ) {
                  ctx.ival = list->count();
                  return EXEC_SEND_INT;
                }
                return EXEC_SEND_NIL;
              case DO_LSET:
                if ( lstatus == LIST_OK )
                  return EXEC_SEND_OK;
                return ERR_BAD_RANGE;
            }
          }
          count = 2;
          ndata = valuelen + retry;
          retry += 16;
          datalen = list->resize_size( count, ndata );
          msg = new ( tmpm ) MsgCtx( this->kctx.ht, this->kctx.thr_ctx );
          msg->set_key( ctx.kbuf );
          msg->set_hash( ctx.hash1, ctx.hash2 );
          ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );
          if ( ctx.kstatus != KEY_OK )
            break;
          old_list = list;
          list = new ( (void *) &tmp[ n++%2 ] ) ListData( data, datalen );
          list->init( count, ndata );
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_pop( RedisKeyCtx &ctx,  int flags )
{
  void       * data;
  size_t       datalen;
  ListVal      lv;
  const char * arg      = NULL;
  size_t       arglen   = 0,
               sz, cnt, pos;
  int64_t      ival     = 0,
               start    = 0,
               stop     = 0;

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

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      switch ( flags & ( DO_LPOP | DO_RPOP | DO_LREM | DO_LTRIM ) ) {
        case DO_LPOP:
        case DO_RPOP:  return EXEC_SEND_NIL;
        case DO_LREM:  return EXEC_SEND_ZERO;
        case DO_LTRIM: return EXEC_SEND_OK;
      }
      return EXEC_SEND_NIL;
    case KEY_OK:
      if ( ctx.type != MD_LIST && ctx.type != MD_NODATA )
        return ERR_BAD_TYPE;
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        ListData list( data, datalen );
        ListStatus lstatus = LIST_OK;
        list.open();
        switch ( flags & ( DO_LPOP | DO_RPOP | DO_LREM | DO_LTRIM ) ) {
          case DO_LPOP: lstatus = list.lpop( lv ); break;
          case DO_RPOP: lstatus = list.rpop( lv ); break;
          case DO_LREM:
            cnt = 0;
            if ( ival < 0 ) {
              pos = list.count();
              do {
                if ( list.scan_rev( arg, arglen, pos ) == LIST_NOT_FOUND )
                  break;
                list.lrem( pos );
                cnt++;
              } while ( ++ival != 0 );
            }
            else {
              pos = 0;
              do {
                if ( list.scan_fwd( arg, arglen, pos ) == LIST_NOT_FOUND )
                  break;
                list.lrem( pos );
                cnt++;
              } while ( --ival != 0 );
            }
            ctx.ival = cnt;
            return EXEC_SEND_INT;

          case DO_LTRIM:
	    cnt = list.count();
	    if ( start < 0 )
	      start = cnt + start;
	    if ( stop < 0 )
	      stop = cnt + stop;
	    start = min<int64_t>( cnt, max<int64_t>( 0, start ) );
	    stop = min<int64_t>( cnt, max<int64_t>( 0, stop + 1 ) );
	    stop = cnt - stop;
	    list.ltrim( start );
	    list.rtrim( stop );
            return EXEC_SEND_OK;
        }
        /* lpop & rpop */
        if ( lstatus == LIST_NOT_FOUND ) /* success */
          return EXEC_SEND_NIL;
        sz = this->send_concat_string( lv.data, lv.sz, lv.data2, lv.sz2 );
        this->strm.sz += sz;
        return EXEC_OK;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

