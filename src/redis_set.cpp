#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_set.h>
#include <raids/set_bits.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

enum {
  DO_SCARD       = 1<<0,
  DO_SISMEMBER   = 1<<1,
  DO_SMEMBERS    = 1<<2,
  DO_SRANDMEMBER = 1<<3,
  DO_SPOP        = 1<<4,
  DO_SADD        = 1<<5,
  DO_SREM        = 1<<6,
  DO_SMOVE       = 1<<7,
  DO_SSCAN       = 1<<8,
  DO_SDIFF       = 1<<9,
  DO_SDIFFSTORE  = 1<<10,
  DO_SINTER      = 1<<11,
  DO_SINTERSTORE = 1<<12,
  DO_SUNION      = 1<<13,
  DO_SUNIONSTORE = 1<<14
};

ExecStatus
RedisExec::exec_sadd( RedisKeyCtx &ctx )
{
  /* SADD key member [member ...] */
  return this->exec_swrite( ctx, DO_SADD );
}

ExecStatus
RedisExec::exec_scard( RedisKeyCtx &ctx )
{
  /* SCARD key */
  return this->exec_sread( ctx, DO_SCARD );
}

ExecStatus
RedisExec::exec_sdiff( RedisKeyCtx &ctx )
{
  /* SDIFF key [key ...] */
  return this->exec_ssetop( ctx, DO_SDIFF );
}

ExecStatus
RedisExec::exec_sdiffstore( RedisKeyCtx &ctx )
{
  /* SDIFFSTORE dest key [key ...] */
  return this->exec_ssetop( ctx, DO_SDIFFSTORE );
}

ExecStatus
RedisExec::exec_sinter( RedisKeyCtx &ctx )
{
  /* SINTER key [key ...] */
  return this->exec_ssetop( ctx, DO_SINTER );
}

ExecStatus
RedisExec::exec_sinterstore( RedisKeyCtx &ctx )
{
  /* SINTERSTORE dest key [key ...] */
  return this->exec_ssetop( ctx, DO_SINTERSTORE );
}

ExecStatus
RedisExec::exec_sismember( RedisKeyCtx &ctx )
{
  /* SISMEMBER key member */
  return this->exec_sread( ctx, DO_SISMEMBER );
}

ExecStatus
RedisExec::exec_smembers( RedisKeyCtx &ctx )
{
  /* SMEMBERS key */
  return this->exec_smultiscan( ctx, DO_SMEMBERS, NULL );
}

ExecStatus
RedisExec::exec_smove( RedisKeyCtx &ctx )
{
  /* SMOVE src dest member */
  if ( ctx.argn == 2 && this->key_done == 0 )
    return EXEC_DEPENDS;
  return this->exec_swrite( ctx, DO_SMOVE );
}

ExecStatus
RedisExec::exec_spop( RedisKeyCtx &ctx )
{
  /* SPOP key [count] */
  return this->exec_smultiscan( ctx, DO_SPOP, NULL );
}

ExecStatus
RedisExec::exec_srandmember( RedisKeyCtx &ctx )
{
  /* SRANDMEMBER key [count] */
  return this->exec_smultiscan( ctx, DO_SRANDMEMBER, NULL );
}

ExecStatus
RedisExec::exec_srem( RedisKeyCtx &ctx )
{
  /* SREM key member [member ...] */
  return this->exec_swrite( ctx, DO_SREM );
}

ExecStatus
RedisExec::exec_sunion( RedisKeyCtx &ctx )
{
  /* SUNION key [key  ...] */
  return this->exec_ssetop( ctx, DO_SUNION );
}

ExecStatus
RedisExec::exec_sunionstore( RedisKeyCtx &ctx )
{
  /* SUNIONSTORE dest key [key ...] */
  return this->exec_ssetop( ctx, DO_SUNIONSTORE );
}

ExecStatus
RedisExec::exec_sscan( RedisKeyCtx &ctx )
{
  /* SSCAN key curs [match pat] [count cnt] */
  ScanArgs   sa;
  ExecStatus status;
  if ( (status = this->match_scan_args( sa, 2 )) != EXEC_OK )
    return status;
  status = this->exec_smultiscan( ctx, DO_SSCAN, &sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::exec_sread( RedisKeyCtx &ctx,  int flags )
{
  void       * data;
  size_t       datalen;
  const char * arg    = NULL;
  size_t       arglen = 0;
  uint8_t      lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t     llen;
  HashPos      pos;
  ExecStatus   status = EXEC_OK;

  /* SCARD key */
  /* SISMEMBER key member */
  if ( ( flags & DO_SISMEMBER ) != 0 ) {
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      return EXEC_SEND_ZERO;
    case KEY_OK:
      if ( ctx.type != MD_SET && ctx.type != MD_NODATA )
        return ERR_BAD_TYPE;
      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        SetData set( data, datalen );
        set.open( lhdr, llen );
        switch ( flags & ( DO_SCARD | DO_SISMEMBER ) ) {
          case DO_SCARD:
            ctx.ival = set.hcount();
            status = EXEC_SEND_INT;
            break;
          case DO_SISMEMBER:
            if ( set.sismember( arg, arglen, pos ) == SET_OK )
              status = EXEC_SEND_ONE;
            else
              status = EXEC_SEND_ZERO;
            break;
        }
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return status;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_swrite( RedisKeyCtx &ctx,  int flags )
{
  const char * arg    = NULL;
  size_t       arglen = 0;
  HashPos      pos;

  /* SADD key member [member ...] */
  /* SREM key member [member ...] */
  if ( ( flags & DO_SMOVE ) == 0 ) {
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  /* SMOVE src-key dest-key member */
  else {
    if ( ! this->msg.get_arg( 3, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  pos.init( arg, arglen );

  size_t       count,
               ndata;
  void       * data;
  size_t       datalen,
               argi    = 3,
               retry   = 0;
  SetData    * old_set = NULL,
             * set     = NULL,
               tmp[ 2 ];
  MsgCtx     * msg     = NULL;
  MsgCtxBuf    tmpm;
  uint32_t     n       = 0;
  SetStatus    sstatus = SET_OK;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      if ( ( flags & DO_SREM ) != 0 ) /* no data to move or remove */
        return EXEC_SEND_ZERO;
      if ( ( flags & DO_SMOVE ) != 0 && ctx.argn == 1 )
        return EXEC_ABORT_SEND_ZERO;
      if ( ( flags & DO_SADD ) != 0 ) {
        count = this->argc; /* set by alloc_size() */
        ndata = 2 + arglen; /* determine length of all sadd args */
        for ( size_t i = 3; i < this->argc; i++ ) {
          const char * tmparg;
          size_t       tmplen;
          if ( ! this->msg.get_arg( i, tmparg, tmplen ) )
            return ERR_BAD_ARGS;
          ndata += 2 + tmplen;
        }
      }
      else {
    erase_existing_data:;
        count = 2; /* SMOVE to destination not yet initialized */
        ndata = 2;
        if ( ( flags & DO_SMOVE ) != 0 )
          ndata += arglen;
      }
      datalen = SetData::alloc_size( count, ndata );
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        set = new ( &tmp[ n++%2 ]) SetData( data, datalen );
        set->init( count, ndata );
      }
      if ( 0 ) {
        fallthrough;
    case KEY_OK:
        if ( ctx.type != MD_SET ) {
          if ( ( flags & DO_SMOVE ) != 0 && ctx.argn == 2 ) {
            this->kctx.set_type( MD_SET ); /* XXX redis does not allow this */
            goto erase_existing_data;
          }
          if ( ctx.type != MD_NODATA )
            return ERR_BAD_TYPE;
        }
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus == KEY_OK ) {
          set = new ( &tmp[ n++%2 ] ) SetData( data, datalen );
          set->open();
        }
      }
      if ( set != NULL ) {
        for (;;) {
          if ( old_set != NULL ) {
            old_set->copy( *set );
            ctx.kstatus = this->kctx.load( *msg ); /* swap new and old */
            if ( ctx.kstatus != KEY_OK )
              break;
            old_set = NULL;
          }
        set_next_value:;
          switch ( flags & ( DO_SADD | DO_SREM | DO_SMOVE ) ) {
            case DO_SMOVE:
              if ( ctx.argn == 1 ) { /* src */
                sstatus = set->srem( arg, arglen, pos );
                if ( sstatus == SET_OK )
                  return EXEC_OK;
                return EXEC_ABORT_SEND_ZERO;
              }
              /* dest */
              fallthrough;
            case DO_SADD:
              sstatus = set->sadd( arg, arglen, pos );
              if ( sstatus == SET_UPDATED )
                ctx.ival++;
              break;
            case DO_SREM:
              sstatus = set->srem( arg, arglen, pos );
              if ( sstatus == SET_OK )
                ctx.ival++;
              break;
          }
          if ( sstatus != SET_FULL ) {
            if ( ( flags & DO_SMOVE ) == 0 ) {
              if ( this->argc > argi ) {
                if ( ! this->msg.get_arg( argi++, arg, arglen ) )
                  return ERR_BAD_ARGS;
                pos.init( arg, arglen );
                goto set_next_value;
              }
              return EXEC_SEND_INT;
            }
            else { /* DO_SMOVE */
              return EXEC_SEND_ONE;
            }
          }
          count = 2;
          ndata = arglen + 1 + retry;
          retry += 16;
          datalen = set->resize_size( count, ndata );
          msg = new ( tmpm ) MsgCtx( this->kctx.ht, this->kctx.thr_ctx );
          msg->set_key( ctx.kbuf );
          msg->set_hash( ctx.hash1, ctx.hash2 );
          ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );
          if ( ctx.kstatus != KEY_OK )
            break;
          old_set = set;
          set = new ( (void *) &tmp[ n++%2 ] ) SetData( data, datalen );
          set->init( count, ndata );
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_smultiscan( RedisKeyCtx &ctx,  int flags,  ScanArgs *sa )
{
  /* SSCAN key cursor [MATCH pat] */
  /* SMEMBERS key */
  /* SRANDMEMBER key [COUNT] */
  /* SPOP key [COUNT] */
  SetBits bits;
  int64_t ival = 1;

  if ( ( flags & ( DO_SRANDMEMBER | DO_SPOP ) ) != 0 ) {
    if ( this->argc > 2 )
      if ( ! this->msg.get_arg( 2, ival ) )
        return ERR_BAD_ARGS;
  }
  StreamBuf::BufQueue q( this->strm );
  void    * data;
  size_t    datalen,
            count   = 0,
            itemcnt = 0,
            i       = ( sa != NULL && sa->pos > 0 ? sa->pos : 0 ),
            maxcnt  = ( sa != NULL ? sa->maxcnt : 0 );
  uint8_t   lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t  llen;
  ListVal   lv;
  SetStatus sstatus;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW: /* SPOP is a write op */
    case KEY_NOT_FOUND:
      break;
    case KEY_OK:
      if ( ctx.type == MD_NODATA )
        break;
      if ( ctx.type != MD_SET )
        return ERR_BAD_TYPE;

      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        SetData set( data, datalen );
        set.open( lhdr, llen );
        if ( (count = set.hcount()) == 0 )
          break;

        bool use_bits = false;
        if ( ( flags & ( DO_SRANDMEMBER | DO_SPOP ) ) != 0 ) {
          if ( (size_t) ival >= count ) {
            ival = count;
          }
          else { /* generate random set elements */
            rand::xoroshiro128plus &r = this->kctx.thr_ctx.rng;
            if ( ival == 1 ) {
              i = r.next() & set.index_mask;
              while ( i >= count )
                i >>= 1;
              maxcnt = 1;
            }
            else {
              bool flip = false;
              if ( (size_t) ival > count / 2 ) {
                ival = count - ival; /* invert bits if more than half */
                flip = true;
              }
              for (;;) {
                uint64_t n = r.next();
                for ( int k = 0; k < 64; k += 16 ) {
                  size_t m = n & set.index_mask;
                  while ( m >= count )
                    m >>= 1;
                  if ( ! bits.test_set( m ) )
                    if ( --ival == 0 )
                      goto break_loop;
                  n >>= 16;
                }
              }
            break_loop:;
              if ( flip )
                bits.flip( count );
              i = 0;
              use_bits = true;
            }
          }
        }

        for (;;) {
          if ( i >= count || ( maxcnt != 0 && itemcnt >= maxcnt ) )
            break;
          if ( use_bits ) {
            if ( ! bits.next( i ) )
              break;
          }
          /* set index 1 -> count */
          sstatus = (SetStatus) set.lindex( ++i, lv );
          if ( sstatus != SET_OK )
            break;
          /* match wildcard */
          if ( ( flags & DO_SSCAN ) != 0 ) {
            if ( sa->re != NULL ) {
              char buf[ 256 ];
              void * subj;
              size_t subjlen;
              bool is_alloced = false;
              subjlen = lv.unitary( subj, buf, sizeof( buf ), is_alloced );
              int rc = pcre2_match( sa->re, (PCRE2_SPTR8) subj, subjlen,
                                    0, 0, sa->md, 0 );
              if ( is_alloced )
                ::free( subj );
              if ( rc < 1 )
                continue;
            }
          }
          if ( q.append_string( lv.data, lv.sz, lv.data2, lv.sz2 ) == 0 )
            return ERR_ALLOC_FAIL;
          itemcnt++;
        }
        q.finish_tail();
        /* pop the items in case of SPOP */
        if ( ( flags & DO_SPOP ) != 0 && itemcnt > 0 ) {
          if ( itemcnt == 1 ) {
            set.spopn( i );
          }
          else if ( itemcnt == count ) {
            set.spopall();
          }
          else {
            /* pop the members in reverse so that the index is correct */
            for ( size_t j = count; ; ) {
              if ( ! bits.prev( j ) )
                break;
              set.spopn( j + 1 );
            }
          }
        }
        break;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }

  if ( ( flags & DO_SSCAN ) != 0 )
    q.prepend_cursor_array( i == count ? 0 : i, itemcnt );
  else
    q.prepend_array( itemcnt );

  if ( ( flags & DO_SPOP ) != 0 ||
       (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
    this->strm.append_iov( q );
    return EXEC_OK;
  }
  return ERR_KV_STATUS;
}

ExecStatus
RedisExec::exec_ssetop( RedisKeyCtx &ctx,  int flags )
{
  void   * data;
  uint64_t datalen;
  size_t   src = 0;

  /* SDIFFSTORE dest key [key ...] */
  /* SINTERSTORE dest key [key ...] */
  /* SUNIONSTORE dest key [key  ...] */
  if ( ( flags & ( DO_SUNIONSTORE | DO_SINTERSTORE | DO_SDIFFSTORE ) ) != 0 ) {
    /* if is the dest key */
    if ( ctx.argn == 1 ) {
      /* wait for src keys */
      if ( this->key_cnt != this->key_done + 1 )
        return EXEC_DEPENDS;
      src = 1; /* src starts at key[1] */
    }
  }
  /* fetch the src data */
  if ( src == 0 ) {
    /* SDIFF key [key ...] */
    /* SINTER key [key ...] */
    /* SUNION key [key  ...] */
    data    = NULL;
    datalen = 0;
    switch ( this->exec_key_fetch( ctx, true ) ) {
      case KEY_NOT_FOUND: if ( 0 ) {
      case KEY_OK:
          if ( ctx.type != MD_SET )
            return ERR_BAD_TYPE;
          if ( ctx.type != MD_NODATA ) {
            ctx.kstatus = this->kctx.value( &data, datalen );
            if ( ctx.kstatus != KEY_OK )
              return ERR_KV_STATUS;
          }
        }
        if ( datalen == 0 ) {
          data    = (void *) mt_list;
          datalen = sizeof( mt_list );
        }
        if ( ! this->save_data( ctx, data, datalen ) )
          return ERR_ALLOC_FAIL;
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
          if ( this->key_cnt == this->key_done + 1 )
            break;
          return EXEC_OK;
        }
      fallthrough;
      default: return ERR_KV_STATUS;
    }
  }
  /* if only one key argument, not an array */
  if ( this->key_cnt == 1 ) {
    data    = this->key->part->data( 0 );
    datalen = this->key->part->size;
  }
  else { /* src is 0 when not stored, src is 1 when have dest key */
    data    = this->keys[ src ]->part->data( 0 );
    datalen = this->keys[ src ]->part->size;
  }

  SetData   tmp[ 2 ];
  SetData * set,
          * old_set;
  size_t    i,
            retry = 0,
            ndata,
            count;
  int       n = 0;
  SetStatus sstatus;
  /* first set */
  set = new ( (void *) &tmp[ n++%2 ] ) SetData( data, datalen );
  set->open();
  /* merge key[ src+1 ] into key[ src ], if more than one source */
  for ( i = src+1; i < this->key_cnt; i++ ) {
    SetData set2( this->keys[ i ]->part->data( 0 ),
                  this->keys[ i ]->part->size );
    MergeCtx ctx;
    SetStatus sstat = SET_OK;
    ctx.init();
    set2.open();
    for (;;) {
      switch ( flags & ( DO_SUNION | DO_SUNIONSTORE |
                         DO_SINTER | DO_SINTERSTORE |
                         DO_SDIFF  | DO_SDIFFSTORE ) ) {
        case DO_SUNION:
        case DO_SUNIONSTORE:
          sstat = set->sunion( set2, ctx );
          break;
        case DO_SINTER:
        case DO_SINTERSTORE:
          sstat = set->sinter( set2, ctx );
          break;
        case DO_SDIFF:
        case DO_SDIFFSTORE:
          sstat = set->sdiff( set2, ctx );
          break;
      }
      if ( sstat == SET_OK ) /* merge successful */
        break;
      /* SET_FULL, merge out of space, resize set */
      count = set2.count() + 2;
      ndata = set2.data_len() + retry;
      retry += 16;
      datalen = set->resize_size( count, ndata );
      data = this->strm.alloc( datalen );
      if ( data == NULL )
        return ERR_ALLOC_FAIL;
      old_set = set;
      set = new ( (void *) &tmp[ n++%2 ] ) SetData( data, datalen );
      set->init( count, ndata );
      old_set->copy( *set );
    }
  }
  /* if no dest key, return the result */
  if ( src == 0 ) {
    StreamBuf::BufQueue q( this->strm );
    ListVal lv;
    size_t  itemcnt = 0;
    count = set->count();
    for ( i = 0; ; ) {
      if ( i == count )
        break;
      /* set index 1 -> count */
      sstatus = (SetStatus) set->lindex( ++i, lv );
      if ( sstatus != SET_OK )
        break;
      if ( q.append_string( lv.data, lv.sz, lv.data2, lv.sz2 ) == 0 )
        return ERR_ALLOC_FAIL;
      itemcnt++;
    }
    q.finish_tail();
    q.prepend_array( itemcnt );
    this->strm.append_iov( q );
    return EXEC_OK;
  }
  /* cmd has a dest key, store the result and return the set member count */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, set->size );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, set->listp, set->size );
        ctx.ival = set->hcount();
        return EXEC_SEND_INT;
      }
    fallthrough;
    default: return ERR_KV_STATUS;
  }
}

