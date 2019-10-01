#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raimd/md_types.h>
#include <raimd/md_set.h>
#include <raids/set_bits.h>
#include <raids/exec_list_ctx.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

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
RedisExec::exec_sadd( EvKeyCtx &ctx )
{
  /* SADD key member [member ...] */
  return this->do_swrite( ctx, DO_SADD );
}

ExecStatus
RedisExec::exec_scard( EvKeyCtx &ctx )
{
  /* SCARD key */
  return this->do_sread( ctx, DO_SCARD );
}

ExecStatus
RedisExec::exec_sdiff( EvKeyCtx &ctx )
{
  /* SDIFF key [key ...] */
  return this->do_ssetop( ctx, DO_SDIFF );
}

ExecStatus
RedisExec::exec_sdiffstore( EvKeyCtx &ctx )
{
  /* SDIFFSTORE dest key [key ...] */
  return this->do_ssetop( ctx, DO_SDIFFSTORE );
}

ExecStatus
RedisExec::exec_sinter( EvKeyCtx &ctx )
{
  /* SINTER key [key ...] */
  return this->do_ssetop( ctx, DO_SINTER );
}

ExecStatus
RedisExec::exec_sinterstore( EvKeyCtx &ctx )
{
  /* SINTERSTORE dest key [key ...] */
  return this->do_ssetop( ctx, DO_SINTERSTORE );
}

ExecStatus
RedisExec::exec_sismember( EvKeyCtx &ctx )
{
  /* SISMEMBER key member */
  return this->do_sread( ctx, DO_SISMEMBER );
}

ExecStatus
RedisExec::exec_smembers( EvKeyCtx &ctx )
{
  /* SMEMBERS key */
  return this->do_smultiscan( ctx, DO_SMEMBERS, NULL );
}

ExecStatus
RedisExec::exec_smove( EvKeyCtx &ctx )
{
  /* SMOVE src dest member */
  if ( ctx.argn == 2 && this->key_done == 0 )
    return EXEC_DEPENDS;
  return this->do_swrite( ctx, DO_SMOVE );
}

ExecStatus
RedisExec::exec_spop( EvKeyCtx &ctx )
{
  /* SPOP key [count] */
  return this->do_smultiscan( ctx, DO_SPOP, NULL );
}

ExecStatus
RedisExec::exec_srandmember( EvKeyCtx &ctx )
{
  /* SRANDMEMBER key [count] */
  return this->do_smultiscan( ctx, DO_SRANDMEMBER, NULL );
}

ExecStatus
RedisExec::exec_srem( EvKeyCtx &ctx )
{
  /* SREM key member [member ...] */
  return this->do_swrite( ctx, DO_SREM );
}

ExecStatus
RedisExec::exec_sunion( EvKeyCtx &ctx )
{
  /* SUNION key [key  ...] */
  return this->do_ssetop( ctx, DO_SUNION );
}

ExecStatus
RedisExec::exec_sunionstore( EvKeyCtx &ctx )
{
  /* SUNIONSTORE dest key [key ...] */
  return this->do_ssetop( ctx, DO_SUNIONSTORE );
}

ExecStatus
RedisExec::exec_sscan( EvKeyCtx &ctx )
{
  /* SSCAN key curs [match pat] [count cnt] */
  ScanArgs   sa;
  ExecStatus status;
  if ( (status = this->match_scan_args( sa, 2 )) != EXEC_OK )
    return status;
  status = this->do_smultiscan( ctx, DO_SSCAN, &sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::do_sread( EvKeyCtx &ctx,  int flags )
{
  ExecListCtx<SetData, MD_SET> set( *this, ctx );
  const char * arg    = NULL;
  size_t       arglen = 0;
  HashPos      pos;
  ExecStatus   status = EXEC_OK;

  /* SCARD key */
  /* SISMEMBER key member */
  if ( ( flags & DO_SISMEMBER ) != 0 ) {
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
  switch ( set.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_OK:        break;
  }
  if ( ! set.open_readonly() )
    return ERR_KV_STATUS;
  switch ( flags & ( DO_SCARD | DO_SISMEMBER ) ) {
    case DO_SCARD:
      ctx.ival = set.x->hcount();
      status = EXEC_SEND_INT;
      break;
    case DO_SISMEMBER:
      if ( set.x->sismember( arg, arglen, pos ) == SET_OK )
        status = EXEC_SEND_ONE;
      else
        status = EXEC_SEND_ZERO;
      break;
  }
  if ( ! set.validate_value() )
    return ERR_KV_STATUS;
  return status;
}

ExecStatus
RedisExec::do_swrite( EvKeyCtx &ctx,  int flags )
{
  ExecListCtx<SetData, MD_SET> set( *this, ctx );
  const char * arg     = NULL;
  size_t       arglen  = 0;
  HashPos      pos;
  size_t       count,
               ndata,
               argi    = 3;
  SetStatus    sstatus = SET_OK;

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

  switch ( set.get_key_write() ) {
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
        ndata = 2 + arglen;
      }
      if ( ! set.create( count, ndata ) )
        return ERR_KV_STATUS;
      break;
    case KEY_NO_VALUE:
      if ( ( flags & DO_SMOVE ) != 0 && ctx.argn == 2 )
        goto erase_existing_data; /* XXX redis does not allow this */
      return ERR_BAD_TYPE;
    case KEY_OK:
      if ( ! set.open() )
        return ERR_KV_STATUS;
      break;
    default: return ERR_KV_STATUS;
  }

  for (;;) {
    switch ( flags & ( DO_SADD | DO_SREM | DO_SMOVE ) ) {
      case DO_SMOVE:
        if ( ctx.argn == 1 ) { /* src */
          sstatus = set.x->srem( arg, arglen, pos );
          if ( sstatus == SET_OK ) {
            ctx.flags |= EKF_KEYSPACE_EVENT;
            if ( set.x->hcount() == 0 ) {
              ctx.flags |= EKF_KEYSPACE_DEL;
              if ( ! set.tombstone() )
                return ERR_KV_STATUS;
            }
            return EXEC_OK;
          }
          return EXEC_ABORT_SEND_ZERO;
        }
        /* dest */
        /* FALLTHRU */
      case DO_SADD:
        sstatus = set.x->sadd( arg, arglen, pos );
        if ( sstatus == SET_UPDATED )
          ctx.ival++;
        break;
      case DO_SREM:
        sstatus = set.x->srem( arg, arglen, pos );
        if ( sstatus == SET_OK )
          ctx.ival++;
        break;
    }
    if ( sstatus == SET_FULL ) {
      if ( ! set.realloc( arglen + 2 ) )
        return ERR_KV_STATUS;
      continue;
    }
    if ( ( flags & DO_SMOVE ) != 0 ) {
      ctx.flags |= EKF_KEYSPACE_EVENT;
      return EXEC_SEND_ONE;
    }
    if ( this->argc == argi ) {
      if ( ctx.ival > 0 ) {
        ctx.flags |= EKF_KEYSPACE_EVENT;
        if ( ( flags & DO_SREM ) != 0 ) {
          if ( set.x->hcount() == 0 ) {
            ctx.flags |= EKF_KEYSPACE_DEL;
            if ( ! set.tombstone() )
              return ERR_KV_STATUS;
          }
        }
      }
      return EXEC_SEND_INT;
    }
    if ( ! this->msg.get_arg( argi++, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
}

ExecStatus
RedisExec::do_smultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa )
{
  ExecListCtx<SetData, MD_SET> set( *this, ctx );
  StreamBuf::BufQueue q( this->strm );
  size_t    count   = 0,
            itemcnt = 0,
            i       = ( sa != NULL && sa->pos > 0 ? sa->pos : 0 ),
            maxcnt  = ( sa != NULL ? sa->maxcnt : 0 );
  ListVal   lv;
  SetBits   bits;
  int64_t   ival = 1;
  SetStatus sstatus;
  bool      use_bits = false;

  /* SSCAN key cursor [MATCH pat] */
  /* SMEMBERS key */
  /* SRANDMEMBER key [COUNT] */
  /* SPOP key [COUNT] */
  if ( ( flags & ( DO_SRANDMEMBER | DO_SPOP ) ) != 0 ) {
    if ( this->argc > 2 )
      if ( ! this->msg.get_arg( 2, ival ) )
        return ERR_BAD_ARGS;
  }

  if ( ( flags & DO_SPOP ) != 0 ) {
    switch ( set.get_key_write() ) {
      default:            return ERR_KV_STATUS;
      case KEY_NO_VALUE:  return ERR_BAD_TYPE;
      case KEY_IS_NEW:    goto finished;
      case KEY_OK:        break;
    }
    if ( ! set.open() )
      return ERR_KV_STATUS;
  }
  else {
    switch ( set.get_key_read() ) {
      default:            return ERR_KV_STATUS;
      case KEY_NO_VALUE:  return ERR_BAD_TYPE;
      case KEY_NOT_FOUND: goto finished;
      case KEY_OK:        break;
    }
    if ( ! set.open_readonly() )
      return ERR_KV_STATUS;
  }
  if ( (count = set.x->hcount()) == 0 )
    goto finished;

  if ( ( flags & ( DO_SRANDMEMBER | DO_SPOP ) ) != 0 ) {
    if ( (size_t) ival >= count ) {
      ival = count;
    }
    else { /* generate random set elements */
      rand::xoroshiro128plus &r = this->kctx.thr_ctx.rng;
      if ( ival == 1 ) {
        i = r.next() & set.x->index_mask;
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
            size_t m = n & set.x->index_mask;
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
    sstatus = (SetStatus) set.x->lindex( ++i, lv );
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
  /* pop the items in case of SPOP */
  if ( ( flags & DO_SPOP ) != 0 && itemcnt > 0 ) {
    if ( itemcnt == 1 ) {
      set.x->spopn( i );
    }
    else if ( itemcnt == count ) {
      set.x->spopall();
    }
    else {
      /* pop the members in reverse so that the index is correct */
      for ( size_t j = count; ; ) {
        if ( ! bits.prev( j ) )
          break;
        set.x->spopn( j + 1 );
      }
    }
    ctx.flags |= EKF_KEYSPACE_EVENT;
    if ( set.x->hcount() == 0 ) {
      ctx.flags |= EKF_KEYSPACE_DEL;
      if ( ! set.tombstone() )
        return ERR_KV_STATUS;
    }
  }

finished:;
  q.finish_tail();
  if ( ( flags & DO_SSCAN ) != 0 )
    q.prepend_cursor_array( i == count ? 0 : i, itemcnt );
  else
    q.prepend_array( itemcnt );

  if ( ( flags & DO_SPOP ) == 0 ) {
    if ( ! set.validate_value() )
      return ERR_KV_STATUS;
  }
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::do_ssetop( EvKeyCtx &ctx,  int flags )
{
  void * data;
  size_t datalen,
         src = 0;
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
    ExecListCtx<SetData, MD_SET> set( *this, ctx );
    /* SDIFF key [key ...] */
    /* SINTER key [key ...] */
    /* SUNION key [key  ...] */
    data    = NULL;
    datalen = 0;
    switch ( set.get_key_read() ) {
      case KEY_NO_VALUE:  return ERR_BAD_TYPE;
      case KEY_OK:
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus != KEY_OK )
          return ERR_KV_STATUS;
      /* FALLTHRU */
      case KEY_NOT_FOUND:
        if ( datalen == 0 ) {
          data    = (void *) mt_list; /* empty */
          datalen = sizeof( mt_list );
        }
        if ( ! this->save_data( ctx, data, datalen, 0 ) )
          return ERR_ALLOC_FAIL;
        if ( set.validate_value() ) {
          if ( this->key_cnt == this->key_done + 1 ) /* the last key */
            break;
          return EXEC_OK;
        }
      /* FALLTHRU */
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
  switch ( this->get_key_write( ctx, MD_SET ) ) {
    case KEY_NO_VALUE: /* overwrite key (gen del event?)*/
#if 0
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = MD_SET;
#endif
      /* FALLTHRU */
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, set->size );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, set->listp, set->size );
        ctx.ival   = set->hcount();
        ctx.type   = MD_SET;
        ctx.flags |= EKF_IS_NEW | EKF_KEYSPACE_EVENT;
        return EXEC_SEND_INT;
      }
    /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }
}

