#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_transaction.h>

using namespace rai;
using namespace ds;
using namespace kv;

void
RedisExec::discard_multi( void ) noexcept
{
  if ( this->multi != NULL ) {
    if ( this->multi->mm_lock != NULL )
      this->multi_release_lock();
    this->multi->wrk.release_all();
    delete this->multi;
    this->multi = NULL;
    this->cmd_state &= ~( CMD_STATE_MULTI_QUEUED | CMD_STATE_EXEC_MULTI );
  }
}

ExecStatus
RedisExec::exec_discard( void ) noexcept
{
  if ( this->multi == NULL )
    return ERR_BAD_DISCARD;
  this->discard_multi();
  return EXEC_SEND_OK;
}

void
RedisExec::multi_key_fetch( EvKeyCtx &ctx,  bool force_read ) noexcept
{
  RedisMultiExec & mul  = *this->multi;
  KeyCtx         * kptr = mul.mm_iter->kptr[ ctx.key_idx ];

  ::memcpy( (void *) &this->kctx, kptr, sizeof( this->kctx ) );
  this->key = &ctx;
  if ( kptr == &mul.mm_iter->karr[ ctx.key_idx ] ) {
    if ( ( this->cmd_flags & CMD_READ_FLAG ) != 0 || force_read ) {
      ctx.flags |= EKF_IS_READ_ONLY;
      if ( ctx.kstatus == KEY_IS_NEW )
        ctx.kstatus = KEY_NOT_FOUND;
      else if ( ctx.kstatus == KEY_OK && this->kctx.is_expired() ) {
        /* if expired, release entry and find it as new element */
        this->kctx.expire();
        ctx.flags |= EKF_IS_EXPIRED | EKF_KEYSPACE_EVENT;
        ctx.kstatus = KEY_NOT_FOUND;
      }
    }
    else if ( ( this->cmd_flags & CMD_WRITE_FLAG ) != 0 ) {
      if ( ctx.kstatus == KEY_OK && this->kctx.is_expired() ) {
        this->kctx.expire();
        ctx.flags  |= EKF_IS_EXPIRED | EKF_KEYSPACE_EVENT | EKF_IS_NEW;
        ctx.kstatus = KEY_IS_NEW;
      }
      else {
        ctx.flags |= ( ( ctx.kstatus == KEY_IS_NEW ) ? EKF_IS_NEW : 0 );
      }
      ctx.flags &= ~EKF_IS_READ_ONLY;
      mul.wr_kctx = kptr;
    }
    else {
      ctx.kstatus = KEY_NO_VALUE;
      ctx.status  = ERR_BAD_CMD;
      ctx.flags  |= EKF_IS_READ_ONLY;
    }
  }
  else {
    EvKeyCtx * key = mul.get_dup_key( ctx, true );
    /* if status changed, key could be updated or deleted,
       KEY_IS_NEW -> KEY_OK,  : created
       KEY_OK -> KEY_IS_NEW   : deleted */
    if ( key->kstatus == KEY_OK ) {
      /* if key didn't exist before, can be created */
      if ( ctx.kstatus == KEY_IS_NEW && ( key->flags & EKF_IS_NEW ) != 0 )
        ctx.kstatus = key->kstatus;
      /* if pop or del, caused a keyspace delete, then it is new again */
      if ( ctx.kstatus == KEY_OK && ( key->flags & EKF_KEYSPACE_DEL ) != 0 )
        ctx.kstatus = KEY_IS_NEW;
    }
    /* if read only, change is_new to not_found */
    if ( ( this->cmd_flags & CMD_READ_FLAG ) != 0 || force_read ) {
      ctx.flags |= EKF_IS_READ_ONLY;
      if ( ctx.kstatus == KEY_IS_NEW )
        ctx.kstatus = KEY_NOT_FOUND;
    }
    /* if writing, change not_found to new */
    else {
      if ( ctx.kstatus == KEY_NOT_FOUND )
        ctx.kstatus = KEY_IS_NEW;
      ctx.flags |= ( ( ctx.kstatus == KEY_IS_NEW ) ? EKF_IS_NEW : 0 );
      ctx.flags &= ~EKF_IS_READ_ONLY;
      mul.wr_kctx = kptr;
    }
  }
}

bool
RedisExec::multi_try_lock( void ) noexcept
{
  RedisMultiExec & mul = *this->multi;
  mul.mm_lock = NULL;
  for ( RedisMultiMsg * mm = mul.msg_list.hd; mm != NULL; mm = mm->next ) {
    mul.mm_lock = mm;
    for ( mm->n = 0; mm->n < mm->key_count; mm->n++ ) {
      KeyCtx   * kp  = mm->kptr[ mm->n ];
      EvKeyCtx * key = mm->keys[ mm->n ];
      if ( kp == &mm->karr[ mm->n ] ) {
        kp->set( KEYCTX_MULTI_KEY_ACQUIRE );
        kp->wrk = &mul.wrk;
        key->kstatus = kp->acquire();
        if ( key->kstatus == KEY_BUSY )
          return false;
      }
      else {
        key->kstatus = mul.get_dup_key( *key, false )->kstatus;
      }
    }
  }
  return true;
}

void
RedisExec::multi_release_lock( void ) noexcept
{
  RedisMultiExec & mul = *this->multi;
  if ( mul.mm_lock == NULL )
    return;
  for ( RedisMultiMsg * mm = mul.mm_lock; ; ) {
    while ( mm->n > 0 ) {
      mm->n -= 1;
      if ( mm->kptr[ mm->n ] == &mm->karr[ mm->n ] )
        mm->karr[ mm->n ].release();
    }
    mm = mm->back;
    if ( mm == NULL )
      break;
    mm->n = mm->key_count;
  }
  mul.mm_lock = NULL;
}

ExecStatus
RedisExec::exec_exec( void ) noexcept
{
  RedisMultiExec * mul;
  RedisMultiMsg  * mm;
  ExecStatus       status;
  char           * str;
  size_t           sz, cnt,
                   i, j;

  if ( (mul = this->multi) == NULL )
    return ERR_BAD_EXEC;
  if ( mul->multi_abort ) {
    status = ERR_ABORT_TRANS;
    goto abort_trans;
  }
  this->cmd_state &= ~CMD_STATE_MULTI_QUEUED; /* move to execution */
  this->cmd_state |= CMD_STATE_EXEC_MULTI;
  /* lock all the keys */
  while ( ! this->multi_try_lock() )
    this->multi_release_lock(); /* release and try again */
  /* send an array of results */
  cnt = mul->msg_count;
  mm  = mul->msg_list.hd;
  if ( mul->watch_count > 0 ) {
    status = EXEC_SEND_NULL; /* status when failed */
    if ( cnt == 0 )
      goto abort_trans;
    cnt -= 1;
    /* has a watch list, check that vars didn't change */
    i = 0;
    for ( RedisWatchList *wl = mul->watch_list.hd; wl != NULL;
          wl = wl->next ) {
      switch( wl->key.kstatus ) {
        case KEY_OK:
          if ( wl->serial != mm->kptr[ i ]->serial ||
               wl->key.hash1 != mm->kptr[ i ]->key ||
               wl->key.hash2 != mm->kptr[ i ]->key2 )
            goto abort_trans;
          break;
        default: /* KEY_IS_NEW */
          if ( wl->serial != 0 )
            goto abort_trans;
          break;
      }
      i++;
    }
    mm = mm->next; /* watch success */
  }
  str = this->strm.alloc( 32 );
  if ( str == NULL ) {
    status = ERR_ALLOC_FAIL;
    goto abort_trans;
  }
  str[ 0 ] = '*';
  sz = 1 + uint_to_str( cnt, &str[ 1 ] );
  this->strm.sz += crlf( str, sz );
  /* execute commands in transaction */
  for ( ; mm != NULL; mm = mm->next ) {
    /* setup new command */
    mul->mm_iter = mm;
    this->msg.ref( *mm->msg );
    this->setup_cmd( cmd_db[ mm->cmd ] );
    this->cmd      = mm->cmd;
    this->key_cnt  = mm->key_count;
    this->key_done = 0;
    this->keys     = mm->keys;
    this->key      = NULL;
    /* run command with keys */
    if ( mm->key_count >= 1 ) {
      j = 0;
      /* continue running until all keys are finished */
      for ( i = 0; ; ) {
        if ( this->keys[ i ]->status == EXEC_CONTINUE ||
             this->keys[ i ]->status == EXEC_DEPENDS ) {
          mul->wr_kctx = NULL;
          ExecStatus x = this->exec_key_continue( *this->keys[ i ] );
          /* the kctx is reused for each key, copy state after operation */
          if ( mul->wr_kctx != NULL )
            mul->wr_kctx->copy_acquire_state( this->kctx );
          if ( x == EXEC_SUCCESS )
            break;
          j = 0;
        }
        else if ( ++j == this->key_cnt )
          break;
        if ( ++i == this->key_cnt )
          i = 0;
      }
    }
    /* run command that doesn't have keys */
    else {
      this->send_status( this->exec_nokeys(), KEY_OK );
    }
    if ( this->strm.alloc_fail )
      this->send_status( ERR_ALLOC_FAIL, KEY_OK );
  }
  status = EXEC_OK;
abort_trans:;
  this->discard_multi();
  return status;
}

ExecStatus
RedisExec::multi_queued( EvSocket *svc ) noexcept
{
  RedisMultiExec & mul = *this->multi;
  size_t           key_count = 0;

  if ( this->first > 0 )
    key_count = this->calc_key_count();
  size_t sz = key_count * ( sizeof( EvKeyCtx * ) +
                            sizeof( KeyCtx ) +
                            sizeof( KeyCtx * ) ) + sizeof( RedisMultiMsg );
  void * p  = mul.wrk.alloc( sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  RedisMsg      * m  = this->msg.dup( mul.wrk );
  RedisMultiMsg * mm = new ( p ) RedisMultiMsg( m, key_count, this->cmd );

  mul.msg_list.push_tl( mm );
  mul.msg_count++;

  if ( key_count > 0 ) {
    KeyCtx * karray;
    mm->keys = (EvKeyCtx **) (void *) &mm[ 1 ];
    mm->kptr = (KeyCtx **) (void *) &mm->keys[ key_count ];
    karray   = (KeyCtx *) (void *) &mm->kptr[ key_count ];
    mm->karr = karray;

    const char * key;
    size_t       keylen;
    int          i = this->first;
    size_t       j = 0;

    mm->key_count = 0;
    do {
      if ( ! this->msg.get_arg( i, key, keylen ) )
        return ERR_BAD_ARGS;
      p = mul.wrk.alloc( EvKeyCtx::size( keylen ) );
      if ( p == NULL )
        return ERR_ALLOC_FAIL;
      EvKeyCtx * ctx = new ( p ) EvKeyCtx( this->kctx.ht, svc, key, keylen, i,
                                           j, this->hs );
      ctx->status = EXEC_CONTINUE;
      new ( &karray[ j ] ) KeyCtx( this->kctx );
      mm->keys[ j ] = ctx;
      mm->kptr[ j ] = &karray[ j ];
      ctx->set( karray[ j ] );

      if ( mul.test_set_filter( ctx->hash1 ) ) {
        KeyCtx * kp = mul.get_dup_kctx( *ctx );
        if ( kp != NULL )
          mm->kptr[ j ] = kp;
      }
      mm->key_count = ++j;
    } while ( this->next_key( i ) );
    if ( j != key_count )
      return ERR_BAD_ARGS;
  }
  return EXEC_OK;
}

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

KeyCtx *
RedisMultiExec::get_dup_kctx( EvKeyCtx &ctx ) const noexcept
{
  for ( const RedisMultiMsg *p = this->msg_list.hd; p != NULL; p = p->next ) {
    for ( size_t k = 0; k < p->key_count; k++ ) {
      if ( p->keys[ k ]->hash1 == ctx.hash1 &&
           p->keys[ k ]->hash2 == ctx.hash2 ) {
        return &p->karr[ k ];
      }
    }
  }
  return NULL;
}

EvKeyCtx *
RedisMultiExec::get_dup_key( EvKeyCtx &ctx,  bool post_exec ) const noexcept
{
  EvKeyCtx * ctx2 = NULL;
  for ( const RedisMultiMsg *p = this->msg_list.hd; p != NULL; p = p->next ) {
    for ( size_t k = 0; k < p->key_count; k++ ) {
      if ( p->keys[ k ]->hash1 == ctx.hash1 &&
           p->keys[ k ]->hash2 == ctx.hash2 ) {
        if ( ! post_exec )
          return p->keys[ k ];
        if ( &ctx == p->keys[ k ] ) {
          if ( ctx2 == NULL )
            return p->keys[ k ];
          return ctx2;
        }
        /* find the last key that was written to, if any */
        if ( ( p->keys[ k ]->flags & EKF_IS_READ_ONLY ) == 0 ||
             ( p->keys[ k ]->flags & EKF_IS_EXPIRED ) != 0 )
          ctx2 = p->keys[ k ];
      }
    }
  }
  return ctx2;
}

bool
RedisExec::make_multi( void ) noexcept
{
  void * p = aligned_malloc( sizeof( RedisMultiExec ) );
  if ( p == NULL )
    return false;
  this->multi = new ( p ) RedisMultiExec();
  return true;
}

ExecStatus
RedisExec::exec_multi( void ) noexcept
{
  if ( this->multi != NULL ) {
    if ( this->multi->multi_start )
      return ERR_BAD_MULTI;
  }
  else {
    if ( ! this->make_multi() )
      return ERR_ALLOC_FAIL;
  }
  this->cmd_state |= CMD_STATE_MULTI_QUEUED;

  RedisMultiExec & mul = *this->multi;
  mul.multi_start = true;
  if ( mul.watch_count != 0 ) {
    size_t sz = mul.watch_count * ( sizeof( EvKeyCtx * ) +
                                    sizeof( KeyCtx ) +
                                    sizeof( KeyCtx * ) );
    void * p  = mul.wrk.alloc( sz );
    if ( p == NULL )
      return ERR_ALLOC_FAIL;

    RedisMultiMsg & mm = mul.watch_msg;
    KeyCtx * karray;
    mm.keys = (EvKeyCtx **) p;
    mm.kptr = (KeyCtx **) (void *) &mm.keys[ mul.watch_count ];
    karray  = (KeyCtx *) (void *) &mm.kptr[ mul.watch_count ];
    mm.karr = karray;

    mul.msg_list.push_hd( &mm ); /* first stmt, check watch list */
    mul.msg_count++;

    size_t j = 0;
    for ( RedisWatchList * wl = mul.watch_list.hd; wl != NULL; wl = wl->next ) {
      new ( &karray[ j ] ) KeyCtx( this->kctx );
      mm.keys[ j ] = &wl->key;
      mm.kptr[ j ] = &karray[ j ];
      wl->key.set( karray[ j ] );

      if ( mul.test_set_filter( wl->key.hash1 ) ) {
        KeyCtx * kp = mul.get_dup_kctx( wl->key );
        if ( kp != NULL )
          mm.kptr[ j ] = kp;
      }
      mm.key_count = ++j;
    }
  }
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_unwatch( void ) noexcept
{
  if ( this->multi != NULL ) {
    this->multi->watch_list.init();
    this->multi->watch_count = 0;
    if ( this->multi->msg_count == 0 && ! this->multi->multi_start )
      this->discard_multi();
  }
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_watch( EvKeyCtx &ctx ) noexcept
{
  uint64_t sn = 0, pos = ZOMBIE64;

  switch ( this->exec_key_fetch( ctx, true ) ) {
    case KEY_OK:
      sn  = this->kctx.serial;
      pos = this->kctx.pos;
      break;
    case KEY_NOT_FOUND:
      break;
    default:
      return ERR_KV_STATUS;
  }
  if ( this->multi == NULL ) {
    if ( ! this->make_multi() )
      return ERR_ALLOC_FAIL;
  }

  RedisMultiExec & mul = *this->multi;
  void *p = mul.wrk.alloc( sizeof( RedisWatchList ) +
                           EvKeyCtx::size( ctx.kbuf.keylen ) );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  RedisWatchList * wl = new ( p ) RedisWatchList( sn, pos, ctx );
  mul.watch_list.push_tl( wl );
  mul.watch_count++;
  return EXEC_SEND_OK;
}
