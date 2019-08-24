#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;

void
RedisExec::discard_multi( void )
{
  if ( this->multi != NULL ) {
    this->multi->wrk.release_all();
    delete this->multi;
    this->multi = NULL;
  }
}

ExecStatus
RedisExec::exec_discard( void )
{
  if ( this->multi == NULL )
    return ERR_BAD_DISCARD;
  this->discard_multi();
  return EXEC_SEND_OK;
}

ExecStatus
RedisExec::exec_exec( void )
{
  RedisMultiExec * mul;
  ExecStatus       status;
  char           * str;
  size_t           sz;

  if ( (mul = this->multi) == NULL )
    return ERR_BAD_EXEC;
  this->multi = NULL;

  if ( mul->watch_count > 0 ) {
    status = EXEC_SEND_NULL;
    for ( RedisWatchList *wl = mul->watch_list.hd; wl != NULL; wl = wl->next ) {
      switch( this->kctx.fetch( &this->wrk, wl->pos ) ) {
        case KEY_OK:
          if ( wl->serial != this->kctx.serial ||
               wl->hash1  != this->kctx.key ||
               wl->hash2  != this->kctx.key2 )
            goto watch_failed;
          break;
        default:
          if ( wl->serial != 0 )
            goto watch_failed;
          break;
      }
    }
  }
  str = this->strm.alloc( 32 );
  if ( str == NULL )
    return ERR_ALLOC_FAIL;
  str[ 0 ] = '*';
  sz = 1 + uint_to_str( mul->msg_count, &str[ 1 ] );
  this->strm.sz += crlf( str, sz );
  for ( RedisMsgList * ml = mul->msg_list.hd; ml != NULL; ml = ml->next ) {
    this->msg.ref( *ml->msg );
    if ( (status = this->exec( NULL, NULL )) == EXEC_OK )
      if ( this->strm.alloc_fail )
        status = ERR_ALLOC_FAIL;
    switch ( status ) {
      case EXEC_SETUP_OK:
        this->exec_run_to_completion();
        if ( ! this->strm.alloc_fail )
          break; 
        status = ERR_ALLOC_FAIL;
        /* fall through */
      default:
        this->send_err( status );
        break;
      case EXEC_QUIT:
      case EXEC_DEBUG:
        break;
    }
  }
  status = EXEC_OK;
watch_failed:;
  this->multi = mul;
  this->discard_multi();
  return status;
}

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( kv::BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( kv::BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

bool
RedisExec::make_multi( void )
{
  void * p = aligned_malloc( sizeof( RedisMultiExec ) );
  if ( p == NULL )
    return false;
  this->multi = new ( p ) RedisMultiExec();
  return true;
}

ExecStatus
RedisExec::exec_multi( void )
{
  if ( this->multi != NULL ) {
    if ( this->multi->multi_start )
      return ERR_BAD_MULTI;
  }
  else {
    if ( ! this->make_multi() )
      return ERR_ALLOC_FAIL;
  }
  this->multi->multi_start = true;
  return EXEC_SEND_OK;
}

bool
RedisMultiExec::append_msg( RedisMsg &msg )
{
  void *p = this->wrk.alloc( sizeof( RedisMsgList ) );
  if ( p == NULL )
    return false;
  RedisMsgList * ml = new ( p ) RedisMsgList();
  ml->msg = msg.dup( this->wrk );
  if ( ml->msg == NULL )
    return false;
  this->msg_list.push_tl( ml );
  this->msg_count++;
  return true;
}

bool
RedisMultiExec::append_watch( uint64_t h1,  uint64_t h2,  uint64_t sn,
                              uint64_t pos )
{
  void *p = this->wrk.alloc( sizeof( RedisWatchList ) );
  if ( p == NULL )
    return false;
  RedisWatchList * wl = new ( p ) RedisWatchList( h1, h2, sn, pos );
  this->watch_list.push_tl( wl );
  this->watch_count++;
  return true;
}

ExecStatus
RedisExec::exec_unwatch( void )
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
RedisExec::exec_watch( EvKeyCtx &ctx )
{
  uint64_t sn = 0, k1, k2, pos;

  switch ( this->exec_key_fetch( ctx, true ) ) {
    case KEY_OK:
      sn = this->kctx.serial;
      /* FALLTHRU */
    case KEY_NOT_FOUND:
      k1 = this->kctx.key;
      k2 = this->kctx.key2;
      pos = this->kctx.pos;
      break;
    default:
      return ERR_KV_STATUS;
  }
  if ( this->multi == NULL ) {
    if ( ! this->make_multi() )
      return ERR_ALLOC_FAIL;
  }
  if ( ! this->multi->append_watch( k1, k2, sn, pos ) )
    return ERR_ALLOC_FAIL;
  return EXEC_SEND_OK;
}

