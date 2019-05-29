#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <raikv/util.h>
#include <raikv/key_hash.h>
#include <raids/redis_exec.h>
#include <raimd/md_hll.h>

using namespace rai;
using namespace ds;
using namespace md;

ExecStatus
RedisExec::exec_pfadd( RedisKeyCtx &ctx )
{
  HyperLogLog * hll     = NULL;
  void        * data    = NULL;
  size_t        datalen = sizeof( HyperLogLog );

  HyperLogLog::ginit();
  /* PFADD key elem [elem ...] */
  switch ( this->get_key_write( ctx, MD_HYPERLOGLOG ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      if ( ! HLLMsg::is_hllmsg( data, 0, datalen, 0 ) )
        return ERR_BAD_TYPE;
      hll = (HyperLogLog *) data;
      break;

    case KEY_IS_NEW:
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      hll = (HyperLogLog *) data;
      hll->init();
      break;

    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
  ctx.ival = 0;
  for ( size_t i = 2; i < this->argc; i++ ) {
    const char * value;
    size_t       valuelen;
    uint64_t     h1 = 0, h2 = 0;
    if ( ! this->msg.get_arg( i, value, valuelen ) )
      return ERR_BAD_ARGS;
     kv_hash_aes128( value, valuelen, &h1, &h2 );
     ctx.ival += hll->add( h1 ^ h2 );
  }
  if ( ctx.ival > 0 )
    return EXEC_SEND_ONE;
  return EXEC_SEND_ZERO;
}

ExecStatus
RedisExec::exec_pfcount( RedisKeyCtx &ctx )
{
  void   * data;
  uint64_t datalen;

  HyperLogLog::ginit();
  /* PFCOUNT key [key ...] */
  switch ( this->get_key_read( ctx, MD_HYPERLOGLOG ) ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_OK:        break;
  }
  ctx.kstatus = this->kctx.value( &data, datalen );
  if ( ctx.kstatus == KEY_OK ) {
    /* if only one key, estimate and return */
    if ( this->key_cnt == 1 ) {
      if ( ! HLLMsg::is_hllmsg( data, 0, datalen, 0 ) )
        return ERR_BAD_TYPE;
      HyperLogLog * hll = (HyperLogLog *) data;
      ctx.ival = (int64_t) ( hll->estimate() + 0.5 );
      if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
        return EXEC_SEND_INT;
    }
    /* if last key, merge and estimate */
    else if ( this->key_cnt == this->key_done + 1 ) {
      HyperLogLog hll;
      if ( ! HLLMsg::is_hllmsg( data, 0, datalen, 0 ) )
        return ERR_BAD_TYPE;
      ::memcpy( &hll, data, datalen );

      if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
        for ( size_t i = 0; i < this->key_cnt; i++ ) {
          if ( this->keys[ i ]->part != NULL ) {
            hll.merge( *(HyperLogLog *) this->keys[ i ]->part->data( 0 ) );
          }
        }
        ctx.ival = (int64_t) ( hll.estimate() + 0.5 );
        return EXEC_SEND_INT;
      }
    }
    /* multiple keys, save copies until the last key */
    else {
      if ( ! HLLMsg::is_hllmsg( data, 0, datalen, 0 ) )
        return ERR_BAD_TYPE;
      if ( ! this->save_data( ctx, data, datalen, 0 ) )
        return ERR_ALLOC_FAIL;
      if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
        return EXEC_OK;
    }
  }
  return ERR_KV_STATUS;
}

ExecStatus
RedisExec::exec_pfmerge( RedisKeyCtx &ctx )
{
  void   * data;
  uint64_t datalen;

  HyperLogLog::ginit();
  /* PFMERGE dkey skey [skey ...] */
  if ( ctx.argn == 1 && this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;
  /* if not dest key, fetch set */
  if ( ctx.argn != 1 ) {
    data    = NULL;
    datalen = 0;
    switch ( this->get_key_read( ctx, MD_HYPERLOGLOG ) ) {

      case KEY_OK:
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus != KEY_OK )
          return ERR_KV_STATUS;
        if ( ! HLLMsg::is_hllmsg( data, 0, datalen, 0 ) )
          return ERR_BAD_TYPE;
        /* FALLTHRU */

      case KEY_NOT_FOUND:
        if ( datalen == 0 )
          return EXEC_OK;
        if ( ! this->save_data( ctx, data, datalen, 0 ) )
          return ERR_ALLOC_FAIL;
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_OK;
        /* FALLTHRU */

      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
    }
  }
  HyperLogLog hll;
  bool first = true;
  /* merge keys together into dest */
  for ( size_t i = 1; i < this->key_cnt; i++ ) {
    if ( this->keys[ i ]->part != NULL ) {
      if ( first ) {
        ::memcpy( &hll, this->keys[ i ]->part->data( 0 ),
                  this->keys[ i ]->part->size );
        first = false;
      }
      else {
        hll.merge( *(HyperLogLog *) this->keys[ i ]->part->data( 0 ) );
      }
    }
  }
  if ( first )
    hll.init();

  switch ( this->get_key_write( ctx, MD_HYPERLOGLOG ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, sizeof( HyperLogLog ) );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, &hll, sizeof( HyperLogLog ) );
        return EXEC_SEND_OK;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}
