#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <raikv/util.h>
#include <raikv/key_hash.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#define HLL_GLOBAL_VARS
#include <raids/redis_hyperloglog.h>

using namespace rai;
using namespace ds;
#define fallthrough __attribute__ ((fallthrough))

namespace rai {
namespace ds {
typedef HyperLogLog<14> HLLCountEst14;

struct HLLSix {
  HLLCountEst14 he[ 6 ];

  void init( void ) {
    for ( int i = 0; i < 6; i++ )
      this->he[ i ].init( 0 );
  }

  static void globals_init( void ) {
    HLLCountEst14::ginit(); /* static log() value tables */
  }

  int add( uint64_t h1,  uint64_t h2 ) {
    int cnt = 0;
    cnt += this->he[ 0 ].add( h1 );
    cnt += this->he[ 1 ].add( h2 );
    cnt += this->he[ 2 ].add( ( h1 >> 32 ) | ( h2 << 32 ) );
    cnt += this->he[ 3 ].add( ( h1 << 32 ) | ( h2 >> 32 ) );
    cnt += this->he[ 4 ].add( ( h1 >> 48 ) | ( h2 << 16 ) );
    cnt += this->he[ 5 ].add( ( h1 << 16 ) | ( h2 >> 48 ) );
    return cnt > 0 ? 1 : 0; /* if any one is unique, then hash is unique */
  }

  double estimate( void ) const {
    bool isl;
    double e = 0;
    for ( int i = 0; i < 6; i++ )
      e += this->he[ i ].estimate( isl );
    return e / 6.0; /* average all counts */
  }

  void merge( const HLLSix &six ) {
    for ( int i = 0; i < 6; i++ )
      this->he[ i ].merge( six.he[ i ] );
  }

  bool valid( void ) const {
    size_t sum = 0;
    for ( int i = 0; i < 6; i++ )
      sum += this->he[ i ].size();
    return sum == sizeof( *this );
  }

  void copy( const void *data,  size_t size ) {
    if ( size == sizeof( *this ) ) {
      ::memcpy( this, data, sizeof( *this ) );
      if ( this->valid() )
        return;
    }
    this->init();
  }
};
}
}

ExecStatus
RedisExec::exec_pfadd( RedisKeyCtx &ctx )
{
  HLLSix     * six     = NULL;
  void       * data    = NULL;
  size_t       datalen = sizeof( HLLSix );

  HLLSix::globals_init();
  /* PFADD key elem [elem ...] */
  switch ( this->get_key_write( ctx, MD_HYPERLOGLOG ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      six = (HLLSix *) data;
      if ( ! six->valid() )
        return ERR_BAD_TYPE;
      break;

    case KEY_IS_NEW:
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      six = (HLLSix *) data;
      six->init();
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
     ctx.ival += six->add( h1, h2 );
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

  HLLSix::globals_init();
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
      HLLSix * six = (HLLSix *) data;
      if ( ! six->valid() )
        return ERR_BAD_TYPE;
      ctx.ival = (int64_t) ( six->estimate() + 0.5 );
      if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
        return EXEC_SEND_INT;
    }
    /* if last key, merge and estimate */
    else if ( this->key_cnt == this->key_done + 1 ) {
      HLLSix six;
      six.copy( data, datalen );
      if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
        for ( size_t i = 0; i < this->key_cnt; i++ ) {
          if ( this->keys[ i ]->part != NULL ) {
            six.merge( *(HLLSix *) this->keys[ i ]->part->data( 0 ) );
          }
        }
        ctx.ival = (int64_t) ( six.estimate() + 0.5 );
        return EXEC_SEND_INT;
      }
    }
    /* multiple keys, save copies until the last key */
    else {
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

  HLLSix::globals_init();
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
        fallthrough;

      case KEY_NOT_FOUND:
        if ( datalen == 0 )
          return EXEC_OK;
        if ( ! this->save_data( ctx, data, datalen, 0 ) )
          return ERR_ALLOC_FAIL;
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_OK;
        fallthrough;

      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
    }
  }
  HLLSix six;
  bool first = true;
  /* merge keys together into dest */
  for ( size_t i = 1; i < this->key_cnt; i++ ) {
    if ( this->keys[ i ]->part != NULL ) {
      if ( first ) {
        six.copy( this->keys[ i ]->part->data( 0 ),
                  this->keys[ i ]->part->size );
        first = false;
      }
      else {
        six.merge( *(HLLSix *) this->keys[ i ]->part->data( 0 ) );
      }
    }
  }
  if ( first )
    six.init();

  switch ( this->get_key_write( ctx, MD_HYPERLOGLOG ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, sizeof( HLLSix ) );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, &six, sizeof( HLLSix ) );
        return EXEC_SEND_OK;
      }
      fallthrough;
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}
