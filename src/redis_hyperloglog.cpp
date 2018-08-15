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

  static void ginit( void ) {
    HLLCountEst14::ginit();
  }

  int add( uint64_t h1,  uint64_t h2 ) {
    int cnt = 0;
    cnt += this->he[ 0 ].add( h1 );
    cnt += this->he[ 1 ].add( h2 );
    cnt += this->he[ 2 ].add( ( h1 >> 32 ) | ( h2 << 32 ) );
    cnt += this->he[ 3 ].add( ( h1 << 32 ) | ( h2 >> 32 ) );
    cnt += this->he[ 4 ].add( ( h1 >> 48 ) | ( h2 << 16 ) );
    cnt += this->he[ 5 ].add( ( h1 << 16 ) | ( h2 >> 48 ) );
    return cnt > 0 ? 1 : 0;
  }

  double estimate( void ) const {
    bool isl;
    double e = 0;
    for ( int i = 0; i < 6; i++ )
      e += this->he[ i ].estimate( isl );
    return e / 6.0;
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
  const char * value;
  size_t       valuelen;
  uint64_t     h1, h2;

  HLLSix::ginit();
  /* PFADD key elem [elem ...] */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        six = (HLLSix *) data;
        six->init();
      }
      if ( 0 ) {
    case KEY_OK:
        if ( ctx.type != MD_HYPERLOGLOG && ctx.type != MD_NODATA )
          return ERR_BAD_TYPE;
        ctx.kstatus = this->kctx.value( &data, datalen );
        six = (HLLSix *) data;
        if ( ! six->valid() )
          return ERR_BAD_TYPE;
      }
      if ( ctx.kstatus == KEY_OK ) {
        ctx.ival = 0;
        for ( size_t i = 2; i < this->argc; i++ ) {
          if ( ! this->msg.get_arg( i, value, valuelen ) )
            return ERR_BAD_ARGS;
           h1 = h2 = 0;
           kv_hash_aes128( value, valuelen, &h1, &h2 );
           ctx.ival += six->add( h1, h2 );
        }
        if ( ctx.ival > 0 )
          return EXEC_SEND_ONE;
        return EXEC_SEND_ZERO;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_pfcount( RedisKeyCtx &ctx )
{
  void   * data;
  uint64_t datalen;

  HLLSix::ginit();
  /* PFCOUNT key [key ...] */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK:
      if ( ctx.type != MD_HYPERLOGLOG ) {
        if ( ctx.type == MD_NODATA )
          return EXEC_SEND_ZERO;
        return ERR_BAD_TYPE;
      }
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        if ( this->key_cnt == 1 ) {
          HLLSix * six = (HLLSix *) data;
          if ( ! six->valid() )
            return ERR_BAD_TYPE;
          ctx.ival = (int64_t) ( six->estimate() + 0.5 );
          if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
            return EXEC_SEND_INT;
        }
        else if ( this->key_cnt == this->key_done + 1 ) { /* last one */
          HLLSix six;
          six.copy( data, datalen );
          if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
            for ( size_t i = 1; i < this->key_cnt; i++ ) {
              if ( this->keys[ i ]->part != NULL ) {
                six.merge( *(HLLSix *) this->keys[ i ]->part->data( 0 ) );
              }
            }
            ctx.ival = (int64_t) ( six.estimate() + 0.5 );
            if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
              return EXEC_SEND_INT;
          }
        }
        else {
          if ( ! this->save_data( ctx, data, datalen ) )
            return ERR_ALLOC_FAIL;
          if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
            return EXEC_OK;
        }
      }
      fallthrough;
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
  }
}

ExecStatus
RedisExec::exec_pfmerge( RedisKeyCtx &ctx )
{
  void   * data;
  uint64_t datalen;

  HLLSix::ginit();
  /* PFMERGE dkey skey [skey ...] */
  if ( ctx.argn == 1 && this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;
  /* if not dest key, fetch set */
  if ( ctx.argn != 1 ) {
    data    = NULL;
    datalen = 0;
    switch ( this->exec_key_fetch( ctx, true ) ) {
      case KEY_NOT_FOUND: if ( 0 ) {
      case KEY_OK:
          if ( ctx.type != MD_HYPERLOGLOG )
            return ERR_BAD_TYPE;
          if ( ctx.type != MD_NODATA ) {
            ctx.kstatus = this->kctx.value( &data, datalen );
            if ( ctx.kstatus != KEY_OK )
              return ERR_KV_STATUS;
          }
        }
        if ( datalen == 0 )
          return EXEC_OK;
        if ( ! this->save_data( ctx, data, datalen ) )
          return ERR_ALLOC_FAIL;
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_OK;
      fallthrough;
      default: return ERR_KV_STATUS;
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

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, sizeof( HLLSix ) );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, &six, sizeof( HLLSix ) );
        return EXEC_SEND_OK;
      }
    fallthrough;
    default: return ERR_KV_STATUS;
  }
}

