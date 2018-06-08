#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;
using namespace kv;

ExecStatus
RedisExec::exec_append( RedisKeyCtx &ctx )
{
  void       * data    = NULL,
             * ndata;
  const char * value;
  uint64_t     data_sz = 0;
  size_t       valuelen;

  if ( ! this->msg.get_arg( 2, value, valuelen ) ) /* APPEND KEY VALUE */
    return EXEC_BAD_ARGS;

  switch ( ctx.kstatus ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, data_sz );
      if ( ctx.kstatus == KEY_OK ) {
    case KEY_IS_NEW:
        ctx.ival    = data_sz + valuelen;
        ctx.kstatus = this->kctx.resize( &ndata, ctx.ival );
        if ( ctx.kstatus == KEY_OK ) {
          if ( data_sz > 0 && ndata != data )
            ::memcpy( ndata, data, data_sz );
          ::memcpy( &((uint8_t *) ndata)[ data_sz ], value, valuelen );
        }
        return EXEC_SEND_INT;
      }
      /* fall through */
    default:
      return EXEC_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_bitcount( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_bitfield( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_bitop( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_bitpos( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_decr( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_decrby( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_get( RedisKeyCtx &ctx )
{
  static char not_found[] = "$-1\r\n";
  char   * str;
  void   * data;
  uint64_t data_sz;
  size_t   sz;

  if ( ctx.kstatus == KEY_OK )
    ctx.kstatus = this->kctx.value( &data, data_sz );
  switch ( ctx.kstatus ) {
    case KEY_OK:
      sz = 16 + data_sz;
      str = this->strm.alloc( sz );
      if ( str != NULL ) {
        str[ 0 ] = '$';
        sz = 1 + uint64_to_string( data_sz, &str[ 1 ] );
        str[ sz ] = '\r'; str[ sz + 1 ] = '\n';
        ::memcpy( &str[ sz + 2 ], data, data_sz );
        sz += 2 + data_sz;
        str[ sz ] = '\r'; str[ sz + 1 ] = '\n';
        this->strm.sz += sz + 2;
      }
      return EXEC_OK;

    case KEY_NOT_FOUND:
      this->strm.append( not_found, 5 );
      return EXEC_OK;
    default:
      return EXEC_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_getbit( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_getrange( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_getset( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_incr( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_incrby( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_incrbyfloat( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_mget( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_mset( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_msetnx( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_psetex( RedisKeyCtx &ctx )
{
  int64_t  ival;
  uint64_t ns;

  if ( ! this->msg.get_arg( 2, ival ) )
    return EXEC_BAD_ARGS;
  ns = (uint64_t) ival * 1000 * 1000;
  if ( ns < this->kctx.ht.hdr.current_stamp )
    ns += this->kctx.ht.hdr.current_stamp;
  this->kctx.expire_ns = ns;

  return this->exec_set_value( ctx, 3 ); /* PSET KEY MS VALUE */
}

ExecStatus
RedisExec::exec_set( RedisKeyCtx &ctx )
{
  const char * op;
  size_t       oplen;
  int64_t      ival;
  uint64_t     ns;

  this->kctx.expire_ns = 0;
  if ( this->argc > 3 ) {
    for ( int i = 3; i < (int) this->argc; ) {
      if ( ! this->msg.get_arg( i, op, oplen ) ||
           ( oplen != 2 || toupper( op[ 1 ] ) != 'X' ) )
        return EXEC_BAD_ARGS;
      switch ( toupper( op[ 0 ] ) ) {
        case 'E':                     /* SET KEY VALUE [EX secs] */
          if ( ! this->msg.get_arg( i + 1, ival ) )
            return EXEC_BAD_ARGS;
          ns = (uint64_t) ival * 1000 * 1000 * 1000;
          if ( ns < this->kctx.ht.hdr.current_stamp )
            ns += this->kctx.ht.hdr.current_stamp;
          this->kctx.expire_ns = ns;
          i += 2;
          break;
        case 'P':                     /* SET KEY VALUE [PX ms] */
          if ( ! this->msg.get_arg( i + 1, ival ) )
            return EXEC_BAD_ARGS;
          ns = (uint64_t) ival * 1000 * 1000;
          if ( ns < this->kctx.ht.hdr.current_stamp )
            ns += this->kctx.ht.hdr.current_stamp;
          this->kctx.expire_ns = ns;
          i += 2;
          break;
        case 'N':                     /* SET KEY VALUE [NX] */
          if ( ctx.kstatus == KEY_OK ) /* key already exists */
            return EXEC_SEND_NIL;
          i += 1;
          break;
        case 'X':                     /* SET KEY VALUE [XX] */
          if ( ctx.kstatus == KEY_IS_NEW ) /* key doesn't exist */
            return EXEC_SEND_NIL;
          i += 1;
          break;
        default:
          return EXEC_BAD_ARGS;
      }
    }
  }
  return this->exec_set_value( ctx, 2 );
}

ExecStatus
RedisExec::exec_setbit( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_setex( RedisKeyCtx &ctx )
{
  int64_t  ival;
  uint64_t ns;

  if ( ! this->msg.get_arg( 2, ival ) )
    return EXEC_BAD_ARGS;
  ns = (uint64_t) ival * 1000 * 1000 * 1000;
  if ( ns < this->kctx.ht.hdr.current_stamp )
    ns += this->kctx.ht.hdr.current_stamp;
  this->kctx.expire_ns = ns;

  return this->exec_set_value( ctx, 3 ); /* SETEX KEY SECS VALUE */
}

ExecStatus
RedisExec::exec_setnx( RedisKeyCtx &ctx )
{
  if ( ctx.kstatus == KEY_OK ) /* key already exists */
    return EXEC_SEND_NIL;

  return this->exec_set_value( ctx, 2 ); /* SETNX KEY VALUE */
}

ExecStatus
RedisExec::exec_setrange( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_strlen( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

