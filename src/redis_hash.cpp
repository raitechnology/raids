#define __STDC_WANT_DEC_FP__ 1
#include <stdio.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/msg_ctx.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_hash.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

enum {
  DO_HEXISTS      = 1<<1,
  DO_HGET         = 1<<2,
  DO_HGETALL      = 1<<3,
  DO_HKEYS        = 1<<4,
  DO_HLEN         = 1<<5,
  DO_HMGET        = 1<<5,
  DO_HVALS        = 1<<7,
  DO_HSTRLEN      = 1<<8,
  DO_HSCAN        = 1<<9,
  DO_HINCRBY      = 1<<10,
  DO_HINCRBYFLOAT = 1<<11,
  DO_HMSET        = 1<<12,
  DO_HSET         = 1<<13,
  DO_HSETNX       = 1<<14
};

ExecStatus
RedisExec::exec_hdel( RedisKeyCtx &ctx )
{
  void       * data;
  size_t       datalen;
  const char * arg;
  size_t       arglen, i = 2;

  /* HDEL key field [field ...] */
  if ( ! this->msg.get_arg( i, arg, arglen ) )
    return ERR_BAD_ARGS;
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      return EXEC_SEND_ZERO;
    case KEY_OK:
      if ( ctx.type != MD_HASH && ctx.type != MD_NODATA )
        return ERR_BAD_TYPE;
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        HashData hash( data, datalen );
        HashStatus hstat = HASH_OK;
        hash.open();
        ctx.ival = 0;
        for (;;) {
          hstat = hash.hdel( arg, arglen );
          if ( hstat == HASH_OK )
            ctx.ival++;
          if ( ++i == this->argc )
            break;
          if ( ! this->msg.get_arg( i, arg, arglen ) )
            return ERR_BAD_ARGS;
        }
        return EXEC_SEND_INT;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_hexists( RedisKeyCtx &ctx )
{
  /* HEXISTS key field */
  return this->exec_hread( ctx, DO_HEXISTS );
}

ExecStatus
RedisExec::exec_hget( RedisKeyCtx &ctx )
{
  /* HGET key field */
  return this->exec_hread( ctx, DO_HGET );
}

ExecStatus
RedisExec::exec_hgetall( RedisKeyCtx &ctx )
{
  /* HGETALL key */
  return this->exec_hmultiread( ctx, DO_HGETALL );
}

ExecStatus
RedisExec::exec_hincrby( RedisKeyCtx &ctx )
{
  /* HINCRBY key field int */
  return this->exec_hwrite( ctx, DO_HINCRBY );
}

ExecStatus
RedisExec::exec_hincrbyfloat( RedisKeyCtx &ctx )
{
  /* HINCRBYFLOAT key field float */
  return this->exec_hwrite( ctx, DO_HINCRBYFLOAT );
}

ExecStatus
RedisExec::exec_hkeys( RedisKeyCtx &ctx )
{
  /* HKEYS key */
  return this->exec_hmultiread( ctx, DO_HKEYS );
}

ExecStatus
RedisExec::exec_hlen( RedisKeyCtx &ctx )
{
  /* HLEN key */
  return this->exec_hread( ctx, DO_HLEN );
}

ExecStatus
RedisExec::exec_hmget( RedisKeyCtx &ctx )
{
  /* HMGET key field [field ...] */
  return this->exec_hmultiread( ctx, DO_HMGET );
}

ExecStatus
RedisExec::exec_hmset( RedisKeyCtx &ctx )
{
  /* HMSET key field val [field val ...] */
  return this->exec_hwrite( ctx, DO_HMSET );
}

ExecStatus
RedisExec::exec_hset( RedisKeyCtx &ctx )
{
  /* HSET key field val */
  return this->exec_hwrite( ctx, DO_HSET );
}

ExecStatus
RedisExec::exec_hsetnx( RedisKeyCtx &ctx )
{
  /* HSETNX key field val */
  return this->exec_hwrite( ctx, DO_HSETNX );
}

ExecStatus
RedisExec::exec_hstrlen( RedisKeyCtx &ctx )
{
  /* HSTRLEN key field */
  return this->exec_hread( ctx, DO_HSTRLEN );
}

ExecStatus
RedisExec::exec_hvals( RedisKeyCtx &ctx )
{
  /* HVALS key */
  return this->exec_hmultiread( ctx, DO_HVALS );
}

namespace rai {
namespace ds {
struct HScanArgs {
  int64_t            pos,
                     maxcnt;
  pcre2_code       * re;
  pcre2_match_data * md;
  HScanArgs() : pos( 0 ), maxcnt( 10 ), re( 0 ), md( 0 ) {}
};
}
}

ExecStatus
RedisExec::exec_hscan( RedisKeyCtx &ctx )
{
  /* HSCAN key cursor [MATCH pat] */
  uint8_t      buf[ 1024 ],
             * bf = buf;
  size_t       erroff,
               blen = sizeof( buf );
  int          rc,
               error;
  const char * pattern = NULL;
  size_t       patlen  = 0;
  HScanArgs    hs;

  /* HSCAN key cursor [MATCH pat] */
  if ( ! this->msg.get_arg( 2, hs.pos ) )
    return ERR_BAD_ARGS;
  for ( size_t i = 3; i < this->argc; i += 2 ) {
    switch ( this->msg.match_arg( i, "match", 5,
                                     "count", 5, NULL ) ) {
      case 1:
        if ( ! this->msg.get_arg( i+1, pattern, patlen ) )
          return ERR_BAD_ARGS;
        break;
      case 2:
        if ( ! this->msg.get_arg( i+1, hs.maxcnt ) )
          return ERR_BAD_ARGS;
        break;
      default:
        return ERR_BAD_ARGS;
    }
  }
  if ( pattern != NULL ) {
    rc = pcre2_pattern_convert( (PCRE2_SPTR8) pattern, patlen,
                                PCRE2_CONVERT_GLOB_NO_WILD_SEPARATOR,
                                &bf, &blen, 0 );
    if ( rc != 0 )
      return ERR_BAD_ARGS;
    hs.re = pcre2_compile( bf, blen, 0, &error, &erroff, 0 );
    if ( hs.re == NULL )
      return ERR_BAD_ARGS;
    hs.md = pcre2_match_data_create_from_pattern( hs.re, NULL );
    if ( hs.md == NULL ) {
      pcre2_code_free( hs.re );
      return ERR_BAD_ARGS;
    }
  }
  ExecStatus status = this->exec_hmultiread( ctx, DO_HSCAN, &hs );
  if ( hs.re != NULL ) {
    pcre2_match_data_free( hs.md );
    pcre2_code_free( hs.re );
  }
  return status;
}

ExecStatus
RedisExec::exec_hmultiread( RedisKeyCtx &ctx,  int flags,  HScanArgs *hs )
{
  /* HSCAN key cursor [MATCH pat] */
  /* HMGET key value [value...] */
  /* HGETALL/HKEYS/HVALS key */
  StreamBuf::BufList
           * hd     = NULL,
           * tl     = NULL;
  char     * keybuf = NULL;
  void     * data;
  size_t     datalen,
             count   = 0,
             itemlen,
             buflen  = 0,
             used    = 0,
             itemcnt = 0,
             argi    = 2,
             i       = ( hs != NULL && hs->pos > 0 ? hs->pos : 1 ),
             maxcnt  = ( hs != NULL ? hs->maxcnt * 2 : 0 );
  HashVal    kv;
  HashStatus hstatus;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      break;
    case KEY_OK:
      if ( ctx.type != MD_HASH )
        return ERR_BAD_TYPE;
      if ( ctx.type == MD_NODATA )
        break;

      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        HashData hash( data, datalen );
        hash.open();
        count = hash.count();

        for (;;) {
          if ( ( flags & ( DO_HGETALL | DO_HKEYS | DO_HVALS |
                           DO_HSCAN ) ) != 0 ) {
            if ( i >= count || ( maxcnt != 0 && itemcnt >= maxcnt ) )
              break;
            hstatus = hash.hindex( i++, kv );
            if ( ( flags & DO_HSCAN ) != 0 ) {
              if ( hs->re != NULL ) {
                int rc = pcre2_match( hs->re, (PCRE2_SPTR8) kv.key, kv.keylen,
                                      0, 0, hs->md, 0 );
                if ( rc < 1 )
                  continue;
              }
            }
          }
          else {
            const char * key;
            size_t       keylen;
            if ( argi >= this->argc )
              break;
            if ( ! this->msg.get_arg( argi++, key, keylen ) )
              return ERR_BAD_ARGS;
            hstatus = hash.hget( key, keylen, kv );
          }
          itemlen = 0;
          if ( hstatus == HASH_OK ) {
            if ( ( flags & ( DO_HGETALL | DO_HKEYS ) ) != 0 )
              itemlen += kv.keylen;
            if ( ( flags & ( DO_HGETALL | DO_HVALS | DO_HMGET ) ) != 0 )
              itemlen += kv.sz + kv.sz2;
          }
          if ( itemlen + 48 > buflen - used ) {
            if ( tl != NULL )
              tl->used = used;
            used   = 0;
            buflen = 500;
            if ( buflen < (size_t) itemlen + 48 )
              buflen = itemlen + 48;
            if ( buflen > 30000 ) {
              if ( (ctx.kstatus = this->kctx.validate_value()) != KEY_OK )
                return ERR_KV_STATUS;
            }
            tl = this->strm.alloc_buf_list( hd, tl, buflen );
            if ( tl == NULL )
              return ERR_ALLOC_FAIL;
            keybuf = tl->buf( 0 );
          }

          if ( ( flags & ( DO_HGETALL | DO_HKEYS | DO_HSCAN ) ) != 0 ) {
            keybuf[ used ] = '$';
            if ( hstatus == HASH_OK ) {
              used += 1 + RedisMsg::int_to_str( kv.keylen, &keybuf[ used + 1 ]);
              used = crlf( keybuf, used );
              ::memcpy( &keybuf[ used ], kv.key, kv.keylen );
              used = crlf( keybuf, used + kv.keylen );
            }
            else {
              keybuf[ used + 1 ] = '-';
              keybuf[ used + 2 ] = '1';
              used = crlf( keybuf, used + 3 );
            }
            itemcnt++;
          }
          if ( ( flags & ( DO_HGETALL | DO_HVALS | DO_HMGET |
                           DO_HSCAN ) ) != 0 ) {
            keybuf[ used ] = '$';
            if ( hstatus == HASH_OK ) {
              itemlen = kv.sz + kv.sz2;
              used += 1 + RedisMsg::int_to_str( itemlen, &keybuf[ used + 1 ] );
              used = crlf( keybuf, used );
              ::memcpy( &keybuf[ used ], kv.data, kv.sz );
              if ( kv.sz2 > 0 )
                ::memcpy( &keybuf[ used + kv.sz ], kv.data2, kv.sz2 );
              used = crlf( keybuf, used + itemlen );
            }
            else {
              keybuf[ used + 1 ] = '-';
              keybuf[ used + 2 ] = '1';
              used = crlf( keybuf, used + 3 );
            }
            itemcnt++;
          }
        }
        if ( tl != NULL )
          tl->used = used;
        break;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }

  char *hdr = (char *) this->strm.alloc_temp( 32 );
  used = 0;
  if ( ( flags & DO_HSCAN ) != 0 ) {
    if ( i < count ) { /* next cursor */
    /* construct [cursor, [key, ...]] */
      ::strcpy( hdr, "*2\r\n$" );
      size_t len = RedisMsg::uint_digits( i );
      used  = 5 + RedisMsg::uint_to_str( len, &hdr[ 5 ] );
      used  = crlf( hdr, used );
      used += RedisMsg::uint_to_str( i, &hdr[ used ], len );
      used  = crlf( hdr, used );
    }
    else {
      ::strcpy( hdr, "*2\r\n$1\r\n0\r\n" );
      used = 11;
    }
  }
  hdr[ used ] = '*';
  used += 1 + RedisMsg::int_to_str( itemcnt, &hdr[ used + 1 ] );
  used = crlf( hdr, used );
  if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
    this->strm.append_iov( hdr, used );
    while ( hd != NULL ) {
      if ( hd->used > 0 )
        this->strm.append_iov( hd->buf( 0 ), hd->used );
      hd = hd->next;
    }
    return EXEC_OK;
  }
  return ERR_KV_STATUS;
}

ExecStatus
RedisExec::exec_hread( RedisKeyCtx &ctx,  int flags )
{
  void       * data;
  size_t       datalen;
  const char * arg    = NULL;
  size_t       arglen = 0;

  if ( ( flags & ( DO_HEXISTS | DO_HGET | DO_HSTRLEN ) ) != 0 ) {
    /* HEXISTS/HGET/HSTRLEN key field */
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
  }
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      switch ( flags & ( DO_HEXISTS | DO_HGET | DO_HLEN | DO_HSTRLEN ) ) {
        case DO_HEXISTS: fallthrough;
        case DO_HLEN:    fallthrough;
        case DO_HSTRLEN: return EXEC_SEND_ZERO;
        case DO_HGET:    return EXEC_SEND_NIL;
      }
    case KEY_OK:
      if ( ctx.type != MD_HASH && ctx.type != MD_NODATA )
        return ERR_BAD_TYPE;
      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        HashData   hash( data, datalen );
        ListVal    lv;
        size_t     sz = 0;
        HashStatus hstat;
        ExecStatus status;
        hash.open();

        switch ( flags & ( DO_HEXISTS | DO_HGET | DO_HLEN | DO_HSTRLEN ) ) {
          case DO_HEXISTS:
            if ( hash.hexists( arg, arglen ) == HASH_OK )
              status = EXEC_SEND_ONE;
            else
              status = EXEC_SEND_ZERO;
            break;
          case DO_HLEN:
            if ( (ctx.ival = hash.count()) > 0 )
              ctx.ival -= 1;
            status = EXEC_SEND_INT;
            break;
          case DO_HSTRLEN:
          case DO_HGET:
            hstat = hash.hget( arg, arglen, lv );
            if ( flags == DO_HGET ) {
              if ( hstat == HASH_OK ) {
                sz = lv.sz + lv.sz2;
                if ( sz > 30000 ) {
                  if ( (ctx.kstatus = this->kctx.validate_value()) != KEY_OK )
                    return ERR_KV_STATUS;
                }
                sz = this->send_concat_string( lv.data, lv.sz,
                                               lv.data2, lv.sz2 );
                status = EXEC_OK;
              }
              else {
                status = EXEC_SEND_NIL;
              }
            }
            else {
              if ( hstat == HASH_OK ) {
                ctx.ival = lv.sz + lv.sz2;
                status = EXEC_SEND_INT;
              }
              else {
                status = EXEC_SEND_ZERO;
              }
            }
            break;
          default:
            status = ERR_BAD_CMD;
            break;
        }
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
          this->strm.sz += sz;
          return status;
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_hwrite( RedisKeyCtx &ctx,  int flags )
{
  static char  DDfmt[5] = { '%', 'D', 'D', 'a', 0 };
  void       * data;
  size_t       datalen;
  const char * arg    = NULL;
  size_t       arglen = 0;
  const char * val    = NULL;
  size_t       vallen = 0,
               argi   = 4;

  if ( ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET | DO_HINCRBYFLOAT ) ) != 0 ) {
    /* HSET/HSETNX/HMSET/HINCRBYFLOAT key field value */
    if ( ! this->msg.get_arg( 2, arg, arglen ) ||
         ! this->msg.get_arg( 3, val, vallen ) )
      return ERR_BAD_ARGS;
  }
  else if ( ( flags & DO_HINCRBY ) != 0 ) {
    /* HINCRBY key field ival */
    if ( ! this->msg.get_arg( 2, arg, arglen ) ||
         ! this->msg.get_arg( 3, ctx.ival ) )
      return ERR_BAD_ARGS;
  }
  size_t       count    = 2, /* set by alloc_size() */
               ndata    = vallen;
  HashData   * old_hash = NULL,
             * hash     = NULL,
               tmp[ 2 ];
  MsgCtx     * msg      = NULL;
  MsgCtxBuf    tmpm;
  ListVal      lv;
  FindPos      pos;
  const char * idata;
  char         ibuf[ 64 ],
             * str      = NULL;
  size_t       sz       = 0,
               retry    = 0;
  int64_t      ival     = 0;
  uint32_t     n        = 0;
  HashStatus   hstatus  = HASH_OK;

  switch ( this->exec_key_fetch( ctx ) ) {

    case KEY_IS_NEW:
      datalen = HashData::alloc_size( count, ndata );
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        hash = new ( &tmp[ n++%2 ]) HashData( data, datalen );
        hash->init( count, ndata );
      }
      if ( 0 ) {
        fallthrough;

    case KEY_OK:
        if ( ctx.type != MD_HASH && ctx.type != MD_NODATA )
          return ERR_BAD_TYPE;
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus == KEY_OK ) {
          hash = new ( &tmp[ n++%2 ] ) HashData( data, datalen );
          hash->open();
        }
      }
      if ( hash != NULL ) {
        for (;;) {
          if ( old_hash != NULL ) {
            old_hash->copy( *hash );
            ctx.kstatus = this->kctx.load( *msg ); /* swap new and old */
            if ( ctx.kstatus != KEY_OK )
              break;
            old_hash = NULL;
          }
        set_next_value:; /* HMSET can have multiple kv pairs */
          switch ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET |
                             DO_HINCRBYFLOAT | DO_HINCRBY ) ) {
            case DO_HSET:
            case DO_HMSET:
              hstatus = hash->hset( arg, arglen, val, vallen );
              break;
            case DO_HSETNX:
              hstatus = hash->hsetnx( arg, arglen, val, vallen );
              break;
            case DO_HINCRBY:
              hstatus = hash->hgetpos( arg, arglen, lv, pos );
              ival = ctx.ival;
              if ( hstatus == HASH_OK ) { /* exists */
                sz = lv.sz + lv.sz2;
                if ( lv.sz2 == 0 )
                  idata = (const char *) lv.data;
                else if ( lv.sz == 0 )
                  idata = (const char *) lv.data2;
                else {
                  sz = lv.concat( ibuf, sizeof( ibuf ) );
                  idata = ibuf;
                }
                int64_t jval;
                if ( RedisMsg::str_to_int( idata, sz, jval ) == REDIS_MSG_OK )
                  ival += jval;
              }
              str = this->strm.alloc( 32 );
              str[ 0 ] = ':';
              sz = 1 + RedisMsg::int_to_str( ival, &str[ 1 ] );
              sz = crlf( str, sz );
              hstatus = hash->hsetpos( arg, arglen, &str[ 1 ], sz - 3, pos );
              break;
            case DO_HINCRBYFLOAT: {
              _Decimal128 fp;
              int fvallen;
              hstatus = hash->hgetpos( arg, arglen, lv, pos );
              if ( hstatus == HASH_OK ) { /* exists */
                sz = lv.concat( ibuf, sizeof( ibuf ) - 1 );
                ibuf[ sz ] = '\0';
                fp = ::strtod128( ibuf, NULL );
              }
              else {
                fp = 0.0DL;
              }
              sz = min<size_t>( vallen, sizeof( ibuf ) - 1 );
              ::memcpy( ibuf, val, sz ); ibuf[ sz ] = '\0';
              fp += ::strtod128( ibuf, NULL );
              fvallen = ::snprintf( ibuf, sizeof( ibuf ), DDfmt, fp );
              sz = 32 + fvallen * 2;
              str = this->strm.alloc( sz );
              str[ 0 ] = '$';
              sz = 1 + RedisMsg::int_to_str( fvallen, &str[ 1 ] );
              sz = crlf( str, sz ); 
              ::memcpy( &str[ sz ], ibuf, fvallen );
              sz = crlf( str, sz + fvallen );
              hstatus = hash->hsetpos( arg, arglen, ibuf, fvallen, pos );
              break;
            }
          }
          if ( hstatus != HASH_FULL ) { /* no realloc */
            switch ( flags & ( DO_HSET | DO_HSETNX | DO_HMSET |
                               DO_HINCRBYFLOAT | DO_HINCRBY ) ) {
              case DO_HSET:
                if ( hstatus == HASH_OK )
                  return EXEC_SEND_ONE; /* new item indicated with 1 */
                return EXEC_SEND_ZERO; /* HASH_UPDATED, replaced old item */
              case DO_HSETNX:
                if ( hstatus == HASH_EXISTS )
                  return EXEC_SEND_ZERO; /* did not update, already exists */
                return EXEC_SEND_ONE; /* new item */
              case DO_HMSET:
                if ( this->argc > argi ) {
                  if ( ! this->msg.get_arg( argi, arg, arglen ) ||
                       ! this->msg.get_arg( argi+1, val, vallen ) )
                    return ERR_BAD_ARGS;
                  argi += 2;
                  goto set_next_value;
                }
                return EXEC_SEND_OK; /* send OK status */
              case DO_HINCRBYFLOAT:
              case DO_HINCRBY:
                this->strm.sz += sz;
                return EXEC_OK;
            }
          }
          count = 2;
          ndata = arglen + vallen + 1 + retry;
          retry += 16;
          datalen = hash->resize_size( count, ndata );
          msg = new ( tmpm ) MsgCtx( this->kctx.ht, this->kctx.thr_ctx );
          msg->set_key( ctx.kbuf );
          msg->set_hash( ctx.hash1, ctx.hash2 );
          ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );
          if ( ctx.kstatus != KEY_OK )
            break;
          old_hash = hash;
          hash = new ( (void *) &tmp[ n++%2 ] ) HashData( data, datalen );
          hash->init( count, ndata );
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }

  return EXEC_OK;
}
