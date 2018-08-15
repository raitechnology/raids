#define __STDC_WANT_DEC_FP__ 1
#include <stdio.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raikv/key_hash.h>
#include <raids/redis_zset.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

enum {
  DO_ZADD             = 1<<0,
  DO_ZCARD            = 1<<1,
  DO_ZCOUNT           = 1<<2,
  DO_ZINCRBY          = 1<<3,
  DO_ZINTERSTORE      = 1<<4,
  DO_ZLEXCOUNT        = 1<<5,
  DO_ZRANGE           = 1<<6,
  DO_ZRANGEBYLEX      = 1<<7,
  DO_ZREVRANGEBYLEX   = 1<<8,
  DO_ZRANGEBYSCORE    = 1<<9,
  DO_ZRANK            = 1<<10,
  DO_ZREM             = 1<<11,
  DO_ZREMRANGEBYLEX   = 1<<12,
  DO_ZREMRANGEBYRANK  = 1<<13,
  DO_ZREMRANGEBYSCORE = 1<<14,
  DO_ZREVRANGE        = 1<<15,
  DO_ZREVRANGEBYSCORE = 1<<16,
  DO_ZREVRANK         = 1<<17,
  DO_ZSCORE           = 1<<18,
  DO_ZUNIONSTORE      = 1<<19,
  DO_ZSCAN            = 1<<20
};

ExecStatus
RedisExec::exec_zadd( RedisKeyCtx &ctx )
{
  /* ZADD key [NX|XX] [CH] [INCR] score mem */
  return this->exec_zwrite( ctx, DO_ZADD );
}

ExecStatus
RedisExec::exec_zcard( RedisKeyCtx &ctx )
{
  /* ZCARD key */
  return this->exec_zread( ctx, DO_ZCARD );
}

ExecStatus
RedisExec::exec_zcount( RedisKeyCtx &ctx )
{
  /* ZCOUNT key min max */
  return this->exec_zread( ctx, DO_ZCOUNT );
}

ExecStatus
RedisExec::exec_zincrby( RedisKeyCtx &ctx )
{
  /* ZINCRBY key incr mem */
  return this->exec_zwrite( ctx, DO_ZINCRBY );
}

ExecStatus
RedisExec::exec_zinterstore( RedisKeyCtx &ctx )
{
  /* ZINTERSTORE dest num key1 keyN */
  return this->exec_zsetop( ctx, DO_ZINTERSTORE );
}

ExecStatus
RedisExec::exec_zlexcount( RedisKeyCtx &ctx )
{
  /* ZLEXCOUNT key min max */
  return this->exec_zread( ctx, DO_ZLEXCOUNT );
}

ExecStatus
RedisExec::exec_zrange( RedisKeyCtx &ctx )
{
  /* ZRANGE key start stop [WITHSCORES] */
  return this->exec_zmultiscan( ctx, DO_ZRANGE, NULL );
}

ExecStatus
RedisExec::exec_zrangebylex( RedisKeyCtx &ctx )
{
  /* ZRANGEBYLEX key min max [LIMIT off cnt] */
  return this->exec_zmultiscan( ctx, DO_ZRANGEBYLEX, NULL );
}

ExecStatus
RedisExec::exec_zrevrangebylex( RedisKeyCtx &ctx )
{
  /* ZREVRANGEBYLEX key min max [LIMIT off cnt] */
  return this->exec_zmultiscan( ctx, DO_ZREVRANGEBYLEX, NULL );
}

ExecStatus
RedisExec::exec_zrangebyscore( RedisKeyCtx &ctx )
{
  /* ZRANGEBYSCORE key min max [WITHSCORES] */
  return this->exec_zmultiscan( ctx, DO_ZRANGEBYSCORE, NULL );
}

ExecStatus
RedisExec::exec_zrank( RedisKeyCtx &ctx )
{
  /* ZRANK key mem */
  return this->exec_zread( ctx, DO_ZRANK );
}

ExecStatus
RedisExec::exec_zrem( RedisKeyCtx &ctx )
{
  /* ZREM key mem [mem] */
  return this->exec_zwrite( ctx, DO_ZREM );
}

ExecStatus
RedisExec::exec_zremrangebylex( RedisKeyCtx &ctx )
{
  /* ZREMRANGEBYLEX key start stop */
  return this->exec_zremrange( ctx, DO_ZREMRANGEBYLEX );
}

ExecStatus
RedisExec::exec_zremrangebyrank( RedisKeyCtx &ctx )
{
  /* ZREMRANGEBYRANK key start stop */
  return this->exec_zremrange( ctx, DO_ZREMRANGEBYRANK );
}

ExecStatus
RedisExec::exec_zremrangebyscore( RedisKeyCtx &ctx )
{
  /* ZREMRANGEBYSCORE key start stop */
  return this->exec_zremrange( ctx, DO_ZREMRANGEBYSCORE );
}

ExecStatus
RedisExec::exec_zrevrange( RedisKeyCtx &ctx )
{
  /* ZREVRANGE key start stop [WITHSCORES] */
  return this->exec_zmultiscan( ctx, DO_ZREVRANGE, NULL );
}

ExecStatus
RedisExec::exec_zrevrangebyscore( RedisKeyCtx &ctx )
{
  /* ZREVRANGEBYLEX key min max [LIMIT off cnt] */
  return this->exec_zmultiscan( ctx, DO_ZREVRANGEBYSCORE, NULL );
}

ExecStatus
RedisExec::exec_zrevrank( RedisKeyCtx &ctx )
{
  /* ZREVRANK key mem */
  return this->exec_zread( ctx, DO_ZREVRANK );
}

ExecStatus
RedisExec::exec_zscore( RedisKeyCtx &ctx )
{
  /* ZSCORE key mem */
  return this->exec_zread( ctx, DO_ZSCORE );
}

ExecStatus
RedisExec::exec_zunionstore( RedisKeyCtx &ctx )
{
  /* ZUNIONSTORE dest num key1 keyN [WEIGHTS for-each key] [AGGR sum|min|max] */
  return this->exec_zsetop( ctx, DO_ZUNIONSTORE );
}

ExecStatus
RedisExec::exec_zscan( RedisKeyCtx &ctx )
{
  /* ZSCAN key curs [MATCH pat] [COUNT cnt] */
  ScanArgs   sa;
  ExecStatus status;
  if ( (status = this->match_scan_args( sa, 2 )) != EXEC_OK )
    return status;
  status = this->exec_zmultiscan( ctx, DO_ZSCAN, &sa );
  this->release_scan_args( sa );
  return status;
}

static ZScore
str_to_score( const char *score,  size_t scorelen )
{
  char scbuf[ 64 ];
  scorelen = min<size_t>( scorelen, sizeof( scbuf ) - 1 );
  ::memcpy( scbuf, score, scorelen );
  scbuf[ scorelen ] = '\0';
  return ::strtod64( scbuf, NULL );
}

ExecStatus
RedisExec::exec_zread( RedisKeyCtx &ctx,  int flags )
{
  static char Dfmt[4] = { '%', 'D', 'a', 0 };
  void       * data;
  size_t       datalen;
  const char * arg    = NULL;
  size_t       arglen = 0;
  const char * lo     = NULL,
             * hi     = NULL;
  size_t       lolen  = 0,
               hilen  = 0;
  uint8_t      lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t     llen;
  HashPos      pos;
  size_t       sz     = 0;
  ExecStatus   status = EXEC_OK;

  /* ZSCORE key mem */
  /* ZRANK key mem */
  /* ZREVRANK key mem */
  if ( ( flags & ( DO_ZSCORE | DO_ZRANK | DO_ZREVRANK ) ) != 0 ) {
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
  /* ZCOUNT key min max */
  /* ZLEXCOUNT key min max */
  else if ( ( flags & ( DO_ZCOUNT | DO_ZLEXCOUNT ) ) != 0 ) {
    if ( ! msg.get_arg( 2, lo, lolen ) )
      return ERR_BAD_ARGS;
    if ( ! msg.get_arg( 3, hi, hilen ) )
      return ERR_BAD_ARGS;
  }
  /* ZCARD key */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      return EXEC_SEND_ZERO;
    case KEY_OK:
      if ( ctx.type != MD_SORTEDSET && ctx.type != MD_NODATA )
        return ERR_BAD_TYPE;
      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        ZSetData zset( data, datalen );
        zset.open( lhdr, llen );
        switch ( flags & ( DO_ZCARD | DO_ZCOUNT | DO_ZSCORE |
                           DO_ZRANK | DO_ZREVRANK | DO_ZLEXCOUNT ) ) {
          case DO_ZCARD:
            ctx.ival = zset.hcount();
            status = EXEC_SEND_INT;
            break;
          case DO_ZSCORE:
          case DO_ZRANK:
          case DO_ZREVRANK: {
            ZScore score;
            if ( zset.zexists( arg, arglen, pos, score ) == ZSET_OK ) {
              /* return the score */
              if ( ( flags & DO_ZSCORE ) != 0 ) {
                char   fpdata[ 64 ];
                size_t fvallen;
                fvallen = ::snprintf( fpdata, sizeof( fpdata ), Dfmt, score );
                sz      = this->send_string( fpdata, fvallen );
                status  = EXEC_OK;
              }
              /* return the rank */
              else {
                if ( ( flags & DO_ZRANK ) != 0 )
                  ctx.ival = pos.i - 1;
                else
                  ctx.ival = zset.hcount() - pos.i;
                status = EXEC_SEND_INT;
              }
            }
            /* nothing there */
            else
              status = EXEC_SEND_NIL;
            break;
          }
          case DO_ZCOUNT:
          case DO_ZLEXCOUNT: {
            size_t i, j;
            bool lo_incl = true,
                 hi_incl = true;
            if ( lo[ 0 ] == '(' ) { lo++; lolen--; lo_incl = false; }
            else if ( lo[ 0 ] == '[' ) { lo++; lolen--; }
            if ( hi[ 0 ] == '(' ) { hi++; hilen--; hi_incl = false; }
            else if ( hi[ 0 ] == '[' ) { hi++; hilen--; }
            /* calculate the member count between scores or lex vals */
            if ( ( flags & DO_ZCOUNT ) != 0 ) {
              ZScore loval = str_to_score( lo, lolen ),
                     hival = str_to_score( hi, hilen ),
                     r3;
              zset.zbsearch( loval, i, lo_incl ? false : true, r3 );
              zset.zbsearch( hival, j, hi_incl ? true : false, r3 );
            }
            else {
              zset.zbsearch_all( lo, lolen, lo_incl ? false : true, i );
              zset.zbsearch_all( hi, hilen, hi_incl ? true : false, j );
            }
            ctx.ival = j - i; /* if inclusive */
            status = EXEC_SEND_INT;
            break;
          }
        }
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
          if ( status == EXEC_OK ) {
            this->strm.sz += sz;
            return EXEC_OK;
          }
          return status;
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}


ExecStatus
RedisExec::exec_zwrite( RedisKeyCtx &ctx,  int flags )
{
  static char Dfmt[4] = { '%', 'D', 'a', 0 };
  const char * arg    = NULL;
  size_t       arglen = 0,
               argi;
  HashPos      pos;
  int          add_fl = 0;
  ZScore       score;

  /* ZADD key [NX|XX] [CH] [INCR] score mem */
  if ( ( flags & DO_ZADD ) != 0 ) {
    for ( argi = 2; argi < this->argc; argi++ ) {
      switch ( this->msg.match_arg( argi, "nx", 2,
                                          "xx", 2,
                                          "ch", 2,
                                          "incr", 4, NULL ) ) {
        case 1: add_fl |= ZADD_MUST_NOT_EXIST; break; /* nx */
        case 2: add_fl |= ZADD_MUST_EXIST;     break; /* xx */
        case 3: add_fl |= ZADD_RET_CHANGED;    break; /* ch */
        case 4: add_fl |= ZADD_INCR;           break; /* incr */
        default:
          goto break_loop;
      }
    }
  break_loop:;
  }
  /* ZINCRBY key incr mem */
  /* ZREM key mem [mem] */
  else {
    if ( ( flags & DO_ZINCRBY ) != 0 )
      add_fl = ZADD_INCR;
    argi = 2;
  }
  if ( ( flags & DO_ZREM ) == 0 ) {
    if ( ! this->msg.get_arg( argi++, arg, arglen ) )
      return ERR_BAD_ARGS;
    score = str_to_score( arg, arglen );
  }
  else {
    score = 0;
  }
  if ( ! this->msg.get_arg( argi++, arg, arglen ) )
    return ERR_BAD_ARGS;
  pos.init( arg, arglen );

  size_t       count,
               ndata;
  void       * data;
  size_t       datalen,
               retry    = 0;
  ZSetData   * old_zset = NULL,
             * zset     = NULL,
               tmp[ 2 ];
  MsgCtx     * msg      = NULL;
  MsgCtxBuf    tmpm;
  uint32_t     n        = 0;
  ZSetStatus   zstatus = ZSET_OK;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      if ( ( flags & DO_ZREM ) != 0 ) /* no data to move or remove */
        return EXEC_SEND_ZERO;
      if ( ( flags & DO_ZADD ) != 0 ) {
        count = this->argc / 2; /* set by alloc_size() */
        ndata = 2 + arglen + sizeof( ZScore ); /* length of all zadd args */
        for ( size_t j = argi; j < this->argc; j += 2 ) {
          const char * tmparg;
          size_t       tmplen;
          if ( ! this->msg.get_arg( j, tmparg, tmplen ) )
            return ERR_BAD_ARGS;
          ndata += 2 + tmplen + sizeof( ZScore );
        }
      }
      datalen = ZSetData::alloc_size( count, ndata );
      ctx.kstatus = this->kctx.resize( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        zset = new ( &tmp[ n++%2 ]) ZSetData( data, datalen );
        zset->init( count, ndata );
      }
      if ( 0 ) {
        fallthrough;
    case KEY_OK:
        if ( ctx.type != MD_SORTEDSET ) {
          if ( ctx.type != MD_NODATA )
            return ERR_BAD_TYPE;
        }
        ctx.kstatus = this->kctx.value( &data, datalen );
        if ( ctx.kstatus == KEY_OK ) {
          zset = new ( &tmp[ n++%2 ] ) ZSetData( data, datalen );
          zset->open();
        }
      }
      if ( zset != NULL ) {
        for (;;) {
          if ( old_zset != NULL ) {
            old_zset->copy( *zset );
            ctx.kstatus = this->kctx.load( *msg ); /* swap new and old */
            if ( ctx.kstatus != KEY_OK )
              break;
            old_zset = NULL;
          }
        set_next_value:;
          switch ( flags & ( DO_ZADD | DO_ZINCRBY | DO_ZREM ) ) {
            case DO_ZADD:
            case DO_ZINCRBY:
              zstatus = zset->zadd( arg, arglen, score, pos, add_fl, &score );
              if ( zstatus == ZSET_UPDATED )
                ctx.ival++;
              break;
            case DO_ZREM:
              zstatus = zset->zrem( arg, arglen, pos );
              if ( zstatus == ZSET_OK )
                ctx.ival++;
              break;
          }
          if ( zstatus != ZSET_FULL ) {
            /* if more members to add/rem */
            if ( this->argc > argi ) {
              if ( ( flags & DO_ZREM ) == 0 ) {
                if ( ! this->msg.get_arg( argi++, arg, arglen ) )
                  return ERR_BAD_ARGS;
                score = str_to_score( arg, arglen );
              }
              if ( ! this->msg.get_arg( argi++, arg, arglen ) )
                return ERR_BAD_ARGS;
              pos.init( arg, arglen );
              goto set_next_value;
            }
            /* return result of score incrby */
            if ( ( add_fl & ZADD_INCR ) != 0 ) {
              char   fpdata[ 64 ];
              size_t fvallen;
              fvallen = ::snprintf( fpdata, sizeof( fpdata ), Dfmt, score );
              this->strm.sz += this->send_string( fpdata, fvallen );
              return EXEC_OK;
            }
            /* return number members updated */
            return EXEC_SEND_INT;
          }
          count = 2;
          ndata = arglen + 1 + retry;
          retry += 16;
          datalen = zset->resize_size( count, ndata );
          msg = new ( tmpm ) MsgCtx( this->kctx.ht, this->kctx.thr_ctx );
          msg->set_key( ctx.kbuf );
          msg->set_hash( ctx.hash1, ctx.hash2 );
          ctx.kstatus = msg->alloc_segment( &data, datalen, 8 );
          if ( ctx.kstatus != KEY_OK )
            break;
          old_zset = zset;
          zset = new ( (void *) &tmp[ n++%2 ] ) ZSetData( data, datalen );
          zset->init( count, ndata );
        }
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_zmultiscan( RedisKeyCtx &ctx,  int flags,  ScanArgs *sa )
{
  static char Dfmt[4] = { '%', 'D', 'a', 0 };
  const char * lo         = NULL,
             * hi         = NULL;
  size_t       lolen      = 0,
               hilen      = 0;
  int64_t      ival       = 0,
               jval       = 0,
               zoff       = 0,
               zcnt       = 0;
  bool         withscores = false;

  /* ZRANGE key start stop [WITHSCORES] */
  /* ZREVRANGE key start stop [WITHSCORES] */
  if ( ( flags & ( DO_ZRANGE | DO_ZREVRANGE ) ) != 0 ) {
    if ( ! this->msg.get_arg( 2, ival ) || ! this->msg.get_arg( 3, jval ) )
      return ERR_BAD_ARGS;
    withscores = ( this->msg.match_arg( 4, "withscores", 10, NULL ) == 1 );
  }
  else if ( ( flags & ( DO_ZRANGEBYLEX | DO_ZRANGEBYSCORE |
                        DO_ZREVRANGEBYLEX | DO_ZREVRANGEBYSCORE ) ) != 0 ) {
    if ( ! msg.get_arg( 2, lo, lolen ) || ! msg.get_arg( 3, hi, hilen ) )
      return ERR_BAD_ARGS;

    for ( size_t i = 4; i < this->argc; i++ ) {
      switch ( this->msg.match_arg( i, "withscores", 10, "limit", 5, NULL ) ) {
        case 1: withscores = true; break;
        case 2:
          if ( ! this->msg.get_arg( i+1, zoff ) ||
               ! this->msg.get_arg( i+2, zcnt ) )
            return ERR_BAD_ARGS;
          i += 2;
          break;
        default:
          return ERR_BAD_ARGS;
      }
    }
  }
  /* ZSCAN key curs [MATCH pat] [COUNT cnt] */
  else if ( ( flags & DO_ZSCAN ) != 0 ) {
    withscores = true; /* always with scores */
  }
  StreamBuf::BufQueue q( this->strm );
  void     * data;
  size_t     datalen,
             count   = 0,
             itemcnt = 0,
             i       = ( sa != NULL && sa->pos > 0 ? sa->pos : 0 ),
             j, k,
             maxcnt  = (sa != NULL ? sa->maxcnt : zcnt) * ( withscores ? 2:1 );
  uint8_t    lhdr[ LIST_HDR_OOB_SIZE ];
  uint64_t   llen;
  ZSetVal    zv;
  char       fpdata[ 64 ];
  size_t     fvallen;
  ZSetStatus zstatus;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      break;
    case KEY_OK:
      if ( ctx.type == MD_NODATA )
        break;
      if ( ctx.type != MD_SORTEDSET )
        return ERR_BAD_TYPE;

      llen = sizeof( lhdr );
      ctx.kstatus = this->kctx.value_copy( &data, datalen, lhdr, llen );
      if ( ctx.kstatus == KEY_OK ) {
        ZSetData zset( data, datalen );
        zset.open( lhdr, llen );
        if ( (count = zset.hcount()) == 0 )
          break;
        /* if by range by rank */
        if ( ( flags & ( DO_ZRANGE | DO_ZREVRANGE ) ) != 0 ) {
          if ( ival < 0 )
            ival = count + ival;
          if ( jval < 0 )
            jval = count + jval;
          ival = min<int64_t>( count, max<int64_t>( 0, ival ) );
          jval = min<int64_t>( count, max<int64_t>( 0, jval + 1 ) );
          if ( ival >= jval )
            break;
          i = ival;
          j = jval;
        }
        else {
          /* lo, hi inclusive */
          bool lo_incl = ( flags & ( DO_ZRANGEBYSCORE | DO_ZRANGEBYLEX ) ) != 0,
               hi_incl = lo_incl;
          if ( lo[ 0 ] == '(' ) { lo++; lolen--; lo_incl = ! lo_incl; }
          else if ( lo[ 0 ] == '[' ) { lo++; lolen--; }
          if ( hi[ 0 ] == '(' ) { hi++; hilen--; hi_incl = ! hi_incl; }
          else if ( hi[ 0 ] == '[' ) { hi++; hilen--; }
          /* if range by score */
          if ( ( flags & ( DO_ZRANGEBYSCORE | DO_ZREVRANGEBYSCORE ) ) != 0 ) {
            ZScore loval = str_to_score( lo, lolen ),
                   hival = str_to_score( hi, hilen ),
                   r3;
            zset.zbsearch( loval, i, lo_incl ? false : true, r3 );
            zset.zbsearch( hival, j, hi_incl ? true : false, r3 );
            if ( ( flags & DO_ZRANGEBYSCORE ) != 0 ) {
              i  = i - 1 + zoff;
              j -= 1;
            }
            else {
              i = count + 1 - i + zoff;
              j = count + 1 - j;
            }
          }
          /* if range by lex */
          else if ( ( flags & ( DO_ZRANGEBYLEX | DO_ZREVRANGEBYLEX ) ) != 0 ) {
            zset.zbsearch_all( lo, lolen, lo_incl ? false : true, i );
            zset.zbsearch_all( hi, hilen, hi_incl ? true : false, j );
            if ( ( flags & DO_ZRANGEBYLEX ) != 0 ) {
              i  = i - 1 + zoff;
              j -= 1;
            }
            else {
              i = count + 1 - i + zoff;
              j = count + 1 - j;
            }
          }
          /* match pattern */
          else { /* DO_ZSCAN */
            j = count;
          }
        }

        for (;;) {
          if ( i >= j )
            break;
          if ( maxcnt != 0 && itemcnt >= maxcnt )
            break;
          i += 1;
          if ( ( flags & ( DO_ZREVRANGE | DO_ZREVRANGEBYSCORE |
                           DO_ZREVRANGEBYLEX ) ) != 0 )
            k = ( count + 1 ) - i;
          else
            k = i;
          zstatus = zset.zindex( k, zv );
          if ( zstatus != ZSET_OK )
            break;
          /* match wildcard */
          if ( ( flags & DO_ZSCAN ) != 0 ) {
            if ( sa->re != NULL ) {
              char buf[ 256 ];
              void * subj;
              size_t subjlen;
              bool is_alloced = false;
              subjlen = zv.unitary( subj, buf, sizeof( buf ), is_alloced );
              int rc = pcre2_match( sa->re, (PCRE2_SPTR8) subj, subjlen,
                                    0, 0, sa->md, 0 );
              if ( is_alloced )
                ::free( subj );
              if ( rc < 1 )
                continue;
            }
          }
          if ( q.append_string( zv.data, zv.sz, zv.data2, zv.sz2 ) == 0 )
            return ERR_ALLOC_FAIL;
          itemcnt++;
          if ( withscores ) {
            fvallen = ::snprintf( fpdata, sizeof( fpdata ), Dfmt, zv.score );
            if ( q.append_string( fpdata, fvallen ) == 0 )
              return ERR_ALLOC_FAIL;
            itemcnt++;
          }
        }
        q.finish_tail();
        break;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }

  if ( ( flags & DO_ZSCAN ) != 0 )
    q.prepend_cursor_array( i == count ? 0 : i, itemcnt );
  else
    q.prepend_array( itemcnt );

  if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK ) {
    this->strm.append_iov( q );
    return EXEC_OK;
  }
  return ERR_KV_STATUS;
}

ExecStatus
RedisExec::exec_zremrange( RedisKeyCtx &ctx,  int flags )
{
  const char * lo         = NULL,
             * hi         = NULL;
  size_t       lolen      = 0,
               hilen      = 0;
  int64_t      ival       = 0,
               jval       = 0;

  /* ZREMRANGEBYRANK key start stop */
  if ( ( flags & DO_ZREMRANGEBYRANK ) != 0 ) {
    if ( ! this->msg.get_arg( 2, ival ) || ! this->msg.get_arg( 3, jval ) )
      return ERR_BAD_ARGS;
  }
  /* ZREMRANGEBYLEX key start stop */
  /* ZREMRANGEBYSCORE key start stop */
  else {
    if ( ! msg.get_arg( 2, lo, lolen ) || ! msg.get_arg( 3, hi, hilen ) )
      return ERR_BAD_ARGS;
  }
  void     * data;
  size_t     datalen,
             count,
             i, j;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
      break;
    case KEY_OK:
      if ( ctx.type == MD_NODATA )
        break;
      if ( ctx.type != MD_SORTEDSET )
        return ERR_BAD_TYPE;

      ctx.kstatus = this->kctx.value( &data, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        ZSetData zset( data, datalen );
        zset.open();
        if ( (count = zset.hcount()) == 0 )
          break;
        if ( ( flags & DO_ZREMRANGEBYRANK ) != 0 ) {
          if ( ival < 0 )
            ival = count + ival;
          if ( jval < 0 )
            jval = count + jval;
          ival = min<int64_t>( count, max<int64_t>( 0, ival ) );
          jval = min<int64_t>( count, max<int64_t>( 0, jval + 1 ) );
          if ( ival >= jval )
            break;
          i = ival;
          j = jval;
        }
        else {
          /* lo, hi inclusive */
          bool lo_incl = true,
               hi_incl = true;
          if ( lo[ 0 ] == '(' ) { lo++; lolen--; lo_incl = false; }
          else if ( lo[ 0 ] == '[' ) { lo++; lolen--; }
          if ( hi[ 0 ] == '(' ) { hi++; hilen--; hi_incl = false; }
          else if ( hi[ 0 ] == '[' ) { hi++; hilen--; }
          if ( ( flags & DO_ZREMRANGEBYSCORE ) != 0 ) {
            ZScore loval = str_to_score( lo, lolen ),
                   hival = str_to_score( hi, hilen ),
                   r3;
            zset.zbsearch( loval, i, lo_incl ? false : true, r3 );
            zset.zbsearch( hival, j, hi_incl ? true : false, r3 );
            i  = i - 1;
            j -= 1;
          }
          else /*if ( ( flags & DO_ZREMRANGEBYLEX ) != 0 )*/ {
            zset.zbsearch_all( lo, lolen, lo_incl ? false : true, i );
            zset.zbsearch_all( hi, hilen, hi_incl ? true : false, j );
            i  = i - 1;
            j -= 1;
          }
        }

        ctx.ival = j - i;
        if ( (size_t) ctx.ival == count ) {
          zset.zremall();
        }
        else {
          while ( i >= j ) {
            zset.zrem_index( j );
            j -= 1;
          }
        }
        return EXEC_SEND_INT;
      }
      fallthrough;
    default: return ERR_KV_STATUS;
  }
  return EXEC_SEND_ZERO;
}

ExecStatus
RedisExec::exec_zsetop( RedisKeyCtx &ctx,  int flags )
{
  void   * data;
  uint64_t datalen;

  /* ZINTERSTORE dest nkeys key [key ...] */
  /* ZUNIONSTORE dest nkeys key [key  ...] */
  /* if is the dest key, check if need to wait for src keys */
  if ( ctx.argn == 1 && this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;
  /* if not dest key, fetch set */
  if ( ctx.argn != 1 ) {
    data    = NULL;
    datalen = 0;
    switch ( this->exec_key_fetch( ctx, true ) ) {
      case KEY_NOT_FOUND: if ( 0 ) {
      case KEY_OK:
          if ( ctx.type != MD_SORTEDSET )
            return ERR_BAD_TYPE;
          ctx.kstatus = this->kctx.value( &data, datalen );
          if ( ctx.kstatus != KEY_OK )
            return ERR_KV_STATUS;
        }
        if ( datalen == 0 ) {
          data    = (void *) mt_list;
          datalen = sizeof( mt_list );
        }
        if ( ! this->save_data( ctx, data, datalen ) )
          return ERR_ALLOC_FAIL;
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_OK;
      fallthrough;
      default: return ERR_KV_STATUS;
    }
  }
  ZSetData       tmp[ 2 ];
  ZSetData     * zset,
               * old_zset;
  ZScore       * weight = NULL;
  size_t         i,
                 retry  = 0,
                 ndata,
                 count;
  int            n = 0;
  ZAggregateType aggregate_type = ZAGGREGATE_SUM;
  bool           has_weights    = false;

  for ( i = 3 + this->key_cnt - 1; i < this->argc; ) {
    switch ( this->msg.match_arg( i, "aggregate", 9,
                                     "weights",   7, NULL ) ) {
      case 1:
        i += 1;
        switch ( this->msg.match_arg( i, "sum", 3,
                                         "min", 3,
                                         "max", 3,
                                         "none", 4, NULL ) ) {
          default: return ERR_BAD_ARGS;
          case 1: aggregate_type = ZAGGREGATE_SUM; break;
          case 2: aggregate_type = ZAGGREGATE_MIN; break;
          case 3: aggregate_type = ZAGGREGATE_MAX; break;
          case 4: aggregate_type = ZAGGREGATE_NONE; break;
        }
        i += 1;
        break;
      case 2:
        weight = (ZScore *) this->strm.alloc( sizeof( ZScore ) *
                                              ( this->key_cnt - 1 ) );
        if ( weight == NULL )
          return ERR_ALLOC_FAIL;
        i += 1;
        data = this->strm.alloc( datalen );
        for ( size_t k = 0; k < this->key_cnt - 1; k++ ) {
          const char *str;
          size_t len;
          if ( ! this->msg.get_arg( i, str, len ) )
            return ERR_BAD_ARGS;
          weight[ k ] = str_to_score( str, len );
          i += 1;
        }
        has_weights = true;
        break;
      default:
        return ERR_BAD_ARGS;
    }
  }
  /* first source key */
  data    = this->keys[ 1 ]->part->data( 0 );
  datalen = this->keys[ 1 ]->part->size;

  zset = new ( (void *) &tmp[ n++%2 ] ) ZSetData( data, datalen );
  zset->open();
  /* if first set is weighted, scale it */
  if ( has_weights && weight[ 0 ] != 0 )
    zset->zscale( weight[ 0 ] );

  /* merge the source keys together */
  for ( i = 2; i < this->key_cnt; i++ ) {
    ZSetData set2( this->keys[ i ]->part->data( 0 ),
                   this->keys[ i ]->part->size );
    ZMergeCtx  ctx;
    ZSetStatus zstat = ZSET_OK;
    ctx.init( has_weights ? weight[ i - 1 ] : 1, aggregate_type );
    set2.open();
    for (;;) {
      switch ( flags & ( DO_ZUNIONSTORE | DO_ZINTERSTORE  ) ) {
        case DO_ZUNIONSTORE:
          zstat = zset->zunion( set2, ctx );
          break;
        case DO_ZINTERSTORE:
          zstat = zset->zinter( set2, ctx );
          break;
      }
      if ( zstat == ZSET_OK )
        break;
      /* resize set */
      count = set2.count() + 2;
      ndata = set2.data_len() + retry;
      retry += 16;
      datalen = zset->resize_size( count, ndata );
      data = this->strm.alloc( datalen );
      if ( data == NULL )
        return ERR_ALLOC_FAIL;
      old_zset = zset;
      zset = new ( (void *) &tmp[ n++%2 ] ) ZSetData( data, datalen );
      zset->init( count, ndata );
      old_zset->copy( *zset );
    }
  }
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data, zset->size );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, zset->listp, zset->size );
        ctx.ival = zset->hcount();
        return EXEC_SEND_INT;
      }
    fallthrough;
    default: return ERR_KV_STATUS;
  }
}

