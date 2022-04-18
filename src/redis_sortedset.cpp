#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raikv/key_hash.h>
#include <raimd/md_zset.h>
#include <raimd/md_geo.h>
#include <raids/exec_list_ctx.h>
#include <h3api.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

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
  DO_ZSCAN            = 1<<20,
  DO_ZPOPMIN          = 1<<21,
  DO_ZPOPMAX          = 1<<22,
  DO_BZPOPMIN         = 1<<23,
  DO_BZPOPMAX         = 1<<24,
  ZSET_POP_CMDS       = DO_ZPOPMAX | DO_ZPOPMIN |
                        DO_BZPOPMAX | DO_BZPOPMIN
};

ExecStatus
RedisExec::exec_zadd( EvKeyCtx &ctx ) noexcept
{
  /* ZADD key [NX|XX] [CH] [INCR] score mem */
  return this->do_zwrite( ctx, DO_ZADD );
}

ExecStatus
RedisExec::exec_zcard( EvKeyCtx &ctx ) noexcept
{
  /* ZCARD key */
  return this->do_zread( ctx, DO_ZCARD );
}

ExecStatus
RedisExec::exec_zcount( EvKeyCtx &ctx ) noexcept
{
  /* ZCOUNT key min max */
  return this->do_zread( ctx, DO_ZCOUNT );
}

ExecStatus
RedisExec::exec_zincrby( EvKeyCtx &ctx ) noexcept
{
  /* ZINCRBY key incr mem */
  return this->do_zwrite( ctx, DO_ZINCRBY );
}

ExecStatus
RedisExec::exec_zinterstore( EvKeyCtx &ctx ) noexcept
{
  /* ZINTERSTORE dest num key1 keyN */
  return this->do_zsetop( ctx, DO_ZINTERSTORE );
}

ExecStatus
RedisExec::exec_zlexcount( EvKeyCtx &ctx ) noexcept
{
  /* ZLEXCOUNT key min max */
  return this->do_zread( ctx, DO_ZLEXCOUNT );
}

ExecStatus
RedisExec::exec_zrange( EvKeyCtx &ctx ) noexcept
{
  /* ZRANGE key start stop [WITHSCORES] */
  return this->do_zmultiscan( ctx, DO_ZRANGE, NULL );
}

ExecStatus
RedisExec::exec_zrangebylex( EvKeyCtx &ctx ) noexcept
{
  /* ZRANGEBYLEX key min max [LIMIT off cnt] */
  return this->do_zmultiscan( ctx, DO_ZRANGEBYLEX, NULL );
}

ExecStatus
RedisExec::exec_zrevrangebylex( EvKeyCtx &ctx ) noexcept
{
  /* ZREVRANGEBYLEX key min max [LIMIT off cnt] */
  return this->do_zmultiscan( ctx, DO_ZREVRANGEBYLEX, NULL );
}

ExecStatus
RedisExec::exec_zrangebyscore( EvKeyCtx &ctx ) noexcept
{
  /* ZRANGEBYSCORE key min max [WITHSCORES] */
  return this->do_zmultiscan( ctx, DO_ZRANGEBYSCORE, NULL );
}

ExecStatus
RedisExec::exec_zrank( EvKeyCtx &ctx ) noexcept
{
  /* ZRANK key mem */
  return this->do_zread( ctx, DO_ZRANK );
}

ExecStatus
RedisExec::exec_zrem( EvKeyCtx &ctx ) noexcept
{
  /* ZREM key mem [mem] */
  return this->do_zwrite( ctx, DO_ZREM );
}

ExecStatus
RedisExec::exec_zremrangebylex( EvKeyCtx &ctx ) noexcept
{
  /* ZREMRANGEBYLEX key start stop */
  return this->do_zremrange( ctx, DO_ZREMRANGEBYLEX );
}

ExecStatus
RedisExec::exec_zremrangebyrank( EvKeyCtx &ctx ) noexcept
{
  /* ZREMRANGEBYRANK key start stop */
  return this->do_zremrange( ctx, DO_ZREMRANGEBYRANK );
}

ExecStatus
RedisExec::exec_zremrangebyscore( EvKeyCtx &ctx ) noexcept
{
  /* ZREMRANGEBYSCORE key start stop */
  return this->do_zremrange( ctx, DO_ZREMRANGEBYSCORE );
}

ExecStatus
RedisExec::exec_zrevrange( EvKeyCtx &ctx ) noexcept
{
  /* ZREVRANGE key start stop [WITHSCORES] */
  return this->do_zmultiscan( ctx, DO_ZREVRANGE, NULL );
}

ExecStatus
RedisExec::exec_zrevrangebyscore( EvKeyCtx &ctx ) noexcept
{
  /* ZREVRANGEBYLEX key min max [LIMIT off cnt] */
  return this->do_zmultiscan( ctx, DO_ZREVRANGEBYSCORE, NULL );
}

ExecStatus
RedisExec::exec_zrevrank( EvKeyCtx &ctx ) noexcept
{
  /* ZREVRANK key mem */
  return this->do_zread( ctx, DO_ZREVRANK );
}

ExecStatus
RedisExec::exec_zscore( EvKeyCtx &ctx ) noexcept
{
  /* ZSCORE key mem */
  return this->do_zread( ctx, DO_ZSCORE );
}

ExecStatus
RedisExec::exec_zunionstore( EvKeyCtx &ctx ) noexcept
{
  /* ZUNIONSTORE dest num key1 keyN [WEIGHTS for-each key] [AGGR sum|min|max] */
  return this->do_zsetop( ctx, DO_ZUNIONSTORE );
}

ExecStatus
RedisExec::exec_zscan( EvKeyCtx &ctx ) noexcept
{
  /* ZSCAN key curs [MATCH pat] [COUNT cnt] */
  ScanArgs   sa;
  ExecStatus status;
  if ( (status = this->match_scan_args( sa, 2 )) != EXEC_OK )
    return status;
  status = this->do_zmultiscan( ctx, DO_ZSCAN, &sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::exec_zpopmin( EvKeyCtx &ctx ) noexcept
{
  /* ZPOPMIN key [cnt] */
  return this->do_zremrange( ctx, DO_ZPOPMIN );
}

ExecStatus
RedisExec::exec_zpopmax( EvKeyCtx &ctx ) noexcept
{
  /* ZPOPMAX key [cnt] */
  return this->do_zremrange( ctx, DO_ZPOPMAX );
}

ExecStatus
RedisExec::exec_bzpopmin( EvKeyCtx &ctx ) noexcept
{
  /* BZPOPMIN key [key...] timeout */
  ExecStatus status = this->do_zremrange( ctx, DO_BZPOPMIN );
  switch ( status ) {
    case EXEC_SEND_ZEROARR: {
      double timeout;
      if ( ! this->msg.get_arg( this->argc - 1, timeout ) || timeout <= 0.0 )
        timeout = 0;
      ctx.ival = (int64_t) ( timeout * 1000000000.0 );
      return EXEC_BLOCKED;
    }
    case EXEC_OK: return EXEC_SEND_DATA;
    default:      return status;
  }
}

ExecStatus
RedisExec::exec_bzpopmax( EvKeyCtx &ctx ) noexcept
{
  /* BZPOPMAX key [key...] timeout */
  ExecStatus status = this->do_zremrange( ctx, DO_BZPOPMAX );
  switch ( status ) {
    case EXEC_SEND_ZEROARR: {
      double timeout;
      if ( ! this->msg.get_arg( this->argc - 1, timeout ) || timeout <= 0.0 )
        timeout = 0;
      ctx.ival = (int64_t) ( timeout * 1000000000.0 );
      return EXEC_BLOCKED;
    }
    case EXEC_OK: return EXEC_SEND_DATA;
    default:      return status;
  }
}

static ZScore
str_to_score( const char *score,  size_t scorelen )
{
  return ZScore::parse_len( score, scorelen );
}

ExecStatus
RedisExec::do_zread( EvKeyCtx &ctx,  int flags ) noexcept
{
  const char * arg    = NULL;
  size_t       arglen = 0;
  const char * lo     = NULL,
             * hi     = NULL;
  size_t       lolen  = 0,
               hilen  = 0;
  char         fpdata[ 64 ];
  size_t       fvallen;
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
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    default:            return ERR_KV_STATUS;
    case KEY_OK:
      if ( ctx.type != MD_ZSET && ctx.type != MD_GEO ) {
        if ( ctx.type == MD_NODATA )
          return EXEC_SEND_ZERO;
        return ERR_BAD_TYPE;
      }
      break;
  }

  if ( ctx.type == MD_ZSET ) {
    ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );
    if ( ! zset.open_readonly() )
      return ERR_KV_STATUS;
    switch ( flags & ( DO_ZCARD | DO_ZCOUNT | DO_ZSCORE |
                       DO_ZRANK | DO_ZREVRANK | DO_ZLEXCOUNT ) ) {
      case DO_ZCARD:
        ctx.ival = zset.x->hcount();
        status = EXEC_SEND_INT;
        break;
      case DO_ZSCORE:
      case DO_ZRANK:
      case DO_ZREVRANK: {
        ZScore score;
        if ( zset.x->zexists( arg, arglen, pos, score ) == ZSET_OK ) {
          /* return the score */
          if ( ( flags & DO_ZSCORE ) != 0 ) {
            fvallen = score.to_string( fpdata );
            sz      = this->send_string( fpdata, fvallen );
            status  = EXEC_OK;
          }
          /* return the rank */
          else {
            if ( ( flags & DO_ZRANK ) != 0 )
              ctx.ival = pos.i - 1;
            else
              ctx.ival = zset.x->hcount() - pos.i;
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
        if ( lolen == 1 && lo[ 0 ] == '-' ) {
          i = 1;
        }
        else if ( ( flags & DO_ZCOUNT ) != 0 ) {
          ZScore loval = str_to_score( lo, lolen ),
                 r3;
          zset.x->zbsearch( loval, i, lo_incl ? false : true, r3 );
        }
        else {
          zset.x->zbsearch_all( lo, lolen, lo_incl ? false : true, i );
        }
        if ( hilen == 1 && hi[ 0 ] == '+' ) {
          j = zset.x->hcount() + 1;
        }
        else if ( ( flags & DO_ZCOUNT ) != 0 ) {
          ZScore hival = str_to_score( hi, hilen ),
                 r3;
          zset.x->zbsearch( hival, j, hi_incl ? true : false, r3 );
        }
        else {
          zset.x->zbsearch_all( hi, hilen, hi_incl ? true : false, j );
        }
        ctx.ival = j - i; /* if inclusive */
        status = EXEC_SEND_INT;
        break;
      }
    }
  }
  else {
    ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
    if ( ! geo.open_readonly() )
      return ERR_KV_STATUS;
    switch ( flags & ( DO_ZCARD | DO_ZCOUNT | DO_ZSCORE |
                       DO_ZRANK | DO_ZREVRANK | DO_ZLEXCOUNT ) ) {
      case DO_ZCARD:
        ctx.ival = geo.x->hcount();
        status = EXEC_SEND_INT;
        break;
      case DO_ZSCORE:
      case DO_ZRANK:
      case DO_ZREVRANK: {
        H3Index score;
        if ( geo.x->geoexists( arg, arglen, pos, score ) == GEO_OK ) {
          /* return the score */
          if ( ( flags & DO_ZSCORE ) != 0 ) {
            fvallen = uint64_to_string( score, fpdata );
            sz      = this->send_string( fpdata, fvallen );
            status  = EXEC_OK;
          }
          /* return the rank */
          else {
            if ( ( flags & DO_ZRANK ) != 0 )
              ctx.ival = pos.i - 1;
            else
              ctx.ival = geo.x->hcount() - pos.i;
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
          if ( lolen == 1 && lo[ 0 ] == '-' ) {
            i = 1;
          }
          else {
            H3Index loval, r3;
            RedisMsg::str_to_uint( lo, lolen, loval );
            geo.x->geobsearch( loval, i, lo_incl ? false : true, r3 );
          }
          if ( hilen == 1 && hi[ 0 ] == '+' ) {
            j = geo.x->hcount() + 1;
          }
          else {
            H3Index hival, r3;
            RedisMsg::str_to_uint( hi, hilen, hival );
            geo.x->geobsearch( hival, j, hi_incl ? true : false, r3 );
          }
        }
        else {
          /* geo data always has scores */
          return ERR_BAD_TYPE;
        }
        ctx.ival = j - i; /* if inclusive */
        status = EXEC_SEND_INT;
        break;
      }
    }
  }
  if ( (ctx.kstatus = this->kctx.validate_value()) != KEY_OK )
    return ERR_KV_STATUS;
  if ( status == EXEC_OK ) {
    this->strm.sz += sz;
    return EXEC_OK;
  }
  return status;
}


ExecStatus
RedisExec::do_zwrite( EvKeyCtx &ctx,  int flags ) noexcept
{
  const char * arg    = NULL;
  size_t       arglen = 0,
               argi;
  HashPos      pos;
  int          add_fl = 0;
  ZScore       score;

  /* ZADD key [NX|XX] [CH] [INCR] score mem */
  if ( ( flags & DO_ZADD ) != 0 ) {
    for ( argi = 2; argi < this->argc; argi++ ) {
      switch ( this->msg.match_arg( argi, MARG( "nx" ),
                                          MARG( "xx" ),
                                          MARG( "ch" ),
                                          MARG( "incr" ), NULL ) ) {
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
    score.zero();
  }
  if ( ! this->msg.get_arg( argi++, arg, arglen ) )
    return ERR_BAD_ARGS;
  pos.init( arg, arglen );

  size_t count = 0,
         ndata = 0;

  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK:
      if ( ctx.type != MD_NODATA )
        break;
      /* FALLTHRU */
    case KEY_IS_NEW:
      if ( ( flags & DO_ZREM ) != 0 ) /* no data to move or remove */
        return EXEC_SEND_ZERO;
      ctx.flags |= EKF_IS_NEW;
      if ( ( flags & ( DO_ZADD | DO_ZINCRBY ) ) != 0 ) {
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
      break;
    default:
      return ERR_KV_STATUS;
  }

  if ( ctx.type == MD_GEO ) {
    ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
    /* only allow ZREM with geo data, ZADD and ZINCRBY don't make sense */
    if ( ( flags & DO_ZREM ) == 0 )
      return ERR_BAD_TYPE;
    if ( ! geo.open() )
      return ERR_KV_STATUS;
    for (;;) {
      if ( geo.x->georem( arg, arglen, pos ) == GEO_OK )
        ctx.ival++;
      if ( this->argc == argi )
        break;
      if ( ! this->msg.get_arg( argi++, arg, arglen ) )
        return ERR_BAD_ARGS;
      pos.init( arg, arglen );
    }
    if ( ctx.ival > 0 ) {
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_GEO;
      if ( geo.x->hcount() == 0 ) {
        ctx.flags |= EKF_KEYSPACE_DEL;
        if ( ! geo.tombstone() )
          return ERR_KV_STATUS;
      }
    }
    return EXEC_SEND_INT;
  }

  ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );
  if ( ctx.is_new() ) {
    if ( ! zset.create( count, ndata ) )
      return ERR_KV_STATUS;
  }
  else {
    if ( ! zset.open() )
      return ERR_KV_STATUS;
  }

  for (;;) {
    ZSetStatus zstatus = ZSET_OK;
    switch ( flags & ( DO_ZADD | DO_ZINCRBY | DO_ZREM ) ) {
      case DO_ZADD:
      case DO_ZINCRBY:
        zstatus = zset.x->zadd( arg, arglen, score, pos, add_fl, &score );
        if ( zstatus == ZSET_UPDATED )
          ctx.ival++;
        break;
      case DO_ZREM:
        zstatus = zset.x->zrem( arg, arglen, pos );
        if ( zstatus == ZSET_OK )
          ctx.ival++;
        break;
    }
    /* if resize */
    if ( zstatus == ZSET_FULL ) {
      if ( ! zset.realloc( arglen + 1 + sizeof( ZScore ) ) )
        return ERR_KV_STATUS;
      continue; /* try again */
    }
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
      continue;
    }
    /* return result of score incrby */
    if ( ( add_fl & ZADD_INCR ) != 0 ) {
      char   fpdata[ 64 ];
      size_t fvallen;
      fvallen = score.to_string( fpdata );
      this->strm.sz += this->send_string( fpdata, fvallen );
      return EXEC_OK;
    }
    /* return number members updated */
    if ( ctx.ival > 0 ) {
      if ( ( flags & DO_ZADD ) != 0 )
        ctx.flags |= EKF_ZSETBLKD_NOT;
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_ZSET;
      if ( zset.x->hcount() == 0 ) {
        ctx.flags |= EKF_KEYSPACE_DEL;
        if ( ! zset.tombstone() )
          return ERR_KV_STATUS;
      }
    }
    return EXEC_SEND_INT;
  }
}

ExecStatus
RedisExec::do_zmultiscan( EvKeyCtx &ctx,  int flags,  ScanArgs *sa ) noexcept
{
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
    withscores = ( this->msg.match_arg( 4, MARG( "withscores" ), NULL ) == 1 );
  }
  else if ( ( flags & ( DO_ZRANGEBYLEX | DO_ZRANGEBYSCORE |
                        DO_ZREVRANGEBYLEX | DO_ZREVRANGEBYSCORE ) ) != 0 ) {
    if ( ! msg.get_arg( 2, lo, lolen ) || ! msg.get_arg( 3, hi, hilen ) )
      return ERR_BAD_ARGS;

    for ( size_t i = 4; i < this->argc; i++ ) {
      switch ( this->msg.match_arg( i, MARG( "withscores" ),
                                       MARG( "limit" ), NULL ) ) {
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
  size_t count   = 0,
         itemcnt = 0,
         i       = ( sa != NULL && sa->pos > 0 ? sa->pos : 0 ),
         j, k,
         maxcnt  = (sa != NULL ? sa->maxcnt : zcnt) * ( withscores ? 2:1 );
  char   fpdata[ 64 ];
  size_t fvallen;
  /* lo, hi inclusive, rev range is opposite */
  bool   lo_incl = ( flags & ( DO_ZRANGEBYSCORE | DO_ZRANGEBYLEX ) ) != 0,
         hi_incl = lo_incl;

  if ( lo != NULL ) {
    if ( lo[ 0 ] == '(' ) {
      lo++; lolen--; lo_incl = ! lo_incl;
    }
    else if ( lo[ 0 ] == '[' ) {
      lo++; lolen--;
    }
  }
  if ( hi != NULL ) {
    if ( hi[ 0 ] == '(' ) {
      hi++; hilen--; hi_incl = ! hi_incl;
    }
    else if ( hi[ 0 ] == '[' ) {
      hi++; hilen--;
    }
  }
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND:
      goto finished;
    case KEY_OK:
      if ( ctx.type == MD_NODATA )
        goto finished;
      if ( ctx.type != MD_ZSET && ctx.type != MD_GEO )
        return ERR_BAD_TYPE;
      break;
    default:
      return ERR_KV_STATUS;
  }

  if ( ctx.type == MD_ZSET ) {
    ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );
    ZSetVal    zv;
    ZSetStatus zstatus;

    if ( ! zset.open_readonly() )
      return ERR_KV_STATUS;
    if ( (count = zset.x->hcount()) == 0 )
      goto finished;

    /* if by range by rank */
    if ( ( flags & ( DO_ZRANGE | DO_ZREVRANGE ) ) != 0 ) {
      if ( ival < 0 )
        ival = count + ival;
      if ( jval < 0 )
        jval = count + jval;
      ival = min_int<int64_t>( count, max_int<int64_t>( 0, ival ) );
      jval = min_int<int64_t>( count, max_int<int64_t>( 0, jval + 1 ) );
      if ( ival >= jval )
        goto finished;
      i = ival;
      j = jval;
    }
    else {
      /* if range by score */
      if ( ( flags & ( DO_ZRANGEBYSCORE | DO_ZREVRANGEBYSCORE ) ) != 0 ) {
        ZScore loval = str_to_score( lo, lolen ),
               hival = str_to_score( hi, hilen ),
               r3;
        zset.x->zbsearch( loval, i, lo_incl ? false : true, r3 );
        zset.x->zbsearch( hival, j, hi_incl ? true : false, r3 );
        if ( ( flags & DO_ZRANGEBYSCORE ) != 0 ) {
          i  = i - 1 + zoff;
          j -= 1;
        }
        else { /* else REVRANGE */
          i = count + 1 - i + zoff;
          j = count + 1 - j;
        }
      }
      /* if range by lex */
      else if ( ( flags & ( DO_ZRANGEBYLEX |
                            DO_ZREVRANGEBYLEX ) ) != 0 ) {
        if ( lolen == 1 && lo[ 0 ] == '-' ) /* 1st element */
          i = 1;
        else if ( lolen == 1 && lo[ 0 ] == '+' ) /* last element */
          i = zset.x->hcount() + 1;
        else
          zset.x->zbsearch_all( lo, lolen, lo_incl ? false : true, i );
        if ( hilen == 1 && hi[ 0 ] == '+' ) /* last element */
          j = zset.x->hcount() + 1;
        else if ( hilen == 1 && hi[ 0 ] == '-' ) /* 1st element */
          j = 1;
        else
          zset.x->zbsearch_all( hi, hilen, hi_incl ? true : false, j );
        if ( ( flags & DO_ZRANGEBYLEX ) != 0 ) {
          i  = i - 1 + zoff; /* zoff = offset into range */
          j -= 1;
        }
        else { /* else REVRANGE */
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
      zstatus = zset.x->zindex( k, zv );
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
        fvallen = zv.score.to_string( fpdata );
        if ( q.append_string( fpdata, fvallen ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
      }
    }
  }
  /* ctx.type == MD_GEO */
  else {
    ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
    GeoVal    gv;
    GeoStatus gstatus;

    if ( ! geo.open_readonly() )
      return ERR_KV_STATUS;
    if ( (count = geo.x->hcount()) == 0 )
      goto finished;

    /* if by range by rank */
    if ( ( flags & ( DO_ZRANGE | DO_ZREVRANGE ) ) != 0 ) {
      if ( ival < 0 )
        ival = count + ival;
      if ( jval < 0 )
        jval = count + jval;
      ival = min_int<int64_t>( count, max_int<int64_t>( 0, ival ) );
      jval = min_int<int64_t>( count, max_int<int64_t>( 0, jval + 1 ) );
      if ( ival >= jval )
        goto finished;
      i = ival;
      j = jval;
    }
    else {
      /* if range by score */
      if ( ( flags & ( DO_ZRANGEBYSCORE | DO_ZREVRANGEBYSCORE ) ) != 0 ) {
        H3Index loval, hival, r3;
        RedisMsg::str_to_uint( lo, lolen, loval );
        RedisMsg::str_to_uint( hi, hilen, hival );
        geo.x->geobsearch( loval, i, lo_incl ? false : true, r3 );
        geo.x->geobsearch( hival, j, hi_incl ? true : false, r3 );
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
      else if ( ( flags & ( DO_ZRANGEBYLEX |
                            DO_ZREVRANGEBYLEX ) ) != 0 ) {
        return ERR_BAD_TYPE;
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
      gstatus = geo.x->geoindex( k, gv );
      if ( gstatus != GEO_OK )
        break;
      /* match wildcard */
      if ( ( flags & DO_ZSCAN ) != 0 ) {
        if ( sa->re != NULL ) {
          char buf[ 256 ];
          void * subj;
          size_t subjlen;
          bool is_alloced = false;
          subjlen = gv.unitary( subj, buf, sizeof( buf ), is_alloced );
          int rc = pcre2_match( sa->re, (PCRE2_SPTR8) subj, subjlen,
                                0, 0, sa->md, 0 );
          if ( is_alloced )
            ::free( subj );
          if ( rc < 1 )
            continue;
        }
      }
      if ( q.append_string( gv.data, gv.sz, gv.data2, gv.sz2 ) == 0 )
        return ERR_ALLOC_FAIL;
      itemcnt++;
      if ( withscores ) {
        fvallen = uint64_to_string( gv.score, fpdata );
        if ( q.append_string( fpdata, fvallen ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
      }
    }
  }
finished:;
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
RedisExec::do_zremrange( EvKeyCtx &ctx,  int flags ) noexcept
{
  const char * lo    = NULL,
             * hi    = NULL;
  size_t       lolen = 0,
               hilen = 0;
  int64_t      ival  = 0,
               jval  = 0;

  /* ZPOPMIN/MAX key [cnt] or BZPOPMIN/MAX key key2 key3 timeout */
  if ( ( flags & ZSET_POP_CMDS ) != 0 ) {
    if ( ( flags & ( DO_ZPOPMAX | DO_ZPOPMIN ) ) != 0 && this->argc > 2 ) {
      if ( ! this->msg.get_arg( 2, jval ) )
        return ERR_BAD_ARGS;
    }
    else /* default and bz is always 1 */
      jval = 1;
  }
  else if ( ( flags & DO_ZREMRANGEBYRANK ) != 0 ) {
    if ( ! this->msg.get_arg( 2, ival ) || ! this->msg.get_arg( 3, jval ) )
      return ERR_BAD_ARGS;
  }
  /* ZREMRANGEBYLEX key start stop */
  /* ZREMRANGEBYSCORE key start stop */
  else {
    if ( ! msg.get_arg( 2, lo, lolen ) || ! msg.get_arg( 3, hi, hilen ) )
      return ERR_BAD_ARGS;
  }
  size_t count,
         i, j;
  /* lo, hi inclusive, rev range is opposite */
  bool   lo_incl = true,
         hi_incl = true;

  if ( lo != NULL ) {
    if ( lo[ 0 ] == '(' ) {
      lo++; lolen--; lo_incl = ! lo_incl;
    }
    else if ( lo[ 0 ] == '[' ) {
      lo++; lolen--;
    }
  }
  if ( hi != NULL ) {
    if ( hi[ 0 ] == '(' ) {
      hi++; hilen--; hi_incl = ! hi_incl;
    }
    else if ( hi[ 0 ] == '[' ) {
      hi++; hilen--;
    }
  }
  switch ( this->exec_key_fetch( ctx ) ) {
    default:
      return ERR_KV_STATUS;
    case KEY_OK:
      if ( ctx.type == MD_NODATA ) {
    case KEY_IS_NEW:
        if ( ( flags & ZSET_POP_CMDS ) != 0 )
          return EXEC_SEND_ZEROARR;
        return EXEC_SEND_ZERO;
      }
      if ( ctx.type != MD_ZSET && ctx.type != MD_GEO )
        return ERR_BAD_TYPE;
      break;
  }

  if ( ctx.type == MD_ZSET ) {
    ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );

    if ( ! zset.open() )
      return ERR_KV_STATUS;
    if ( (count = zset.x->hcount()) == 0 ) {
      if ( ( flags & ZSET_POP_CMDS ) != 0 )
        return EXEC_SEND_ZEROARR;
      return EXEC_SEND_ZERO;
    }
    if ( ( flags & ZSET_POP_CMDS ) != 0 ) {
      if ( ( flags & ( DO_ZPOPMIN | DO_BZPOPMIN ) ) != 0 ) {
        ival = 0;
        if ( jval > (int64_t) count )
          jval = count;
        else if ( jval < 0 )
          jval = 0;
      }
      else { /* ZPOPMAX, BZPOPMAX */
        ival = jval;
        jval = count; 
        if ( ival > 0 && (size_t) ival <= count )
          ival = count - ival;
        else
          ival = count;
      }
      i = ival;
      j = jval;
    }
    else if ( ( flags & DO_ZREMRANGEBYRANK ) != 0 ) {
      if ( ival < 0 )
        ival = count + ival;
      if ( jval < 0 )
        jval = count + jval;
      ival = min_int<int64_t>( count, max_int<int64_t>( 0, ival ) );
      jval = min_int<int64_t>( count, max_int<int64_t>( 0, jval + 1 ) );
      if ( ival >= jval )
        return EXEC_SEND_ZERO;
      i = ival;
      j = jval;
    }
    else {
      if ( ( flags & DO_ZREMRANGEBYSCORE ) != 0 ) {
        ZScore loval = str_to_score( lo, lolen ),
               hival = str_to_score( hi, hilen ),
               r3;
        zset.x->zbsearch( loval, i, lo_incl ? false : true, r3 );
        zset.x->zbsearch( hival, j, hi_incl ? true : false, r3 );
        i  = i - 1;
        j -= 1;
      }
      else /*if ( ( flags & DO_ZREMRANGEBYLEX ) != 0 )*/ {
        zset.x->zbsearch_all( lo, lolen, lo_incl ? false : true, i );
        zset.x->zbsearch_all( hi, hilen, hi_incl ? true : false, j );
        i  = i - 1;
        j -= 1;
      }
    }

    if ( ( flags & ZSET_POP_CMDS ) != 0 ) {
      StreamBuf::BufQueue q( this->strm );
      ZSetVal    zv;
      ZSetStatus zstatus;
      char       fpdata[ 64 ];
      size_t     fvallen, itemcnt = 0, k;

      for (;;) {
        if ( i >= j )
          break;
        if ( ( flags & ( DO_ZPOPMAX | DO_BZPOPMAX ) ) != 0 )
          k = j--;
        else
          k = ++i;
        zstatus = zset.x->zindex( k, zv );
        if ( zstatus != ZSET_OK )
          break;
        if ( ( flags & ( DO_BZPOPMIN | DO_BZPOPMAX ) ) != 0 ) {
          if ( q.append_string( ctx.kbuf.u.buf, ctx.kbuf.keylen - 1 ) == 0 )
            return ERR_ALLOC_FAIL;
          itemcnt++;
        }
        if ( q.append_string( zv.data, zv.sz, zv.data2, zv.sz2 ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
        fvallen = zv.score.to_string( fpdata );
        if ( q.append_string( fpdata, fvallen ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
      }
      q.prepend_array( itemcnt );
      this->strm.append_iov( q );

      i = ival;
      j = jval;
    }

    ctx.ival = j - i;
    if ( (size_t) ctx.ival == count ) {
      zset.x->zremall();
    }
    else {
      while ( i < j ) {
        zset.x->zrem_index( j );
        j -= 1;
      }
    }
    if ( ctx.ival > 0 ) {
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_ZSET;
      if ( zset.x->hcount() == 0 ) {
        ctx.flags |= EKF_KEYSPACE_DEL;
        if ( ! zset.tombstone() )
          return ERR_KV_STATUS;
      }
    }
  }
  else { /* MD_GEO */
    ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );

    if ( ! geo.open() )
      return ERR_KV_STATUS;
    if ( (count = geo.x->hcount()) == 0 ) {
      if ( ( flags & ZSET_POP_CMDS ) != 0 )
        return EXEC_SEND_ZEROARR;
      return EXEC_SEND_ZERO;
    }
    if ( ( flags & ZSET_POP_CMDS ) != 0 ) {
      if ( ( flags & ( DO_ZPOPMIN | DO_BZPOPMIN ) ) != 0 ) {
        ival = 0;
        if ( jval > (int64_t) count )
          jval = count;
        else if ( jval < 0 )
          jval = 0;
      }
      else { /* ZPOPMAX, BZPOPMAX */
        ival = jval;
        jval = count; 
        if ( ival > 0 && (size_t) ival <= count )
          ival = count - ival;
        else
          ival = count;
      }
      i = ival;
      j = jval;
    }
    else if ( ( flags & DO_ZREMRANGEBYRANK ) != 0 ) {
      if ( ival < 0 )
        ival = count + ival;
      if ( jval < 0 )
        jval = count + jval;
      ival = min_int<int64_t>( count, max_int<int64_t>( 0, ival ) );
      jval = min_int<int64_t>( count, max_int<int64_t>( 0, jval + 1 ) );
      if ( ival >= jval )
        return EXEC_SEND_ZERO;
      i = ival;
      j = jval;
    }
    else { /* zremrangebyscore, zremrangebylex ?? maybe ?? */
      return ERR_BAD_TYPE;
    }

    if ( ( flags & ZSET_POP_CMDS ) != 0 ) {
      StreamBuf::BufQueue q( this->strm );
      GeoVal     gv;
      GeoStatus  gstatus;
      char       fpdata[ 64 ];
      size_t     fvallen, itemcnt = 0, k;

      for (;;) {
        if ( i >= j )
          break;
        if ( ( flags & ( DO_ZPOPMAX | DO_BZPOPMAX ) ) != 0 )
          k = j--;
        else
          k = ++i;
        gstatus = geo.x->geoindex( k, gv );
        if ( gstatus != GEO_OK )
          break;
        if ( ( flags & ( DO_BZPOPMIN | DO_BZPOPMAX ) ) != 0 ) {
          if ( q.append_string( ctx.kbuf.u.buf, ctx.kbuf.keylen - 1 ) == 0 )
            return ERR_ALLOC_FAIL;
          itemcnt++;
        }
        if ( q.append_string( gv.data, gv.sz, gv.data2, gv.sz2 ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
        fvallen = uint64_to_string( gv.score, fpdata );
        if ( q.append_string( fpdata, fvallen ) == 0 )
          return ERR_ALLOC_FAIL;
        itemcnt++;
      }
      q.prepend_array( itemcnt );
      this->strm.append_iov( q );

      i = ival;
      j = jval;
    }

    ctx.ival = j - i;
    if ( (size_t) ctx.ival == count ) {
      geo.x->georemall();
    }
    else {
      while ( i < j ) {
        geo.x->georem_index( j );
        j -= 1;
      }
    }
    if ( ctx.ival > 0 ) {
      ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_GEO;
      if ( geo.x->hcount() == 0 ) {
        ctx.flags |= EKF_KEYSPACE_DEL;
        if ( ! geo.tombstone() )
          return ERR_KV_STATUS;
      }
    }
  }
  if ( ( flags & ZSET_POP_CMDS ) != 0 )
    return EXEC_OK;
  return EXEC_SEND_INT;
}

namespace {
struct zsetop_data {
  void   * data;
  uint64_t datalen;
  uint8_t  type;
  zsetop_data() : data( 0 ), datalen( 0 ), type( MD_NODATA ) {}
};
}

ExecStatus
RedisExec::do_zsetop( EvKeyCtx &ctx,  int flags ) noexcept
{
  /* ZINTERSTORE dest nkeys key [key ...] */
  /* ZUNIONSTORE dest nkeys key [key  ...] */
  /* if is the dest key, check if need to wait for src keys */
  if ( ctx.argn == 1 && this->key_cnt != this->key_done + 1 )
    return EXEC_DEPENDS;
  /* if not dest key, fetch set and save */
  if ( ctx.argn != 1 ) {
    zsetop_data z, * zptr;
    switch ( this->exec_key_fetch( ctx, true ) ) {
      case KEY_OK:
        if ( ctx.type == MD_ZSET || ctx.type == MD_GEO ) {
          ctx.kstatus = this->kctx.value( &z.data, z.datalen );
          z.type = ctx.type;
          if ( ctx.kstatus != KEY_OK )
            return ERR_KV_STATUS;
        }
        else if ( ctx.type != MD_NODATA )
          return ERR_BAD_TYPE;
      /* FALLTHRU */
      case KEY_NOT_FOUND:
        if ( z.datalen == 0 ) {
          z.data    = (void *) mt_list; /* empty */
          z.datalen = sizeof( mt_list );
        }
        zptr = (zsetop_data *)
               this->save_data2( ctx, &z, sizeof( z ), z.data, z.datalen );
        if ( zptr == NULL )
          return ERR_ALLOC_FAIL;
        zptr->data = (void *) &zptr[ 1 ];
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_OK;
      /* FALLTHRU */
      default: return ERR_KV_STATUS;
    }
  }
  /* is dest key */
  return this->do_zsetop_store( ctx, flags );
}

ExecStatus
RedisExec::do_zsetop_store( EvKeyCtx &ctx,  int flags ) noexcept
{
  zsetop_data  * zptr;
  void         * data2,
               * data;
  uint64_t       datalen;
  ZScore       * weight = NULL;
  size_t         i,
                 retry  = 0,
                 ndata,
                 count;
  uint16_t       kspc_type;
  uint8_t        type;
  ZAggregateType aggregate_type = ZAGGREGATE_SUM;
  bool           has_weights    = false;

  /* parse the weights and aggregate type */
  for ( i = 3 + this->key_cnt - 1; i < this->argc; ) {
    switch ( this->msg.match_arg( i, MARG( "aggregate" ),
                                     MARG( "weights" ), NULL ) ) {
      case 1:
        i += 1;
        switch ( this->msg.match_arg( i, MARG( "sum" ),
                                         MARG( "min" ),
                                         MARG( "max" ),
                                         MARG( "none" ), NULL ) ) {
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
  zptr    = (zsetop_data *) this->keys[ 1 ]->part->data( 0 );
  data    = zptr->data;
  datalen = zptr->datalen;
  type    = zptr->type;

  if ( type == MD_NODATA ) {
    for ( i = 2; i < this->key_cnt; i++ ) {
      zptr = (zsetop_data *) this->keys[ i ]->part->data( 0 );
      type = zptr->type;
      if ( type != MD_NODATA )
        break;
    }
    if ( type == MD_NODATA )
      type = MD_ZSET; /* empty set */
  }

  if ( type == MD_ZSET ) {
    ZSetData   tmp[ 2 ];
    ZSetData * zset,
             * old_zset;
    int        n = 0;

    zset = new ( (void *) &tmp[ n++%2 ] ) ZSetData( data, datalen );
    zset->open();
    /* if first set is weighted, scale it */
    if ( has_weights ) {
      if ( weight[ 0 ] != ZScore::itod( 0 ) )
        zset->zscale( weight[ 0 ] );
    }
    /* merge the source keys together */
    for ( i = 2; i < this->key_cnt; i++ ) {
      zptr = (zsetop_data *) this->keys[ i ]->part->data( 0 );
      ZSetData set2( zptr->data, zptr->datalen );
      type = zptr->type;
      if ( type != MD_ZSET && type != MD_NODATA )
        return ERR_BAD_TYPE; /* prevent mixing sortedset with geo */
      ZMergeCtx  ctx;
      ZSetStatus zstat = ZSET_OK;
      ctx.init( has_weights ? weight[ i - 1 ] : ZScore::itod( 1 ),
                aggregate_type, has_weights );
      set2.open();
      for (;;) {
        switch ( flags & ( DO_ZUNIONSTORE | DO_ZINTERSTORE ) ) {
          case DO_ZUNIONSTORE:
            zstat = zset->zunion( set2, ctx );
            break;
          case DO_ZINTERSTORE:
            zstat = zset->zinter( set2, ctx );
            break;
        }
        if ( zstat != ZSET_FULL )
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
    data      = zset->listp;
    datalen   = zset->size;
    count     = zset->hcount();
    kspc_type = EKF_KEYSPACE_ZSET;
  }
  else { /* type == MD_GEO */
    GeoData   tmp[ 2 ];
    GeoData * geo,
            * old_geo;
    int       n = 0;

    geo = new ( (void *) &tmp[ n++%2 ] ) GeoData( data, datalen );
    geo->open();

    /* merge the source keys together */
    for ( i = 2; i < this->key_cnt; i++ ) {
      zptr = (zsetop_data *) this->keys[ i ]->part->data( 0 );
      GeoData set2( zptr->data, zptr->datalen );
      type = zptr->type;
      if ( type != MD_GEO && type != MD_NODATA )
        return ERR_BAD_TYPE; /* prevent mixing sortedset with geo */
      GeoMergeCtx ctx;
      GeoStatus gstat = GEO_OK;
      ctx.init( 1, ZAGGREGATE_NONE, false );
      set2.open();
      for (;;) {
        switch ( flags & ( DO_ZUNIONSTORE | DO_ZINTERSTORE ) ) {
          case DO_ZUNIONSTORE:
            gstat = geo->geounion( set2, ctx );
            break;
          case DO_ZINTERSTORE:
            gstat = geo->geointer( set2, ctx );
            break;
        }
        if ( gstat != GEO_FULL )
          break;
        /* resize set */
        count = set2.count() + 2;
        ndata = set2.data_len() + retry;
        retry += 16;
        datalen = geo->resize_size( count, ndata );
        data = this->strm.alloc( datalen );
        if ( data == NULL )
          return ERR_ALLOC_FAIL;
        old_geo = geo;
        geo = new ( (void *) &tmp[ n++%2 ] ) GeoData( data, datalen );
        geo->init( count, ndata );
        old_geo->copy( *geo );
      }
    }
    data      = geo->listp;
    datalen   = geo->size;
    count     = geo->hcount();
    kspc_type = EKF_KEYSPACE_GEO;
  }

  /* save the result */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_IS_NEW:
    case KEY_OK:
      ctx.kstatus = this->kctx.resize( &data2, datalen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data2, data, datalen );
        ctx.ival   = count;
        ctx.type   = type;
        ctx.flags |= EKF_IS_NEW | EKF_KEYSPACE_EVENT |
                     kspc_type | EKF_ZSETBLKD_NOT;
        return EXEC_SEND_INT;
      }
    /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }
}
