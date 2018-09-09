#define __STDC_WANT_DEC_FP__ 1
#include <stdio.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_exec.h>
#include <raikv/work.h>
#include <raikv/key_hash.h>
#include <raids/redis_zset.h>

static const char *
zset_status_string[] = { "ok", "not found", "full", "updated", "exists" };

using namespace rai;
using namespace ds;
#define fallthrough __attribute__ ((fallthrough))

static ZSetData *
resize_zset( ZSetData *curr,  size_t add_len,  bool is_copy = false )
{
  size_t count;
  size_t data_len;
  count = ( add_len >> 3 ) | 1;
  data_len = add_len + 1;
  if ( curr != NULL ) {
    data_len  = add_len + curr->data_len();
    data_len += data_len / 2 + 2;
    count     = curr->count();
    count    += count / 2 + 2;
  }
  size_t asize = ZSetData::alloc_size( count, data_len );
  printf( "asize %ld, count %ld, data_len %ld\n", asize, count, data_len );
  void *m = malloc( sizeof( ZSetData ) + asize );
  void *p = &((char *) m)[ sizeof( ZSetData ) ];
  ZSetData *newbe = new ( m ) ZSetData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    int x, y;
    printf( "verify curr: %d\n", x = curr->zverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d\n", y = newbe->zverify() );
    if ( x != 0 || y != 0 ) {
      printf( "curr: " );
      curr->lprint();
      printf( "newbe: " );
      newbe->lprint();
    }
    if ( ! is_copy )
      delete curr;
  }
  printf( "%.2f%% data %.2f%% hash\n",
          ( newbe->data_len() + add_len ) * 100.0 / newbe->data_size(),
          ( newbe->count() + 1 ) * 100.0 / newbe->max_count() );
  return newbe;
}

struct ZSetKey {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  ZSetKey  * next;
  ZSetData * zset;
  uint32_t  hash;
  ZSetKey() : next( 0 ), zset( 0 ), hash( 0 ) {}
  ~ZSetKey() {
    if ( this->zset != NULL )
      delete this->zset;
  }

  ZSetKey *copy( void ) {
    void *p = ::malloc( sizeof( ZSetKey ) );
    if ( p == NULL ) return NULL;
    ZSetKey *sk = new ( p ) ZSetKey();
    sk->hash = this->hash;
    sk->zset  = resize_zset( this->zset, 0, true );
    sk->next = NULL;
    return sk;
  }
};

struct ZSetDB {
  ZSetKey *list;

  ZSetKey *fetch( const char *k,  size_t klen ) {
    ZSetKey *sk = this->list;
    uint32_t h = kv_crc_c( k, klen, 0 );
    for ( ; sk != NULL; sk = sk->next ) {
      if ( sk->hash == h )
        return sk;
    }
    void *p = ::malloc( sizeof( ZSetKey ) );
    if ( p == NULL ) return NULL;
    sk = new ( p ) ZSetKey();
    sk->hash = h;
    sk->zset  = resize_zset( NULL, 16 );
    sk->next = this->list;
    this->list = sk;
    return sk;
  }
  void save( const char *k,  size_t klen,  ZSetKey *sk ) {
    sk->hash = kv_crc_c( k, klen, 0 );
    sk->next = this->list;
    this->list = sk;
    for ( ZSetKey *p = sk; p->next != NULL; p = p->next ) {
      if ( p->next->hash == sk->hash ) {
        ZSetKey *q = p->next;
        p->next = q->next;
        delete q;
        break;
      }
    }
  }
} zsetdb;

static ZScore
str_to_score( const char *score,  size_t scorelen )
{
  char scbuf[ 32 ];;
  scorelen = kv::min<size_t>( scorelen, sizeof( scbuf ) - 1 );
  ::memcpy( scbuf, score, scorelen );
  scbuf[ scorelen ] = '\0';
  return ::strtod64( scbuf, NULL );
}

static bool
isfwd( RedisCmd cmd )
{
  switch ( cmd ) {
    case ZREVRANGEBYLEX_CMD:
    case ZREVRANGE_CMD:
    case ZREVRANGEBYSCORE_CMD:
    case ZREVRANK_CMD:
      return false;
    default:
      return true;
  }
}

static void
swap( size_t &i,  size_t &j )
{
  size_t k = i; i = j; j = k;
}

int
main( int, char ** )
{
  static char Dfmt[4] = { '%', 'D', 'a', 0 };
  ZSetKey *sk, *sk2;
  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  char upper_cmd[ 32 ], key[ 256 ], thelimit[ 64 ] = { 0 };
  size_t i, j, sz, count, namelen, num_keys;
  size_t cmdlen, argcount, scorelen, scorelen2, arglen, keylen;
  const char *cmdbuf, *name, *score, *score2, *arg;
  const char *lexlo, *lexhi;
  size_t lexlolen, lexhilen;
  HashPos pos;
  ZSetVal zv;
  ZScore r, r2, r3, weight[ 8 ];
  int64_t ival, jval;
  int fl;
  RedisMsgStatus mstatus;
  ZSetStatus zstat;
  RedisCmd cmd;
  ZAggregateType agg_type;
  bool withscores = false, limit = false, aggregate, weights;

  printf( "> " ); fflush( stdout );
  for (;;) {
    if ( fgets( buf, sizeof( buf ), stdin ) == NULL )
      break;
    if ( buf[ 0 ] == '#' || buf[ 0 ] == '\n' )
      continue;
    tmp.reset();
    cmdlen = ::strlen( buf );
    if ( buf[ 0 ] == '[' )
      mstatus = msg.unpack_json( buf, cmdlen, tmp );
    else
      mstatus = msg.unpack( buf, cmdlen, tmp );
    if ( mstatus != REDIS_MSG_OK ) {
      printf( "error %d/%s\n", mstatus, redis_msg_status_string( mstatus ) );
      continue;
    }
    cmdbuf = msg.command( cmdlen, argcount );
    if ( cmdlen >= 32 ) {
      printf( "cmd to large\n" );
      continue;
    }
    if ( cmdlen == 0 )
      continue;
    str_to_upper( cmdbuf, upper_cmd, cmdlen );

    cmd = get_redis_cmd( upper_cmd, cmdlen );
    if ( cmd == NO_CMD ) {
      printf( "no cmd\n" );
      continue;
    }
    sz = msg.to_almost_json( buf2 );
    printf( "\nexec %.*s\n", (int) sz, buf2 );
    /* ZINTERSTORE dest num key [key] */
    if ( cmd == ZINTERSTORE_CMD || cmd == ZUNIONSTORE_CMD ) {
      if ( ! msg.get_arg( 3, name, namelen ) )
        goto bad_args;
    }
    else {
      if ( ! msg.get_arg( 1, name, namelen ) )
        goto bad_args;
    }
    sk = zsetdb.fetch( name, namelen );
    if ( sk == NULL ) {
      printf( "out of memory\n" );
      return 1;
    }
    switch ( cmd ) {
      case ZADD_CMD:            /* ZADD key [NX|XX] [CH] [INCR] score mem */
      case ZINCRBY_CMD:         /* ZINCRBY key incr mem */
        fl = 0;
        count = 0;
        if ( cmd == ZINCRBY_CMD ) {
          fl |= ZADD_INCR;
          i = 2;
          goto get_zadd_args;
        }
        else {
          for ( i = 2; i < argcount; i++ ) {
            switch ( msg.match_arg( i, "nx", 2,
                                       "xx", 2,
                                       "ch", 2,
                                       "incr", 4, NULL ) ) {
              case 1: fl |= ZADD_MUST_NOT_EXIST; break;
              case 2: fl |= ZADD_MUST_EXIST;     break;
              case 3: fl |= ZADD_RET_CHANGED;    break;
              case 4: fl |= ZADD_INCR;           break;
              default:
              get_zadd_args:;
                if ( ! msg.get_arg( i, score, scorelen ) ||
                     ! msg.get_arg( i+1, arg, arglen ) )
                  goto bad_args;
                i += 2;
                goto do_add;
            }
          }
        }
      do_add:;
        r = str_to_score( score, scorelen );
        pos.init( arg, arglen );
        for (;;) {
          zstat = sk->zset->zadd( arg, arglen, r, pos, fl, NULL );
          printf( "%s\n", zset_status_string[ zstat ] );
          if ( zstat != ZSET_FULL ) break;
          sk->zset = resize_zset( sk->zset, arglen );
        }
        if ( zstat == ZSET_UPDATED )
          count++;
        if ( i < argcount ) {
          if ( ! msg.get_arg( i, score, scorelen ) ||
               ! msg.get_arg( i+1, arg, arglen ) )
            goto bad_args;
          i += 2;
          goto do_add;
        }
        printf( "%s %ld\n",
                ( fl & ZADD_RET_CHANGED ) != 0 ? "Updated" : "Added", count );

        break;
      case ZCARD_CMD:           /* ZCARD key */
        count = sk->zset->hcount();
        printf( "%ld\n", count );
        break;
      case ZRANGE_CMD:          /* ZRANGE key start stop [WITHSCORES] */
      case ZREVRANGE_CMD:       /* ZREVRANGE key start stop [WITHSCORES] */
        withscores = ( msg.match_arg( 4, "withscores", 10, NULL ) == 1 );
        fallthrough;
      case ZREMRANGEBYRANK_CMD: /* ZREMRANGEBYRANK key start stop */
        if ( ! msg.get_arg( 2, ival ) || ! msg.get_arg( 3, jval ) )
          goto bad_args;
        count = sk->zset->hcount();
        if ( ival < 0 )
          ival = count + ival;
        if ( jval < 0 )
          jval = count + jval;
        ival = kv::min<int64_t>( count, kv::max<int64_t>( 0, ival ) );
        jval = kv::min<int64_t>( count, kv::max<int64_t>( 0, jval + 1 ) );
        if ( cmd == ZREMRANGEBYRANK_CMD )
          goto do_rem_range;
      do_range:;
        if ( ival >= jval ) {
          printf( "null range\n" );
          break;
        }
        i = ival + 1;
        j = jval;
        if ( ! isfwd( cmd ) )
          swap( i, j );
        for (;;) {
          zstat = sk->zset->zindex( i, zv );
          if ( zstat == ZSET_OK ) {
            printf( "%ld. off(%ld) %sscore(", i, sk->zset->offset( i ),
                    withscores ? "*" : "" );
            printf( Dfmt, zv.score );
            printf( ") %.*s", (int) zv.sz, (const char *) zv.data );
            if ( zv.sz2 > 0 )
              printf( "%.*s", (int) zv.sz2, (const char *) zv.data2 );
            printf( "\n" );
          }
          if ( i == j )
            break;
          i += ( isfwd( cmd ) ? 1 : -1 );
        }
        withscores = false;
        printf( "count %lu of %lu\n", count, sk->zset->max_count() - 1 );
        printf( "bytes %lu of %lu\n", (size_t) sk->zset->data_len(),
                sk->zset->data_size() );
        printf( "[" ); sk->zset->print_hashes(); printf( "]\n" );

        i = ival + 1;
        j = jval;
        if ( ! isfwd( cmd ) )
          swap( i, j );
        for (;;) {
          zstat = sk->zset->zindex( i, zv );
          if ( zstat == ZSET_OK ) {
            keylen = kv::min<size_t>( zv.sz, sizeof( key ) );
            ::memcpy( key, zv.data, keylen );
            sz = kv::min<size_t>( sizeof( key ) - keylen, zv.sz2 );
            ::memcpy( &key[ keylen ], zv.data2, sz );
            keylen += sz;
            pos.init( key, keylen );
            zstat = sk->zset->zexists( key, keylen, pos, r );
            if ( zstat == ZSET_OK )
              printf( "%ld. idx(%ld) h(%u) %.*s\n", i, pos.i, (uint8_t) pos.h,
                      (int) keylen, key );
            else
              printf( "%ld. idx(****) h(%u) %.*s\n", i, (uint8_t) pos.h,
                      (int) keylen, key );
          }
          else {
            printf( "%ld. status=%d\n", i, (int) zstat );
          }
          if ( i == j )
            break;
          i += ( isfwd( cmd ) ? 1 : -1 );
        }
#if 0
        if ( cmd != SMEMBERS_CMD ) {
          if ( cmd == SDIFFSTORE_CMD || cmd == SINTERSTORE_CMD ||
               cmd == SUNIONSTORE_CMD ) {
            if ( ! msg.get_arg( 1, name, namelen ) )
              goto bad_args;
            setdb.save( name, namelen, sk );
          }
          else {
            delete sk;
          }
        }
#endif
        break;
      case ZRANGEBYLEX_CMD:     /* ZRANGEBYLEX key min max [LIMIT off cnt] */
      case ZREVRANGEBYLEX_CMD:  /* ZREVRANGEBYLEX key min max [LIMIT off cnt] */
        limit = ( msg.match_arg( 4, "limit", 5, NULL ) == 1 );
        fallthrough;
      case ZLEXCOUNT_CMD:       /* ZLEXCOUNT key min max */
      case ZREMRANGEBYLEX_CMD:  /* ZREMRANGEBYLEX key min max */
        if ( ! msg.get_arg( 2, lexlo, lexlolen ) ||
             ! msg.get_arg( 3, lexhi, lexhilen ) )
          goto bad_args;
        if ( limit ) {
          if ( ! msg.get_arg( 5, ival ) ||
               ! msg.get_arg( 6, jval ) )
            goto bad_args;
          ::snprintf( thelimit, sizeof( thelimit ),
                      " limit off=%ld count=%ld", ival, jval );
        }
        sk->zset->zbsearch_all( lexlo, lexlolen, false, i );
        ival = i - 1;
        sk->zset->zbsearch_all( lexhi, lexhilen, true, i );
        jval = i - 1; /* if inclusive */
        printf( "posit %ld -> %ld%s\n", ival+1, jval+1, thelimit );
        limit = false;
        thelimit[ 0 ] = '\0';
        if ( cmd == ZRANGEBYLEX_CMD || cmd == ZREVRANGEBYLEX_CMD ) {
          count = sk->zset->count();
          goto do_range;
        }
        else if ( cmd == ZREMRANGEBYLEX_CMD )
          goto do_rem_range;
        break;
      case ZCOUNT_CMD:          /* ZCOUNT key min max */
      case ZRANGEBYSCORE_CMD:   /* ZRANGEBYSCORE key min max [WITHSCORES] */
      case ZREMRANGEBYSCORE_CMD:/* ZREMRANGEBYSCORE key start stop */
      case ZREVRANGEBYSCORE_CMD:/* ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT off cnt] */
        if ( ! msg.get_arg( 2, score, scorelen ) ||
             ! msg.get_arg( 3, score2, scorelen2 ) )
          goto bad_args;
        r = str_to_score( score, scorelen );
        printf( "range " ); printf( Dfmt, r );
        r2 = str_to_score( score2, scorelen2 );
        printf( " -> " ); printf( Dfmt, r2 ); printf( "\n" );
        sk->zset->zbsearch( r, i, false, r3 );
        ival = i - 1;
        sk->zset->zbsearch( r2, i, true, r3 );
        jval = i - 1; /* if inclusive */
        printf( "posit %ld -> %ld\n", ival+1, jval+1 );
        if ( cmd == ZCOUNT_CMD ) {
          printf( "%ld\n", jval - ival );
        }
        else if ( cmd == ZRANGEBYSCORE_CMD || cmd == ZREVRANGEBYSCORE_CMD ) {
          count = sk->zset->count();
          goto do_range;
        }
        else if ( cmd == ZREMRANGEBYSCORE_CMD )
          goto do_rem_range;
        break;
      case ZRANK_CMD:           /* ZRANK key mem */
      case ZREVRANK_CMD:        /* ZREVRANK key mem */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        count = sk->zset->count();
        pos.init( arg, arglen );
        zstat = sk->zset->zexists( arg, arglen, pos, r );
        if ( zstat == ZSET_OK )
          printf( "%ld\n",
                  ( cmd == ZRANK_CMD ) ? pos.i - 1 : count - ( pos.i + 1 ) );
        else
          printf( "nil\n" );
        break;
      case ZREM_CMD:            /* ZREM key mem [mem] */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          zstat = sk->zset->zrem( arg, arglen, pos );
          printf( "%.*s: %s\n", (int) arglen, arg, zset_status_string[ zstat ]);
        }
        break;
      do_rem_range:;
        if ( ival >= jval ) {
          printf( "null range\n" );
          break;
        }
        j = ival + 1;
        i = jval;
        count = 0;
        for (;;) {
          zstat = (ZSetStatus) sk->zset->zrem_index( i );
          if ( zstat == ZSET_OK )
            count++;
          if ( i-- == j )
            break;
        }
        printf( "rem %ld\n", count );
        break;
      case ZSCORE_CMD:          /* ZSCORE key mem */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        zstat = sk->zset->zget( arg, arglen, zv, pos );
        if ( zstat == ZSET_OK ) {
          printf( "%ld. off(%ld) score(", pos.i, sk->zset->offset( pos.i ) );
          printf( Dfmt, zv.score );
          printf( ") %.*s", (int) zv.sz, (const char *) zv.data );
          if ( zv.sz2 > 0 )
            printf( "%.*s", (int) zv.sz2, (const char *) zv.data2 );
          printf( "\n" );
        }
        else {
          printf( "%s\n", zset_status_string[ zstat ] );
        }
        break;
      case ZINTERSTORE_CMD:    /* ZINTERSTORE dest num key [key] */
      case ZUNIONSTORE_CMD:    /* ZUNIONSTORE dest num key WEIGHTS [] AGGR [] */
        if ( ! msg.get_arg( 2, ival ) )
          goto bad_args;
        if ( ival <= 0 || (size_t) ival + 3 > argcount )
          goto bad_args;
        num_keys = ival;
        i = num_keys + 3;
        weights = ( msg.match_arg( i, "weights", 7, NULL ) == 1 );
        if ( weights ) {
          i += 1;
          j  = i;
          for ( ; i < j + num_keys; i++ ) {
            if ( ! msg.get_arg( i, score, scorelen ) )
              goto bad_args;
            weight[ i - j ] = str_to_score( score, scorelen );
          }
        }
        else {
          for ( j = 0; j < num_keys; j++ )
            weight[ j ] = 1;
        }
        aggregate = ( msg.match_arg( i, "aggregate", 9, NULL ) == 1 );
        if ( aggregate ) {
          switch ( msg.match_arg( i + 1, "sum", 3,
                                         "min", 3,
                                         "max", 3,
                                         "none", 4, NULL ) ) {
            default: fallthrough;
            case 1: agg_type = ZAGGREGATE_SUM; break;
            case 2: agg_type = ZAGGREGATE_MIN; break;
            case 3: agg_type = ZAGGREGATE_MAX; break;
            case 4: agg_type = ZAGGREGATE_NONE; break;
          }
        }
        else {
          agg_type = ZAGGREGATE_SUM;
        }
        sk = sk->copy();
        if ( sk == NULL ) {
          printf( "out of memory\n" );
          return 1;
        }
        if ( weights && weight[ 0 ] != 1 ) {
          sk->zset->zscale( weight[ 0 ] );
        }
        for ( i = 4; i < num_keys + 3; i++ ) {
          ZMergeCtx ctx;
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          sk2 = zsetdb.fetch( arg, arglen );
          ctx.init( weight[ i - 3 ], agg_type );
          for (;;) {
            if ( cmd == ZUNIONSTORE_CMD )
              zstat = sk->zset->zunion( *sk2->zset, ctx );
            else if ( cmd == ZINTERSTORE_CMD )
              zstat = sk->zset->zinter( *sk2->zset, ctx );
#if 0
            else if ( md == SDIFFSTORE_CMD )
              zstat = sk->zset->sdiff( *sk2->zset );
#endif
            else
              zstat = ZSET_OK;
            printf( "%s\n", zset_status_string[ zstat ] );
            if ( zstat != ZSET_FULL ) break;
            sk->zset = resize_zset( sk->zset, sk2->zset->data_len() );
          }
        }
        if ( ! msg.get_arg( 1, arg, arglen ) )
          goto bad_args;
        zsetdb.save( arg, arglen, sk );
        ival  = 0;
        count = sk->zset->count();
        if ( count > 0 )
          count -= 1;
        jval  = count;
        goto do_range;
        break;
      case ZSCAN_CMD:           /* ZSCAN key curs [MATCH pat] [COUNT cnt] */
        break;
      default:
        printf( "bad cmd\n" );
        if ( 0 ) {
      bad_args:;
          printf( "bad args\n" );
        }
        break;
    }
    printf( "> " ); fflush( stdout );
  }
  return 0;
}

