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
#include <raids/redis_hash.h>

static const char *
hash_status_string[] = { "ok", "not found", "full", "updated", "exists" };

using namespace rai;
using namespace ds;

static HashData *
resize_hash( HashData *curr,  size_t add_len )
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
  size_t asize = HashData::alloc_size( count, data_len );
  printf( "asize %ld, count %ld, data_len %ld\n", asize, count, data_len );
  void *m = malloc( sizeof( HashData ) + asize );
  void *p = &((char *) m)[ sizeof( HashData ) ];
  HashData *newbe = new ( m ) HashData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    int x, y;
    printf( "verify curr: %d\n", x = curr->hverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d\n", y = newbe->hverify() );
    if ( x != 0 || y != 0 ) {
      printf( "curr: " );
      curr->lprint();
      printf( "newbe: " );
      newbe->lprint();
    }
    delete curr;
  }
  printf( "%.2f%% data %.2f%% hash\n",
          newbe->data_len() * 100.0 / newbe->data_size(),
          newbe->count() * 100.0 / newbe->max_count() );
  return newbe;
}

int
main( int, char ** )
{
  HashData *hash = resize_hash( NULL, 16 );

  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  char upper_cmd[ 32 ], ibuf[ 64 ];
  size_t sz, i, count;
  size_t cmdlen, arglen, vallen, fvallen, argcount;
  const char *cmdbuf, *arg, *val, *fval;
  ListVal lv;
  HashVal kv;
  FindPos pos;
  int64_t ival, jval;
  _Decimal128 fp;
  RedisMsgStatus mstatus;
  RedisCmd cmd;
  HashStatus hstat;
  bool is_new;

  printf( "alloc size %lu, index count %lu, data size %lu\n",
          hash->size, hash->max_count(), hash->data_size() );
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
    switch ( cmd ) {
      case HDEL_CMD:    /* HDEL key field [field ...] */
        sz = 0;
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          hstat = hash->hdel( arg, arglen );
          if ( hstat == HASH_OK )
            sz++;
        }
        printf( "%ld\n", sz );
        break;
      case HEXISTS_CMD: /* HEXISTS key field */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        hstat = hash->hexists( arg, arglen );
        printf( "%s\n", hstat == HASH_OK ? "true" : "false" );
        break;
      case HGETALL_CMD: /* HGETALL key */
        count = hash->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hash->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            printf( "%ld. off(%ld) %.*s: ", i, hash->offset( i ),
                    (int) kv.keylen, kv.key );
            printf( "%.*s", (int) kv.sz, (const char *) kv.data );
            if ( kv.sz2 > 0 )
              printf( "%.*s", (int) kv.sz2, (const char *) kv.data2 );
            printf( "\n" );
          }
        }
        printf( "count %lu of %lu\n", count > 0 ? (size_t) count - 1 : 0,
                hash->max_count() - 1 );
        printf( "bytes %lu of %lu\n", (size_t) hash->data_len(),
                hash->data_size() );
        printf( "[" ); hash->print(); printf( "]\n" );

        for ( i = 1; i < count; i++ ) {
          hstat = hash->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            hstat = hash->hgetpos( kv.key, kv.keylen, lv, pos );
            if ( hstat == HASH_OK )
              printf( "%ld. idx(%ld) h(%u) %.*s\n", i, pos.i, (uint8_t) pos.h,
                      (int) kv.keylen, kv.key );
            else
              printf( "%ld. idx(****) h(%u) %.*s\n", i, (uint8_t) pos.h,
                      (int) kv.keylen, kv.key );
          }
          else {
            printf( "%ld. status=%d\n", i, (int) hstat );
          }
        }
        break;
      case HINCRBY_CMD: /* HINCRBY key field int */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, ival ) )
          goto bad_args;
        hstat = hash->hgetpos( arg, arglen, lv, pos );
        is_new = ( hstat == HASH_NOT_FOUND );
        if ( ! is_new ) {
          const char *data;
          sz = lv.sz + lv.sz2;
          if ( sz == lv.sz )
            data = (const char *) lv.data;
          else if ( sz == lv.sz2 )
            data = (const char *) lv.data2;
          else {
            sz   = lv.concat( ibuf, sizeof( ibuf ) );
            data = ibuf;
          }
          mstatus = RedisMsg::str_to_int( data, sz, jval );
          if ( mstatus == REDIS_MSG_OK )
            ival += jval;
        }
        sz = RedisMsg::int_to_str( ival, ibuf );
        for (;;) {
          hstat = hash->hsetpos( arg, arglen, ibuf, sz, pos );
          if ( hstat != HASH_FULL ) {
            printf( "%.*s\n", (int) sz, ibuf );
            break;
          }
          printf( "%s\n", hash_status_string[ hstat ] );
          hash = resize_hash( hash, arglen + vallen + 1 );
        }
        break;
      case HINCRBYFLOAT_CMD: /* HINCRBYFLOAT key field float */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, fval, fvallen ) )
          goto bad_args;
        hstat = hash->hgetpos( arg, arglen, lv, pos );
        is_new = ( hstat == HASH_NOT_FOUND );
        if ( ! is_new ) {
          sz = lv.concat( ibuf, sizeof( ibuf ) - 1 );
          ibuf[ sz ] = '\0';
          fp = ::strtod128( ibuf, NULL );
        }
        else {
          fp = 0.0DL;
        }
        fp += ::strtod128( fval, NULL );
        static char DDfmt[5] = { '%', 'D', 'D', 'a', 0 };
        sz  = ::snprintf( ibuf, sizeof( ibuf ), DDfmt, fp );
        if ( sz == 0 ) sz = ::strlen( ibuf );
        for (;;) {
          hstat = hash->hsetpos( arg, arglen, ibuf, sz, pos );
          if ( hstat != HASH_FULL ) {
            printf( "%.*s\n", (int) sz, ibuf );
            break;
          }
          printf( "%s\n", hash_status_string[ hstat ] );
          hash = resize_hash( hash, arglen + vallen + 1 );
        }
        break;
      case HKEYS_CMD:   /* HKEYS key */
        count = hash->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hash->hindex( i, kv );
          if ( hstat == HASH_OK )
            printf( "%ld. %.*s\n", i, (int) kv.keylen, kv.key );
        }
        break;
      case HLEN_CMD:    /* HLEN key */
        count = hash->count();
        if ( count > 0 ) count -= 1;
        printf( "%ld\n", count );
        break;
      case HMGET_CMD:   /* HMGET key field [field ...] */
      case HGET_CMD:    /* HGET key field */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          hstat = hash->hget( arg, arglen, kv );
          printf( "%.*s: ", (int) arglen, arg );
          if ( hstat == HASH_OK ) {
            printf( "%.*s", (int) kv.sz, (char *) kv.data );
            if ( kv.sz2 > 0 ) printf( "%.*s", (int) kv.sz2, (char *) kv.data2 );
            printf( "\n" );
          }
          else {
            printf( "not found\n" );
          }
        }
        break;
      case HMSET_CMD:   /* HMSET key field val [field val ...] */
      case HSET_CMD:    /* HSET key field val */
        for ( i = 2; i < argcount; i += 2 ) {
          if ( ! msg.get_arg( i, arg, arglen ) ||
               ! msg.get_arg( i+1, val, vallen ) )
            goto bad_args;
          for (;;) {
            hstat = hash->hset( arg, arglen, val, vallen );
            printf( "%s\n", hash_status_string[ hstat ] );
            if ( hstat != HASH_FULL ) break;
            hash = resize_hash( hash, arglen + vallen + 1 );
          }
        }
        break;
      case HSETNX_CMD:  /* HSETNX key field val */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, val, vallen ) )
          goto bad_args;
        for (;;) {
          hstat = hash->hsetnx( arg, arglen, val, vallen );
          printf( "%s\n", hash_status_string[ hstat ] );
          if ( hstat != HASH_FULL ) break;
          hash = resize_hash( hash, arglen + vallen + 1 );
        }
        break;
      case HSTRLEN_CMD: /* HSTRLEN key field */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        hstat = hash->hget( arg, arglen, kv );
        printf( "%.*s: ", (int) arglen, arg );
        if ( hstat == HASH_OK )
          printf( "%ld\n", kv.sz + kv.sz2 );
        else
          printf( "not found\n" );
        break;
      case HVALS_CMD:   /* HVALS key */
        count = hash->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hash->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            printf( "%ld. ", i );
            printf( "%.*s", (int) kv.sz, (const char *) kv.data );
            if ( kv.sz2 > 0 )
              printf( "%.*s", (int) kv.sz2, (const char *) kv.data2 );
            printf( "\n" );
          }
        }
        break;
      case HSCAN_CMD:   /* HSCAN key cursor [MATCH pat] */
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

