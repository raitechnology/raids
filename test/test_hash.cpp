#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_exec.h>
#include <raikv/work.h>
#include <raikv/key_hash.h>
#include <raimd/md_hash.h>
#include <raimd/decimal.h>

static const char *
hash_status_string[]= { "ok", "not found", "full", "updated", "exists", "bad" };

using namespace rai;
using namespace ds;
using namespace md;

static HashData *
resize_hash( HashData *curr,  size_t add_len,  bool is_copy = false )
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
  printf( "asize %" PRId64 ", count %" PRId64 ", data_len %" PRId64 "\n", asize, count, data_len );
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
    if ( ! is_copy )
      delete curr;
  }
  printf( "%.2f%% data %.2f%% hash\n",
          ( newbe->data_len() + add_len ) * 100.0 / newbe->data_size(),
          ( newbe->count() + 1 ) * 100.0 / newbe->max_count() );
  return newbe;
}

struct HashKey {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  HashKey  * next;
  HashData * ht;
  uint32_t  hash;
  HashKey() : next( 0 ), ht( 0 ), hash( 0 ) {}
  ~HashKey() {
    if ( this->ht != NULL )
      delete this->ht;
  }

  HashKey *copy( void ) {
    void *p = ::malloc( sizeof( HashKey ) );
    if ( p == NULL ) return NULL;
    HashKey *sk = new ( p ) HashKey();
    sk->hash = this->hash;
    sk->ht   = resize_hash( this->ht, 0, true );
    sk->next = NULL;
    return sk;
  }
};

struct HashDB {
  HashKey *list;

  HashKey *fetch( const char *k,  size_t klen ) {
    HashKey *hk = this->list;
    uint32_t h = kv_crc_c( k, klen, 0 );
    for ( ; hk != NULL; hk = hk->next ) {
      if ( hk->hash == h )
        return hk;
    }
    void *p = ::malloc( sizeof( HashKey ) );
    if ( p == NULL ) return NULL;
    hk = new ( p ) HashKey();
    hk->hash = h;
    hk->ht   = resize_hash( NULL, 16 );
    hk->next = this->list;
    this->list = hk;
    return hk;
  }
  void save( const char *k,  size_t klen,  HashKey *hk ) {
    hk->hash = kv_crc_c( k, klen, 0 );
    hk->next = this->list;
    this->list = hk;
    for ( HashKey *p = hk; p->next != NULL; p = p->next ) {
      if ( p->next->hash == hk->hash ) {
        HashKey *q = p->next;
        p->next = q->next;
        delete q;
        break;
      }
    }
  }
} hashdb;


int
main( int, char ** )
{
  HashKey *hk;

  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  char ibuf[ 64 ];
  size_t sz, i, count, retry;
  size_t cmdlen, arglen, vallen, fvallen, argcount, namelen;
  const char *cmdbuf, *arg, *val, *fval, *name;
  ListVal lv;
  HashVal kv;
  HashPos pos;
  int64_t ival, jval;
  Decimal128 fp;
  RedisMsgStatus mstatus;
  RedisCmd cmd;
  HashStatus hstat;
  bool is_new;

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
    if ( mstatus != DS_MSG_STATUS_OK ) {
      printf( "error %d/%s\n", mstatus, ds_msg_status_string( mstatus ) );
      continue;
    }
    cmdbuf = msg.command( cmdlen, argcount );
    if ( cmdlen >= 32 ) {
      printf( "cmd to large\n" );
      continue;
    }
    if ( cmdlen == 0 )
      continue;
    cmd = get_redis_cmd( get_redis_cmd_hash( cmdbuf, cmdlen ) );
    if ( cmd == NO_CMD ) {
      printf( "no cmd\n" );
      continue;
    }
    sz = msg.to_almost_json( buf2 );
    printf( "\nexec %.*s\n", (int) sz, buf2 );
    if ( ! msg.get_arg( 1, name, namelen ) )
      goto bad_args;
    hk = hashdb.fetch( name, namelen );
    if ( hk == NULL ) {
      printf( "out of mem\n" );
      return 1;
    }
    retry = 0;

    switch ( cmd ) {
      case HAPPEND_CMD: /* HAPPEND key field [field ...] */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        for ( i = 3; i < argcount; i += 2 ) {
          if ( ! msg.get_arg( i, val, vallen ) )
            goto bad_args;
          lv.data  = val;
          lv.sz    = vallen;
          if ( i + 1 < argcount ) {
            if ( ! msg.get_arg( i+1, val, vallen ) )
              goto bad_args;
            lv.data2 = val;
            lv.sz2   = vallen;
          }
          else {
            lv.data2 = NULL;
            lv.sz2   = 0;
          }
          for (;;) {
            hstat = hk->ht->happend( arg, arglen, lv, pos );
            printf( "%s\n", hash_status_string[ hstat ] );
            if ( hstat != HASH_FULL ) break;
            hk->ht = resize_hash( hk->ht,
                                  arglen + 1 + lv.sz + lv.sz2 + retry++ );
          }
        }
        break;
      case HDEL_CMD:    /* HDEL key field [field ...] */
        sz = 0;
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          hstat = hk->ht->hdel( arg, arglen, pos );
          if ( hstat == HASH_OK )
            sz++;
        }
        printf( "%" PRId64 "\n", sz );
        break;
      case HEXISTS_CMD: /* HEXISTS key field */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        hstat = hk->ht->hexists( arg, arglen, pos );
        printf( "%s\n", hstat == HASH_OK ? "true" : "false" );
        break;
      case HGETALL_CMD: /* HGETALL key */
        count = hk->ht->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hk->ht->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            printf( "%" PRId64 ". off(%" PRId64 ") %.*s: ", i, hk->ht->offset( i ),
                    (int) kv.keylen, kv.key );
            printf( "%.*s", (int) kv.sz, (const char *) kv.data );
            if ( kv.sz2 > 0 )
              printf( "%.*s", (int) kv.sz2, (const char *) kv.data2 );
            printf( "\n" );
          }
        }
        printf( "count %" PRIu64 " of %" PRIu64 "\n", count > 0 ? (size_t) count - 1 : 0,
                hk->ht->max_count() - 1 );
        printf( "bytes %" PRIu64 " of %" PRIu64 "\n", (size_t) hk->ht->data_len(),
                hk->ht->data_size() );
        printf( "[" ); hk->ht->print_hashes(); printf( "]\n" );

        for ( i = 1; i < count; i++ ) {
          hstat = hk->ht->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            pos.init( kv.key, kv.keylen );
            hstat = hk->ht->hget( kv.key, kv.keylen, lv, pos );
            if ( hstat == HASH_OK )
              printf( "%" PRId64 ". idx(%" PRId64 ") h(%u) %.*s\n", i, pos.i, (uint8_t) pos.h,
                      (int) kv.keylen, kv.key );
            else
              printf( "%" PRId64 ". idx(****) h(%u) %.*s\n", i, (uint8_t) pos.h,
                      (int) kv.keylen, kv.key );
          }
          else {
            printf( "%" PRId64 ". status=%d\n", i, (int) hstat );
          }
        }
        break;
      case HINCRBY_CMD: /* HINCRBY key field int */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, ival ) )
          goto bad_args;
        pos.init( arg, arglen );
        hstat = hk->ht->hget( arg, arglen, lv, pos );
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
          mstatus = (RedisMsgStatus) RedisMsg::str_to_int( data, sz, jval );
          if ( mstatus == DS_MSG_STATUS_OK )
            ival += jval;
        }
        sz = kv::int64_to_string( ival, ibuf );
        for (;;) {
          hstat = hk->ht->hupdate( arg, arglen, ibuf, sz, pos );
          if ( hstat != HASH_FULL ) {
            printf( "%.*s\n", (int) sz, ibuf );
            break;
          }
          printf( "%s\n", hash_status_string[ hstat ] );
          hk->ht = resize_hash( hk->ht, arglen + vallen + 1 + retry++ );
        }
        break;
      case HINCRBYFLOAT_CMD: /* HINCRBYFLOAT key field float */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, fval, fvallen ) )
          goto bad_args;
        pos.init( arg, arglen );
        hstat = hk->ht->hget( arg, arglen, lv, pos );
        is_new = ( hstat == HASH_NOT_FOUND );
        if ( ! is_new ) {
          sz = lv.concat( ibuf, sizeof( ibuf ) - 1 );
          ibuf[ sz ] = '\0';
          fp = Decimal128::parse( ibuf );
        }
        else {
          fp.zero();
        }
        fp += Decimal128::parse_len( fval, fvallen );
        sz  = fp.to_string( ibuf );
        for (;;) {
          hstat = hk->ht->hupdate( arg, arglen, ibuf, sz, pos );
          if ( hstat != HASH_FULL ) {
            printf( "%.*s\n", (int) sz, ibuf );
            break;
          }
          printf( "%s\n", hash_status_string[ hstat ] );
          hk->ht = resize_hash( hk->ht, arglen + vallen + 1 + retry++ );
        }
        break;
      case HKEYS_CMD:   /* HKEYS key */
        count = hk->ht->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hk->ht->hindex( i, kv );
          if ( hstat == HASH_OK )
            printf( "%" PRId64 ". %.*s\n", i, (int) kv.keylen, kv.key );
        }
        break;
      case HLEN_CMD:    /* HLEN key */
        count = hk->ht->count();
        if ( count > 0 ) count -= 1;
        printf( "%" PRId64 "\n", count );
        break;
      case HMGET_CMD:   /* HMGET key field [field ...] */
      case HGET_CMD:    /* HGET key field */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          hstat = hk->ht->hget( arg, arglen, kv, pos );
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
          pos.init( arg, arglen );
          for (;;) {
            hstat = hk->ht->hset( arg, arglen, val, vallen, pos );
            printf( "%s\n", hash_status_string[ hstat ] );
            if ( hstat != HASH_FULL ) break;
            hk->ht = resize_hash( hk->ht, arglen + vallen + 1 + retry++ );
          }
        }
        break;
      case HSETNX_CMD:  /* HSETNX key field val */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, val, vallen ) )
          goto bad_args;
        pos.init( arg, arglen );
        for (;;) {
          hstat = hk->ht->hsetnx( arg, arglen, val, vallen, pos );
          printf( "%s\n", hash_status_string[ hstat ] );
          if ( hstat != HASH_FULL ) break;
          hk->ht = resize_hash( hk->ht, arglen + vallen + 1 + retry++ );
        }
        break;
      case HSTRLEN_CMD: /* HSTRLEN key field */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        hstat = hk->ht->hget( arg, arglen, kv, pos );
        printf( "%.*s: ", (int) arglen, arg );
        if ( hstat == HASH_OK )
          printf( "%" PRId64 "\n", kv.sz + kv.sz2 );
        else
          printf( "not found\n" );
        break;
      case HVALS_CMD:   /* HVALS key */
        count = hk->ht->count();
        for ( i = 1; i < count; i++ ) {
          hstat = hk->ht->hindex( i, kv );
          if ( hstat == HASH_OK ) {
            printf( "%" PRId64 ". ", i );
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

