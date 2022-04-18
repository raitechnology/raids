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
#include <raimd/md_set.h>
#include <raids/set_bits.h>

static const char *
set_status_string[] = { "ok", "not found", "full", "updated", "exists" };

using namespace rai;
using namespace ds;
using namespace md;

static SetData *
resize_set( SetData *curr,  size_t add_len,  bool is_copy = false )
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
  size_t asize = SetData::alloc_size( count, data_len );
  printf( "asize %" PRId64 ", count %" PRId64 ", data_len %" PRId64 "\n", asize, count, data_len );
  void *m = malloc( sizeof( SetData ) + asize );
  void *p = &((char *) m)[ sizeof( SetData ) ];
  SetData *newbe = new ( m ) SetData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    int x, y;
    printf( "verify curr: %d\n", x = curr->sverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d\n", y = newbe->sverify() );
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

struct SetKey {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  SetKey  * next;
  SetData * set;
  uint32_t  hash;
  SetKey() : next( 0 ), set( 0 ), hash( 0 ) {}
  ~SetKey() {
    if ( this->set != NULL )
      delete this->set;
  }

  SetKey *copy( void ) {
    void *p = ::malloc( sizeof( SetKey ) );
    if ( p == NULL ) return NULL;
    SetKey *sk = new ( p ) SetKey();
    sk->hash = this->hash;
    sk->set  = resize_set( this->set, 0, true );
    sk->next = NULL;
    return sk;
  }
};

struct SetDB {
  SetKey *list;

  SetKey *fetch( const char *k,  size_t klen ) {
    SetKey *sk = this->list;
    uint32_t h = kv_crc_c( k, klen, 0 );
    for ( ; sk != NULL; sk = sk->next ) {
      if ( sk->hash == h )
        return sk;
    }
    void *p = ::malloc( sizeof( SetKey ) );
    if ( p == NULL ) return NULL;
    sk = new ( p ) SetKey();
    sk->hash = h;
    sk->set  = resize_set( NULL, 16 );
    sk->next = this->list;
    this->list = sk;
    return sk;
  }
  void save( const char *k,  size_t klen,  SetKey *sk ) {
    sk->hash = kv_crc_c( k, klen, 0 );
    sk->next = this->list;
    this->list = sk;
    for ( SetKey *p = sk; p->next != NULL; p = p->next ) {
      if ( p->next->hash == sk->hash ) {
        SetKey *q = p->next;
        p->next = q->next;
        delete q;
        break;
      }
    }
  }
} setdb;

int
main( int, char ** )
{
  SetKey *sk, *sk2;
  char buf[ 1024 ], buf2[ 1024 ], key[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  size_t sz, i, j, count, namelen, keylen;
  size_t cmdlen, arglen, argcount;
  const char *cmdbuf, *arg, *name;
  ListVal lv;
  HashPos pos;
  kv::rand::xoroshiro128plus rand;
  int64_t ival;
  SetBits bits;
  RedisMsgStatus mstatus;
  RedisCmd cmd;
  SetStatus sstat;

  uint64_t t = kv::current_monotonic_time_ns();
  printf( "%" PRIx64 "\n", t );
  rand.init( &t, 8 );
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
    if ( cmd == SDIFFSTORE_CMD || cmd == SINTERSTORE_CMD ||
         cmd == SUNIONSTORE_CMD ) {
      if ( ! msg.get_arg( 2, name, namelen ) )
        goto bad_args;
    }
    else {
      if ( ! msg.get_arg( 1, name, namelen ) )
        goto bad_args;
    }
    sk = setdb.fetch( name, namelen );
    if ( sk == NULL ) {
      printf( "out of memory\n" );
      return 1;
    }
    switch ( cmd ) {
      case SADD_CMD:       /* SADD key mem [mem ...] */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          for (;;) {
            sstat = sk->set->sadd( arg, arglen, pos );
            printf( "%s\n", set_status_string[ sstat ] );
            if ( sstat != SET_FULL ) break;
            sk->set = resize_set( sk->set, arglen );
          }
        }
        break;
      case SCARD_CMD:       /* SCARD key */
        count = sk->set->count();
        if ( count > 0 ) count -= 1;
        printf( "%" PRId64 "\n", count );
        break;
      case SISMEMBER_CMD:   /* SISMEMBER key mem */
        if ( ! msg.get_arg( 2, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        sstat = sk->set->sismember( arg, arglen, pos );
        printf( "%s\n", sstat == SET_OK ? "true" : "false" );
        break;
      case SDIFFSTORE_CMD:  /* SDIFFSTORE dest key [key ...] */
      case SINTERSTORE_CMD: /* SINTERSTORE dest key [key ...] */
      case SUNIONSTORE_CMD: /* SUNIONSTORE dest key [key ...] */
        i = 3; if ( 0 ) {
      case SDIFF_CMD:       /* SDIFF key [key ...] */
      case SINTER_CMD:      /* SINTER key [key ...] */
      case SUNION_CMD:      /* SUNION key [key  ...] */
        i = 2; }
        sk = sk->copy();
        if ( sk == NULL ) {
          printf( "out of memory\n" );
          return 1;
        }
        for ( ; i < argcount; i++ ) {
          MergeCtx ctx;
          if ( ! msg.get_arg( i, name, namelen ) )
            goto bad_args;
          sk2 = setdb.fetch( name, namelen );
          ctx.init();
          for (;;) {
            if ( cmd == SUNION_CMD || cmd == SUNIONSTORE_CMD )
              sstat = sk->set->sunion( *sk2->set, ctx );
            else if ( cmd == SINTER_CMD || cmd == SINTERSTORE_CMD )
              sstat = sk->set->sinter( *sk2->set, ctx );
            else if ( cmd == SDIFF_CMD || cmd == SDIFFSTORE_CMD )
              sstat = sk->set->sdiff( *sk2->set, ctx );
            else
              sstat = SET_OK;
            printf( "%s\n", set_status_string[ sstat ] );
            if ( sstat != SET_FULL ) break;
            sk->set = resize_set( sk->set, sk2->set->data_len() );
          }
        }
        /* FALLTHRU */
      case SMEMBERS_CMD:    /* SMEMBERS key */
        count = sk->set->count();
        for ( i = 1; i < count; i++ ) {
          sstat = (SetStatus) sk->set->lindex( i, lv );
          if ( sstat == SET_OK ) {
            printf( "%" PRId64 ". off(%" PRId64 ") ", i, sk->set->offset( i ) );
            printf( "%.*s", (int) lv.sz, (const char *) lv.data );
            if ( lv.sz2 > 0 )
              printf( "%.*s", (int) lv.sz2, (const char *) lv.data2 );
            printf( "\n" );
          }
        }
        printf( "count %" PRIu64 " of %" PRIu64 "\n", count > 0 ? (size_t) count - 1 : 0,
                sk->set->max_count() - 1 );
        printf( "bytes %" PRIu64 " of %" PRIu64 "\n", (size_t) sk->set->data_len(),
                sk->set->data_size() );
        printf( "[" ); sk->set->print_hashes(); printf( "]\n" );

        for ( i = 1; i < count; i++ ) {
          sstat = (SetStatus) sk->set->lindex( i, lv );
          if ( sstat == SET_OK ) {
            keylen = kv::min_int<size_t>( lv.sz, sizeof( key ) );
            ::memcpy( key, lv.data, keylen );
            sz = kv::min_int<size_t>( sizeof( key ) - keylen, lv.sz2 );
            ::memcpy( &key[ keylen ], lv.data2, sz );
            keylen += sz;
            pos.init( key, keylen );
            sstat = sk->set->sismember( key, keylen, pos );
            if ( sstat == SET_OK )
              printf( "%" PRId64 ". idx(%" PRId64 ") h(%u) %.*s\n", i, pos.i, (uint8_t) pos.h,
                      (int) keylen, key );
            else
              printf( "%" PRId64 ". idx(****) h(%u) %.*s\n", i, (uint8_t) pos.h,
                      (int) keylen, key );
          }
          else {
            printf( "%" PRId64 ". status=%d\n", i, (int) sstat );
          }
        }
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
        break;
      case SMOVE_CMD:       /* SMOVE src dest mem */
        if ( ! msg.get_arg( 2, name, namelen ) )
          goto bad_args;
        sk2 = setdb.fetch( name, namelen );
        if ( ! msg.get_arg( 3, arg, arglen ) )
          goto bad_args;
        pos.init( arg, arglen );
        sstat = sk->set->srem( arg, arglen, pos );
        if ( sstat == SET_OK ) {
          for (;;) {
            sstat = sk2->set->sadd( arg, arglen, pos );
            printf( "%s\n", set_status_string[ sstat ] );
            if ( sstat != SET_FULL ) break;
            sk2->set = resize_set( sk2->set, arglen );
          }
          printf( "1\n" );
        }
        else {
          printf( "0\n" );
        }
        break;
      case SPOP_CMD:        /* SPOP key [count] */
      case SRANDMEMBER_CMD: /* SRANDMEMBER key [count] */
        ival = 1;
        if ( argcount > 2 ) {
          if ( ! msg.get_arg( 2, ival ) )
            goto bad_args;
        }
        count = sk->set->count();
        bits.reset();
        if ( count <= 1 )
          sstat = SET_NOT_FOUND;
        else {
          bool flip = false;
          count -= 1;
          if ( count < (size_t) ival )
            ival = count;
          if ( (size_t) ival > count / 2 ) {
            flip = true;
            ival = count - ival;
          }
          if ( ival > 0 ) {
            for (;;) {
              size_t n = rand.next();
              for ( int i = 0; i < 64; i += 16 ) {
                size_t m = n & sk->set->index_mask;
                while ( m >= count )
                  m >>= 1;
                if ( ! bits.test_set( m ) )
                  if ( --ival == 0 )
                    goto break_loop;
                n >>= 16;
              }
            }
          }
        break_loop:;
          sstat = SET_OK;
          if ( flip ) {
            printf( "flip\n" );
            bits.flip( count );
          }
        }
        printf( "forward\n" );
        for ( i = 0; sstat == SET_OK; ) {
          if ( ! bits.next( i ) )
            break;
          i += 1;
          sstat = (SetStatus) sk->set->lindex( i, lv );
          if ( sstat == SET_OK ) {
            printf( "%" PRId64 ". off(%" PRId64 ") ", i, sk->set->offset( i ) );
            printf( "%.*s", (int) lv.sz, (const char *) lv.data );
            if ( lv.sz2 > 0 )
              printf( "%.*s", (int) lv.sz2, (const char *) lv.data2 );
            printf( "\n" );
          }
          else {
            printf( "%" PRId64 ". %s\n", i, set_status_string[ sstat ] );
          }
        }
        printf( "backward\n" );
        for ( j = count; sstat == SET_OK; ) {
          if ( ! bits.prev( j ) )
            break;
          i = j + 1;
          sstat = (SetStatus) sk->set->lindex( i, lv );
          if ( sstat == SET_OK ) {
            printf( "%" PRId64 ". off(%" PRId64 ") ", i, sk->set->offset( i ) );
            printf( "%.*s", (int) lv.sz, (const char *) lv.data );
            if ( lv.sz2 > 0 )
              printf( "%.*s", (int) lv.sz2, (const char *) lv.data2 );
            printf( "\n" );
          }
          else {
            printf( "%" PRId64 ". %s\n", i, set_status_string[ sstat ] );
          }
        }
        if ( sstat == SET_OK && cmd == SPOP_CMD ) {
          for ( j = count; sstat == SET_OK; ) {
            if ( ! bits.prev( j ) )
              break;
            i = j + 1;
            sstat = sk->set->spopn( i );
          }
        }
        break;
      case SREM_CMD:        /* SREM key mem [mem ...] */
        sz = 0;
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          sstat = sk->set->srem( arg, arglen, pos );
          if ( sstat == SET_OK )
            sz++;
        }
        printf( "%" PRId64 "\n", sz );
        break;
      case SSCAN_CMD:       /* SSCAN key curs [match pat] [count cnt] */
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

