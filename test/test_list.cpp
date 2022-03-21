#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_exec.h>
#include <raikv/work.h>
#include <raimd/md_list.h>

static const char *
list_status_string[] = { "ok", "not found", "full" };

using namespace rai;
using namespace ds;
using namespace md;

static ListData *
resize_list( ListData *curr,  size_t add_len )
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
  size_t asize = ListData::alloc_size( count, data_len );
  printf( "asize %ld, count %ld, data_len %ld\n", asize, count, data_len );
  void *m = malloc( sizeof( ListData ) + asize );
  void *p = &((char *) m)[ sizeof( ListData ) ];
  ListData *newbe = new ( m ) ListData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    printf( "verify curr: %d\n", curr->lverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d\n", newbe->lverify() );
    delete curr;
  }
  printf( "%.2f%% data %.2f%% index\n",
          ( newbe->data_len() + add_len ) * 100.0 / newbe->data_size(),
          ( newbe->count() + 1 ) * 100.0 / newbe->max_count() );
  return newbe;
}

int
main( int, char ** )
{
  ListData *list = resize_list( NULL, 16 );

  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  size_t cmdlen, arglen, pivlen, argcount;
  const char *cmdbuf, *arg, *piv;
  size_t sz, pos;
  ListVal lv;
  int64_t ival, jval, cnt;
  bool after;
  RedisMsgStatus mstatus;
  RedisCmd cmd;
  ListStatus lstat;

  printf( "alloc size %lu, index count %lu, data size %lu\n",
          list->size, list->max_count(), list->data_size() );
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
    switch ( cmd ) {
      case LINDEX_CMD:  /* LINDEX key idx -- get key[ idx ] */
        if ( ! msg.get_arg( 2, ival ) )
          goto bad_args;
        lstat = list->lindex( ival, lv );
        if ( lstat == LIST_OK ) {
          printf( "%ld. %.*s", ival, (int) lv.sz, (char *) lv.data );
          if ( lv.sz2 > 0 ) printf( "%.*s", (int) lv.sz2, (char *) lv.data2 );
          printf( "\n" );
        }
        else {
          printf( "%ld: not found\n", ival );
        }
        break;
      case LINSERT_CMD: /* LINSERT key [before|after] piv val */
        switch ( msg.match_arg( 2, "before", 6,
                                   "after",  5, NULL ) ) {
          case 1: after = false; break;
          case 2: after = true; break;
          default: goto bad_args;
        }
        if ( ! msg.get_arg( 3, piv, pivlen ) ||
             ! msg.get_arg( 4, arg, arglen ) )
          goto bad_args;
        for (;;) {
          lstat = list->linsert( piv, pivlen, arg, arglen, after );
          printf( "%s\n", list_status_string[ lstat ] );
          if ( lstat != LIST_FULL ) break;
          list = resize_list( list, arglen );
        }
        break;
      case LLEN_CMD:    /* LLEN key */
        printf( "%d\n", (int) list->count() );
        break;
      case RPOP_CMD:    /* RPOP key */
        lstat = list->rpop( lv );
        if ( 0 ) {
      case LPOP_CMD:    /* LPOP key */
        lstat = list->lpop( lv ); }
        if ( lstat == LIST_OK ) {
          printf( "%.*s", (int) lv.sz, (char *) lv.data );
          if ( lv.sz2 > 0 ) printf( "%.*s", (int) lv.sz2, (char *) lv.data2 );
          printf( "\n" );
        }
        else {
          printf( "empty\n" );
        }
        break;
      case LPUSH_CMD:   /* LPUSH key value */
        for ( size_t i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          for (;;) {
            lstat = list->lpush( arg, arglen );
            printf( "%s\n", list_status_string[ lstat ] );
            if ( lstat != LIST_FULL ) break;
            list = resize_list( list, arglen );
          }
        }
        break;
      case LRANGE_CMD:  /* LRANGE key start stop */
        if ( ! msg.get_arg( 2, ival ) || ! msg.get_arg( 3, jval ) )
          goto bad_args;
        cnt = list->count();
        if ( ival < 0 )
          ival = cnt + ival;
        if ( jval < 0 )
          jval = cnt + jval;
        ival = kv::min_int<int64_t>( cnt, kv::max_int<int64_t>( 0, ival ) );
        jval = kv::min_int<int64_t>( cnt, kv::max_int<int64_t>( 0, jval + 1 ) );
        if ( ival < jval ) {
          for ( int64_t i = 0; ival < jval; ival++ ) {
            lstat = list->lindex( ival, lv );
            printf( "%ld. off(%ld) ", i, list->offset( ival ) );
            if ( ival != i )
              printf( "[%ld] ", ival );
            if ( lstat != LIST_NOT_FOUND ) {
              printf( "[%.*s]", (int) lv.sz, (char *) lv.data );
              if ( lv.sz2 > 0 )
                printf( "[%.*s]", (int) lv.sz2, (char *) lv.data2 );
            }
            else {
              printf( "empty" );
            }
            printf( "\n" );
            i++;
          }
          printf( " + off(%ld)\n", list->offset( ival ) );
          printf( "count %lu of %lu\n", (size_t) cnt,
                  list->max_count() );
          printf( "bytes %lu of %lu\n", (size_t) list->data_len(),
                  list->data_size() );
        }
        else
          printf( "null range\n" );
        break;
      case LREM_CMD:    /* LREM key count value */
        if ( ! msg.get_arg( 2, ival ) || ! msg.get_arg( 3, arg, arglen ) )
          goto bad_args;
        cnt = 0;
        if ( ival < 0 ) {
          pos = list->count();
          do {
            printf( "scan_rev %ld\n", pos );
            if ( list->scan_rev( arg, arglen, pos ) == LIST_NOT_FOUND )
              break;
            printf( "match %ld\n", pos );
            list->lrem( pos );
            cnt++;
          } while ( ++ival != 0 );
        }
        else {
          pos = 0;
          do {
            printf( "scan_fwd %ld\n", pos );
            if ( list->scan_fwd( arg, arglen, pos ) == LIST_NOT_FOUND )
              break;
            printf( "match %ld\n", pos );
            list->lrem( pos );
            cnt++;
          } while ( --ival != 0 );
        }
        printf( "%ld items matched\n", cnt );
        break;
      case LSET_CMD:    /* LSET key idx value */
        if ( ! msg.get_arg( 3, arg, arglen ) || ! msg.get_arg( 2, ival ) )
          goto bad_args;
        for (;;) {
          lstat = list->lset( ival, arg, arglen );
          printf( "%s\n", list_status_string[ lstat ] );
          if ( lstat != LIST_FULL ) break;
          list = resize_list( list, arglen );
        }
        break;
      case LTRIM_CMD:   /* LTRIM key start stop */
        if ( ! msg.get_arg( 2, ival ) || ! msg.get_arg( 3, jval ) )
          goto bad_args;
        cnt = list->count();
        if ( ival < 0 )
          ival = cnt + ival;
        if ( jval < 0 )
          jval = cnt + jval;
        ival = kv::min_int<int64_t>( cnt, kv::max_int<int64_t>( 0, ival ) );
        jval = kv::min_int<int64_t>( cnt, kv::max_int<int64_t>( 0, jval + 1 ) );
        jval = cnt - jval;
        list->ltrim( ival );
        list->rtrim( jval );
        printf( "ok\n" );
        break;
      case RPUSH_CMD:   /* RPUSH key value */
        for ( size_t i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          for (;;) {
            lstat = list->rpush( arg, arglen );
            printf( "%s\n", list_status_string[ lstat ] );
            if ( lstat != LIST_FULL ) break;
            list = resize_list( list, arglen );
          }
        }
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

