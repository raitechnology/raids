#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_exec.h>
#include <raikv/work.h>
#include <raikv/key_hash.h>
#include <raimd/md_stream.h>
#include <linecook/linecook.h>
#include <linecook/ttycook.h>

static const char *
list_status_string[] = { "ok", "not found", "full" };

using namespace rai;
using namespace ds;
using namespace md;

static void
next_id( char *id,  size_t &idlen )
{
  uint64_t ms = kv::current_realtime_coarse_ms();
  idlen = uint_digits( ms );
  uint_to_str( ms, id, idlen );
  id[ idlen++ ] = '-';
  id[ idlen++ ] = '0';
  id[ idlen ] = '\0';
}

static StreamData *
resize_stream( StreamData *curr,  size_t add_len,
               size_t add_group,  size_t add_pend )
{
  StreamGeom geom;

  geom.add( curr, add_len, add_len?1:0, add_group, add_group?1:0, add_pend,
                  add_pend?1:0 );
  geom.print();

  size_t asize = geom.asize();
  char * m  = (char *) malloc( sizeof( StreamData ) + asize );
  StreamData * newbe = geom.make_new( m, &m[ sizeof( StreamData ) ] );
  if ( curr != NULL ) {
    printf( "verify curr: %d %d %d\n", curr->stream.lverify(),
            curr->group.lverify(), curr->pending.lverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d %d %d\n", newbe->stream.lverify(),
            newbe->group.lverify(), newbe->pending.lverify() );
    delete curr;
  }
  printf( "%.2f%% data %.2f%% index\n",
      ( newbe->stream.data_len() + add_len ) * 100.0 / newbe->stream.data_size(),
      ( newbe->stream.count() + 1 ) * 100.0 / newbe->stream.max_count() );
  return newbe;
}

struct StreamKey {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  StreamKey  * next;
  StreamData * li;
  uint32_t  hash;
  StreamKey() : next( 0 ), li( 0 ), hash( 0 ) {}
  ~StreamKey() {
    if ( this->li != NULL )
      delete this->li;
  }

  StreamKey *copy( void ) {
    void *p = ::malloc( sizeof( StreamKey ) );
    if ( p == NULL ) return NULL;
    StreamKey *sk = new ( p ) StreamKey();
    sk->hash = this->hash;
    sk->li   = resize_stream( this->li, 0, 0, 0 );
    sk->next = NULL;
    return sk;
  }
};

struct ListDB {
  StreamKey *list;

  StreamKey *fetch( const char *k,  size_t klen ) {
    StreamKey *hk = this->list;
    uint32_t h = kv_crc_c( k, klen, 0 );
    for ( ; hk != NULL; hk = hk->next ) {
      if ( hk->hash == h )
        return hk;
    }
    void *p = ::malloc( sizeof( StreamKey ) );
    if ( p == NULL ) return NULL;
    hk = new ( p ) StreamKey();
    hk->hash = h;
    hk->li   = resize_stream( NULL, 100, 8, 8 );
    hk->next = this->list;
    this->list = hk;
    return hk;
  }
  void save( const char *k,  size_t klen,  StreamKey *hk ) {
    hk->hash = kv_crc_c( k, klen, 0 );
    hk->next = this->list;
    this->list = hk;
    for ( StreamKey *p = hk; p->next != NULL; p = p->next ) {
      if ( p->next->hash == hk->hash ) {
        StreamKey *q = p->next;
        p->next = q->next;
        delete q;
        break;
      }
    }
  }
} listdb;


int
main( int, char ** )
{
  LineCook  * lc  = lc_create_state( 80, 25 );
  TTYCook   * tty = lc_tty_create( lc );
  StreamKey * hk;

  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  MDMsgMem tmp;
  kv::WorkAllocT< 2048 > wrk;
  char upper_cmd[ 32 ], id[ 64 ];
  size_t sz, i, j, k, x, y, first, last, cnt;
  size_t cmdlen, arglen, endlen, argcount, namelen, idlen, idxsz, asz;
  uint64_t maxlen;
  int64_t maxcnt;
  int subcmd;
  ssize_t off;
  const char *cmdbuf, *arg, *end, *name, *idval;
  void * mem, * p;
  ListData * xl, ld;
  ListVal lv;
  StreamArgs sa;
  /*int64_t ival, jval;*/
  /*Decimal128 fp;*/
  RedisMsgStatus mstatus;
  RedisCmd cmd;
  ListStatus lstat;
  StreamStatus xstat;
  /*bool is_new;*/
  int n;

  lc_tty_set_locale(); /* setlocale( LC_ALL, "" ) */
  lc_tty_init_fd( tty, STDIN_FILENO, STDOUT_FILENO ); /* use stdin/stdout */
  lc_tty_set_default_prompts( tty );
  lc_tty_init_geom( tty );     /* try to determine lines/cols */
  lc_tty_init_sigwinch( tty ); /* install sigwinch handler */
  lc_tty_open_history( tty, ".test_stream_history" );

  for (;;) {
    n = lc_tty_get_line( tty ); /* retry line and run timed events */
    if ( n == 0 )
      n = lc_tty_poll_wait( tty, 60000 ); /* wait at most 60 seconds */
    if ( n < 0 ) /* if error in one of the above */
      break;
    if ( tty->lc_status != LINE_STATUS_EXEC )
      continue;
    if ( tty->line_len == 1 && tty->line[ 0 ] == 'q' ) /* type 'q' to quit */
      break;
    lc_tty_log_history( tty );
    lc_tty_normal_mode( tty );     /* set terminal to normal mode */
    tmp.reuse();
    wrk.reset();
    sa.zero();
    cmdlen = tty->line_len;
    ::memcpy( buf, tty->line, cmdlen );
    buf[ cmdlen++ ] = '\r';
    buf[ cmdlen++ ] = '\n';
    if ( buf[ 0 ] == '[' )
      mstatus = msg.unpack_json( buf, cmdlen, wrk );
    else
      mstatus = msg.unpack( buf, cmdlen, wrk );
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
    if ( cmd != XREAD_CMD && cmd != XREADGROUP_CMD &&
         cmd != XGROUP_CMD && cmd != XINFO_CMD ) {
      if ( ! msg.get_arg( 1, name, namelen ) )
        goto bad_args;
      hk = listdb.fetch( name, namelen );
      if ( hk == NULL ) {
        printf( "out of mem\n" );
        return 1;
      }
    }
    else {
      hk = NULL;
    }

    switch ( cmd ) {
      case XINFO_CMD:
        subcmd = msg.match_arg( 1, MARG( "consumers" ),
                                   MARG( "groups" ),
                                   MARG( "stream" ), NULL );
        if ( subcmd == 0 )
          goto bad_args;
        if ( ! msg.get_arg( 2, name, namelen ) )
          goto bad_args;
        hk = listdb.fetch( name, namelen );
        if ( hk == NULL ) {
          printf( "out of mem\n" );
          return 1;
        }
        arg    = NULL;
        arglen = 0;
        switch ( subcmd ) {
          case 1: /* [CONSUMERS key groupname] */
            if ( ! msg.get_arg( 3, arg, arglen ) )
              goto bad_args;
            /* [["name","C",
             *   "pending",1,
             *   "idle",269654971]] */
            /* FALLTHRU */
          case 2: /* [GROUPS key] */
            cnt = hk->li->group.count();
            for ( i = 0; i < cnt; i++ ) {
              ListVal * cons_lv   = NULL;
              size_t pending_cnt  = 0,
                     consumer_cnt = 0;
              xstat = hk->li->sindex( hk->li->group, i, ld, tmp );
              if ( xstat == STRM_OK ) {
                if ( subcmd == 1 ) {
                  if ( ld.lindex( 0, lv ) != LIST_OK )
                    continue;
                  if ( lv.cmp_key( arg, arglen ) != 0 )
                    continue;
                }
                printf( "[" );
                for ( k = 0; ld.lindex( k, lv ) == LIST_OK; k++ ) {
                  printf( "%.*s%.*s ", (int) lv.sz, (char *) lv.data,
                           (int) lv.sz2, (char *) lv.data2 );
                }
                printf( "]: " );
                if ( ld.lindex( 0, lv ) == LIST_OK ) {
                  size_t pcnt = hk->li->pending.count();
                  for ( k = 0; k < pcnt; k++ ) {
                    xstat = hk->li->sindex( hk->li->pending, k, ld, tmp );
                    if ( xstat == STRM_OK ) {
                      ListVal lv2;
                      if ( ld.lindex( 1, lv2 ) != LIST_OK ||
                           lv2.cmp_key( lv ) != 0 ) /* group doesn't match */
                        continue;
                      pending_cnt++;
                      if ( ld.lindex( 2, lv2 ) == LIST_OK ) {
                        for ( j = 0; j < consumer_cnt; j++ )
                          if ( lv2.cmp_key( cons_lv[ j ] ) == 0 )
                            break;
                        if ( j == consumer_cnt ) {
                          if ( ( consumer_cnt & 7 ) == 0 ) {
                            ListVal * new_lv = NULL;
                            tmp.alloc( sizeof( cons_lv[ 0 ] ) *
                                       ( consumer_cnt + 8 ), &new_lv );
                            ::memcpy( new_lv, cons_lv,
                                      consumer_cnt * sizeof( cons_lv[ 0 ] ) );
                            cons_lv = new_lv;
                          }
                          cons_lv[ consumer_cnt++ ] = lv2;
                        }
                      }
                    }
                  }
                }
                printf( "pending %lu, consumers %lu: [", pending_cnt,
                        consumer_cnt );
                for ( j = 0; j < consumer_cnt; j++ ) {
                  lv = cons_lv[ j ];
                  printf( "%.*s%.*s ", (int) lv.sz, (char *) lv.data,
                           (int) lv.sz2, (char *) lv.data2 );
                }
                printf( "]\n" );
              }
            }
            /*[["name","G",
             *  "consumers",1,
             *  "pending",1,
             *  "last-delivered-id","1573968405987-0"]]*/
            break;
          case 3: /* [STREAM key] */
            cnt = hk->li->stream.count();
            printf( "len: %lu\n" , cnt );
            cnt = hk->li->group.count();
            printf( "groups: %lu\n" , cnt );
            cnt = hk->li->pending.count();
            printf( "pending: %lu\n" , cnt );
            /* ["length",5,
             *  "radix-tree-keys",1
             *  "radix-tree-nodes",2,
             *  "groups",1,
             *  "last-generated-id","1573968405987-0",
             *  "first-entry",["1572854580543-0",["hello","world"]],
             *  "last-entry",["1573968405987-0",["two","times"]]] */
            break;
        }
        break;
        
      case XADD_CMD:    /* XADD key [MAXLEN [~] N] id field string ... */
        j = 2;
        maxlen = 0;
        if ( ! msg.get_arg( j++, arg, arglen ) )
          goto bad_args;
        if ( arglen == 6 && ::strncasecmp( arg, "MAXLEN", 6 ) == 0 ) {
          if ( ! msg.get_arg( j++, arg, arglen ) )
            goto bad_args;
          if ( arglen == 1 && arg[ 0 ] == '~' ) {
            if ( ! msg.get_arg( j++, arg, arglen ) )
              goto bad_args;
          }
          if ( string_to_uint( arg, arglen, maxlen ) != STR_CVT_OK )
            goto bad_args;
          if ( ! msg.get_arg( j++, arg, arglen ) )
            goto bad_args;
        }
        if ( arglen == 1 && arg[ 0 ] == '*' ) {
          next_id( id, sz );
          arg    = id;
          arglen = sz;
        }
        idval = arg;
        idlen = arglen;
        sz    = arglen;
        for ( i = j; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          sz += arglen;
        }
        idxsz = argcount - 2;
        asz = ListData::alloc_size( idxsz, sz );
        printf( "alloc idx %lu data %lu sz %lu\n", idxsz, sz,
                sizeof( ListData ) + asz );
        tmp.alloc( sizeof( ListData ) + asz, &mem );
        p   = &((char *) mem)[ sizeof( ListData ) ];
        xl  = new ( mem ) ListData( p, asz );
        xl->init( idxsz, sz );
        lstat = xl->rpush( idval, idlen );
        if ( lstat != LIST_OK )
          printf( "xl %s\n", list_status_string[ lstat ] );
        for ( i = j; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          lstat = xl->rpush( arg, arglen );
          if ( lstat != LIST_OK )
            printf( "xl2 %s\n", list_status_string[ lstat ] );
        }
        printf( "xadd %.*s\n", (int) idlen, idval );
        for (;;) {
          lstat = hk->li->stream.rpush( xl->listp, xl->size );
          printf( "%s\n", list_status_string[ lstat ] );
          if ( lstat != LIST_FULL ) break;
          hk->li = resize_stream( hk->li, xl->size, 0, 0 );
        }
        if ( maxlen > 0 ) {
          size_t count = hk->li->stream.count();
          if ( count > maxlen )
            hk->li->stream.ltrim( count - maxlen );
        }
        break;

      case XREVRANGE_CMD: /* XREVRANGE key start end [COUNT count] */
      case XRANGE_CMD:    /* XRANGE key start end [COUNT count] */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, end, endlen ) )
          goto bad_args;
        maxcnt = 0;
        if ( msg.match_arg( 4, MARG( "count" ), NULL ) == 1 ) {
          if ( ! msg.get_arg( 5, maxcnt ) )
            goto bad_args;
        }
        off = hk->li->bsearch_str( hk->li->stream, arg, arglen, false, tmp );
        if ( off < 0 ) {
          printf( "bsearch error\n" );
          break;
        }
        x   = (size_t) off;
        off = hk->li->bsearch_str( hk->li->stream, end, endlen, true, tmp );
        if ( off < 0 ) {
          printf( "bsearch error\n" );
          break;
        }
        y   = (size_t) off;
        i   = ( cmd == XRANGE_CMD ? x : y );
        j   = ( cmd == XRANGE_CMD ? y : x );
        for (;;) {
          ListData ld;
          if ( cmd == XRANGE_CMD ) {
            if ( i >= j )
              break;
          }
          else {
            if ( i <= j )
              break;
            i = i - 1;
          }
          xstat = hk->li->sindex( hk->li->stream, i, ld, tmp );
          for ( k = 0; ld.lindex( k, lv ) == LIST_OK; k++ ) {
            printf( "%.*s%.*s ", (int) lv.sz, (char *) lv.data,
                     (int) lv.sz2, (char *) lv.data2 );
            if ( maxcnt > 0 && --maxcnt == 0 )
              break;
          }
          printf( "\n" );
          if ( cmd == XRANGE_CMD )
            i = i + 1;
        }
        if ( x == y ) {
          printf( "null range (%lu->%lu)\n", x, y );
        }
        break;

      case XTRIM_CMD: /* XTRIM key MAXLEN [~] count */
        j = 2;
        maxlen = 0;
        if ( msg.match_arg( j++, MARG( "maxlen" ), NULL ) == 0 )
          goto bad_args;
        if ( ! msg.get_arg( j++, arg, arglen ) )
          goto bad_args;
        if ( arglen == 1 && arg[ 0 ] == '~' ) {
          if ( ! msg.get_arg( j++, arg, arglen ) )
            goto bad_args;
        }
        if ( string_to_uint( arg, arglen, maxlen ) != STR_CVT_OK )
          goto bad_args;
        if ( maxlen > 0 ) {
          size_t count = hk->li->stream.count();
          if ( count > maxlen )
            hk->li->stream.ltrim( count - maxlen );
        }
        break;

      case XDEL_CMD:  /* XDEL key id [id ...] */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          if ( (off = hk->li->bsearch_eq( hk->li->stream, arg, arglen,
                                          tmp )) < 0 )
            printf( "not found\n" );
          else {
            if ( hk->li->stream.lrem( off ) == LIST_OK )
              printf( "ok: %.*s\n", (int) arglen, arg );
            else
              printf( "not ok\n" );
          }
        }
        break;

      case XLEN_CMD:  /* XLEN key  */
        printf( "%ld\n", hk->li->stream.count() );
        break;

        /* XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...] */
      case XREAD_CMD:
        maxcnt = 0;
        for ( i = 1; i < argcount; ) { 
          switch ( msg.match_arg( i, MARG( "count" ),
                                     MARG( "block" ),
                                     MARG( "streams" ), NULL ) ) {
            default:
              goto bad_args;
            case 1: /* count N */
              if ( ! msg.get_arg( i+1, maxcnt ) )
                goto bad_args;
              i += 2;
              break;
            case 2: /* block millseconds */
              i += 2;
              break;
            case 3: /* streams */
              if ( ++i < argcount ) {
                first = i;
                last  = i + ( argcount - i ) / 2 - 1;
                if ( first + ( last - first + 1 ) * 2 == argcount )
                  goto break_xread;
              }
              goto bad_args;
          }
        }
        goto bad_args;
      break_xread:;
        printf( "xread success\n" );
        j = last + 1;
        k = maxcnt;
        for ( i = first; i <= last; i++ ) {
          if ( ! msg.get_arg( i, name, namelen ) )
            goto bad_args;
          hk = listdb.fetch( name, namelen );
          if ( hk == NULL ) {
            printf( "out of mem\n" );
            return 1;
          }
          if ( ! msg.get_arg( j++, arg, arglen ) )
            goto bad_args;
          off = hk->li->bsearch_str( hk->li->stream, arg, arglen, false, tmp );
          if ( off < 0 ) {
            printf( "bsearch error\n" );
            break;
          }
          printf( "%.*s:\n", (int) namelen, name );
          maxcnt = k;
          for ( x = (size_t) off; ; x++ ) {
            ListData ld;
            xstat = hk->li->sindex( hk->li->stream, x, ld, tmp );
            if ( xstat != STRM_OK )
              break;
            for ( k = 0; ld.lindex( k, lv ) == LIST_OK; k++ ) {
              printf( " %.*s%.*s",
                       (int) lv.sz, (char *) lv.data,
                       (int) lv.sz2, (char *) lv.data2 );
            }
            printf( "\n" );
            if ( maxcnt > 0 && --maxcnt == 0 )
              break;
          }
        }
        break;
        
        /* XGROUP [CREATE key groupname id] [SETID key groupname id]
         *     [DESTROY key groupname]   [DELCONSUMER key groupname consname] */
      case XGROUP_CMD:
        subcmd = msg.match_arg( 1, MARG( "create" ),
                                   MARG( "setid" ),
                                   MARG( "destroy" ),
                                   MARG( "delconsumer" ), NULL );
        if ( subcmd == 0 )
          goto bad_args;
        if ( ! msg.get_arg( 2, name, namelen ) ||
             ! msg.get_arg( 3, sa.gname, sa.glen ) )
          goto bad_args;
        hk = listdb.fetch( name, namelen );
        if ( hk == NULL ) {
          printf( "out of mem\n" );
          return 1;
        }
        if ( subcmd <= 2 ) {
          if ( ! msg.get_arg( 4, sa.idval, sa.idlen ) )
            goto bad_args;
          if ( sa.idlen == 1 && sa.idval[ 0 ] == '$' ) {
            hk->li->sindex_id_last( hk->li->stream, sa.idval, sa.idlen, tmp );
            printf( "last: %.*s\n", (int) sa.idlen, sa.idval );
          }
        }
        if ( subcmd == 4 ) {
          if ( ! msg.get_arg( 4, sa.cname, sa.clen ) )
            goto bad_args;
        }
        switch ( subcmd ) {
          case 1: /* create key groupname id */
          case 2: /* setid key groupname id */
            while ( hk->li->update_group( sa, tmp ) == STRM_FULL )
              hk->li = resize_stream( hk->li, 0, sa.glen + sa.idlen + 8, 0 );
            break;
          case 3: /* destroy key groupname */
            xstat = hk->li->remove_group( sa, tmp );
            printf( "dst grp %.*s: %s\n",
                    (int) sa.glen, sa.gname,
                    list_status_string[ xstat ] );
            break;
          case 4: /* delconsumer key groupname consname */
            xstat = hk->li->remove_pending( sa, tmp );
            printf( "del grp %.*s cons %.*s: %s\n",
                    (int) sa.glen, sa.gname,
                    (int) sa.clen, sa.cname,
                    list_status_string[ xstat ] );
            break;
        }
        break;

        /* XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK]
         *          STREAMS key [key ...] id [id ...] */
      case XREADGROUP_CMD:
        maxcnt = 0;
        sa.ns  = kv::current_realtime_coarse_ns();
        for ( i = 1; i < argcount; ) { 
          switch ( msg.match_arg( i, MARG( "group" ),
                                     MARG( "count" ),
                                     MARG( "block" ),
                                     MARG( "noack" ),
                                     MARG( "streams" ), NULL ) ) {
            default:
              goto bad_args;
            case 1: /* group */
              if ( ! msg.get_arg( i+1, sa.gname, sa.glen ) ||
                   ! msg.get_arg( i+2, sa.cname, sa.clen ) )
                goto bad_args;
              i += 3;
              break;
            case 2: /* count N */
              if ( ! msg.get_arg( i+1, maxcnt ) )
                goto bad_args;
              i += 2;
              break;
            case 3: /* block millseconds */
              i += 2;
              break;
            case 4: /* noack */
              i++;
              break;
            case 5: /* streams */
              if ( ++i < argcount ) {
                first = i;
                last  = i + ( argcount - i ) / 2 - 1;
                if ( first + ( last - first + 1 ) * 2 == argcount )
                  goto break_xreadgroup;
              }
              goto bad_args;
          }
        }
        goto bad_args;
      break_xreadgroup:;
        j = last + 1;
        for ( i = first; i <= last; i++ ) {
          StreamGroupQuery grp;
          if ( ! msg.get_arg( i, name, namelen ) )
            goto bad_args;
          hk = listdb.fetch( name, namelen );
          if ( hk == NULL ) {
            printf( "out of mem\n" );
            return 1;
          }
          if ( ! msg.get_arg( j++, sa.idval, sa.idlen ) )
            goto bad_args;
          if ( hk->li->group_query( sa, maxcnt, grp, tmp ) != STRM_OK ) {
            printf( "group first error\n" );
            break;
          }
          printf( "%.*s:\n", (int) namelen, name );
          if ( grp.idx == NULL ) {
            for ( k = 0; k < grp.count; k++ ) {
              ListData ld;
              xstat = hk->li->sindex( hk->li->stream, grp.first+k, ld, tmp );
              if ( xstat != STRM_OK )
                break;
              for ( size_t n = 0; ld.lindex( n, lv ) == LIST_OK; n++ ) {
                printf( " %.*s%.*s",
                         (int) lv.sz, (char *) lv.data,
                         (int) lv.sz2, (char *) lv.data2 );
              }
              printf( "\n" );

              if ( ld.lindex( 0, lv ) == LIST_OK ) {
                lv.unite( tmp );
                sa.idval = (const char *) lv.data;
                sa.idlen = lv.sz;
                xl = sa.construct_pending( tmp );
                for (;;) {
                  lstat = hk->li->pending.rpush( xl->listp, xl->size );
                  if ( lstat != LIST_FULL ) break;
                  hk->li = resize_stream( hk->li, 0, 0, xl->size );
                }
                /* update index, dup xl since resize above may invalidate lv */
                if ( k + 1 == grp.count ) {
                  xl->lindex( 0, lv );
                  sa.idlen = lv.dup( tmp, &sa.idval );
                  xstat = hk->li->sindex( hk->li->group, grp.pos, ld, tmp );
                  if ( xstat != STRM_OK ||
                       ! hk->li->group.in_mem_region( ld.listp, ld.size ) ||
                       ld.lset( 1, sa.idval, sa.idlen ) != LIST_OK ) {
                    while ( hk->li->update_group( sa, tmp ) == STRM_FULL )
                      hk->li = resize_stream( hk->li, 0, sa.glen+sa.idlen+8, 0);
                  }
                }
              }
            }
          }
          else {
            for ( k = 0; k < grp.count; k++ ) {
              ListData ld;
              xstat = hk->li->sindex( hk->li->stream, grp.idx[ k ], ld, tmp );
              if ( xstat != STRM_OK )
                break;
              for ( size_t n = 0; ld.lindex( n, lv ) == LIST_OK; n++ ) {
                printf( " %.*s%.*s",
                         (int) lv.sz, (char *) lv.data,
                         (int) lv.sz2, (char *) lv.data2 );
              }
              printf( "\n" );
            }
          }
        }
        break;
      case XACK_CMD: /* XACK key group id [id ...] */
        printf( "xack\n" );
        if ( ! msg.get_arg( 2, sa.gname, sa.glen ) )
          goto bad_args;
        for ( k = 3; k < argcount; k++ ) {
          if ( ! msg.get_arg( k, sa.idval, sa.idlen ) )
            goto bad_args;
          xstat = hk->li->ack( sa, tmp );
          printf( "ack %.*s %.*s %s\n", (int) sa.glen, sa.gname,
                                        (int) sa.idlen, sa.idval,
                                        list_status_string[ xstat ] );
        }
        break;
        /* XCLAIM key group consumer min-idle-time id [id ...]
         * [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID] */
      case XCLAIM_CMD: {
        const char ** idar;
        size_t      * idln;
        StreamClaim   cl;
        cl.zero();
        cl.ns = kv::current_realtime_coarse_ns();
        tmp.alloc( ( argcount - 4 ) * sizeof( char * ), &idar );
        tmp.alloc( ( argcount - 4 ) * sizeof( size_t ), &idln );
        if ( ! msg.get_arg( 2, sa.gname, sa.glen ) ||
             ! msg.get_arg( 3, sa.cname, sa.clen ) ||
             ! msg.get_arg( 4, cl.min_idle ) ||
             ! msg.get_arg( 5, idar[ 0 ], idln[ 0 ] ) )
          goto bad_args;
        k = 1;
        for ( i = 6; i < argcount; ) { 
          switch ( msg.match_arg( i, MARG( "idle" ),
                                     MARG( "time" ),
                                     MARG( "retrycount" ),
                                     MARG( "force" ),
                                     MARG( "justid" ), NULL ) ) {
            case 0:
              msg.get_arg( i, idar[ k ], idln[ k ] );
              k++;
              i++;
              break;
            case 1:
              msg.get_arg( i + 1, cl.idle );
              i += 2;
              break;
            case 2:
              msg.get_arg( i + 1, cl.time );
              i += 2;
              break;
            case 3:
              msg.get_arg( i + 1, cl.retrycount );
              i += 2;
              break;
            case 4:
              i++;
              cl.force = true;
              break;
            case 5:
              i++;
              cl.justid = true;
              break;
          }
        }
        for ( i = 0; i < k; i++ ) {
          sa.idval = idar[ i ];
          sa.idlen = idln[ i ];
          for (;;) {
            xstat = hk->li->claim( sa, cl, tmp );
            printf( "%s\n", list_status_string[ xstat ] );
            if ( xstat != STRM_FULL ) break;
            hk->li = resize_stream( hk->li, 0, 0, sa.list_data().size );
          }
        }
        break;
      }
      case XPENDING_CMD: /* XPENDING key group [start end count] [consumer] */
        j = hk->li->pending.count();
        for ( k = 0; k < j; k++ ) {
          ListData ld;
          xstat = hk->li->sindex( hk->li->pending, k, ld, tmp );
          if ( xstat != STRM_OK )
            break;
          for ( size_t n = 0; n < 3; n++ ) {
            if ( ld.lindex( n, lv ) == LIST_OK )
              printf( " %.*s%.*s",
                       (int) lv.sz, (char *) lv.data,
                       (int) lv.sz2, (char *) lv.data2 );
          }
          if ( ld.lindex( 3, lv ) == LIST_OK ) {
            uint64_t ns = kv::current_realtime_coarse_ns();
            printf( " idle %lu", ( ns - lv.u64() ) / 1000000 );
          }
          if ( ld.lindex( 4, lv ) == LIST_OK ) {
            printf( " cnt %u", lv.u32() );
          }
          printf( "\n" );
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
  }

  lc_tty_normal_mode( tty ); /* restore normal terminal handling */
  if ( tty->lc_status < 0 )
    printf( "status: %s\n", LINE_ERR_STR( tty->lc_status ) );
  lc_tty_release( tty );     /* free tty */
  lc_release_state( lc );    /* free lc */

  return 0;
}

