#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/ev_publish.h>
#include <raids/route_db.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raids/pattern_cvt.h>
#include <raimd/md_types.h>
#include <raids/redis_keyspace.h>

using namespace rai;
using namespace ds;
using namespace md;

enum {
  DO_SUBSCRIBE    = 1<<0,
  DO_UNSUBSCRIBE  = 1<<1,
  DO_PSUBSCRIBE   = 1<<2,
  DO_PUNSUBSCRIBE = 1<<3
};

void
RedisExec::rem_all_sub( void )
{
  RedisSubRoutePos     pos;
  RedisPatternRoutePos ppos;
  RedisContinuePos     cpos;
  uint32_t             rcnt;
  if ( this->sub_tab.first( pos ) ) {
    do {
      rcnt = this->sub_route.del_route( pos.rt->hash, this->sub_id );
      this->sub_route.rte.notify_unsub( pos.rt->hash, pos.rt->value,
                                        pos.rt->len, this->sub_id, rcnt, 'R' );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    char       buf[ 1024 ];
    PatternCvt cvt( buf, sizeof( buf ) );
    do {
      if ( cvt.convert_glob( ppos.rt->value, ppos.rt->len ) == 0 ) {
        rcnt = this->sub_route.del_pattern_route( ppos.rt->hash, this->sub_id,
                                                  cvt.prefixlen );
        this->sub_route.rte.notify_punsub( ppos.rt->hash, buf, cvt.off,
                                           ppos.rt->value, cvt.prefixlen,
                                           this->sub_id, rcnt, 'R' );
      }
    } while ( this->pat_tab.next( ppos ) );
  }
  if ( this->continue_tab.first( cpos ) ) {
    do {
      rcnt = this->sub_route.del_route( cpos.rt->hash, this->sub_id );
      this->sub_route.rte.notify_unsub( cpos.rt->hash, cpos.rt->value,
                                        cpos.rt->len, this->sub_id, rcnt, 'R' );
      RedisContinueMsg *cm = cpos.rt->continue_msg;
      if ( (cm->state & CM_WAIT_LIST) != 0 )
        this->wait_list.pop( cm );
      if ( (cm->state & CM_CONT_LIST) != 0 )
        this->cont_list.pop( cm );
      cm->state &= ~( CM_WAIT_LIST | CM_CONT_LIST );
    } while ( this->continue_tab.next( cpos ) );
  }
}

void
RedisPatternMap::release( void )
{
  RedisPatternRoutePos ppos;

  if ( this->first( ppos ) ) {
    do {
      if ( ppos.rt->md != NULL ) {
        pcre2_match_data_free( ppos.rt->md );
        ppos.rt->md = NULL;
      }
      if ( ppos.rt->re != NULL ) {
        pcre2_code_free( ppos.rt->re );
        ppos.rt->re = NULL;
      }
    } while ( this->next( ppos ) );
  }
  this->tab.release();
}

void
RedisContinueMap::release( void )
{
  kv::DLinkList<RedisContinueMsg> list;
  RedisContinueMsg *cm;
  RedisContinuePos cpos;

  if ( this->first( cpos ) ) {
    do {
      cm = cpos.rt->continue_msg;
      if ( cm != NULL ) {
        if ( ( cm->state & CM_RELEASE ) == 0 ) {
          list.push_tl( cm );
          cm->state |= CM_RELEASE;
        }
      }
    } while ( this->next( cpos ) );
  }
  this->tab.release();

  while ( ! list.is_empty() ) {
    cm = list.pop_hd();
    delete cm;
  }
}

bool
RedisExec::pub_message( EvPublish &pub,  RedisPatternRoute *rt )
{
  static const char   hdr[]   = "*3\r\n$7\r\nmessage\r\n",
                      phdr[]  = "*4\r\n$8\r\npmessage\r\n";
  static const size_t hdr_sz  = sizeof( hdr ) - 1,
                      phdr_sz = sizeof( phdr ) - 1;
  size_t              sz, off,
                      sdigits,
                      pdigits;
  size_t              msg_len_digits =
                       ( pub.msg_len_digits > 0 ? pub.msg_len_digits :
                         uint_digits( pub.msg_len ) );
  char              * msg;

  off     = 0;
  sdigits = uint_digits( pub.subject_len );
  pdigits = 0;

  if ( rt == NULL ) {
        /* *3 .. $subject_len       subject ..        */
    sz = hdr_sz + 1 + sdigits + 2 + pub.subject_len + 2 +
      /* $msg_len ..                  msg */
         1 + msg_len_digits + 2 + pub.msg_len + 2;

    msg = this->strm.alloc( sz );
    if ( msg == NULL )
      return false;
    ::memcpy( &msg[ off ], hdr, hdr_sz );
    off += hdr_sz;
  }
  else {
    pdigits = uint_digits( rt->len );
        /* *4 .. */
    sz = phdr_sz +
      /* $pattern_len       pattern ..        */
         1 + pdigits + 2 + rt->len +
      /* $subject_len       subject ..        */
         1 + sdigits + 2 + pub.subject_len + 2 +
      /* $msg_len ..                  msg */
         1 + msg_len_digits + 2 + pub.msg_len + 2;

    msg = this->strm.alloc( sz );
    if ( msg == NULL )
      return false;
    ::memcpy( &msg[ off ], phdr, phdr_sz );
    off += phdr_sz;
    msg[ off++ ] = '$';
    off += uint_to_str( rt->len, &msg[ off ], pdigits );
    off  = crlf( msg, off );
    ::memcpy( &msg[ off ], rt->value, rt->len );
    off  = crlf( msg, off + rt->len );
  }
  msg[ off++ ] = '$';
  off += uint_to_str( pub.subject_len, &msg[ off ], sdigits );
  off  = crlf( msg, off );
  ::memcpy( &msg[ off ], pub.subject, pub.subject_len );
  off  = crlf( msg, off + pub.subject_len );
  msg[ off++ ] = '$';
  if ( pub.msg_len_digits == 0 )
    uint_to_str( pub.msg_len, &msg[ off ], msg_len_digits );
  else
    ::memcpy( &msg[ off ], pub.msg_len_buf, msg_len_digits );
  off += msg_len_digits;
  off  = crlf( msg, off );
  ::memcpy( &msg[ off ], pub.msg, pub.msg_len );
  crlf( msg, off + pub.msg_len );

  this->strm.sz += sz;
  return true;
}

int
RedisExec::do_pub( EvPublish &pub,  RedisContinueMsg *&cm )
{
  int status = 0;
  /* don't publish to self ?? (redis does not allow pub w/sub on same conn) */
  if ( (uint32_t) this->sub_id == pub.src_route )
    return false;
  uint32_t pub_cnt = 0;
  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    if ( pub.subj_hash == pub.hash[ cnt ] ) {
      RedisSubStatus ret;
      ret = this->sub_tab.updcnt( pub.subj_hash, pub.subject, pub.subject_len );
      if ( ret == REDIS_SUB_OK ) {
        this->pub_message( pub, NULL );
        pub_cnt++;
      }
      if ( pub.subject_len > 10 && pub.subject[ 0 ] == '_' &&
           ! this->continue_tab.is_null() ) {
        RedisContinue * rt;
        RouteLoc        loc;
        ret = this->continue_tab.find( pub.subj_hash, pub.subject,
                                       pub.subject_len, rt, loc );
        if ( ret == REDIS_SUB_OK ) {
          uint32_t keynum, keycnt, i;
          cm     = rt->continue_msg;
          keynum = rt->keynum;
          keycnt = rt->keycnt;
          rt     = NULL; /* removing will junk this ptr */
          this->continue_tab.tab.remove( loc );
          status |= 2;
          if ( keycnt > 1 ) { /* remove the other keys */
            for ( i = 0; i < keycnt; i++ ) {
              if ( i != keynum ) {
                this->continue_tab.tab.remove( cm->ptr[ i ].hash,
                                               cm->ptr[ i ].value,
                                               cm->ptr[ i ].len );
              }
            }
          }
          cm->state &= ~CM_CONT_TAB;
          for ( i = 0; i < keycnt; i++ ) {
            uint32_t rcnt = this->sub_route.del_route( cm->ptr[ i ].hash,
                                                       this->sub_id );
            this->sub_route.rte.notify_unsub( cm->ptr[ i ].hash,
                                              cm->ptr[ i ].value,
                                              cm->ptr[ i ].len,
                                              this->sub_id, rcnt, 'R' );
          }
        }
      }
    }
    else {
      RedisPatternRoute * rt = NULL;
      RouteLoc            loc;
      rt = this->pat_tab.tab.find_by_hash( pub.hash[ cnt ], loc );
      for (;;) {
        if ( rt == NULL )
          break;
        if ( pcre2_match( rt->re, (const uint8_t *) pub.subject,
                          pub.subject_len, 0, 0, rt->md, 0 ) == 1 ) {
          rt->msg_cnt++;
          this->pub_message( pub, rt );
          pub_cnt++;
        }
        rt = this->pat_tab.tab.find_next_by_hash( pub.hash[ cnt ], loc );
      }
    }
  }
  if ( pub_cnt > 0 )
    status |= 1;
  return status;
}

bool
RedisExec::continue_expire( uint64_t event_id,  RedisContinueMsg *&cm )
{
  for ( cm = this->wait_list.hd; cm != NULL; cm = cm->next ) {
    if ( cm->msgid == (uint32_t) event_id ) {
      cm->state |= CM_TIMEOUT;
      cm->state &= ~CM_TIMER;
      return true;
    }
  }
  return false;
}

void
RedisExec::push_continue_list( RedisContinueMsg *cm )
{
  if ( ( cm->state & CM_CONT_TAB ) != 0 ) {
    uint32_t keycnt, i;
    keycnt = cm->keycnt;
    for ( i = 0; i < keycnt; i++ ) {
      this->continue_tab.tab.remove( cm->ptr[ i ].hash,
                                     cm->ptr[ i ].value,
                                     cm->ptr[ i ].len );
    }
    cm->state &= ~CM_CONT_TAB;
    for ( i = 0; i < keycnt; i++ ) {
      uint32_t rcnt = this->sub_route.del_route( cm->ptr[ i ].hash,
                                                 this->sub_id );
      this->sub_route.rte.notify_unsub( cm->ptr[ i ].hash,
                                        cm->ptr[ i ].value,
                                        cm->ptr[ i ].len,
                                        this->sub_id, rcnt, 'R' );
    }
  }
  if ( ( cm->state & CM_WAIT_LIST ) != 0 ) {
    this->wait_list.pop( cm );
    cm->state &= ~CM_WAIT_LIST;
  }
  if ( ( cm->state & CM_CONT_LIST ) == 0 ) {
    this->cont_list.push_tl( cm );
    cm->state |= CM_CONT_LIST;
  }
}

ExecStatus
RedisExec::exec_psubscribe( void )
{
  /* PSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_PSUBSCRIBE );
}

bool
RedisExec::do_hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  RedisSubRoute * rt;
  if ( (rt = this->sub_tab.tab.find_by_hash( h )) != NULL ) {
    ::memcpy( key, rt->value, rt->len );
    keylen = rt->len;
    return true;
  }
  return false;
}

ExecStatus
RedisExec::exec_pubsub( void )
{
  StreamBuf::BufQueue q( this->strm );
  size_t cnt = 0;

  /* PUBSUB [channels [pattern] | numsub channel-1 [, ...] | numpat] */
  switch ( this->msg.match_arg( 1, MARG( "channels" ),
                                   MARG( "numsub" ),
                                   MARG( "numpat" ), NULL ) ) {
    default: return ERR_BAD_CMD;
    case 1: { /* channels [pattern] */
      pcre2_real_code_8       * re = NULL; /* pcre regex compiled */
      pcre2_real_match_data_8 * md = NULL; /* pcre match context  */
      const char * pattern = NULL;
      size_t       patlen  = 0;
      uint32_t     pos, h, v;
      int          rc;

      if ( this->msg.get_arg( 2, pattern, patlen ) &&
           ( patlen > 1 || pattern[ 0 ] != '*' ) ) {
        char       buf[ 1024 ];
        size_t     erroff;
        int        error;
        PatternCvt cvt( buf, sizeof( buf ) );
        if ( cvt.convert_glob( pattern, patlen ) != 0 )
          return ERR_BAD_ARGS;
        re = pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
        if ( re == NULL )
          return ERR_BAD_ARGS;
        md = pcre2_match_data_create_from_pattern( re, NULL );
        if ( md == NULL ) {
          pcre2_code_free( re );
          return ERR_BAD_ARGS;
        }
      }
      if ( this->sub_route.first_hash( pos, h, v ) ) {
        rc = 1;
        do {
          uint32_t id = this->sub_route.decompress_one( v );
          char   key[ 256 ];
          size_t keylen;
          if ( this->sub_route.rte.hash_to_sub( id, h, key, keylen ) ) {
            if ( re != NULL )
              rc = pcre2_match( re, (PCRE2_SPTR8) key, keylen, 0, 0, md, 0 );
            if ( rc > 0 ) {
              q.append_string( key, keylen );
              cnt++;
            }
          }
        } while ( this->sub_route.next_hash( pos, h, v ) );
      }
      if ( re != NULL ) {
        pcre2_match_data_free( md );
        pcre2_code_free( re );
      }
      break;
    }
    case 2: { /* numsub channel-1 [...] */
      for ( size_t i = 2; i < this->argc; i++ ) {
        const char *val;
        size_t vallen;
        if ( this->msg.get_arg( i, val, vallen ) ) {
          uint32_t h = kv_crc_c( val, vallen, 0 );
          uint32_t rcnt = this->sub_route.get_route_count( h );
          q.append_string( val, vallen );
          q.append_uint( rcnt );
        }
      }
      cnt = ( this->argc - 2 ) * 2;
      break;
    }
    case 3: { /* numpat */
      size_t sz  = 4;
      char * msg = this->strm.alloc( sz );
      if ( msg == NULL )
        return ERR_ALLOC_FAIL;
      ::memcpy( msg, ":0\r\n", 4 );
      this->strm.sz += sz;
      return EXEC_OK;
    }
  }
  q.prepend_array( cnt );
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_publish( void )
{
  const char * subj;
  size_t       subj_len;
  const char * msg;
  size_t       msg_len;
  char       * buf;
  uint32_t     rte_digits,
               rcount;

  /* PUBLISH subj msg */
  if ( ! this->msg.get_arg( 1, subj, subj_len ) ||
       ! this->msg.get_arg( 2, msg, msg_len ) )
    return ERR_BAD_ARGS;

  uint32_t h = kv_crc_c( subj, subj_len, 0 );
  EvPublish pub( subj, subj_len, NULL, 0, msg, msg_len,
                 this->sub_id, h, NULL, 0, MD_STRING, 'p' );
  this->sub_route.rte.forward_msg( pub, &rcount, 0, NULL );
  rte_digits = uint_digits( rcount );
  buf = this->strm.alloc( rte_digits + 3 ); 
  if ( buf == NULL )
    return ERR_ALLOC_FAIL;
  buf[ 0 ] = ':';
  uint_to_str( rcount, &buf[ 1 ], rte_digits );
  crlf( buf, rte_digits + 1 );
  this->strm.sz += rte_digits + 3;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_punsubscribe( void )
{
  /* UNSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_PUNSUBSCRIBE );
}

ExecStatus
RedisExec::exec_subscribe( void )
{
  /* SUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_SUBSCRIBE );
}

ExecStatus
RedisExec::exec_unsubscribe( void )
{
  /* UNSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_UNSUBSCRIBE );
}


ExecStatus
RedisExec::do_subscribe( const char *sub,  size_t len )
{
  uint32_t h, rcnt;

  h = kv_crc_c( sub, len, 0 );
  if ( this->sub_tab.put( h, sub, len ) == REDIS_SUB_OK ) {
    rcnt = this->sub_route.add_route( h, this->sub_id );
    this->sub_route.rte.notify_sub( h, sub, len, this->sub_id, rcnt, 'R' );
    return EXEC_OK;
  }
  return ERR_KEY_EXISTS;
}

ExecStatus
RedisExec::do_unsubscribe( const char *sub,  size_t len )
{
  uint32_t h, rcnt;

  h = kv_crc_c( sub, len, 0 );
  if ( this->sub_tab.rem( h, sub, len ) == REDIS_SUB_OK ) {
    rcnt = 0;
    /* check for duplicate hashes */
    if ( this->sub_tab.tab.find_by_hash( h ) == NULL )
      rcnt = this->sub_route.del_route( h, this->sub_id );
    this->sub_route.rte.notify_unsub( h, sub, len, this->sub_id,
                                      rcnt, 'R' );
    return EXEC_OK;
  }
  return ERR_KEY_DOESNT_EXIST;
}

ExecStatus
RedisExec::do_psubscribe( const char *sub,  size_t len )
{
  RedisPatternRoute * rt;
  char       buf[ 1024 ];
  PatternCvt cvt( buf, sizeof( buf ) );
  uint32_t   h, rcnt;

  if ( cvt.convert_glob( sub, len ) == 0 ) {
    h = kv_crc_c( sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );
    if ( this->pat_tab.put( h, sub, len, rt ) == REDIS_SUB_OK ) {
      size_t erroff;
      int    error;
      rt->re = pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
      if ( rt->re == NULL ) {
        fprintf( stderr, "re failed\n" );
      }
      else {
        rt->md = pcre2_match_data_create_from_pattern( rt->re, NULL );
        if ( rt->md == NULL ) {
          pcre2_code_free( rt->re );
          rt->re = NULL;
          fprintf( stderr, "md failed\n" );
        }
      } 
      if ( rt->re == NULL )
        this->pat_tab.tab.remove( h, sub, len );
      else {
        rcnt = this->sub_route.add_pattern_route( h, this->sub_id,
                                                  cvt.prefixlen );
        this->sub_route.rte.notify_psub( h, buf, cvt.off, sub, cvt.prefixlen,
                                         this->sub_id, rcnt, 'R' );
        return EXEC_OK;
      }
    }
  }
  return ERR_KEY_EXISTS;
}

ExecStatus
RedisExec::do_punsubscribe( const char *sub,  size_t len )
{
  char                buf[ 1024 ];
  PatternCvt          cvt( buf, sizeof( buf ) );
  RouteLoc            loc;
  RedisPatternRoute * rt;
  uint32_t            h, rcnt;

  if ( cvt.convert_glob( sub, len ) == 0 ) {
    h = kv_crc_c( sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );
    if ( (rt = this->pat_tab.tab.find( h, sub, len, loc )) != NULL ) {
      if ( rt->md != NULL ) {
        pcre2_match_data_free( rt->md );
        rt->md = NULL;
      }
      if ( rt->re != NULL ) {
        pcre2_code_free( rt->re );
        rt->re = NULL;
      }
      this->pat_tab.tab.remove( loc );
      rcnt = this->sub_route.del_pattern_route( h, this->sub_id,
                                                cvt.prefixlen );
      this->sub_route.rte.notify_punsub( h, buf, cvt.off, sub, cvt.prefixlen,
                                         this->sub_id, rcnt, 'R' );
      return EXEC_OK;
    }
  }
  return ERR_KEY_DOESNT_EXIST;
}

ExecStatus
RedisExec::do_sub( int flags )
{
  const char * hdr;
  size_t       hdr_sz;
  size_t       cnt = this->sub_tab.sub_count() + this->pat_tab.sub_count(),
               j   = 0,
               sz  = 0;
  const char * sub[ 8 ];
  size_t       len[ 8 ],
               ldig[ 8 ],
               cval[ 8 ],
               cdig[ 8 ];

  if ( ( flags & DO_SUBSCRIBE ) != 0 ) {
    static const char   sub_hdr[]     = "*3\r\n$9\r\nsubscribe\r\n";
    static const size_t sub_hdr_sz    = sizeof( sub_hdr ) - 1;
    hdr    = sub_hdr;
    hdr_sz = sub_hdr_sz;
  }
  else if ( ( flags & DO_UNSUBSCRIBE ) != 0 ) {
    static const char   unsub_hdr[]   = "*3\r\n$11\r\nunsubscribe\r\n";
    static const size_t unsub_hdr_sz  = sizeof( unsub_hdr ) - 1;
    hdr    = unsub_hdr;
    hdr_sz = unsub_hdr_sz;
  }
  else if ( ( flags & DO_PSUBSCRIBE ) != 0 ) {
    static const char   psub_hdr[]    = "*3\r\n$10\r\npsubscribe\r\n";
    static const size_t psub_hdr_sz   = sizeof( psub_hdr ) - 1;
    hdr    = psub_hdr;
    hdr_sz = psub_hdr_sz;
  }
  else /*if ( ( flags & DO_PUNSUBSCRIBE ) != 0 )*/ {
    static const char   punsub_hdr[]  = "*3\r\n$12\r\npunsubscribe\r\n";
    static const size_t punsub_hdr_sz = sizeof( punsub_hdr ) - 1;
    hdr    = punsub_hdr;
    hdr_sz = punsub_hdr_sz;
  }
  for ( size_t i = 1; i < this->argc; i++ ) {
    if ( ! this->msg.get_arg( i, sub[ j ], len[ j ] ) )
      return ERR_BAD_ARGS;

    if ( ( flags & DO_SUBSCRIBE ) != 0 ) {
      if ( this->do_subscribe( sub[ j ], len[ j ] ) == EXEC_OK )
        cnt++;
    }
    else if ( ( flags & DO_UNSUBSCRIBE ) != 0 ) {
      if ( this->do_unsubscribe( sub[ j ], len[ j ] ) == EXEC_OK )
        cnt--;
    }
    else if ( ( flags & DO_PSUBSCRIBE ) != 0 ) {
      if ( this->do_psubscribe( sub[ j ], len[ j ] ) == EXEC_OK )
        cnt++;
    }
    else /*if ( ( flags & DO_PUNSUBSCRIBE ) != 0 )*/ {
      if ( this->do_punsubscribe( sub[ j ], len[ j ] ) == EXEC_OK )
        cnt--;
    }
    ldig[ j ] = uint_digits( len[ j ] );
    cval[ j ] = cnt;
    cdig[ j ] = uint_digits( cnt );
         /* *3 .. $len ..              subject ..     :cnt */
    sz += hdr_sz + 1 + ldig[ j ] + 2 + len[ j ] + 2 + 1 + cdig[ j ] + 2;

    if ( ++j == 8 || i + 1 == this->argc ) {
      char * msg = this->strm.alloc( sz );
      size_t off = 0;
      if ( msg == NULL )
        return ERR_ALLOC_FAIL;
      for ( size_t k = 0; k < j; k++ ) {
        ::memcpy( &msg[ off ], hdr, hdr_sz );
        off += hdr_sz;
        msg[ off++ ] = '$';
        off += uint_to_str( len[ k ], &msg[ off ], ldig[ k ] );
        off  = crlf( msg, off );
        ::memcpy( &msg[ off ], sub[ k ], len[ k ] );
        off  = crlf( msg, off + len[ k ] );
        msg[ off++ ] = ':';
        off += uint_to_str( cval[ k ], &msg[ off ], cdig[ k ] );
        off  = crlf( msg, off );
      }
      this->strm.sz += sz;
      j  = 0;
      sz = 0;
    }
  }
  return EXEC_OK;
}

RedisContinueMsg::RedisContinueMsg( size_t mlen,  uint16_t kcnt )
            : next( 0 ), back( 0 ), keycnt( kcnt ), state( 0 ),
              msgid( 0 ), msglen( mlen )
{
  this->ptr = (RedisContinuePtr *) (void *) &this[ 1 ];
  this->msg = (char *) (void *) &this->ptr[ kcnt ];
}

ExecStatus
RedisExec::save_blocked_cmd( int64_t timeout_val )
{
  size_t             msglen = this->msg.pack_size();
  char             * buf;
  RedisKeyspace      kspc( *this );
  RedisContinue    * rt;
  RedisContinueMsg * cm;
  void             * p;
  char             * sub;
  size_t             len,
                     save_len = 0;
  uint32_t           h, rcnt, i, sz;
  const RedisCatg    catg = get_cmd_category( this->cmd );

  /* calculate length of buf[] */
  len = 0;
  for ( i = 0; i < this->key_cnt; i++ ) {
    kspc.key    = (char *) this->keys[ i ]->kbuf.u.buf;
    kspc.keylen = this->keys[ i ]->kbuf.keylen - 1;
    if ( catg == LIST_CATG )
      sz = kspc.make_listblkd_subj();
    else if ( catg == STREAM_CATG ) {
      sz = kspc.make_strmblkd_subj();
      if ( ( this->keys[ i ]->flags & EKF_IS_SAVED_CONT ) != 0 &&
           this->keys[ i ]->part != NULL )
        save_len = this->keys[ i ]->part->size;
      else
        save_len = 0;
    }
    else if ( catg == SORTED_SET_CATG )
      sz = kspc.make_zsetblkd_subj();
    else
      return ERR_BAD_CMD;
    if ( sz == 0 ) /* can't allocate temp space for subject */
      return ERR_ALLOC_FAIL;
    len += sz + save_len;
  }
  len += sizeof( RedisContinueMsg ) +
         sizeof( RedisContinuePtr ) * this->key_cnt;
  /* space for msg and ptrs to other keys */
  buf = (char *) ::malloc( kv::align<size_t>( msglen, 8 ) + len );
  if ( buf == NULL )
    return ERR_ALLOC_FAIL;
  cm = new ( buf ) RedisContinueMsg( msglen, this->key_cnt );
  /* pack msg for later continuation */
  this->msg.pack( cm->msg );

  /* if more than one key, will have multiple entries in table */
  p = &cm->msg[ kv::align<size_t>( msglen, 8 ) ];

  /* create a subject for each key in command */
  for ( i = 0; i < this->key_cnt; i++ ) {
    kspc.key    = (char *) this->keys[ i ]->kbuf.u.buf;
    kspc.keylen = this->keys[ i ]->kbuf.keylen - 1;
    if ( catg == LIST_CATG )
      len = kspc.make_listblkd_subj();
    else if ( catg == STREAM_CATG ) {
      len = kspc.make_strmblkd_subj();
      if ( ( this->keys[ i ]->flags & EKF_IS_SAVED_CONT ) != 0 &&
           this->keys[ i ]->part != NULL )
        save_len = this->keys[ i ]->part->size;
      else
        save_len = 0;
    }
    else
      len = kspc.make_zsetblkd_subj();
    sub = kspc.subj;
    h   = kv_crc_c( sub, len, 0 );
    /* subscribe to the continuation notification (the keyspace subject) */
    rcnt = this->sub_route.add_route( h, this->sub_id );
    this->sub_route.rte.notify_sub( h, sub, len, this->sub_id, rcnt, 'R' );
    if ( this->continue_tab.put( h, sub, len, rt ) == REDIS_SUB_OK ) {
      rt->continue_msg = cm; /* continue msg and ptrs to other subject keys */
      rt->keynum = i;
      rt->keycnt = this->key_cnt;
      cm->state |= CM_CONT_TAB;
    }
    /* save the ptr to the subject and hash */
    ::memcpy( p, sub, len );
    if ( save_len != 0 )
      ::memcpy( &((char *) p)[ len ], this->keys[ i ]->part->data( 0 ),
                save_len );
    cm->ptr[ i ].hash     = h;
    cm->ptr[ i ].len      = len;
    cm->ptr[ i ].save_len = save_len;
    cm->ptr[ i ].value    = (char *) p;
    p = &((char *) p)[ len + save_len ];
  }
  if ( timeout_val != 0 ) {
    bool b;
    cm->msgid = ++this->next_event_id;
    if ( catg == STREAM_CATG ) /* blocked in millisecs */
      b = this->sub_route.rte.add_timer_millis( this->sub_id, timeout_val,
                                                this->timer_id, cm->msgid );
    else /* others are in seconds */
      b = this->sub_route.rte.add_timer_seconds( this->sub_id, timeout_val,
                                                 this->timer_id, cm->msgid );
    if ( b ) {
      this->wait_list.push_tl( cm );
      cm->state |= CM_WAIT_LIST | CM_TIMER;
    }
    else {
      fprintf( stderr, "add_timer failed\n" );
    }
  }
  return EXEC_OK;
}

void
RedisExec::drain_continuations( EvSocket *svc )
{
  RedisContinueMsg * cm;
  RedisMsgStatus     mstatus;
  ExecStatus         status;

  while ( ! this->cont_list.is_empty() ) {
    cm = this->cont_list.pop_hd();
    cm->state &= ~CM_CONT_LIST;
    if ( ( cm->state & CM_TIMEOUT ) != 0 )
      this->timeout = true;
    else
      this->timeout = false;
    mstatus = this->msg.unpack( cm->msg, cm->msglen, this->strm.tmp );
    if ( mstatus == REDIS_MSG_OK ) {
      if ( (status = this->exec( svc, NULL )) == EXEC_OK )
        if ( this->strm.alloc_fail )
          status = ERR_ALLOC_FAIL;
      switch ( status ) {
        case EXEC_SETUP_OK:
          for ( size_t i = 0; i < cm->keycnt; i++ ) {
            size_t j = cm->ptr[ i ].len,
                   k = cm->ptr[ i ].save_len;
            if ( k != 0 ) {
              this->save_data( *this->keys[ i ], &cm->ptr[ i ].value[ j ], k );
              this->keys[ i ]->flags |= EKF_IS_SAVED_CONT;
            }
          }
          this->exec_run_to_completion();
          if ( ! this->strm.alloc_fail )
            break;
          status = ERR_ALLOC_FAIL;
          /* FALLTHRU */
        default:
          this->send_err( status );
          break;
      }
    }
    this->timeout = false;
    delete cm;
  }
}

