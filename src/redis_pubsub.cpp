#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raikv/ev_publish.h>
#include <raikv/route_db.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>
#include <raimd/md_types.h>
#include <raids/redis_keyspace.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

enum {
  DO_SUBSCRIBE    = 1<<0,
  DO_UNSUBSCRIBE  = 1<<1,
  DO_PSUBSCRIBE   = 1<<2,
  DO_PUNSUBSCRIBE = 1<<3
};

void
RedisExec::rem_all_sub( void ) noexcept
{
  RedisSubRoutePos     pos;
  RedisPatternRoutePos ppos;
  RedisContinuePos     cpos;
  uint32_t             rcnt;
  if ( this->sub_tab.first( pos ) ) {
    do {
      rcnt = this->sub_route.del_sub_route( pos.rt->hash, this->sub_id );
      this->sub_route.rte.notify_unsub( pos.rt->hash, pos.rt->value,
                                        pos.rt->len, this->sub_id, rcnt, 'R' );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    PatternCvt cvt;
    do {
      if ( cvt.convert_glob( ppos.rt->value, ppos.rt->len ) == 0 ) {
        rcnt = this->sub_route.del_pattern_route( ppos.rt->hash, this->sub_id,
                                                  cvt.prefixlen );
        this->sub_route.rte.notify_punsub( ppos.rt->hash, cvt.out, cvt.off,
                                           ppos.rt->value, cvt.prefixlen,
                                           this->sub_id, rcnt, 'R' );
      }
    } while ( this->pat_tab.next( ppos ) );
  }
  if ( this->continue_tab.first( cpos ) ) {
    do {
      rcnt = this->sub_route.del_sub_route( cpos.rt->hash, this->sub_id );
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
RedisPatternRoute::release( void )
{
  if ( this->md != NULL ) {
    pcre2_match_data_free( this->md );
    this->md = NULL;
  }
  if ( this->re != NULL ) {
    pcre2_code_free( this->re );
    this->re = NULL;
  }
}

namespace rai {
namespace ds {
template <>
void RedisPatDataMap<RedisPatternRoute>::release( void ) noexcept
{
  RedisPatternRoutePos ppos;

  if ( this->first( ppos ) ) {
    do {
      ppos.rt->release();
    } while ( this->next( ppos ) );
  }
  this->tab.release();
}

template <>
void RedisDataMap<RedisSubRoute>::release( void ) noexcept
{
  this->tab.release();
}

template <>
void RedisDataMap<RedisContinue>::release( void ) noexcept
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
}
}

bool
RedisExec::pub_message( EvPublish &pub,  RedisPatternRoute *rt ) noexcept
{
  static const char   hdr[]   = "*3\r\n$7\r\nmessage\r\n",
                      phdr[]  = "*4\r\n$8\r\npmessage\r\n";
  static const size_t hdr_sz  = sizeof( hdr ) - 1,
                      phdr_sz = sizeof( phdr ) - 1;
  size_t              sz, off,
                      sdigits,
                      pdigits;
  size_t              msg_len_digits,
                      msg_len_frame;
  char              * msg;

  off     = 0;
  sdigits = uint64_digits( pub.subject_len );
  pdigits = 0;

  if ( pub.msg_enc == MD_MESSAGE &&
       pub.msg_len > 0 &&
       RedisMsg::valid_type_char( ((const char *) pub.msg)[ 0 ] ) ) {
    msg_len_digits = 0;
    msg_len_frame  = 0;
  }
  else {
    msg_len_digits = ( pub.msg_len_digits > 0 ? pub.msg_len_digits :
                       uint64_digits( pub.msg_len ) );
    msg_len_frame  = 1 + msg_len_digits + 2 + 2;
  }
  if ( rt == NULL ) {
        /* *3 .. $subject_len       subject ..        */
    sz = hdr_sz + 1 + sdigits + 2 + pub.subject_len + 2 +
      /* $msg_len ..     msg */
         msg_len_frame + pub.msg_len;

    msg = this->strm.alloc( sz );
    if ( msg == NULL )
      return false;
    ::memcpy( &msg[ off ], hdr, hdr_sz );
    off += hdr_sz;
  }
  else {
    pdigits = uint64_digits( rt->len );
        /* *4 .. */
    sz = phdr_sz +
      /* $pattern_len       pattern ..        */
         1 + pdigits + 2 + rt->len + 2 +
      /* $subject_len       subject ..        */
         1 + sdigits + 2 + pub.subject_len + 2 +
      /* $msg_len ..     msg */
         msg_len_frame + pub.msg_len;

    msg = this->strm.alloc( sz );
    if ( msg == NULL )
      return false;
    ::memcpy( &msg[ off ], phdr, phdr_sz );
    off += phdr_sz;
    msg[ off++ ] = '$';
    off += uint64_to_string( rt->len, &msg[ off ], pdigits );
    off  = crlf( msg, off );
    ::memcpy( &msg[ off ], rt->value, rt->len );
    off  = crlf( msg, off + rt->len );
  }
  msg[ off++ ] = '$';
  off += uint64_to_string( pub.subject_len, &msg[ off ], sdigits );
  off  = crlf( msg, off );
  ::memcpy( &msg[ off ], pub.subject, pub.subject_len );
  off  = crlf( msg, off + pub.subject_len );

  if ( msg_len_frame != 0 ) {
    msg[ off++ ] = '$';
    if ( pub.msg_len_digits == 0 )
      uint64_to_string( pub.msg_len, &msg[ off ], msg_len_digits );
    else
      ::memcpy( &msg[ off ], pub.msg_len_buf, msg_len_digits );
    off += msg_len_digits;
    off  = crlf( msg, off );
    ::memcpy( &msg[ off ], pub.msg, pub.msg_len );
    crlf( msg, off + pub.msg_len );
  }
  else {
    ::memcpy( &msg[ off ], pub.msg, pub.msg_len );
  }
  this->strm.sz += sz;
  return true;
}

int
RedisExec::do_pub( EvPublish &pub,  RedisContinueMsg *&cm ) noexcept
{
  int status = 0;
  /* don't publish to self ?? (redis does not allow pub w/sub on same conn) */
  if ( (uint32_t) this->sub_id == pub.src_route )
    return 0;
  uint32_t pub_cnt = 0;
  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    if ( pub.subj_hash == pub.hash[ cnt ] ) {
      RedisSubRoute * rt;;
      RedisSubStatus  ret;
      ret = this->sub_tab.updcnt( pub.subj_hash, pub.subject, pub.subject_len,
                                  rt );
      if ( ret == REDIS_SUB_OK ) {
        pub_cnt++;
        if ( rt->callback == NULL ) {
          this->pub_message( pub, NULL );
        }
        else {
          ds_event_t event;

          event.subject.strval = (char *) pub.subject;
          event.subject.len    = pub.subject_len;
          event.subject.type   = DS_BULK_STRING;
          event.subscription   = event.subject;
          event.reply.strval   = (char *) pub.reply;
          event.reply.len      = pub.reply_len;
          event.reply.type     = DS_BULK_STRING;
          msg.strval           = (char *) pub.msg;
          msg.len              = pub.msg_len;
          msg.type             = DS_BULK_STRING;
          rt->callback( &event, &msg, rt->closure );
        }
      }
      if ( pub.subject_len > 10 && pub.subject[ 0 ] == '_' &&
           ! this->continue_tab.is_null() ) {
        RedisContinue * rt;
        RouteLoc        loc;
        ret = this->continue_tab.find( pub.subj_hash, pub.subject,
                                       pub.subject_len, rt, loc );
        if ( ret == REDIS_SUB_OK ) {
          if ( ( rt->continue_msg->state & CM_PUB_HIT ) == 0 ) {
            cm         = rt->continue_msg;
            cm->state |= CM_PUB_HIT;
            status    |= RPUB_CONTINUE_MSG;
          }
#if 0
          uint32_t keynum, keycnt, i;
          keynum = rt->keynum;
          keycnt = rt->keycnt;
          rt     = NULL; /* removing will junk this ptr */
          this->continue_tab.tab.remove( loc );
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
            uint32_t rcnt = this->sub_route.del_sub_route( cm->ptr[ i ].hash,
                                                       this->sub_id );
            this->sub_route.rte.notify_unsub( cm->ptr[ i ].hash,
                                              cm->ptr[ i ].value,
                                              cm->ptr[ i ].len,
                                              this->sub_id, rcnt, 'R' );
          }
#endif
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
          rt->incr();
          pub_cnt++;
          if ( rt->callback == NULL ) {
            this->pub_message( pub, rt );
          }
          else {
            ds_event_t event;

            event.subject.strval      = (char *) pub.subject;
            event.subject.len         = pub.subject_len;
            event.subject.type        = DS_BULK_STRING;
            event.subscription.strval = rt->value;
            event.subscription.len    = rt->len;
            event.subscription.type   = DS_BULK_STRING;
            event.reply.strval        = (char *) pub.reply;
            event.reply.len           = pub.reply_len;
            event.reply.type          = DS_BULK_STRING;
            msg.strval                = (char *) pub.msg;
            msg.len                   = pub.msg_len;
            msg.type                  = DS_BULK_STRING;
            rt->callback( &event, &msg, rt->closure );
          }
        }
        rt = this->pat_tab.tab.find_next_by_hash( pub.hash[ cnt ], loc );
      }
    }
  }
  if ( pub_cnt > 0 )
    status |= RPUB_FORWARD_MSG;
  return status;
}

bool
RedisExec::continue_expire( uint64_t event_id,
                            RedisContinueMsg *&cm ) noexcept
{
  for ( cm = this->wait_list.hd; cm != NULL; cm = cm->next ) {
    if ( cm->msgid == (uint32_t) event_id ) {
      if ( ( cm->state & CM_TIMEOUT ) == 0 ) {
        cm->state |= CM_TIMEOUT;
        cm->state &= ~CM_TIMER;
        return true;
      }
      return false;
    }
  }
  return false;
}

void
RedisExec::pop_continue_tab( RedisContinueMsg *cm ) noexcept
{
  if ( ( cm->state & CM_CONT_TAB ) == 0 )
    return;

  uint32_t keycnt, i;
  keycnt = cm->keycnt;
  for ( i = 0; i < keycnt; i++ ) {
    this->continue_tab.tab.remove( cm->ptr[ i ].hash,
                                   cm->ptr[ i ].value,
                                   cm->ptr[ i ].len );
  }
  cm->state &= ~CM_CONT_TAB;
  for ( i = 0; i < keycnt; i++ ) {
    uint32_t rcnt = this->sub_route.del_sub_route( cm->ptr[ i ].hash,
                                               this->sub_id );
    this->sub_route.rte.notify_unsub( cm->ptr[ i ].hash,
                                      cm->ptr[ i ].value,
                                      cm->ptr[ i ].len,
                                      this->sub_id, rcnt, 'R' );
  }
  this->msg_route_cnt++;
}

void
RedisExec::push_continue_list( RedisContinueMsg *cm ) noexcept
{
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
RedisExec::exec_psubscribe( void ) noexcept
{
  /* PSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_PSUBSCRIBE );
}

bool
RedisExec::do_hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
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
RedisExec::exec_pubsub( void ) noexcept
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
      size_t       patlen  = 0, pos;
      uint32_t     h, v;
      int          rc;

      if ( this->msg.get_arg( 2, pattern, patlen ) &&
           ( patlen > 1 || pattern[ 0 ] != '*' ) ) {
        size_t     erroff;
        int        error;
        PatternCvt cvt;
        if ( cvt.convert_glob( pattern, patlen ) != 0 )
          return ERR_BAD_ARGS;
        re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error, &erroff,
                            0 );
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
          uint32_t rcnt = this->sub_route.get_sub_route_count( h );
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
RedisExec::exec_publish( void ) noexcept
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
  this->msg_route_cnt += rcount;
  if ( rcount <= 1 ) {
    if ( rcount == 0 )
      return EXEC_SEND_ZERO;
    return EXEC_SEND_ONE;
  }
  rte_digits = uint64_digits( rcount );
  buf = this->strm.alloc( rte_digits + 3 ); 
  if ( buf == NULL )
    return ERR_ALLOC_FAIL;
  buf[ 0 ] = ':';
  uint64_to_string( rcount, &buf[ 1 ], rte_digits );
  crlf( buf, rte_digits + 1 );
  this->strm.sz += rte_digits + 3;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_punsubscribe( void ) noexcept
{
  /* UNSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_PUNSUBSCRIBE );
}

ExecStatus
RedisExec::exec_subscribe( void ) noexcept
{
  /* SUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_SUBSCRIBE );
}

ExecStatus
RedisExec::exec_unsubscribe( void ) noexcept
{
  /* UNSUBSCRIBE subj [subj ..] */
  return this->do_sub( DO_UNSUBSCRIBE );
}


ExecStatus
RedisExec::do_unsubscribe( const char *sub,  size_t len ) noexcept
{
  uint32_t h, rcnt;

  h = kv_crc_c( sub, len, 0 );
  if ( this->sub_tab.rem( h, sub, len ) == REDIS_SUB_OK ) {
    rcnt = 0;
    /* check for duplicate hashes */
    if ( this->sub_tab.tab.find_by_hash( h ) == NULL )
      rcnt = this->sub_route.del_sub_route( h, this->sub_id );
    this->sub_route.rte.notify_unsub( h, sub, len, this->sub_id, rcnt, 'R' );
    this->msg_route_cnt++;
    return EXEC_OK;
  }
  return ERR_KEY_DOESNT_EXIST;
}

ExecStatus
RedisExec::do_subscribe_cb( const char *sub,  size_t len,
                            ds_on_msg_t cb,  void *cl ) noexcept
{
  RedisSubRoute * rt;
  uint32_t h, rcnt;

  h = kv_crc_c( sub, len, 0 );
  if ( this->sub_tab.put( h, sub, len, rt ) == REDIS_SUB_OK ) {
    rt->callback = cb;
    rt->closure  = cl;
    rcnt = this->sub_route.add_sub_route( h, this->sub_id );
    this->sub_route.rte.notify_sub( h, sub, len, this->sub_id, rcnt, 'R' );
    this->msg_route_cnt++;
    return EXEC_OK;
  }
  return ERR_KEY_EXISTS;
}

ExecStatus
RedisExec::do_psubscribe_cb( const char *sub,  size_t len,
                             ds_on_msg_t cb,  void *cl ) noexcept
{
  RedisPatternRoute * rt;
  PatternCvt cvt;
  uint32_t   h, rcnt;

  if ( cvt.convert_glob( sub, len ) == 0 ) {
    h = kv_crc_c( sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );
    if ( this->pat_tab.put( h, sub, len, rt ) == REDIS_SUB_OK ) {
      size_t erroff;
      int    error;
      rt->callback = cb;
      rt->closure  = cl;
      rt->re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error,
                              &erroff, 0 );
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
        this->sub_route.rte.notify_psub( h, cvt.out, cvt.off, sub,
                                         cvt.prefixlen, this->sub_id, rcnt,'R');
        this->msg_route_cnt++;
        return EXEC_OK;
      }
    }
  }
  return ERR_KEY_EXISTS;
}

ExecStatus
RedisExec::do_punsubscribe( const char *sub,  size_t len ) noexcept
{
  PatternCvt          cvt;
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
      this->sub_route.rte.notify_punsub( h, cvt.out, cvt.off, sub,
                                         cvt.prefixlen, this->sub_id, rcnt,'R');
      this->msg_route_cnt++;
      return EXEC_OK;
    }
  }
  return ERR_KEY_DOESNT_EXIST;
}

ExecStatus
RedisExec::do_sub( int flags ) noexcept
{
  const char * hdr;
  size_t       hdr_sz;
  size_t       cnt = this->sub_tab.sub_count() + this->pat_tab.sub_count(),
               i   = 1,
               j   = 0,
               k   = 0,
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
  for ( bool last = false; ; ) {
    if ( i < this->argc ) {
      if ( ! this->msg.get_arg( i, sub[ k ], len[ k ] ) )
        return ERR_BAD_ARGS;
      if ( ++i == this->argc )
        last = true;
      k++;
    }
    else {
      /* unsubscribe all */
      if ( ( flags & DO_UNSUBSCRIBE ) != 0 ) {
        RedisSubRoutePos pos;
        if ( this->sub_tab.first( pos ) ) {
          len[ k ] = pos.rt->len;
          char *s = this->strm.alloc_temp( len[ k ] + 1 );
          if ( s == NULL )
            return ERR_ALLOC_FAIL;
          ::memcpy( s, pos.rt->value, pos.rt->len );
          s[ pos.rt->len ] = '\0';
          sub[ k ] = s;
          k++;
        }
      }
      /* punsubscribe all */
      else if ( ( flags & DO_PUNSUBSCRIBE ) != 0 ) {
        RedisPatternRoutePos ppos;
        if ( this->pat_tab.first( ppos ) ) {
          len[ k ] = ppos.rt->len;
          char *s = this->strm.alloc_temp( len[ k ] + 1 );
          if ( s == NULL )
            return ERR_ALLOC_FAIL;
          ::memcpy( s, ppos.rt->value, ppos.rt->len );
          s[ ppos.rt->len ] = '\0';
          sub[ k ] = s;
          k++;
        }
      }
    }
    /* if no more left, is last */
    if ( j == k )
      last = true;
    else {
      if ( ( flags & DO_SUBSCRIBE ) != 0 ) {
        if ( this->do_subscribe_cb( sub[ j ], len[ j ] ) == EXEC_OK )
          cnt++;
      }
      else if ( ( flags & DO_UNSUBSCRIBE ) != 0 ) {
        if ( this->do_unsubscribe( sub[ j ], len[ j ] ) == EXEC_OK )
          cnt--;
      }
      else if ( ( flags & DO_PSUBSCRIBE ) != 0 ) {
        if ( this->do_psubscribe_cb( sub[ j ], len[ j ] ) == EXEC_OK )
          cnt++;
      }
      else /*if ( ( flags & DO_PUNSUBSCRIBE ) != 0 )*/ {
        if ( this->do_punsubscribe( sub[ j ], len[ j ] ) == EXEC_OK )
          cnt--;
      }
      ldig[ j ] = uint64_digits( len[ j ] );
      cval[ j ] = cnt;
      cdig[ j ] = uint64_digits( cnt );
           /* *3 .. $len ..              subject ..     :cnt */
      sz += hdr_sz + 1 + ldig[ j ] + 2 + len[ j ] + 2 + 1 + cdig[ j ] + 2;
      j++;
    }
    if ( j == 8 || last ) {
      if ( j == 0 ) /* no more in this segment */
        return EXEC_OK;
      char * msg = this->strm.alloc( sz );
      size_t off = 0;
      if ( msg == NULL )
        return ERR_ALLOC_FAIL;
      for ( size_t k = 0; k < j; k++ ) {
        ::memcpy( &msg[ off ], hdr, hdr_sz );
        off += hdr_sz;
        msg[ off++ ] = '$';
        off += uint64_to_string( len[ k ], &msg[ off ], ldig[ k ] );
        off  = crlf( msg, off );
        ::memcpy( &msg[ off ], sub[ k ], len[ k ] );
        off  = crlf( msg, off + len[ k ] );
        msg[ off++ ] = ':';
        off += uint64_to_string( cval[ k ], &msg[ off ], cdig[ k ] );
        off  = crlf( msg, off );
      }
      this->strm.sz += sz;
      if ( last )
        return EXEC_OK;
      j  = 0;
      k  = 0;
      sz = 0;
    }
  }
}

RedisContinueMsg::RedisContinueMsg( size_t mlen,  uint16_t kcnt ) noexcept
            : next( 0 ), back( 0 ), keycnt( kcnt ), state( 0 ),
              msgid( 0 ), msglen( mlen )
{
  this->ptr = (RedisContinuePtr *) (void *) &this[ 1 ];
  this->msg = (char *) (void *) &this->ptr[ kcnt ];
}

ExecStatus
RedisExec::save_blocked_cmd( int64_t timeout_val ) noexcept
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

  /* calculate length of buf[] */
  len = 0;
  for ( i = 0; i < this->key_cnt; i++ ) {
    kspc.key    = (char *) this->keys[ i ]->kbuf.u.buf;
    kspc.keylen = this->keys[ i ]->kbuf.keylen - 1;
    if ( this->catg == LIST_CATG )
      sz = kspc.make_listblkd_subj();
    else if ( this->catg == STREAM_CATG ) {
      sz = kspc.make_strmblkd_subj();
      if ( ( this->keys[ i ]->state & EKS_IS_SAVED_CONT ) != 0 &&
           this->keys[ i ]->part != NULL )
        save_len = this->keys[ i ]->part->size;
      else
        save_len = 0;
    }
    else if ( this->catg == SORTED_SET_CATG )
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
    if ( this->catg == LIST_CATG )
      len = kspc.make_listblkd_subj();
    else if ( this->catg == STREAM_CATG ) {
      len = kspc.make_strmblkd_subj();
      /* the xread command mutates the arguments based on the current
       * end of stream;  the saved continuation data caches this value
       * so that when xread resumes, it knows what data was added to
       * the stream */
      if ( ( this->keys[ i ]->state & EKS_IS_SAVED_CONT ) != 0 &&
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
    rcnt = this->sub_route.add_sub_route( h, this->sub_id );
    this->sub_route.rte.notify_sub( h, sub, len, this->sub_id, rcnt, 'R' );
    this->msg_route_cnt++;
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
    if ( this->catg == STREAM_CATG ) /* blocked in millisecs */
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
RedisExec::drain_continuations( EvSocket *svc ) noexcept
{
  RedisContinueMsg * cm;
  RedisMsgStatus     mstatus;
  ExecStatus         status;

  this->blk_state = 0;
  while ( ! this->cont_list.is_empty() ) {
    cm = this->cont_list.pop_hd();
    cm->state &= ~CM_CONT_LIST;
    if ( ( cm->state & CM_TIMEOUT ) != 0 )
      this->blk_state |= RBLK_CMD_TIMEOUT;
    if ( ( cm->state & CM_PUB_HIT ) != 0 )
      this->blk_state |= RBLK_CMD_KEY_CHANGE;
    mstatus = this->msg.unpack( cm->msg, cm->msglen, this->strm.tmp );
    if ( mstatus == DS_MSG_STATUS_OK ) {
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
              this->keys[ i ]->state |= EKS_IS_SAVED_CONT;
            }
          }
          this->exec_run_to_completion();
          if ( ! this->strm.alloc_fail )
            break;
          status = ERR_ALLOC_FAIL;
          /* FALLTHRU */
        default:
          this->send_status( status, KEY_OK );
          break;
      }
    }
    if ( ( this->blk_state & RBLK_CMD_COMPLETE ) != 0 ) {
      this->pop_continue_tab( cm );
      delete cm;
    }
    else {
      cm->state &= ~CM_PUB_HIT;
    }
    this->blk_state = 0;
  }
}
