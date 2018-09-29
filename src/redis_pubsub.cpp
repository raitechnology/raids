#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/ev_publish.h>
#include <raids/route_db.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;

enum {
  DO_SUBSCRIBE    = 1<<0,
  DO_UNSUBSCRIBE  = 1<<1,
  DO_PSUBSCRIBE   = 1<<2,
  DO_PUNSUBSCRIBE = 1<<3
};

void
RedisExec::rem_all_sub( void )
{
  HashPos pos;
  if ( this->sub_tab.first( pos ) ) {
    do {
      this->sub_route.del_route( pos.h, this->sub_id );
    } while ( this->sub_tab.next( pos ) );
  }
}

bool
RedisExec::do_pub( EvPublish &pub )
{
  /* don't publish to self ?? */
  if ( (uint32_t) this->sub_id != pub.src_route ) {
    HashPos sub_pos( pub.subj_hash );
    if ( this->sub_tab.updcnt( pub.subject, pub.subject_len,
                               sub_pos ) == SUB_OK ) {
      static const char   hdr[]  = "*3\r\n$7\r\nmessage\r\n";
      static const size_t hdr_sz = sizeof( hdr ) - 1;
      size_t sz,
             sdigits = RedisMsg::uint_digits( pub.subject_len );

          /* *3 .. $subject_len       subject ..        */
      sz = hdr_sz + 1 + sdigits + 2 + pub.subject_len + 2 +
        /* $msg_len ..                  msg */
           1 + pub.msg_len_digits + 2 + pub.msg_len + 2;

      char * msg = this->strm.alloc( sz );
      size_t off = 0;
      if ( msg == NULL )
        return false;
      ::memcpy( &msg[ off ], hdr, hdr_sz );
      off += hdr_sz;
      msg[ off++ ] = '$';
      off += RedisMsg::uint_to_str( pub.subject_len, &msg[ off ], sdigits );
      off  = crlf( msg, off );
      ::memcpy( &msg[ off ], pub.subject, pub.subject_len );
      off  = crlf( msg, off + pub.subject_len );
      msg[ off++ ] = '$';
      ::memcpy( &msg[ off ], pub.msg_len_buf, pub.msg_len_digits );
      off += pub.msg_len_digits;
      off  = crlf( msg, off );
      ::memcpy( &msg[ off ], pub.msg, pub.msg_len );
      crlf( msg, off + pub.msg_len );

      this->strm.sz += sz;
      return true;
    }
  }
  return false;
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
  HashPos pos( h );
  HashVal kv;
  if ( this->sub_tab.h == NULL )
    return false;
  if ( this->sub_tab.h->hscan( pos, kv ) != HASH_OK )
    return false;
  for ( ; ; pos.i++ ) {
    uint32_t h2 = kv_crc_c( kv.key, kv.keylen, 0 );
    if ( ( h | UIntHashTab::SLOT_USED ) == ( h2 | UIntHashTab::SLOT_USED ) ) {
      ::memcpy( key, kv.key, kv.keylen );
      keylen = kv.keylen;
      return true;
    }
    if ( this->sub_tab.h->hscan( pos, kv ) != HASH_OK )
      return false;
  }
}

ExecStatus
RedisExec::exec_pubsub( void )
{
  StreamBuf::BufQueue q( this->strm );
  size_t cnt = 0;

  /* PUBSUB [channels [pattern] | numsub channel-1 [, ...] | numpat] */
  switch ( this->msg.match_arg( 1, "channels", 8,
                                   "numsub",   6,
                                   "numpat",   6, NULL ) ) {
    default: return ERR_BAD_CMD;
    case 1: { /* channels [pattern] */
      uint32_t     pos, h, v;
      uint8_t      buf[ 1024 ],
                 * bf = buf;
      size_t       erroff,
                   blen = sizeof( buf );
      int          rc,
                   error;
      const char * pattern = NULL;
      size_t       patlen  = 0;
      pcre2_real_code_8       * re = NULL; /* pcre regex compiled */
      pcre2_real_match_data_8 * md = NULL; /* pcre match context  */

      if ( this->msg.get_arg( 2, pattern, patlen ) &&
           ( patlen > 1 || pattern[ 0 ] != '*' ) ) {
        rc = pcre2_pattern_convert( (PCRE2_SPTR8) pattern, patlen,
                                    PCRE2_CONVERT_GLOB_NO_WILD_SEPARATOR,
                                    &bf, &blen, 0 );
        if ( rc != 0 )
          return ERR_BAD_ARGS;
        re = pcre2_compile( bf, blen, 0, &error, &erroff, 0 );
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
          HashPos pos( val, vallen );
          uint32_t rcnt = this->sub_route.get_route_count( pos.h );
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
  q.finish_tail();
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
  HashPos      sub_pos;
  char       * buf;
  uint32_t   * routes,
               rcnt,
               rte_digits;
  /* PUBLISH subj msg */
  if ( ! this->msg.get_arg( 1, subj, subj_len ) ||
       ! this->msg.get_arg( 2, msg, msg_len ) )
    return ERR_BAD_ARGS;

  sub_pos.init( subj, subj_len ); 
  rcnt = this->sub_route.get_route( sub_pos.h, routes );
  if ( rcnt > 0 ) {
    char   msg_len_buf[ 24 ];
    size_t msg_len_digits = RedisMsg::uint_digits( msg_len );
    RedisMsg::uint_to_str( msg_len, msg_len_buf, msg_len_digits );
    EvPublish pub( subj, subj_len,
                   NULL, 0,
                   msg, msg_len,
                   routes, rcnt, this->sub_id, sub_pos.h,
                   msg_len_buf, msg_len_digits  );
    this->sub_route.rte.publish( pub );
  }
  rte_digits = RedisMsg::uint_digits( rcnt );
  buf = this->strm.alloc( rte_digits + 3 ); 
  if ( buf == NULL )
    return ERR_ALLOC_FAIL;
  buf[ 0 ] = ':';
  RedisMsg::uint_to_str( rcnt, &buf[ 1 ], rte_digits );
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
RedisExec::do_sub( int flags )
{
  const char * hdr;
  size_t       hdr_sz;
  size_t       cnt = this->sub_tab.sub_count(),
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
    HashPos pos( sub[ j ], len[ j ] );
    if ( ( flags & ( DO_SUBSCRIBE | DO_PSUBSCRIBE ) ) != 0 ) {
      if ( this->sub_tab.put( sub[ j ], len[ j ], pos ) == SUB_OK ) {
        this->sub_route.add_route( pos.h, this->sub_id );
        cnt++;
      }
    }
    else {
      if ( this->sub_tab.rem( sub[ j ], len[ j ], pos ) == SUB_OK ) {
        this->sub_route.del_route( pos.h, this->sub_id );
        cnt--;
      }
    }
    ldig[ j ] = RedisMsg::uint_digits( len[ j ] );
    cval[ j ] = cnt;
    cdig[ j ] = RedisMsg::uint_digits( cnt );
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
        off += RedisMsg::uint_to_str( len[ k ], &msg[ off ], ldig[ k ] );
        off  = crlf( msg, off );
        ::memcpy( &msg[ off ], sub[ k ], len[ k ] );
        off  = crlf( msg, off + len[ k ] );
        msg[ off++ ] = ':';
        off += RedisMsg::uint_to_str( cval[ k ], &msg[ off ], cdig[ k ] );
        off  = crlf( msg, off );
      }
      this->strm.sz += sz;
      j  = 0;
      sz = 0;
    }
  }
  return EXEC_OK;
}

