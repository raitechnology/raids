#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/signal.h>
#include <sys/signalfd.h>

#include <raids/ev_publish.h>
#include <raids/ev_capr.h>
#include <raids/kv_pubsub.h>
#include <raids/cube_route.h>
#include <raids/redis_msg.h>
#include <raimd/md_types.h>
#include <raimd/hex_dump.h>

using namespace rai;
using namespace kv;
using namespace ds;
using namespace md;

static const uint16_t KV_CTX_BYTES = KV_MAX_CTX_ID / 8;
#if __cplusplus > 201103L
  static_assert( KV_MAX_CTX_ID == sizeof( CubeRoute128 ) * 8, "CubeRoute128" );
  static_assert( 5 == sizeof( KvPrefHash ), "KvPrefHash" );
#endif

static inline char hdigit( uint8_t h ) {
  if ( h < 10 )
    return '0' + h;
  return 'a' + ( h - 10 );
}

static const char   sys_mc[]  = "_SYS.MC",
                    sys_ibx[] = "_SYS.IBX.";
static const size_t mc_name_size  = sizeof( sys_mc ),
                    ibx_name_size = 12;
int use_dsunix = 0;

static void
make_ibx( char *ibname,  uint16_t ctx_id )
{
  ::strcpy( ibname, sys_ibx );
  ibname[  9 ] = hdigit( ( ctx_id >> 4 ) & 0xf );
  ibname[ 10 ] = hdigit( ctx_id & 0xf );
  ibname[ 11 ] = '\0';
}

static inline size_t
make_dsunix_sockname( struct sockaddr_un &un,  uint32_t id )
{
  static const char path[] = "/tmp/dsXX.sock";
  un.sun_family = AF_UNIX;
  ::memcpy( un.sun_path, path, sizeof( path ) );
  un.sun_path[ 7 ] = hdigit( ( id >> 4 ) & 0xf );
  un.sun_path[ 8 ] = hdigit( id & 0xf );
  return sizeof( path ) - 1 + offsetof( struct sockaddr_un, sun_path );
}

const char *
KvMsg::msg_type_string( uint8_t msg_type )
{
  switch ( (KvMsgType) msg_type ) {
    case KV_MSG_HELLO:   return "hello";
    case KV_MSG_BYE:     return "bye";
    case KV_MSG_STATUS:  return "status";
    case KV_MSG_SUB:     return "sub";
    case KV_MSG_UNSUB:   return "unsub";
    case KV_MSG_PSUB:    return "psub";
    case KV_MSG_PUNSUB:  return "punsub";
    case KV_MSG_PUBLISH: return "publish";
  }
  return "unknown";
}

const char *
KvMsg::msg_type_string( void ) const
{
  return KvMsg::msg_type_string( this->msg_type );
}

static void
dump_hex( void *ptr,  uint64_t size )
{
  MDHexDump hex;
  for ( uint64_t off = 0; off < size; ) {
    off = hex.fill_line( ptr, off, size );
    printf( "%s\r\n", hex.line );
    hex.flush_line();
  }
}

void
KvMsg::print( void )
{
  printf( "\r\nsession_id : %lx\r\n"
          "seqno      : %lu\r\n"
          "size       : %u\r\n"
          "src        : %u\r\n"
          "dest_start : %u\r\n"
          "dest_end   : %u\r\n"
          "msg_type   : %s\r\n",
    this->session_id, this->seqno, this->size, this->src, this->dest_start,
    this->dest_end, msg_type_string( (KvMsgType) this->msg_type ) );

  if ( this->msg_type >= KV_MSG_SUB && this->msg_type <= KV_MSG_PUBLISH ) {
    KvSubMsg &sub = (KvSubMsg &) *this;
    uint8_t prefix_cnt = sub.prefix_cnt();
    printf( "hash       : %x\r\n"
            "msg_size   : %u\r\n"
            "sublen     : %u\r\n"
            "prefix_cnt : %u\r\n"
            "replylen   : %u\r\n"
            "subject()  : %s\r\n"
            "reply()    : %s\r\n",
      sub.hash, sub.msg_size, sub.sublen, prefix_cnt,
      sub.replylen, sub.subject(), sub.reply() );
    if ( prefix_cnt > 0 ) {
      for ( uint8_t i = 0; i < prefix_cnt; i++ ) {
        KvPrefHash &pf = sub.prefix_hash( i );
        printf( "pf[ %u ] : %u, %x\r\n", i, pf.pref, pf.get_hash() );
      }
    }
    if ( this->msg_type >= KV_MSG_PUBLISH ) {
      printf( "msg_data() : %.*s\r\n", sub.msg_size, (char *) sub.msg_data() );
    }
  }
  dump_hex( this, this->size );
}

KvPubSub *
KvPubSub::create( EvPoll &poll )
{
  struct sockaddr_un un;
  KvPubSub * ps;
  int        fd;
  void     * p,
           * ibptr,
           * mcptr;
  size_t     i, len;
  char       ibname[ 12 ];

  if ( use_dsunix ) {
    if ( (fd = socket( AF_UNIX, SOCK_DGRAM, 0 )) < 0 )
      return NULL;
    len = make_dsunix_sockname( un, poll.ctx_id );
    ::unlink( un.sun_path );
    if ( ::bind( fd, (struct sockaddr *) &un, len ) < 0 ) {
      ::close( fd );
      return NULL;
    }
    ::fcntl( fd, F_SETFL, O_NONBLOCK | ::fcntl( fd, F_GETFL ) );
  }
  else {
    sigset_t mask;

    sigemptyset( &mask );
    sigaddset( &mask, SIGUSR1 );

    if ( sigprocmask( SIG_BLOCK, &mask, NULL ) == -1 ) {
      perror("sigprocmask");
      return NULL;
    }
    fd = signalfd( -1, &mask, SFD_NONBLOCK );
    if ( fd == -1 ) {
      perror( "signalfd" );
      return NULL;
    }
  }
  i = MAX_CTX_ID + 1;
  if ( (p = aligned_malloc( sizeof( KvPubSub ) +
                            sizeof( KvMsgQueue ) * i + 32 * i )) == NULL )
    return NULL;
  mcptr = (void *) &((uint8_t *) p)[ sizeof( KvPubSub ) ];
  ibptr = (void *) &((uint8_t *) mcptr)[ sizeof( KvMsgQueue ) + 32 ];
  ps = new ( p ) KvPubSub( poll, fd, mcptr, sys_mc, mc_name_size );
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    make_ibx( ibname, i );
    ps->inbox[ i ] =
      new ( ibptr ) KvMsgQueue( ps->kctx, ibname, ibx_name_size );
    ibptr = &((uint8_t *) ibptr)[ sizeof( KvMsgQueue ) + 32 ];
  }
  if ( ! ps->register_mcast( true ) || poll.add_sock( ps ) < 0 ) {
    ::close( fd );
    return NULL;
  }
  ps->idle_push( EV_PROCESS );
  ps->push( EV_WRITE );
  return ps;
}

bool
KvPubSub::register_mcast( bool activate )
{
  void    * val;
  KeyStatus status;
  bool      res = false;

  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW );
    if ( (status = this->kctx.resize( &val, KV_CTX_BYTES, true )) == KEY_OK ) {
      CubeRoute128 &cr = *(CubeRoute128 *) val;
      if ( is_new )
        cr.zero();
      if ( activate ) {
        cr.set( this->ctx_id );
        this->create_kvmsg( KV_MSG_HELLO, sizeof( KvMsg ) );
        res = true;
      }
      else if ( cr.is_set( this->ctx_id ) ) {
        cr.clear( this->ctx_id );
        this->create_kvmsg( KV_MSG_BYE, sizeof( KvMsg ) );
        res = true;
      }
    }
    this->kctx.release();
  }
  if ( ! res && activate ) {
    fprintf( stderr, "Unable to register mcast, kv status %d\n",
             (int) status );
  }
  return res;
}

bool
KvPubSub::subscribe_mcast( const char *sub,  size_t len,  bool activate,
                           bool use_find )
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;
  bool          res = false;

  if ( len + 1 > MAX_KEY_BUF_SIZE ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->wrkq.alloc( sz );
  }
  kb->keylen = len + 1;
  ::memcpy( kb->u.buf, sub, len );
  kb->u.buf[ len ] = '\0';
  hash1 = this->seed1;
  hash2 = this->seed2;
  kb->hash( hash1, hash2 );
  this->kctx.set_key( *kb );
  this->kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  if ( use_find ) {
    if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
      if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
           sz == sizeof( CubeRoute128 ) ) {
        CubeRoute128 cr;
        cr.copy_from( val );
        if ( activate ) {
          if ( cr.is_set( this->ctx_id ) )
            return true;
        }
        else {
          if ( ! cr.is_set( this->ctx_id ) )
            return true;
        }
      }
    }
  }
  /* doesn't exist or skip find, use acquire */
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    CubeRoute128 *cr;
    /* new sub */
    if ( status == KEY_IS_NEW ) {
      if ( activate ) {
        status = this->kctx.resize( &val, KV_CTX_BYTES );
        if ( status == KEY_OK ) {
          cr = (CubeRoute128 *) val;
          cr->zero();
          cr->set( this->ctx_id );
          res = true;
        }
      }
      else { /* doesn't exist, don't create it just to clear it */
        res = true;
      }
    }
    else { /* exists, get the value and set / clear the ctx bit */
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        res = true;
        cr = (CubeRoute128 *) val;
        if ( activate ) {
          cr->set( this->ctx_id );
        }
        else {
          cr->clear( this->ctx_id );
          if ( cr->is_empty() )
            this->kctx.tombstone(); /* is the last subscriber */
        }
      }
    }
    this->kctx.release();
  }
  if ( ! res && activate ) {
    fprintf( stderr, "Unable to register subject %.*s mcast, kv status %d\n",
             (int) len, sub, (int) status );
  }
  return res;
}

KvMsg *
KvPubSub::create_kvmsg( KvMsgType mtype,  size_t sz )
{
  KvMsgList * l = (KvMsgList *) this->wrkq.alloc( sizeof( KvMsgList ) + sz + 8);
  KvMsg   & msg = l->msg;

  l->range.w = 0;
  msg.session_id = this->session_id;
  msg.seqno      = this->next_seqno++;
  msg.size       = sz;
  msg.src        = this->ctx_id;
  msg.dest_start = 0;
  msg.dest_end   = KV_MAX_CTX_ID;
  msg.msg_type   = mtype;
  this->sendq.push_tl( l );
  return &msg;
}

KvSubMsg *
KvPubSub::create_kvpublish( uint32_t h,  const char *sub,  size_t len,
                            const uint8_t *pref,  const uint32_t *hash,
                            uint8_t pref_cnt,  const char *reply,  size_t rlen,
                            const void *msgdata,  size_t msgsz,
                            char src_type,  KvMsgType mtype,
                            uint8_t code,  uint8_t msg_enc )
{
  KvSubMsg * msg;
  size_t     sz = KvSubMsg::calc_size( len, rlen, msgsz, pref_cnt );
  msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  msg->hash    = h;
  msg->code    = code;
  msg->msg_enc = msg_enc;
  msg->set_subject( sub, len );
  msg->set_reply( reply, rlen );
  msg->src_type() = src_type;
  msg->prefix_cnt() = pref_cnt;
  for ( uint8_t i = 0; i < pref_cnt; i++ ) {
    KvPrefHash &pf = msg->prefix_hash( i );
    pf.pref = pref[ i ];
    pf.set_hash( hash[ i ] );
  }
  msg->set_msg_data( msgdata, msgsz );
  return msg;
}

KvSubMsg *
KvPubSub::create_kvsubmsg( uint32_t h,  const char *sub,  size_t len,
                           char src_type,  KvMsgType mtype,  const char *rep,
                           size_t rlen )
{
  KvSubMsg * msg;
  size_t     sz = KvSubMsg::calc_size( len, rlen, 0, 0 );
  msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  msg->hash     = h;
  msg->msg_size = 0;
  msg->code     = CAPR_LISTEN;
  msg->msg_enc  = 0;
  msg->set_subject( sub, len );
  msg->set_reply( rep, rlen );
  msg->src_type() = src_type;
  msg->prefix_cnt() = 0;
  return msg;
}

KvSubMsg *
KvPubSub::create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t len,
                            const char *prefix,  uint8_t prefix_len,
                            char src_type,  KvMsgType mtype )
{
  KvSubMsg * msg;
  size_t     sz = KvSubMsg::calc_size( len, prefix_len, 0, 1 );
  msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  msg->hash     = h;
  msg->msg_size = 0;
  msg->code     = CAPR_LISTEN;
  msg->msg_enc  = 0;
  msg->set_subject( pattern, len );
  msg->set_reply( prefix, prefix_len );
  msg->src_type() = src_type;
  msg->prefix_cnt() = 1;
  KvPrefHash & ph = msg->prefix_hash( 0 );
  ph.pref = prefix_len;
  ph.set_hash( h );
  return msg;
}

void
KvPubSub::do_sub( uint32_t h,  const char *sub,  size_t len,
                  uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type,
                  const char *rep,  size_t rlen )
{
  bool use_find = true;
  if ( rcnt == 1 ) /* first route added */
    use_find = false;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = false;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  this->subscribe_mcast( sub, len, true, use_find );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, len, src_type, KV_MSG_SUB, rep, rlen );
/*printf( "subscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type );*/
  this->idle_push( EV_WRITE );
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::do_unsub( uint32_t h,  const char *sub,  size_t len,
                    uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type )
{
  bool do_unsubscribe = false;
  if ( rcnt == 0 ) /* no more routes left */
    do_unsubscribe = true;
  else if ( rcnt == 1 ) { /* if the only route left is not in my server */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      do_unsubscribe = true;
  }
  if ( do_unsubscribe )
    this->subscribe_mcast( sub, len, false, false );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, len, src_type, KV_MSG_UNSUB, NULL, 0 );
/*printf( "unsubscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type);*/
  this->idle_push( EV_WRITE );
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::do_psub( uint32_t h,  const char *pattern,  size_t len,
                   const char *prefix,  uint8_t prefix_len,
                   uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type )
{
  bool use_find = true;
  if ( rcnt == 1 ) /* first route added */
    use_find = false;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = false;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  SysWildSub w( prefix, prefix_len );
  this->subscribe_mcast( w.sub, w.len, true, use_find );

  KvSubMsg *submsg =
    this->create_kvpsubmsg( h, pattern, len, prefix, prefix_len, src_type,
                            KV_MSG_PSUB );
  this->idle_push( EV_WRITE );
/*printf( "psubscribe %x %.*s %s %u:%c rcnt=%u\n",
          h, (int) len, pattern, w.sub, sub_id, src_type, rcnt );*/
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::do_punsub( uint32_t h,  const char *pattern,  size_t len,
                     const char *prefix,  uint8_t prefix_len,
                     uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type )
{
  bool do_unsubscribe = false;
  if ( rcnt == 0 ) /* no more routes left */
    do_unsubscribe = true;
  else if ( rcnt == 1 ) { /* if the only route left is not in my server */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      do_unsubscribe = true;
  }
  SysWildSub w( prefix, prefix_len );
  if ( do_unsubscribe )
    this->subscribe_mcast( w.sub, w.len, false, false );
  KvSubMsg *submsg =
    this->create_kvpsubmsg( h, pattern, len, prefix, prefix_len, src_type,
                            KV_MSG_PUNSUB );
  this->idle_push( EV_WRITE );
/*printf( "punsubscribe %x %.*s %s %u:%c rcnt=%u\n",
          h, (int) len, pattern, w.sub, sub_id, src_type, rcnt );*/
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::process( void )
{
  CubeRoute128  cr;
  HashTab     * map = this->poll.map;
  KeyFragment * kp;
  KeyCtx        scan_kctx( *map, this->poll.ctx_id, NULL );
  uint64_t      ht_size = map->hdr.ht_size, sz;
  void        * val;
  KeyStatus     status;
  for ( uint64_t pos = 0; pos < ht_size; pos++ ) {
    status = scan_kctx.fetch( &this->wrk, pos );
    if ( status == KEY_OK && scan_kctx.entry->test( FL_DROPPED ) == 0 ) {
      if ( scan_kctx.get_db() == this->kctx.db_num ) {
        status = scan_kctx.get_key( kp );
        if ( status == KEY_OK ) {
          bool    is_sys = false,
                  is_sys_wild = false;
          uint8_t prefixlen = 0;
          size_t  plen = sizeof( SYS_WILD_PREFIX ) - 1;
          if ( ::memcmp( kp->u.buf, "_SYS.", 5 ) == 0 ) {
            if ( ::memcmp( kp->u.buf, SYS_WILD_PREFIX, plen ) == 0 &&
                 kp->u.buf[ plen ] >= '0' && kp->u.buf[ plen ] <= '9' ) {
              is_sys_wild = true;
              prefixlen = kp->u.buf[ plen ] - '0';
              if ( kp->u.buf[ plen + 1 ] >= '0' &&
                   kp->u.buf[ plen + 1 ] <= '9' ) {
                plen += 1;
                prefixlen = prefixlen * 10 + ( kp->u.buf[ plen ] - '0' );
              }
              plen += 2; /* skip N. in _SYS.WN.prefix */
            }
            is_sys = true;
          }
          if ( ! is_sys || is_sys_wild ) {
            if ( (status = scan_kctx.value( &val, sz )) == KEY_OK &&
                 sz == sizeof( CubeRoute128 ) ) {
              cr.copy_from( val );
              cr.clear( this->ctx_id );
              if ( ! cr.is_empty() && kp->keylen > 0 ) {
              /*printf( "addkey: %.*s\r\n", kp->keylen, kp->u.buf );*/
                uint32_t hash = kv_crc_c( kp->u.buf, kp->keylen - 1, 0 );
                KvSubRoute * rt;
                rt = this->sub_tab.upsert( hash, kp->u.buf, kp->keylen - 1 );
                cr.copy_to( rt->rt_bits );
                if ( ! is_sys_wild )
                  this->poll.add_route( kp->u.buf, kp->keylen - 1, hash,
                                        this->fd );
                else
                  this->poll.add_pattern_route( &kp->u.buf[ plen ], prefixlen,
                                                hash, this->fd );
              }
            }
          }
        }
      }
    }
  }
  this->pop( EV_PROCESS );
}

void
KvPubSub::process_shutdown( void )
{
  if ( this->register_mcast( false ) )
    this->push( EV_WRITE );
  /*else if ( this->test( EV_WRITE ) == 0 )
    this->pushpop( EV_CLOSE, EV_SHUTDOWN );*/
}

void
KvPubSub::process_close( void )
{
  if ( use_dsunix ) {
    struct sockaddr_un un;
    make_dsunix_sockname( un, this->ctx_id );
    ::unlink( un.sun_path );
  }
}

bool
KvPubSub::get_mcast_route( CubeRoute128 &cr )
{
  size_t    sz;
  void    * val;
  KeyStatus status;

  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
    if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
         sz == sizeof( CubeRoute128 ) ) {
      cr.copy_from( val );
      cr.clear( this->ctx_id );
      return ! cr.is_empty();
    }
  }
  return false;
}

bool
KvPubSub::send_msg( KvMsg &msg )
{
  void * ptr;
  KeyStatus status;
  KvMsgQueue & ibx = *this->inbox[ msg.dest_start ];

  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
  if ( (status = this->kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
    status = this->kctx.append_msg( &ptr, msg.size );
    if ( status == KEY_OK )
      ::memcpy( ptr, &msg, msg.size );
    this->kctx.release();
  }
  return status == KEY_OK;
}

bool
KvPubSub::send_vec( size_t cnt,  void *vec,  uint64_t *siz,  size_t dest )
{
  KeyStatus status;
  KvMsgQueue & ibx = *this->inbox[ dest ];

  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
  if ( (status = this->kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
    status = this->kctx.append_vector( cnt, vec, siz );
    this->kctx.release();
  }
  return status == KEY_OK;
}

void
KvPubSub::write( void )
{
  CubeRoute128 mcast;
  /* if there are other contexts recving msgs */
  if ( this->get_mcast_route( mcast ) ) {
    static const uint8_t LAST_SIZE = 32;
    CubeRoute128 used, cr;
    size_t       mc_cnt = 0;
    range_t      mc_range;
    size_t       cnt = 0;
    KvSubRoute * rt;
    KvMsgList  * l;
    size_t       i, dest,
                 veccnt = 0;
    uint8_t      last_buf[ LAST_SIZE ];
    KvLast       last[ LAST_SIZE ];
    KvMsgList  * llast[ LAST_SIZE ];
    uint8_t      j = 0, k = 0;

    used.zero();
    for ( l = this->sendq.hd; l != NULL; l = l->next ) {
      KvMsg & msg   = l->msg;
      uint8_t start = msg.dest_start,
              end   = msg.dest_end;

      /*print_msg( msg );*/
      if ( start != end ) { /* if not to a single node */
        if ( is_kv_bcast( msg.msg_type ) ) { /* calculate the dest range */
          if ( start == 0 ) {
            if ( mc_cnt == 0 ) {
              mc_range.w = 0;
              mc_cnt = mcast.branch4( this->ctx_id, 0, MAX_CTX_ID, mc_range.b );
            }
            if ( mc_cnt > 0 )
              l->range.w = mc_range.w;
            cnt = mc_cnt;
          }
          else {
            cnt = mcast.branch4( this->ctx_id, start, end, l->range.b );
          }
        }
        else {
          KvSubMsg &submsg = (KvSubMsg &) msg;
          uint8_t h = (uint8_t) submsg.hash;
          /* cache the last routes, to avoid branch4 for the same subj */
          const uint8_t *p;
          cnt = 0;
          if ( k > 0 &&
               (p = (const uint8_t *) ::memchr( last_buf, h, k )) != NULL ) {
            i = p - last_buf;
            if ( last[ i ].equals( start, end, submsg, llast[ i ] ) ) {
              cnt = last[ i ].cnt;
              l->range.w = llast[ i ]->range.w;
            }
          }
          /* find the route for subject */
          if ( cnt == 0 ) {
            uint8_t pref_cnt = submsg.prefix_cnt();
            cr.zero();
            for ( uint8_t i = 0; i < pref_cnt; i++ ) {
              KvPrefHash & pf = submsg.prefix_hash( i );
              uint32_t h = pf.get_hash();
              if ( pf.pref == 64 ) {
                rt = this->sub_tab.find( h, submsg.subject(), submsg.sublen );
              }
              else {
                SysWildSub w( submsg.subject(), pf.pref );
                rt = this->sub_tab.find( h, w.sub, w.len );
              }
              if ( rt != NULL )
                cr.or_from( rt->rt_bits );
            }
            cnt = cr.branch4( this->ctx_id, start, end, l->range.b );
            last[ j ].set( start, end, cnt );
            llast[ j ] = l;
            j = ( j + 1 ) % LAST_SIZE;
            k = ( k < LAST_SIZE ? k + 1 : LAST_SIZE );
          }
        }
      }
      else if ( mcast.is_set( start ) ) { /* is single node */
        l->range.b[ 0 ] = start;
        l->range.b[ 1 ] = end;
        cnt = 2;
      }
      if ( cnt > 0 ) { /* set a bit for the destination  */
        for ( i = 0; i < cnt; i += 2 ) {
          if ( used.test_set( l->range.b[ i ] ) )
            veccnt++; /* can be a vector, multiple msgs to send */
        }
      }
    }
    if ( veccnt > 0 ) { /* if at least two msgs go to the same dest */
      if ( used.first_set( dest ) ) {
        size_t j = 0;
        uint64_t siz[ 256 ];
        void   * vec[ 256 ];
        do {
          for ( l = this->sendq.hd; l != NULL; l = l->next ) {
            for ( i = 0; i < 8; i += 2 )
              if ( l->range.b[ i ] == (uint8_t) dest )
                break;
            if ( i < 8 ) {
              KvMsg & msg = l->msg;
              msg.dest_start = l->range.b[ i ];
              msg.dest_end   = l->range.b[ i + 1 ];
              vec[ j ] = &msg;
              siz[ j ] = msg.size;
              if ( ++j == 256 ) {
                this->send_vec( 256, vec, siz, dest );
                j = 0;
              }
            }
          }
          if ( j > 0 ) {
            this->send_vec( j, vec, siz, dest );
            j = 0;
          }
        } while ( used.next_set( dest ) );
      }
    }
    else { /* no vectors, send each msg one at a time */
      for ( l = this->sendq.hd; l != NULL; l = l->next ) {
        for ( i = 0; i < 8; i += 2 ) {
          if ( l->range.b[ i ] == 0 )
            break;
          KvMsg & msg = l->msg;
          msg.dest_start = l->range.b[ i ];
          msg.dest_end   = l->range.b[ i + 1 ];
          this->send_msg( msg );
        }
      }
    }
    if ( ( this->flags & KV_DO_NOTIFY ) != 0 ) {
      /* notify each dest fd through poll() */
      if ( use_dsunix ) {
        if ( used.first_set( dest ) ) {
          do {
            struct sockaddr_un un;
            size_t len;
            uint8_t buf[ 1 ];
            buf[ 0 ] = (uint8_t) this->ctx_id;
            len = make_dsunix_sockname( un, dest );
            ::sendto( this->fd, buf, sizeof( buf ), 0, (struct sockaddr *) &un,
                      len );
          } while ( used.next_set( dest ) );
        }
      }
      else {
        if ( used.first_set( dest ) ) {
          do {
            uint32_t pid = this->poll.map->ctx[ dest ].ctx_pid;
            if ( pid > 0 )
              ::kill( pid, SIGUSR1 );
          } while ( used.next_set( dest ) );
        }
      }
    }
  }
  /* reset sendq, free mem */
  this->sendq.init();
  this->wrkq.reset();
  this->pop( EV_WRITE );
}

bool
KvPubSub::get_sub_mcast( const char *sub,  size_t len,  CubeRoute128 &cr )
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;

  if ( len + 1 > MAX_KEY_BUF_SIZE ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->wrkq.alloc( sz );
  }
  kb->keylen = len + 1;
  ::memcpy( kb->u.buf, sub, len );
  kb->u.buf[ len ] = '\0';
  hash1 = this->seed1;
  hash2 = this->seed2;
  kb->hash( hash1, hash2 );

  this->rt_kctx.set_key( *kb );
  this->rt_kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  for ( int cnt = 0; ; ) {
    if ( (status = this->rt_kctx.find( &this->rt_wrk )) == KEY_OK ) {
      if ( (status = this->rt_kctx.value( &val, sz )) == KEY_OK &&
           sz == sizeof( CubeRoute128 ) ) {
        cr.copy_from( val );
        return true;
      }
    }
    if ( status == KEY_NOT_FOUND )
      break;
    if ( ++cnt == 50 ) {
      fprintf( stderr, "error kv_pubsub mc lookup (%.*s): (%s) %s\n",
               (int) len, sub,
               kv_key_status_string( status ),
               kv_key_status_description( status ) );
      break;
    }
  }
  cr.zero();
  return false;
}

void
KvSubNotifyList::on_sub( KvSubMsg & )
{
}

void
KvPubSub::forward_sub( KvSubMsg &submsg )
{
  for ( KvSubNotifyList * l = this->sub_notifyq.hd; l != NULL; l = l->next )
    l->on_sub( submsg );
}

void
KvPubSub::route_msg_from_shm( KvMsg &msg ) /* inbound from shm */
{
  /*print_msg( msg );*/
  /* if msg destination has more hops */
  if ( msg.dest_start != msg.dest_end ) {
    KvMsgList * l = (KvMsgList *)
                    this->wrkq.alloc( sizeof( KvMsgList ) + msg.size );
    l->range.w = 0;
    ::memcpy( &l->msg, &msg, msg.size );
    this->sendq.push_tl( l );
    this->idle_push( EV_WRITE );
  }
  switch ( msg.msg_type ) {
    case KV_MSG_SUB: /* update my routing table when sub/unsub occurs */
    case KV_MSG_UNSUB: {
      KvSubMsg &submsg = (KvSubMsg &) msg;
      CubeRoute128 cr;

      this->get_sub_mcast( submsg.subject(), submsg.sublen, cr );
      cr.clear( this->ctx_id ); /* remove my subscriber id */
      /* if no more routes to shm exist, then remove */
      if ( cr.is_empty() ) {
        this->sub_tab.remove( submsg.hash, submsg.subject(), submsg.sublen );
        if ( this->sub_tab.find_by_hash( submsg.hash ) == NULL )
          this->poll.del_route( submsg.subject(), submsg.sublen, submsg.hash,
                                this->fd );
      }
      /* adding a route, publishes will be forwarded to shm */
      else {
        KvSubRoute * rt;
        rt = this->sub_tab.upsert( submsg.hash, submsg.subject(),
                                   submsg.sublen );
        cr.copy_to( rt->rt_bits );
        this->poll.add_route( submsg.subject(), submsg.sublen, submsg.hash,
                              this->fd );
      }
      if ( ! this->sub_notifyq.is_empty() )
        this->forward_sub( submsg );
      break;
    }
    case KV_MSG_PSUB:
    case KV_MSG_PUNSUB: {
      KvSubMsg &submsg = (KvSubMsg &) msg;
      if ( submsg.prefix_cnt() == 1 ) {
        KvPrefHash &pf = submsg.prefix_hash( 0 );
        CubeRoute128 cr;
        SysWildSub w( submsg.reply(), pf.pref );

        this->get_sub_mcast( w.sub, w.len, cr );
        cr.clear( this->ctx_id );
        /* if no more routes to shm exist, then remove */
        if ( cr.is_empty() ) {
          this->sub_tab.remove( submsg.hash, w.sub, w.len );
          if ( this->sub_tab.find_by_hash( submsg.hash ) == NULL )
            this->poll.del_pattern_route( submsg.reply(), pf.pref, submsg.hash,
                                          this->fd );
        }
        /* adding a route, publishes will be forwarded to shm */
        else {
          KvSubRoute * rt;
          rt = this->sub_tab.upsert( submsg.hash, w.sub, w.len );
          cr.copy_to( rt->rt_bits );
          this->poll.add_pattern_route( submsg.reply(), pf.pref, submsg.hash,
                                        this->fd );
        }
      }
      if ( ! this->sub_notifyq.is_empty() )
        this->forward_sub( submsg );
      break;
    }
    /* forward message from publisher to shm */
    case KV_MSG_PUBLISH: {
      KvSubMsg &submsg = (KvSubMsg &) msg;
      EvPublish pub( submsg.subject(), submsg.sublen,
                     submsg.reply(), submsg.replylen,
                     submsg.msg_data(), submsg.msg_size,
                     this->fd, submsg.hash, NULL, 0,
                     submsg.msg_enc, submsg.code );
      this->poll.forward_msg( pub, NULL, submsg.prefix_cnt(),
                              submsg.prefix_array() );
      break;
    }

    default: break; /* HELLO, BYE */
  }
}

void
KvPubSub::read( void )
{
  static const size_t veclen = 1024;
  void   * data[ veclen ];
  uint64_t data_sz[ veclen ];
  KvMsgQueue & ibx = *this->inbox[ this->ctx_id ];
  size_t count = 0;
  char buf[ 8 ];

  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  if ( use_dsunix ) {
    while ( ::recv( this->fd, buf, sizeof( buf ), 0 ) > 0 )
      ;
  }
  else {
    struct signalfd_siginfo fdsi;
    while ( ::read( this->fd, &fdsi, sizeof( fdsi ) ) > 0 )
      ;
  }
  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
  if ( this->kctx.find( &this->wrk ) == KEY_OK ) {
    for (;;) {
      uint64_t seqno  = ibx.ibx_seqno,
               seqno2 = seqno + veclen;
      if ( this->kctx.msg_value( seqno, seqno2, data, data_sz ) != KEY_OK )
        break;
      ibx.ibx_seqno = seqno2;
      seqno2 -= seqno;
      for ( uint64_t i = 0; i < seqno2; i++ ) {
        if ( data_sz[ i ] >= sizeof( KvMsg ) ) {
          KvMsg &msg = *(KvMsg *) data[ i ];
          /* check these, make sure messages are in order ?? */
          this->inbox[ msg.src ]->src_session_id = msg.session_id;
          this->inbox[ msg.src ]->src_seqno      = msg.seqno;
          this->route_msg_from_shm( msg );
        }
      }
      count += seqno2;
      if ( seqno2 < veclen )
        break;
    }
  }
  /* remove msgs consumed */
  if ( count > 0 ) {
    if ( this->kctx.acquire( &this->wrk ) <= KEY_IS_NEW ) {
      this->kctx.trim_msg( ibx.ibx_seqno );
      this->kctx.release();
    }
  }
}

bool
KvPubSub::busy_poll( uint64_t time_ns )
{
  static const size_t veclen = 1024;
  void   * data[ veclen ];
  uint64_t data_sz[ veclen ];
  KvMsgQueue & ibx = *this->inbox[ this->ctx_id ];
  size_t count = 0;
  uint64_t i, seqno, seqno2;

  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
eat_more_time:;
  if ( this->kctx.find( &this->wrk ) == KEY_OK ) {
    for (;;) {
      seqno  = ibx.ibx_seqno,
      seqno2 = seqno + veclen;
      if ( this->kctx.msg_value( seqno, seqno2, data, data_sz ) != KEY_OK )
        break;
      ibx.ibx_seqno = seqno2;
      seqno2 -= seqno;
      for ( i = 0; i < seqno2; i++ ) {
        if ( data_sz[ i ] >= sizeof( KvMsg ) ) {
          KvMsg &msg = *(KvMsg *) data[ i ];
          /* check these, make sure messages are in order ?? */
          this->inbox[ msg.src ]->src_session_id = msg.session_id;
          this->inbox[ msg.src ]->src_seqno      = msg.seqno;
          this->route_msg_from_shm( msg );
        }
      }
      count += seqno2;
      if ( seqno2 < veclen )
        break;
    }
  }
  /* remove msgs consumed */
  if ( count == 0 && time_ns > 0 ) {
    if ( time_ns > 150 ) {
      time_ns -= 150;
      goto eat_more_time;
    }
  }
  else {
    if ( this->kctx.acquire( &this->wrk ) <= KEY_IS_NEW ) {
      this->kctx.trim_msg( ibx.ibx_seqno );
      this->kctx.release();
    }
    return true;
  }
  return false;
}

bool
KvPubSub::on_msg( EvPublish &pub )
{
  /* no publish to self */
  if ( (uint32_t) this->fd != pub.src_route ) {
    this->create_kvpublish( pub.subj_hash, pub.subject, pub.subject_len,
                            pub.prefix, pub.hash, pub.prefix_cnt,
                            (const char *) pub.reply, pub.reply_len, pub.msg,
                            pub.msg_len, 'K', KV_MSG_PUBLISH,
                            pub.pub_type, pub.msg_enc );
    this->idle_push( EV_WRITE );
    /* send backpressure TODO */
  }
  return true;
}

bool
KvPubSub::hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  KvSubRoute * rt;
  if ( (rt = this->sub_tab.find_by_hash( h )) != NULL /*||
     (rt = this->sub_tab.find_by_hash( h | UIntHashTab::SLOT_USED )) != NULL*/){
    ::memcpy( key, rt->value, rt->len );
    keylen = rt->len;
    return true;
  }
  return false;
}

