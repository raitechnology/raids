#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <raids/ev_publish.h>
#include <raids/kv_pubsub.h>
#include <raids/cube_route.h>
#include <raids/redis_msg.h>

using namespace rai;
using namespace kv;
using namespace ds;

static const uint16_t KV_CTX_BYTES = KV_MAX_CTX_ID / 8;
#if __cplusplus > 201103L
  static_assert( KV_MAX_CTX_ID == sizeof( CubeRoute128 ) * 8, "cube route sz" );
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

  if ( (fd = socket( AF_UNIX, SOCK_DGRAM, 0 )) < 0 )
    return NULL;
  len = make_dsunix_sockname( un, poll.ctx_id );
  ::unlink( un.sun_path );
  if ( ::bind( fd, (struct sockaddr *) &un, len ) < 0 ) {
    ::close( fd );
    return NULL;
  }
  ::fcntl( fd, F_SETFL, O_NONBLOCK | ::fcntl( fd, F_GETFL ) );

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
        ::memcpy( cr.w, val, sizeof( CubeRoute128 ) );
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
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW );
    if ( (status = this->kctx.resize( &val, KV_CTX_BYTES, true )) == KEY_OK ) {
      CubeRoute128 &cr = *(CubeRoute128 *) val;
      if ( is_new )
        cr.zero();
      if ( activate ) {
        cr.set( this->ctx_id );
        res = true;
      }
      else if ( cr.is_set( this->ctx_id ) ) {
        cr.clear( this->ctx_id );
        res = true;
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
  KvMsgList * l = (KvMsgList *) this->wrkq.alloc( sizeof( KvMsgList ) + sz );
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
KvPubSub::create_kvsubmsg( uint32_t h,  const char *sub,  size_t len,
                           const char *reply,  size_t rlen,
                           const void *msgdata,  size_t msgsz,
                           uint32_t sub_id,  char src_type,  KvMsgType mtype )
{
  KvSubMsg * msg;
  char     * ptr;
  size_t     sz = KvSubMsg::calc_size( len, rlen, msgsz );
  msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  msg->hash     = h;
  msg->sub_id   = sub_id;
  msg->msg_size = msgsz;
  msg->sublen   = len;
  msg->replylen = rlen;
  ptr = msg->subject();
  ::memcpy( ptr, sub, len );
  ptr[ len ] = '\0';
  ptr = msg->reply();
  if ( rlen > 0 )
    ::memcpy( ptr, reply, rlen );
  ptr[ rlen ] = '\0';
  msg->src_type() = src_type;
  if ( msgsz > 0 )
    ::memcpy( msg->msg_data(), msgdata, msgsz );
  return msg;
}

void
KvPubSub::notify_sub( uint32_t h,  const char *sub,  size_t len,
                      uint32_t sub_id,  uint32_t rcnt,  char src_type )
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

  this->create_kvsubmsg( h, sub, len, NULL, 0, NULL, 0, sub_id, src_type,
                         KV_MSG_SUB );
  printf( "subscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type );
  this->idle_push( EV_WRITE );
}

void
KvPubSub::notify_unsub( uint32_t h,  const char *sub,  size_t len,
                        uint32_t sub_id,  uint32_t rcnt,  char src_type )
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

  this->create_kvsubmsg( h, sub, len, NULL, 0, NULL, 0, sub_id, src_type,
                         KV_MSG_UNSUB );
  printf( "unsubscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type );
  this->idle_push( EV_WRITE );
}

void
KvPubSub::process( bool )
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
          if ( kp->keylen < 5 || kp->u.buf[ 0 ] != '_' ||
               ::memcmp( kp->u.buf, "_SYS.", 5 ) != 0 ) {
            if ( (status = scan_kctx.value( &val, sz )) == KEY_OK &&
                 sz == sizeof( CubeRoute128 ) ) {
              ::memcpy( cr.w, val, sizeof( CubeRoute128 ) );
              if ( ! cr.is_empty() && kp->keylen > 0 ) {
                /*printf( "addkey: %.*s\r\n", kp->keylen, kp->u.buf );*/
                uint32_t hash = kv_crc_c( kp->u.buf, kp->keylen - 1, 0 );
                this->poll.sub_route.add_route( hash, this->fd );
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
  else if ( this->test( EV_WRITE ) == 0 )
    this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
KvPubSub::process_close( void )
{
  struct sockaddr_un un;
  make_dsunix_sockname( un, this->ctx_id );
  ::unlink( un.sun_path );
  this->poll.remove_sock( this );
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
      ::memcpy( cr.w, val, sizeof( CubeRoute128 ) );
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
    CubeRoute128 used, cr;
    size_t       mc_cnt = 0;
    range_t      mc_range;
    size_t       cnt = 0;
    KvMsgList  * l;
    size_t       i, dest,
                 veccnt = 0;
    used.zero();
    for ( l = this->sendq.hd; l != NULL; l = l->next ) {
      KvMsg & msg = l->msg;
      uint8_t start = msg.dest_start;

      if ( start == 0 ) { /* is mcast to all nodes */
        if ( is_kv_bcast( msg.msg_type ) ) {
          if ( mc_cnt == 0 ) {
            mc_range.w = 0;
            mc_cnt = mcast.branch4( this->ctx_id, 0, MAX_CTX_ID, mc_range.b );
          }
          if ( mc_cnt > 0 )
            l->range.w = mc_range.w;
          cnt = mc_cnt;
        }
        else {
          KvSubMsg &submsg = (KvSubMsg &) msg;
          if ( this->get_sub_mcast( submsg.subject(), submsg.sublen, cr ) )
            cnt = cr.branch4( this->ctx_id, 0, MAX_CTX_ID, l->range.b );
          else
            cnt = 0;
        }
      }
      else if ( start != msg.dest_end ) { /* if not to a single node */
        if ( is_kv_bcast( msg.msg_type ) ) {
          /* calculate the dest range */
          cnt = mcast.branch4( this->ctx_id, start, msg.dest_end, l->range.b );
        }
        else {
          KvSubMsg &submsg = (KvSubMsg &) msg;
          if ( this->get_sub_mcast( submsg.subject(), submsg.sublen, cr ) )
            cnt = cr.branch4( this->ctx_id, start, msg.dest_end, l->range.b );
          else
            cnt = 0;
        }
      }
      else if ( mcast.is_set( start ) ) { /* is single node */
        l->range.b[ 0 ] = start;
        l->range.b[ 1 ] = msg.dest_end;
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
    /* notify each dest fd through poll() */
    if ( used.first_set( dest ) ) {
      do {
        struct sockaddr_un un;
        size_t len;
        uint8_t buf[ 1 ];
        buf[ 0 ] = (uint8_t) this->ctx_id;
        len = make_dsunix_sockname( un, dest );
        sendto( this->fd, buf, sizeof( buf ), 0, (struct sockaddr *) &un, len );
      } while ( used.next_set( dest ) );
    }
  }
  /* reset sendq, free mem */
  this->sendq.init();
  this->wrkq.reset();
  this->pop( EV_WRITE );
}

static const char *
msg_type_string( KvMsgType msg_type )
{
  switch ( msg_type ) {
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

static void 
print_msg( KvMsg &msg )
{
  printf( "\r\nsession_id : %lx\r\n"
          "seqno      : %lu\r\n"
          "size       : %u\r\n"
          "src        : %u\r\n"
          "dest_start : %u\r\n"
          "dest_end   : %u\r\n"
          "msg_type   : %s\r\n",
    msg.session_id, msg.seqno, msg.size, msg.src, msg.dest_start,
    msg.dest_end, msg_type_string( (KvMsgType) msg.msg_type ) );

  if ( msg.msg_type >= KV_MSG_SUB && msg.msg_type <= KV_MSG_PUBLISH ) {
    KvSubMsg &sub = (KvSubMsg &) msg;
    printf( "hash       : %x\r\n"
            "sub_id     : %x\r\n"
            "msg_size   : %u\r\n"
            "sublen     : %u\r\n"
            "replylen   : %u\r\n"
            "subject()  : %s\r\n",
      sub.hash, sub.sub_id, sub.msg_size, sub.sublen, sub.replylen,
      sub.subject() );
    if ( msg.msg_type == KV_MSG_PUBLISH ) {
      printf( "msg_data() : %.*s\r\n", sub.msg_size, (char *) sub.msg_data() );
    }
  }
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
  this->kctx.set_key( *kb );
  this->kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
    if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
         sz == sizeof( CubeRoute128 ) ) {
      ::memcpy( cr.w, val, sizeof( CubeRoute128 ) );
      return true;
    }
  }
  cr.zero();
  return false;
}

void
KvPubSub::route_msg( KvMsg &msg )
{
  print_msg( msg );
  /* if msg destination has more hops */
  if ( msg.dest_start != msg.dest_end ) {
    KvMsgList * l = (KvMsgList *)
                    this->wrkq.alloc( sizeof( KvMsgList ) + msg.size );
    l->range.w = 0;
    ::memcpy( &l->msg, &msg, msg.size );
    this->sendq.push_tl( l );
    this->idle_push( EV_WRITE );
  }
  /* update my routing table when sub/unsub occurs */
  if ( msg.msg_type == KV_MSG_SUB ||
       msg.msg_type == KV_MSG_PSUB ||
       msg.msg_type == KV_MSG_UNSUB ||
       msg.msg_type == KV_MSG_PUNSUB ) {
    KvSubMsg &submsg = (KvSubMsg &) msg;
    CubeRoute128 cr;
    if ( msg.msg_type == KV_MSG_SUB || msg.msg_type == KV_MSG_PSUB )
      /* adding a route, publishes will be forwarded to shm */
      this->poll.sub_route.add_route( submsg.hash, this->fd );
    else {
      /* check if no more routes to shm exist, if true then remove */
      bool rm = false;
      if ( ! this->get_sub_mcast( submsg.subject(), submsg.sublen, cr ) )
        rm = true;
      else {
        cr.clear( this->ctx_id );
        rm = cr.is_empty();
      }
      if ( rm )
        this->poll.sub_route.del_route( submsg.hash, this->fd );
    }
  }
  /* forward message from publisher to shm */
  else if ( msg.msg_type == KV_MSG_PUBLISH ) {
    KvSubMsg &submsg = (KvSubMsg &) msg;
    uint32_t * routes, rcnt;
    rcnt = this->poll.sub_route.get_route( submsg.hash, routes );
    printf( "get_route rcnt %u\r\n", rcnt );
    if ( rcnt > 0 ) {
      char   msg_len_buf[ 24 ];
      size_t msg_len_digits = RedisMsg::uint_digits( submsg.msg_size );
      RedisMsg::uint_to_str( submsg.msg_size, msg_len_buf, msg_len_digits );
      EvPublish pub( submsg.subject(), submsg.sublen,
                     submsg.reply(), submsg.replylen,
                     submsg.msg_data(), submsg.msg_size,
                     routes, rcnt, this->fd, submsg.hash,
                     msg_len_buf, msg_len_digits );
      this->poll.sub_route.rte.publish( pub );
    }
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
  while ( recv( this->fd, buf, sizeof( buf ), 0 ) > 0 )
    ;
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
          /* check these, make sure messages are in order */
          this->inbox[ msg.src ]->src_session_id = msg.session_id;
          this->inbox[ msg.src ]->src_seqno      = msg.seqno;
          this->route_msg( msg );
        }
      }
      count += seqno2;
      if ( seqno2 < veclen )
        break;
    }
  }
  if ( count > 0 ) {
    if ( this->kctx.acquire( &this->wrk ) <= KEY_IS_NEW ) {
      this->kctx.trim_msg( ibx.ibx_seqno );
      this->kctx.release();
    }
  }
}

bool
KvPubSub::publish( EvPublish &pub )
{
  /* no publish to self */
  if ( (uint32_t) this->fd != pub.src_route ) {
    this->create_kvsubmsg( pub.subj_hash, pub.subject, pub.subject_len,
                           (const char *) pub.reply, pub.reply_len,
                           pub.msg, pub.msg_len, pub.src_route, 'K',
                           KV_MSG_PUBLISH );
    this->idle_push( EV_WRITE );
    /* send backpressure TODO */
  }
  return true;
}

bool
KvPubSub::hash_to_sub( uint32_t ,  char *,  size_t & )
{
  printf( "hash to sub\n" );
  return true;
}

