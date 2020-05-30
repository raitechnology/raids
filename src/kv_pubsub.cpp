#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
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

/* signal other processes that a message available */
static const int kv_msg_signal = SIGUSR2;
static const uint64_t kv_timer_ival_ms  = 250; /* 4 times a second */
                          /* 20000 per sec / ivals per sec = rate / ival
                           *  if msg rate is above 1 per 50 usecs */
static const uint64_t kv_busy_loop_rate = 20000 / ( 1000 / kv_timer_ival_ms );

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
                    sys_ibx[] = "_SYS.";
static const size_t mc_name_size  = sizeof( sys_mc ),
                    ibx_name_size = sizeof( sys_ibx ) + 2;
/* 8 byte inbox size is the limit for msg list with immediate key */

static void
make_ibx( char *ibname,  uint16_t ctx_id )
{
  ::strcpy( ibname, sys_ibx );
  ibname[ ibx_name_size - 3 ] = hdigit( ( ctx_id >> 4 ) & 0xf );
  ibname[ ibx_name_size - 2 ] = hdigit( ctx_id & 0xf );
  ibname[ ibx_name_size - 1 ] = '\0';
}
#if 0
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
#endif
const char *
KvMsg::msg_type_string( uint8_t msg_type ) noexcept
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
KvMsg::msg_type_string( void ) const noexcept
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
    fflush( stdout );
    hex.flush_line();
  }
}

void
KvMsg::print( void ) noexcept
{
  printf( "\r\nsession_id : %lx\r\n"
          "seqno      : %lu\r\n"
          "size       : %u\r\n"
          "src        : %u\r\n"
          "dest_start : %u\r\n"
          "dest_end   : %u\r\n"
          "msg_type   : %s\r\n",
    this->session_id(), this->seqno(), this->size, this->src, this->dest_start,
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

void
KvMsg::print_sub( void ) noexcept
{
  if ( this->msg_type >= KV_MSG_SUB && this->msg_type <= KV_MSG_PUBLISH ) {
    KvSubMsg &sub = (KvSubMsg &) *this;
    printf( "ctx(%u) %s %s\n", this->src,
                               msg_type_string( (KvMsgType) this->msg_type ),
                               sub.subject() );
  }
}

KvPubSub *
KvPubSub::create( EvPoll &poll,  uint8_t db_num ) noexcept
{
  KvPubSub * ps;
  void     * p,
           * ibptr,
           * mcptr;
  size_t     i, kvsz, mcsz, ibsz, n;
  char       ibname[ 12 ];
  sigset_t   mask;
  int        fd;
  uint32_t   dbx_id;

  dbx_id = poll.map->attach_db( poll.ctx_id, db_num );
  if ( dbx_id == MAX_STAT_ID )
    return NULL;

  sigemptyset( &mask );
  sigaddset( &mask, kv_msg_signal );

  if ( sigprocmask( SIG_BLOCK, &mask, NULL ) == -1 ) {
    perror("sigprocmask");
    return NULL;
  }
  fd = signalfd( -1, &mask, SFD_NONBLOCK );
  if ( fd == -1 ) {
    perror( "signalfd" );
    return NULL;
  }
  kvsz = align<size_t>( sizeof( KvPubSub ), 64 );
  mcsz = align<size_t>( sizeof( KvMcastKey ) + 8, 64 );
  ibsz = align<size_t>( sizeof( KvMsgQueue ) + 8, 64 );
  n    = kvsz + mcsz + ibsz * MAX_CTX_ID;
  if ( (p = aligned_malloc( n )) == NULL )
    return NULL;
  mcptr = (void *) &((uint8_t *) p)[ kvsz ];
  ibptr = (void *) &((uint8_t *) mcptr)[ mcsz ];
  ps = new ( p ) KvPubSub( poll, fd, mcptr, sys_mc, mc_name_size, dbx_id );
  /* for each ctx_id create queue */
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    make_ibx( ibname, i );
    ps->inbox[ i ] =
      new ( ibptr ) KvMsgQueue( ps->kctx, ibname, ibx_name_size, i );
    ibptr = &((uint8_t *) ibptr)[ ibsz ];
  }
  if ( ! ps->register_mcast() || poll.add_sock( ps ) < 0 ) {
    ::close( fd );
    return NULL;
  }
  ps->idle_push( EV_PROCESS );
  ps->push( EV_WRITE );
  return ps;
}

void
KvPubSub::print_backlog( void ) noexcept
{
  size_t i;
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    KvMsgQueue & ibx = *this->inbox[ i ];
    if ( ( ibx.pub_size | ibx.read_size ) != 0 ) {
      printf( "[ %lu ] psize=%lu pcnt=%lu mcnt=%lu rsize=%lu "
              "rcnt=%lu rmsg=%lu bsize=%lu bcnt=%lu sig=%lu high=%lu\n", i,
              ibx.pub_size, ibx.pub_cnt, ibx.pub_msg,
              ibx.read_size, ibx.read_cnt, ibx.read_msg,
              ibx.backlog_size, ibx.backlog_cnt,
              ibx.signal_cnt, ibx.high_water_size );
    }
  }
}

bool
KvPubSub::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  if ( this->timer_id != tid )
    return false;
  /*printf( "timer %lu\n", this->timer_cnt );*/
  this->timer_cnt++;
  if ( ( this->poll.map->ctx[ this->ctx_id ].ctx_flags & KV_NO_SIGUSR ) != 0 ) {
    if ( this->inbox_msg_cnt < kv_busy_loop_rate )
      this->poll.map->ctx[ this->ctx_id ].ctx_flags &= ~KV_NO_SIGUSR;
  }
  else {
    if ( this->test( EV_BUSY_POLL ) ) {
      if ( this->inbox_msg_cnt < kv_busy_loop_rate ) {
        this->pop( EV_BUSY_POLL );
        this->idle_push( EV_READ_LO );
      }
      else
        this->poll.map->ctx[ this->ctx_id ].ctx_flags |= KV_NO_SIGUSR;
    }
  }
  this->sigusr_recv_cnt = 0;
  this->inbox_msg_cnt = 0;
  if ( this->test( EV_BUSY_POLL | EV_READ_LO ) == 0 )
    this->idle_push( EV_READ_LO );
  return true;
}

bool
KvPubSub::register_mcast( void ) noexcept
{
  void    * val;
  KeyStatus status;
  bool      res = false;

  this->dead_cr.zero();
  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  /* add my ctx_id to the mcast key, which is the set of all ctx_ids */
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW );
    if ( (status = this->kctx.resize( &val, KV_CTX_BYTES, true )) == KEY_OK ) {
      CubeRoute128 &cr = *(CubeRoute128 *) val;
      if ( is_new )
        cr.zero();
      else {
        for ( uint32_t id = 1; id < MAX_CTX_ID; id++ ) {
          if ( this->ctx_id == id )
            continue;
          /* check that this route is valid by pinging the pid */
          if ( cr.is_set( id ) ) {
            uint32_t pid = this->kctx.ht.ctx[ id ].ctx_pid;
            if ( pid == 0 ||
                 this->kctx.ht.ctx[ id ].ctx_id == KV_NO_CTX_ID ||
                 ::kill( pid, 0 ) != 0 ) {
              this->dead_cr.set( id );
              fprintf( stderr, "ctx %u pid %u is dead\n", id, pid );
            }
          }
        }
      }
      cr.set( this->ctx_id );
      this->create_kvmsg( KV_MSG_HELLO, sizeof( KvMsg ) );
      res = true;
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to register mcast, kv status %d\n",
             (int) status );
  }
  return res;
}

bool
KvPubSub::clear_mcast_dead_routes( void ) noexcept
{
  void    * val;
  uint64_t  sz;
  KeyStatus status;
  bool      res = false;

  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW ); /* shouldn't be new */
    if ( ! is_new ) {
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        CubeRoute128 &cr = *(CubeRoute128 *) val;
        cr.not_bits( this->dead_cr );
        res = true;
      }
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to clear mcast dead routes, kv status %d\n",
             (int) status );
  }
  else {
    size_t i;
    if ( this->dead_cr.first_set( i ) ) {
      do {
        KvMsgQueue & ibx = *this->inbox[ i ];
        this->kctx.set_key( ibx.kbuf );
        this->kctx.set_hash( ibx.hash1, ibx.hash2 );
        if ( (status = this->kctx.acquire( &this->wrk )) == KEY_OK ) {
          fprintf( stderr, "drop kv inbox %lu\n", i );
          this->kctx.tombstone();
        }
        this->kctx.release();
      } while ( this->dead_cr.next_set( i ) );
      this->dead_cr.zero();
    }
  }
  return res;
}

bool
KvPubSub::unregister_mcast( void ) noexcept
{
  void    * val;
  uint64_t  sz;
  KeyStatus status;
  bool      res = false;

  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW ); /* shouldn't be new */
    if ( ! is_new ) {
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        CubeRoute128 &cr = *(CubeRoute128 *) val;
        if ( cr.is_set( this->ctx_id ) ) {
          cr.clear( this->ctx_id );
          this->kctx.next_serial( ValueCtr::SERIAL_MASK );
          /*printf( "mcast clear %u\n", this->ctx_id );*/
          this->create_kvmsg( KV_MSG_BYE, sizeof( KvMsg ) );
        }
        res = true;
      }
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to unregister mcast, kv status %d\n",
             (int) status );
  }
  return res;
}

bool
KvPubSub::update_mcast_sub( const char *sub,  size_t len,  int flags ) noexcept
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;
  bool          res = false;

  if ( kbuf.copy( sub, len + 1 ) != len + 1 ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->wrkq.alloc( sz );
    ::memcpy( kb->u.buf, sub, len );
  }
  kb->u.buf[ len ] = '\0';
  this->hs.hash( *kb, hash1, hash2 );
  this->kctx.set_key( *kb );
  this->kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  if ( ( flags & USE_FIND ) != 0 ) {
    if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
      if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
           sz == sizeof( CubeRoute128 ) ) {
        CubeRoute128 cr;
        cr.copy_from( val );
        if ( ( flags & ACTIVATE ) != 0 ) {
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
      if ( ( flags & ACTIVATE ) != 0 ) {
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
        if ( ( flags & ACTIVATE ) != 0 ) {
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
  if ( ! res && ( flags & ACTIVATE ) != 0 ) {
    fprintf( stderr, "Unable to register subject %.*s mcast, kv status %d\n",
             (int) len, sub, (int) status );
  }
  return res;
}

KvMsg *
KvPubSub::create_kvmsg( KvMsgType mtype,  size_t sz ) noexcept
{
  KvMsgList * l = (KvMsgList *)
         this->wrkq.alloc( align<size_t>( sizeof( KvMsgList ) + sz, 8 ) );
  KvMsg   & msg = l->msg;

  l->init_route();
  msg.set_session_id( this->session_id );
  msg.set_seqno( this->next_seqno++ );
  msg.size       = sz;
  msg.src        = this->ctx_id;
  msg.dest_start = 0;
  msg.dest_end   = KV_MAX_CTX_ID;
  msg.msg_type   = mtype;
  this->sendq.push_tl( l );
  this->send_size += sz;
  this->send_cnt  += 1;
  return &msg;
}

KvSubMsg *
KvPubSub::create_kvpublish( uint32_t h,  const char *sub,  size_t len,
                            const uint8_t *pref,  const uint32_t *hash,
                            uint8_t pref_cnt,  const char *reply,  size_t rlen,
                            const void *msgdata,  size_t msgsz,
                            char src_type,  KvMsgType mtype,
                            uint8_t code,  uint8_t msg_enc ) noexcept
{
  KvSubMsg * msg;
  size_t     sz = KvSubMsg::calc_size( len, rlen, msgsz, pref_cnt );
  msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  msg->hash    = h;
  msg->code    = code;
  msg->msg_enc = msg_enc;
  msg->set_subject( sub, len );
  msg->set_reply( reply, rlen );
  msg->set_src_type( src_type );
  msg->set_prefix_cnt( pref_cnt );
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
                           size_t rlen ) noexcept
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
  msg->set_src_type( src_type );
  msg->set_prefix_cnt( 0 );
  return msg;
}

KvSubMsg *
KvPubSub::create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t len,
                            const char *prefix,  uint8_t prefix_len,
                            char src_type,  KvMsgType mtype ) noexcept
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
  msg->set_src_type( src_type );
  msg->set_prefix_cnt( 1 );
  KvPrefHash & ph = msg->prefix_hash( 0 );
  ph.pref = prefix_len;
  ph.set_hash( h );
  return msg;
}

void
KvPubSub::do_sub( uint32_t h,  const char *sub,  size_t len,
                  uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type,
                  const char *rep,  size_t rlen ) noexcept
{
  int use_find = USE_FIND;
  if ( rcnt == 1 ) /* first route added */
    use_find = 0;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = 0;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  this->update_mcast_sub( sub, len, use_find | ACTIVATE );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, len, src_type, KV_MSG_SUB, rep, rlen );
/*printf( "subscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type );*/
  this->idle_push( EV_WRITE );
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::do_unsub( uint32_t h,  const char *sub,  size_t len,
                    uint32_t,  uint32_t rcnt,  char src_type ) noexcept
{
  bool do_unsubscribe = false;
  if ( rcnt == 0 ) /* no more routes left */
    do_unsubscribe = true;
  else if ( rcnt == 1 ) { /* if the only route left is not in my server */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      do_unsubscribe = true;
  }
  if ( do_unsubscribe )
    this->update_mcast_sub( sub, len, DEACTIVATE );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, len, src_type, KV_MSG_UNSUB, NULL, 0 );
/*printf( "unsubscribe %x %.*s\n", h, (int) len, sub );*/
  this->idle_push( EV_WRITE );
  if ( ! this->sub_notifyq.is_empty() )
    this->forward_sub( *submsg );
}

void
KvPubSub::do_psub( uint32_t h,  const char *pattern,  size_t len,
                   const char *prefix,  uint8_t prefix_len,
                   uint32_t,  uint32_t rcnt,  char src_type ) noexcept
{
  int use_find = USE_FIND;
  if ( rcnt == 1 ) /* first route added */
    use_find = 0;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = 0;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  SysWildSub w( prefix, prefix_len );
  this->update_mcast_sub( w.sub, w.len, use_find | ACTIVATE );

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
                     uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type ) noexcept
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
    this->update_mcast_sub( w.sub, w.len, DEACTIVATE );
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
KvPubSub::process( void ) noexcept
{
  if ( ( this->flags & KV_INITIAL_SCAN ) == 0 ) {
    this->flags |= KV_INITIAL_SCAN;
    this->scan_ht();
    this->poll.add_timer_millis( fd, kv_timer_ival_ms, this->timer_id, 0 );
  }
  this->pop( EV_PROCESS );
}

void
KvPubSub::scan_ht( void ) noexcept
{
  CubeRoute128  cr;
  HashTab     * map = this->poll.map;
  KeyFragment * kp;
  KeyCtx        scan_kctx( *map, this->dbx_id, NULL );
  uint64_t      ht_size = map->hdr.ht_size, sz;
  void        * val;
  KeyStatus     status;
  bool          have_dead_routes = ! this->dead_cr.is_empty();

  for ( uint64_t pos = 0; pos < ht_size; pos++ ) {
    status = scan_kctx.fetch( &this->wrk, pos );
    if ( status == KEY_OK && scan_kctx.entry->test( FL_DROPPED ) == 0 ) {
      if ( scan_kctx.get_db() == this->kctx.db_num ) {
        status = scan_kctx.get_key( kp );
        if ( status == KEY_OK ) {
          bool    is_sys      = false,
                  is_sys_wild = false;
          uint8_t prefixlen   = 0;
          size_t  plen        = sizeof( SYS_WILD_PREFIX ) - 1;
          const char * key    = (const char *) kp->u.buf;
          /* is it a wildcard? */
          if ( ::memcmp( key, "_SYS.", 5 ) == 0 ) {
            if ( ::memcmp( key, SYS_WILD_PREFIX, plen ) == 0 &&
                 key[ plen ] >= '0' && key[ plen ] <= '9' ) {
              is_sys_wild = true;
              prefixlen = key[ plen ] - '0';
              if ( key[ plen + 1 ] >= '0' && key[ plen + 1 ] <= '9' ) {
                plen += 1;
                prefixlen = prefixlen * 10 + ( key[ plen ] - '0' );
              }
              plen += 2; /* skip N. in _SYS.WN.prefix */
            }
            is_sys = true;
          }
          /* if not an inbox or mcast, absorb the routes */
          if ( ! is_sys || is_sys_wild ) {
            if ( (status = scan_kctx.value( &val, sz )) == KEY_OK &&
                 sz == sizeof( CubeRoute128 ) ) {
              cr.copy_from( val );
              if ( have_dead_routes ) {
                /* if some routes are bad, fix them */
                if ( cr.test_bits( this->dead_cr ) ) {
                  printf( "fixkey: %.*s\n", kp->keylen, key );
                  for (;;) {
                    status = scan_kctx.try_acquire_position( pos );
                    if ( status != KEY_BUSY )
                      break;
                  }
                  /* copy and del if empty */
                  if ( status == KEY_OK ) { /* could be dropped already */
                    status = scan_kctx.resize( &val, KV_CTX_BYTES );
                    if ( status == KEY_OK ) {
                      CubeRoute128 &cr2 = *(CubeRoute128 *) val;
                      cr2.not_bits( this->dead_cr );
                      cr2.clear( this->ctx_id );
                      cr.copy_from( val );
                      if ( cr.is_empty() ) {
                        printf( "emptykey: %.*s\n", kp->keylen, key );
                        scan_kctx.tombstone();
                      }
                    }
                  }
                  else {
                    cr.zero();
                  }
                  scan_kctx.release();
                }
              }
              cr.clear( this->ctx_id );
              if ( ! cr.is_empty() && kp->keylen > 0 ) {
              /*printf( "addkey: %.*s\r\n", kp->keylen, kp->u.buf );*/
                uint32_t hash = kv_crc_c( key, kp->keylen - 1, 0 );
                KvSubRoute * rt;
                rt = this->sub_tab.upsert( hash, key, kp->keylen - 1 );
                cr.copy_to( rt->rt_bits );
                if ( ! is_sys_wild )
                  this->poll.add_route( key, kp->keylen - 1, hash,
                                        this->fd );
                else
                  this->poll.add_pattern_route( &key[ plen ], prefixlen,
                                                hash, this->fd );
              }
            }
          }
        }
      }
    }
  }
  if ( have_dead_routes ) {
    this->clear_mcast_dead_routes();
  }
}

void
KvPubSub::process_shutdown( void ) noexcept
{
  if ( this->unregister_mcast() )
    this->push( EV_WRITE );
  /*else if ( this->test( EV_WRITE ) == 0 )
    this->pushpop( EV_CLOSE, EV_SHUTDOWN );*/
}

void
KvPubSub::process_close( void ) noexcept
{
}
/* this refreshes the routing bits for all ctx_ids in the system */
bool
KvPubSub::update_mcast_route( void ) noexcept
{
  size_t    sz;
  void    * val;
  KeyStatus status;

  if ( this->mc_pos != 0 ) {
    if ( this->kctx.if_value_equals( this->mc_pos, this->mc_value_ctr ) )
      return ! this->mc_cr.is_empty();
  }
  /* push read, in case unsubs are in the inbox */
  this->idle_push( EV_READ_LO );
  /*printf( "mcast changed\n" );*/
  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
    if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
         sz == sizeof( CubeRoute128 ) ) {
      this->mc_cr.copy_from( val );
      this->mc_cr.clear( this->ctx_id );
      this->kctx.get_pos_value_ctr( this->mc_pos, this->mc_value_ctr );
      this->mc_cnt = this->mc_cr.branch4( this->ctx_id, 0, MAX_CTX_ID,
                                          this->mc_range.b );
      return ! this->mc_cr.is_empty();
    }
  }
  this->mc_cr.zero();
  this->mc_cnt = 0;
  this->mc_range.w = 0;
  return false;
}

bool
KvPubSub::push_backlog( KvMsgQueue &ibx,  size_t cnt,  void **vec,
                        msg_size_t *siz,  uint64_t vec_size ) noexcept
{
  uint8_t    *  p;
  void       ** vp;
  msg_size_t *  szp;
  KvBacklog  *  back = ibx.backlog.tl;
  size_t        i, j, k = 0;

  if ( back != NULL && back->cnt + cnt <= 64 ) {
    p = (uint8_t *) ibx.tmp.alloc( vec_size );
    if ( p == NULL )
      return false;

    vp  = back->vec;
    szp = back->siz;
    i   = back->cnt;
    j   = i + cnt;
    back->cnt = j;
  }
  else {
    size_t arsz = ( cnt < 64 ? 64 : cnt );
    p = (uint8_t *) ibx.tmp.alloc( sizeof( KvBacklog ) + vec_size +
                            arsz * ( sizeof( msg_size_t ) + sizeof( void * ) ) );
    if ( p == NULL )
      return false;

    vp   = (void **) (void *) &p[ sizeof( KvBacklog ) ];
    szp  = (msg_size_t *) (void *) &vp[ arsz ];
    back = new ( p ) KvBacklog( cnt, vp, szp, vec_size );
    p    = (uint8_t *) (void *) &szp[ arsz ];
    i    = 0;
    j    = cnt;
  }

  do {
    msg_size_t n = siz[ k ];
    vp[ i ]  = (void *) p;
    szp[ i ] = n;
    ::memcpy( p, vec[ k ], n );
    p = &p[ n ];
    k++;
  } while ( ++i < j );

  ibx.backlog_size += vec_size;
  ibx.backlog_cnt  += cnt;
  if ( back != ibx.backlog.tl ) {
    if ( ibx.backlog.hd == NULL ) {
      this->backlogq.push_tl( &ibx );
      ibx.need_signal = true;
      ibx.backlog_progress = this->time_ns;
    }
    ibx.backlog.push_tl( back );
  }
  this->push( EV_WRITE_HI );

  return true;
}

inline bool
KvPubSub::clear_backlog( KvMsgQueue &ibx ) noexcept
{
  KvBacklog * back;
  KeyStatus   status = KEY_OK;

  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
  for (;;) {
    if ( (back = ibx.backlog.hd) == NULL )
      break;
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW || this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;

      status = this->kctx.append_vector( back->cnt, back->vec, back->siz,
                                         ibx.high_water_size );
      if ( status <= KEY_IS_NEW ) {
        ibx.backlog_size    -= back->vec_size;
        ibx.backlog_cnt     -= back->cnt;
        ibx.backlog_progress = this->time_ns;
        ibx.backlog.pop_hd();
      }
    }
    this->kctx.release();
    if ( status > KEY_IS_NEW )
      break;
  }
  if ( status > KEY_IS_NEW && status != KEY_MSG_LIST_FULL ) {
    fprintf( stderr, "KvPubSub: unable to send backlog, status %d+%s/%s\n",
             status, kv_key_status_string( status ),
             kv_key_status_description( status ) );
  }
  if ( ibx.backlog.hd == NULL )
    return true;
  this->push( EV_WRITE_HI );
  return false;
}

inline bool
KvPubSub::send_msg( KvMsg &msg ) noexcept
{
  KeyStatus status = KEY_OK;
  KvMsgQueue & ibx = *this->inbox[ msg.dest_start ];

  if ( ibx.backlog.hd == NULL ) {
    this->kctx.set_key( ibx.kbuf );
    this->kctx.set_hash( ibx.hash1, ibx.hash2 );
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW || this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;
      status = this->kctx.append_msg( &msg, msg.size, ibx.high_water_size );
      this->kctx.release();
    }
  }
  else {
    status = KEY_MSG_LIST_FULL;
  }
  if ( status == KEY_MSG_LIST_FULL ) {
    void     * p  = (void *) &msg;
    msg_size_t sz = msg.size;
    if ( this->push_backlog( ibx, 1, &p, &sz, sz ) )
      return true;
  }
  else if ( status == KEY_OK ) {
    ibx.pub_size += msg.size;
    ibx.pub_cnt  += 1;
    ibx.pub_msg  += 1;
    return true;
  }
  fprintf( stderr, "KvPubSub: unable to send msg, status %d+%s/%s\n", status,
         kv_key_status_string( status ), kv_key_status_description( status ) );
  return false;
}

inline bool
KvPubSub::send_vec( size_t cnt,  void **vec,  msg_size_t *siz,
                    size_t dest,  uint64_t vec_size ) noexcept
{
  KeyStatus status = KEY_OK;
  KvMsgQueue & ibx = *this->inbox[ dest ];

  if ( ibx.backlog.hd == NULL ) {
    this->kctx.set_key( ibx.kbuf );
    this->kctx.set_hash( ibx.hash1, ibx.hash2 );
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW ||
           this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;
      status = this->kctx.append_vector( cnt, vec, siz, ibx.high_water_size );
      this->kctx.release();
    }
  }
  else {
    status = KEY_MSG_LIST_FULL;
  }
  if ( status == KEY_MSG_LIST_FULL ) {
    if ( this->push_backlog( ibx, cnt, vec, siz, vec_size ) )
      return true;
  }
  else if ( status == KEY_OK ) {
    ibx.pub_size += vec_size;
    ibx.pub_cnt  += 1;
    ibx.pub_msg  += cnt;
    return true;
  }
  fprintf( stderr, "KvPubSub: unable to send vec, status %d+%s/%s\n", status,
         kv_key_status_string( status ), kv_key_status_description( status ) );
  return false;
}

static inline size_t test_set_range( CubeRoute128 &used,  const uint8_t *b,
                                     size_t cnt ) noexcept
{
  size_t j = 0;
  for ( size_t i = 0; i < cnt; i += 2 ) {
    if ( used.test_set( b[ i ] ) )
      j++; /* can be a vector, multiple msgs to send */
  }
  return j;
}

inline void
KvPubSub::write_send_queue( CubeRoute128 &used ) noexcept
{
  KvSubRoute * rt;
  KvMsgList  * l;
  size_t       dest,
               veccnt = 0;
  /* for each message, determine the subscription destinations */
  for ( l = this->sendq.hd; l != NULL; l = l->next ) {
    KvMsg & msg   = l->msg;
    uint8_t start = msg.dest_start,
            end   = msg.dest_end;
    /*print_msg( msg );*/
    if ( start != end ) { /* if not to a single node */
      if ( is_kv_bcast( msg.msg_type ) ) { /* calculate the dest range */
        if ( end - start == MAX_CTX_ID ) {
          l->range.w = this->mc_range.w;
          l->cnt = this->mc_cnt;
        }
        else {
          l->cnt = this->mc_cr.branch4( this->ctx_id, start, end, l->range.b );
        }
      }
      else {
        KvSubMsg &submsg = (KvSubMsg &) msg;
        CubeRoute128 cr;
        uint8_t pref_cnt = submsg.prefix_cnt();
        cr.zero();
        /* or all of the sub matches together, multiple wildcards + exact */
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
        cr.and_bits( this->mc_cr );
        cr.clip( start, end );
        KvRouteCache * p = &this->rte_cache[ cr.fold8() ];
        if ( ! p->cr.equals( cr ) ) {
          p->cr  = cr;
          p->cnt = CubeRoute128::branch4x( this->ctx_id, cr, p->range.b );
        }
        l->cnt = p->cnt;
        l->range.w = p->range.w;
        /*l->cnt = CubeRoute128::branch4x( this->ctx_id, cr, l->range.b );*/
      }
      veccnt += test_set_range( used, l->range.b, l->cnt );
    }
    else if ( this->mc_cr.is_set( start ) ) { /* dest is single node */
      l->range.b[ 0 ] = start;
      l->range.b[ 1 ] = start;
      l->cnt = 2;
      if ( used.test_set( start ) )
        veccnt++; /* can be a vector, multiple msgs to send */
    }
  }
  if ( veccnt > 0 ) { /* if at least two msgs go to the same dest */
    static const size_t VEC_CHUNK_SIZE = 1024;
    size_t       j = 0;
    uint64_t     vec_size = 0;
    msg_size_t   siz[ VEC_CHUNK_SIZE ];
    void       * vec[ VEC_CHUNK_SIZE ];
    KvMsgList  * hd = this->sendq.hd;

    for (;;) {
      while ( hd->off == hd->cnt ) {
        if ( (hd = hd->next) == NULL )
          goto break_loop;
      }
      dest = hd->range.b[ hd->off ];

      for ( l = hd; l != NULL; l = l->next ) {
        if ( l->off < l->cnt && l->range.b[ l->off ] == dest ) {
          KvMsg & msg = l->msg;
          msg.dest_start = l->range.b[ l->off ];
          msg.dest_end   = l->range.b[ l->off + 1 ];
          l->off += 2;
          vec[ j ] = &msg;
          siz[ j ] = msg.size;
          vec_size += msg.size;
          if ( ++j == VEC_CHUNK_SIZE ) {
            this->send_vec( VEC_CHUNK_SIZE, vec, siz, dest, vec_size );
            j = 0;
            vec_size = 0;
          }
        }
      }
      if ( j > 0 ) {
        this->send_vec( j, vec, siz, dest, vec_size );
        j = 0;
        vec_size = 0;
      }
    }
  break_loop:;
  }
  else { /* no vectors, send each msg one at a time */
    for ( l = this->sendq.hd; l != NULL; l = l->next ) {
      while ( l->off < l->cnt ) {
        KvMsg & msg = l->msg;
        msg.dest_start = l->range.b[ l->off ];
        msg.dest_end   = l->range.b[ l->off + 1 ];
        l->off += 2;
        this->send_msg( msg );
      }
    }
  }
}

void
KvPubSub::notify_peers( CubeRoute128 &used ) noexcept
{
  if ( ( this->flags & KV_DO_NOTIFY ) != 0 ) {
    size_t dest;
    /* notify each dest fd through poll() */
    if ( used.first_set( dest ) ) {
      do {
        KvMsgQueue & ibx = *this->inbox[ dest ];
        if ( ibx.need_signal ) {
          ibx.need_signal = false;
          uint32_t pid = this->poll.map->ctx[ dest ].ctx_pid;
          uint16_t fl  = this->poll.map->ctx[ dest ].ctx_flags;
          if ( pid > 0 && ( fl & KV_NO_SIGUSR ) == 0 ) {
            if ( this->poll.quit )
              printf( "quit, notify %d ctx %ld\n", pid, dest );
            ::kill( pid, kv_msg_signal );
            ibx.signal_cnt++;
          }
        }
      } while ( used.next_set( dest ) );
    }
  }
}

void
KvPubSub::write( void ) noexcept
{
  CubeRoute128 used;

  this->pop2( EV_WRITE, EV_WRITE_HI );
  used.zero();
  /* if there are other contexts recving msgs */
  if ( this->update_mcast_route() ) {
    if ( this->send_cnt > this->route_cnt ) {
      this->write_send_queue( used );
      this->route_cnt  = this->send_cnt;
      this->route_size = this->send_size;
    }
  }
  /* if there are contexts with a backlog of msgs */
  if ( this->backlogq.hd != NULL ) {
    KvMsgQueue *ibx = this->backlogq.hd;
    this->time_ns = this->poll.current_coarse_ns();
    while ( ibx != NULL ) {
      KvMsgQueue * next = ibx->next;
      bool is_cleared = false;
      if ( ! this->mc_cr.is_set( ibx->ibx_num ) ) {
        is_cleared = true;
      }
      else if ( this->clear_backlog( *ibx ) ) {
        used.set( ibx->ibx_num );
        is_cleared = true;
      }
      else {
        if ( this->time_ns > ibx->backlog_progress +
             5 * (uint64_t) 1000000000 ) {
          printf( "[ %u ] no progress (%.3f): %lu size, %lu cnt\n",
                  ibx->ibx_num,
              (double) ( this->time_ns - ibx->backlog_progress ) / 1000000000.0,
                  ibx->backlog_size, ibx->backlog_cnt );
          ibx->backlog_progress = this->time_ns;
        }
      }
      if ( is_cleared ) {
        this->backlogq.pop( ibx );
        ibx->tmp.reset();
        ibx->backlog.init();
        ibx->backlog_size = 0;
        ibx->backlog_cnt  = 0;
      }
      ibx = next;
    }
  }
  /* if peers need notify signals */
  this->notify_peers( used );

  /* reset sendq, free mem */
  this->sendq.init();
  this->wrkq.reset();
}

bool
KvPubSub::get_sub_mcast( const char *sub,  size_t len,
                         CubeRoute128 &cr ) noexcept
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;

  if ( kbuf.copy( sub, len + 1 ) != len + 1 ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->wrkq.alloc( sz );
    ::memcpy( kb->u.buf, sub, len );
  }
  kb->u.buf[ len ] = '\0';
  this->hs.hash( *kb, hash1, hash2 );
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
KvSubNotifyList::on_sub( KvSubMsg & ) noexcept
{
}

void
KvPubSub::forward_sub( KvSubMsg &submsg ) noexcept
{
  for ( KvSubNotifyList * l = this->sub_notifyq.hd; l != NULL; l = l->next )
    l->on_sub( submsg );
}

void
KvPubSub::route_msg_from_shm( KvMsg &msg ) noexcept /* inbound from shm */
{
  /*print_msg( msg );*/
  /* if msg destination has more hops */
  if ( msg.dest_start != msg.dest_end ) {
    KvMsgList * l = (KvMsgList *)
                    this->wrkq.alloc( sizeof( KvMsgList ) + msg.size );
    l->init_route();
    ::memcpy( &l->msg, &msg, msg.size );
    this->sendq.push_tl( l );
    this->send_size += msg.size;
    this->send_cnt  += 1;

    this->idle_push( EV_WRITE );
  }
  switch ( msg.msg_type ) {
    case KV_MSG_SUB: /* update my routing table when sub/unsub occurs */
    case KV_MSG_UNSUB: {
      msg.print_sub();
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
      msg.print_sub();
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
KvPubSub::read( void ) noexcept
{
  bool until_empty = false;
  if ( this->test( EV_READ ) ) {
    struct signalfd_siginfo fdsi;
    while ( ::read( this->fd, &fdsi, sizeof( fdsi ) ) > 0 )
      this->sigusr_recv_cnt++;
    if ( this->sigusr_recv_cnt > kv_busy_loop_rate ) {
      this->poll.map->ctx[ this->ctx_id ].ctx_flags |= KV_NO_SIGUSR;
      this->push( EV_BUSY_POLL );
    }
    /* when signal read() and not busy, then read until empty */
    until_empty = ( this->test( EV_BUSY_POLL ) == 0 );
  }
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  this->read_inbox( until_empty );
}

size_t
KvPubSub::read_inbox( bool read_until_empty ) noexcept
{
  static const size_t veclen = 4 * 1024;
  void       * data[ veclen ];
  msg_size_t   data_sz[ veclen ];
  KvMsgQueue & ibx = *this->inbox[ this->ctx_id ];
  size_t       count = 0, total = 0;
  KeyStatus    status;

  /* guard against reentrant calls */
  if ( ( this->flags & KV_READ_INBOX ) != 0 ) {
    this->push( EV_READ_LO );
    return 0;
  }
  this->flags |= KV_READ_INBOX;
  if ( this->ib_kctx.kbuf != &ibx.kbuf ) {
    this->ib_kctx.set_key( ibx.kbuf );
    this->ib_kctx.set_hash( ibx.hash1, ibx.hash2 );
    if ( this->ib_kctx.acquire( &this->ib_wrk ) == KEY_IS_NEW ) {
      uint8_t b = 0;
      this->ib_kctx.append_msg( &b, 1, ibx.high_water_size );
    }
    this->ib_kctx.release();
  }
  for (;;) {
    bool retry = false;
    if ( ibx.ibx_pos == 0 ) {
      status = this->ib_kctx.find( &this->ib_wrk );
    }
    else {
      if ( ! read_until_empty ) {
        if ( this->ib_kctx.if_value_equals( ibx.ibx_pos, ibx.ibx_value_ctr ) )
          break;
      }
      status = this->ib_kctx.fetch( &this->ib_wrk, ibx.ibx_pos );
    }

    if ( status == KEY_OK ) {
      for (;;) {
        size_t   j;
        uint64_t seqno  = ibx.ibx_seqno,
                 seqno2 = seqno + veclen;
        if ( (status = this->ib_kctx.msg_value( seqno, seqno2, data,
                                                data_sz )) != KEY_OK ) {
          if ( status == KEY_MUTATED )
            retry = true;
          break;
        }
        ibx.ibx_seqno = seqno2;
        j = seqno2 - seqno;
        ibx.read_cnt  += 1;
        ibx.read_msg  += j;
        for ( size_t i = 0; i < j; i++ ) {
          if ( data_sz[ i ] >= sizeof( KvMsg ) ) {
            KvMsg &msg = *(KvMsg *) data[ i ];
            ibx.read_size += data_sz[ i ];
            if ( msg.is_valid( data_sz[ i ] ) ) {
              /* check these, make sure messages are in order ?? */
              this->inbox[ msg.src ]->src_session_id = msg.session_id();
              this->inbox[ msg.src ]->src_seqno      = msg.seqno();
              this->route_msg_from_shm( msg );
            }
          }
#if 0
          else {
            printf( "bad (%lu) %u start=%lu seqno=%lu seqno2=%lu sess=%lx\n",
                    i, data_sz[ i ], start, seqno, seqno2, this->session_id );
            printf( "&data[ i ] %lx data[ i ] %lx\n", (long) (void *) &data[ i ],
                    (long) (void *) data[ i ] );
            dump_hex( data[ i ], 256 );
            size_t k = 0;
            while ( i > 0 ) {
              i -= 1;
            printf( "&data[ i ] %lx data[ i ] %lx\n", (long) (void *) &data[ i ],
                    (long) (void *) data[ i ] );
              dump_hex( data[ i ], 256 );
              if ( ++k == 3 )
                break;
            }
            assert( 0 );
          }
#endif
        }
        count += j;
        if ( j != veclen ) { /* didn't fill entire array, find() again */
          break;
        }
      }
    }
    else if ( status == KEY_MUTATED )
      retry = true;
    else if ( status == KEY_NOT_FOUND ) {
      if ( ibx.ibx_pos == 0 ) {
        ibx.ibx_pos = 0;
        retry = true;
      }
    }
    /* remove msgs consumed */
    if ( count > total ) {
      if ( this->ib_kctx.acquire( &this->ib_wrk ) <= KEY_IS_NEW ) {
        this->ib_kctx.trim_msg( ibx.ibx_seqno );

        if ( this->ib_kctx.entry->test( FL_IMMEDIATE_VALUE ) != 0 )
          this->ib_kctx.get_pos_value_ctr( ibx.ibx_pos, ibx.ibx_value_ctr );
        else
          retry = true;
        this->ib_kctx.release();
      }
      total = count;
    }
    if ( ! retry || ! read_until_empty )
      break;
  }
  this->flags &= ~KV_READ_INBOX;
  this->inbox_msg_cnt += total;
  return total;
}

bool
KvPubSub::on_msg( EvPublish &pub ) noexcept
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
KvPubSub::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
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
