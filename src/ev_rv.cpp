#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <raids/ev_rv.h>
#include <raids/redis_msg.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raids/ev_publish.h>
#include <raids/kv_pubsub.h>
#include <raids/timer_queue.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raids/pattern_cvt.h>
#include <raimd/json_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>
#include <raimd/rv_msg.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

/*
 * RV protocol (initial parts are int32 in big endian) (send: svr->client)
 *
 * 1. send ver-rec [0,4,0] {RV protocol version 4.0}
 *    recv ver-rec [0,4,0] 
 *
 *    These are likely legacy from the old CI protocol which had 1.X versions.
 *
 * 2. send info-rec [2, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *    recv info-rec [3, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *    send info-rec [1, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *
 *    Negotiating options, not sure what it means, except that
 *      2 = request, 3 = response, 1 = agreement
 *
 * 3. recv init-rec { mtype: I, userid: user, service: port, network: ip;x;y,
 *                    vmaj: 5, vmin: 4, vupd: 2 }
 *    send init-rec { sub: RVD.INITRESP, mtype R,
 *                    data: { ipaddr: ip, ipport: port, vmaj: 8, vmin: 3,
 *                            vupd: 1, gob: utf*4096 } }
 *    recv init-rec { mtype: I, userid: user, session: <IP>.<PID><TIME><PTR>,
 *                    service: port, network: ip;x;y,
 *                    control: _INBOX.<session>.1, vmaj: 5, vmin: 4, vupd: 2 }
 *
 * 4. send connected { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype D
 *                     data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM,
 *                             ADV_NAME: RVD.CONNECTED } }
 *
 * 5. recv inbox listen { mtype: L, sub: _INBOX.<session>.> }
 *
 * 6. recv sub listen { mtype: L, sub: TEST }
 *    send sub data { mtype: D, sub: TEST, data: msg-data }
 *    recv sub data { mtype: D, sub: TEST, data: msg-data }
 *    send sub cancel { mtype: C, sub: TEST }
 *
 * mtype:
 *   I = informational
 *   L = listen subject
 *   D = publish data on a subject
 *   C = cancel listen
 */
static uint64_t uptime_stamp;

EvRvListen::EvRvListen( EvPoll &p )
            : EvTcpListen( p, this->ops ),
              timer_id( (uint64_t) EV_RV_SOCK << 56 ),
              ipaddr( 0 ),
              ipport( 0 )
{
  if ( uptime_stamp == 0 )
    uptime_stamp = kv_current_realtime_ns();
  md_init_auto_unpack();

  char host[ 1024 ];
  if ( ::gethostname( host, sizeof( host ) ) == 0 ) {
    struct addrinfo * h = NULL,
                    * res;
    if ( ::getaddrinfo( host, NULL, NULL, &h ) == 0 ) {
      for ( res = h; res != NULL; res = res->ai_next ) {
        if ( res->ai_family == AF_INET &&
             res->ai_addrlen >= sizeof( struct sockaddr_in ) ) {
          struct sockaddr_in * in = (struct sockaddr_in *) res->ai_addr;
          ::memcpy( &this->ipaddr, &in->sin_addr.s_addr, 4 );
          break;
        }
      }
      ::freeaddrinfo( h );
    }
  }
}

void
EvRvListen::accept( void )
{
  static int on = 1;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  int sock = ::accept( this->fd, (struct sockaddr *) &addr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
        perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    return;
  }
  EvRvService *c =
    this->poll.get_free_list<EvRvService>( this->poll.free_rv );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return;
  }
  struct linger lin;
  lin.l_onoff  = 1;
  lin.l_linger = 10; /* 10 secs */
  if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof( on ) ) != 0 )
    perror( "warning: SO_KEEPALIVE" );
  if ( ::setsockopt( sock, SOL_SOCKET, SO_LINGER, &lin, sizeof( lin ) ) != 0 )
    perror( "warning: SO_LINGER" );
  if ( ::setsockopt( sock, SOL_TCP, TCP_NODELAY, &on, sizeof( on ) ) != 0 )
    perror( "warning: TCP_NODELAY" );
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );

  if ( this->ipport == 0 ) {
    struct sockaddr_storage xaddr;
    addrlen = sizeof( xaddr );
    if ( ::getsockname( this->fd, (struct sockaddr *) &xaddr,
                        &addrlen ) == 0 ) {
      if ( xaddr.ss_family == AF_INET ) {
        struct sockaddr_in * sin = (struct sockaddr_in *) &xaddr;
        ::memcpy( &this->ipport, &sin->sin_port, 2 );
      }
      else if ( xaddr.ss_family == AF_INET6 ) {
        struct sockaddr_in6 * sin6 = (struct sockaddr_in6 *) &xaddr;
        ::memcpy( &this->ipport, &sin6->sin6_port, 2 );
      }
    }
  }
  c->ipaddr = this->ipaddr;
  c->ipport = this->ipport;

  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "rv" );
  c->initialize_state( ++this->timer_id );
  c->idle_push( EV_WRITE_HI );
  if ( this->poll.add_sock( c ) < 0 ) {
    printf( "failed to add sock %d\n", sock );
    ::close( sock );
    c->push_free_list();
    return;
  }
  uint32_t ver_rec[ 3 ] = { 0, 4, 0 };
  ver_rec[ 1 ] = get_u32<MD_BIG>( &ver_rec[ 1 ] ); /* flip */
  c->append( ver_rec, sizeof( ver_rec ) );
  /*this->poll.timer_queue->add_timer( c->fd, RV_SESSION_IVAL, c->timer_id,
                                       IVAL_SECS, 0 );*/
#if 0
  MDOutput out;
  printf( "-> %lu ->\n", sizeof( ver_rec ) );
  out.print_hex( ver_rec, sizeof( ver_rec ) );
#endif
}

void
EvRvService::send_info( bool agree )
{
  static const uint32_t info_rec_prefix[] = { 2, 2, 0, 1, 0, 4 << 24, 4 << 24 };
  uint32_t info_rec[ 16 ];
  size_t i;

  ::memcpy( info_rec, info_rec_prefix, sizeof( info_rec_prefix ) );
  if ( agree )
    info_rec[ 0 ] = 1;
  for ( i = 0; i < sizeof( info_rec_prefix ) / 4; i++ )
    info_rec[ i ] = get_u32<MD_BIG>( &info_rec[ i ] ); /* flip */
  for ( ; i < sizeof( info_rec ) / 4; i++ )
    info_rec[ i ] = 0; /* zero rest */
  this->append( info_rec, sizeof( info_rec ) );
#if 0
  MDOutput out;
  printf( "-> %lu ->\n", sizeof( info_rec ) );
  out.print_hex( info_rec, sizeof( info_rec ) );
#endif
}

void
EvRvService::process( void )
{
  size_t  buflen, msglen;
  int32_t status = 0;

  do {
    buflen = this->len - this->off;
    if ( buflen < 8 )
      goto break_loop;
#if 0
    MDOutput out;
    printf( "<- %lu <-\n", buflen );
    out.print_hex( &this->recv[ this->off ], buflen );
#endif
    switch ( this->state ) {
      case VERS_RECV:
        if ( buflen < 3 * 4 )
          goto break_loop;
        this->off += 3 * 4;
        this->send_info( false );
        this->state = INFO_RECV;
        break;

      case INFO_RECV:
        if ( buflen < 16 * 4 )
          goto break_loop;
        this->off += 16 * 4;
        this->send_info( true );
        this->state = DATA_RECV;
        break;

      case DATA_RECV:
        msglen = get_u32<MD_BIG>( &this->recv[ this->off ] );
        if ( buflen < msglen )
          goto break_loop;
        status = this->recv_data( &this->recv[ this->off ], msglen );
        this->off += msglen;
        this->br  += msglen;
        this->mr++;
        break;
    }
  } while ( status == 0 );
break_loop:;
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
  if ( status != 0 )
    this->push( EV_CLOSE );
}

int
EvRvService::recv_data( void *msgbuf, size_t msglen )
{
  static const char *rv_status_string[] = RV_STATUS_STRINGS;
  int status;
  if ( (status = this->msg_in.unpack( msgbuf, msglen )) != 0 ) {
    if ( msglen != 0 )
      fprintf( stderr, "rv status %d: \"%s\"\n", status,
               rv_status_string[ status ] );
    return status;
  }
  switch ( this->msg_in.mtype ) {
    case 'C': /* unsubscribe */
      this->rem_sub();
      return 0;
    case 'D': /* publish */
      this->fwd_pub();
      return 0;
    case 'I': /* info */
      return this->respond_info();
    case 'L': /* subscribe */
      this->add_sub();
      return 0;
    default:
      return -1;
  }
  return 0;
}

static bool
match_str( MDName &nm,  MDReference &mref,  const char *s,  char *buf,
           size_t buflen )
{
  if ( ::memcmp( nm.fname, s, nm.fnamelen ) != 0 )
    return false;
  if ( mref.ftype == MD_STRING && mref.fsize < buflen ) {
    ::memcpy( buf, mref.fptr, mref.fsize );
    buf[ mref.fsize ] = '\0';
  }
  else {
    buf[ 0 ] = '\0';
  }
  return true;
}

static bool
match_int( MDName &nm,  MDReference &mref,  const char *s,  uint32_t &ival )
{
  if ( ::memcmp( nm.fname, s, nm.fnamelen ) != 0 )
    return false;
  if ( mref.ftype == MD_INT || mref.ftype == MD_UINT ) {
    ival = get_uint<uint32_t>( mref );
  }
  else {
    ival = 0;
  }
  return true;
}

int
EvRvService::respond_info( void )
{
  RvFieldIter * iter = this->msg_in.iter;
  MDName        nm;
  MDReference   mref;
  uint8_t       buf[ 256 ];
  RvMsgWriter   rvmsg( buf, sizeof( buf ) ),
                submsg( NULL, 0 );
  size_t        sz;

  if ( iter->first() == 0 ) {
    do {
      if ( iter->get_name( nm ) != 0 || iter->get_reference( mref ) != 0 )
        return ERR_RV_REF;
      switch ( nm.fnamelen ) {
        case 7:
          match_str( nm, mref, "userid", this->userid, sizeof( this->userid ) );
          break;
        case 8:
          if ( ! match_str( nm, mref, "session", this->session,
                            sizeof( this->session ) ) )
            if ( ! match_str( nm, mref, "control", this->control,
                              sizeof( this->control ) ) )
              if ( ! match_str( nm, mref, "service", this->service,
                                sizeof( this->service ) ) )
                match_str( nm, mref, "network", this->network,
                           sizeof( this->network ) );
          break;
        case 5:
          if ( ! match_int( nm, mref, "vmaj", this->vmaj ) )
            if ( ! match_int( nm, mref, "vmin", this->vmin ) )
              match_int( nm, mref, "vupd", this->vupd );
          break;
        default:
          break;
      }
    } while ( iter->next() == 0 );
  }

  if ( this->session[ 0 ] == 0 ) {
    /* { sub: RVD.INITRESP, mtype: R,
     *   data: { ipaddr: a.b.c.d, ipport: 7500, vmaj: 5, vmin: 4, vupd: 2 } } */
    rvmsg.append_subject( "sub", 4, "RVD.INITRESP" );
    rvmsg.append_string( "mtype", 6, "R", 2 );
    rvmsg.append_msg( "data", 5, submsg );
    submsg.append_type( "ipaddr", 7, this->ipaddr, MD_IPDATA );
    submsg.append_type( "ipport", 7, this->ipport, MD_IPDATA );
    submsg.append_int<int32_t>( "vmaj", 5, 5 );
    submsg.append_int<int32_t>( "vmin", 5, 4 );
    submsg.append_int<int32_t>( "vupd", 5, 2 );

    struct timeval tv;
    uint64_t n;
    char     gob_buf[ 24 ];
    uint8_t  j, k;
    ::gettimeofday( &tv, NULL );
    n = ( (uint64_t) tv.tv_sec << 12 ) |
        ( ( (uint64_t) tv.tv_usec * 4096 / 1000000 ) &
          ( ( (uint64_t) 1 << 12 ) - 1 ) );
    for ( j = 0; ( n >> j ) != 0; j += 4 )
      ;
    for ( k = 0; j > 0; ) {
      j -= 4;
      uint8_t d = ( n >> j ) & 0xf;
      gob_buf[ k++ ] = ( d < 10 ? d + '0' : d - 10 + 'A' );
    }
    gob_buf[ k++ ] = 0;
    submsg.append_string( "gob", 4, gob_buf, k );
    sz = rvmsg.update_hdr( submsg );
  }
  else {
    /* { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype: D,
     *   data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM, ADV_NAME: RVD.CONNECTED } } */
    rvmsg.append_subject( "sub", 4, "_RV.INFO.SYSTEM.RVD.CONNECTED" );
    rvmsg.append_string( "mtype", 6, "D", 2 );
    rvmsg.append_msg( "data", 5, submsg );
    submsg.append_string( "ADV_CLASS", 10, "INFO", 5 );
    submsg.append_string( "ADV_SOURCE", 11, "SYSTEM", 7 );
    submsg.append_string( "ADV_NAME", 9, "RVD.CONNECTED", 14 );
    sz = rvmsg.update_hdr( submsg );
  }
  this->append( buf, sz );
  return 0;
}

bool
EvRvService::timer_expire( uint64_t tid,  uint64_t )
{
  if ( this->timer_id != tid )
    return false;
  this->idle_push( EV_WRITE );
  return true;
}

void
EvRvService::add_sub( void )
{
  char   * sub = this->msg_in.sub;
  uint32_t len = this->msg_in.sublen;

  if ( ! this->msg_in.is_wild ) {
    uint32_t h = kv_crc_c( sub, len, 0 ),
             rcnt;
    if ( this->sub_tab.put( h, sub, len ) == RV_SUB_OK ) {
      rcnt = this->poll.sub_route.add_route( h, this->fd );
      this->poll.notify_sub( h, sub, len, this->fd, rcnt, 'V',
                             this->msg_in.reply, this->msg_in.replylen );
    }
  }
  else {
    RvPatternRoute * rt;
    char       buf[ 1024 ];
    PatternCvt cvt( buf, sizeof( buf ) );
    uint32_t   h, rcnt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.put( h, sub, len, rt ) == RV_SUB_OK ) {
        size_t erroff;
        int    error;
        rt->re =
          pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
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
          rcnt = this->poll.sub_route.add_pattern_route( h, this->fd,
                                                         cvt.prefixlen );
          this->poll.notify_psub( h, buf, cvt.off, sub, cvt.prefixlen,
                                  this->fd, rcnt, 'V' );
        }
      }
    }
  }
}

void
EvRvService::rem_sub( void )
{
  char   * sub = this->msg_in.sub;
  uint32_t len = this->msg_in.sublen;

  if ( ! this->msg_in.is_wild ) {
    uint32_t h    = kv_crc_c( sub, len, 0 ),
             rcnt = 0;
    if ( this->sub_tab.rem( h, sub, len ) == RV_SUB_OK ) {
      printf( "rem sub %s\n", sub );
      if ( this->sub_tab.tab.find_by_hash( h ) == NULL )
        rcnt = this->poll.sub_route.del_route( h, this->fd );
      this->poll.notify_unsub( h, sub, len, this->fd, rcnt, 'V' );
    }
  }
  else {
    char             buf[ 1024 ];
    PatternCvt       cvt( buf, sizeof( buf ) );
    RouteLoc         loc;
    RvPatternRoute * rt;
    uint32_t         h, rcnt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
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
        rcnt = this->poll.sub_route.del_pattern_route( h, this->fd,
                                                       cvt.prefixlen );
        this->poll.notify_punsub( h, buf, cvt.off, sub, cvt.prefixlen,
                                  this->fd, rcnt, 'V' );
      }
    }
  }
}

void
EvRvService::rem_all_sub( void )
{
  RvSubRoutePos     pos;
  RvPatternRoutePos ppos;
  uint32_t          rcnt;

  if ( this->sub_tab.first( pos ) ) {
    do {
      rcnt = this->poll.sub_route.del_route( pos.rt->hash, this->fd );
      this->poll.notify_unsub( pos.rt->hash, pos.rt->value, pos.rt->len,
                               this->fd, rcnt, 'V' );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    char       buf[ 1024 ];
    PatternCvt cvt( buf, sizeof( buf ) );
    do {
      if ( cvt.convert_rv( ppos.rt->value, ppos.rt->len ) == 0 ) {
        rcnt = this->poll.sub_route.del_pattern_route( ppos.rt->hash,
                                                  this->fd, cvt.prefixlen );
        this->poll.notify_punsub( ppos.rt->hash, buf, cvt.off,
                                  ppos.rt->value, cvt.prefixlen,
                                  this->fd, rcnt, 'V' );
      }
    } while ( this->pat_tab.next( ppos ) );
  }
}

bool
EvRvService::fwd_pub( void )
{
  char   * sub     = this->msg_in.sub,
         * rep     = this->msg_in.reply;
  size_t   sublen  = this->msg_in.sublen,
           replen  = this->msg_in.replylen;
  uint32_t h       = kv_crc_c( sub, sublen, 0 );
  void   * msg     = this->msg_in.data.fptr;
  uint32_t msg_len = this->msg_in.data.fsize;
  uint8_t  ftype   = this->msg_in.data.ftype;

  if ( ftype == MD_MESSAGE || ftype == (uint8_t) RVMSG_TYPE_ID ) {
    ftype = (uint8_t) RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       &this->msg_in.mem );
    if ( m != NULL ) {
      ftype   = (uint8_t) m->get_type_id();
      msg     = &((uint8_t *) m->msg_buf)[ m->msg_off ];
      msg_len = m->msg_end - m->msg_off;
    }
  }
  else if ( ftype == MD_OPAQUE ) {
    MDMsg * m = MDMsg::unpack( msg, 0, msg_len, 0, NULL, &this->msg_in.mem );
    if ( m != NULL )
      ftype = (uint8_t) m->get_type_id();
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->fd, h, NULL, 0, ftype, 'p' );
  return this->poll.forward_msg( pub, NULL, 0, NULL );
}

bool
EvRvService::on_msg( EvPublish &pub )
{
  uint32_t pub_cnt = 0;
  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    RvSubStatus ret;
    if ( pub.subj_hash == pub.hash[ cnt ] ) {
      ret = this->sub_tab.updcnt( pub.subj_hash, pub.subject, pub.subject_len );
      if ( ret == RV_SUB_OK ) {
        if ( pub_cnt == 0 )
          this->fwd_msg( pub, NULL, 0 );
        pub_cnt++;
      }
    }
    else {
      RvPatternRoute * rt = NULL;
      RouteLoc         loc;
      rt = this->pat_tab.tab.find_by_hash( pub.hash[ cnt ], loc );
      for (;;) {
        if ( rt == NULL )
          break;
        if ( pcre2_match( rt->re, (const uint8_t *) pub.subject,
                          pub.subject_len, 0, 0, rt->md, 0 ) == 1 ) {
          rt->msg_cnt++;
          if ( pub_cnt == 0 )
            this->fwd_msg( pub, NULL, 0 );
          pub_cnt++;
        }
        rt = this->pat_tab.tab.find_next_by_hash( pub.hash[ cnt ], loc );
      }
    }
  }
  return true;
}

bool
EvRvService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  RouteLoc     loc;
  RvSubRoute * rt = this->sub_tab.tab.find_by_hash( h, loc );
  if ( rt == NULL )
    return false;
  keylen = rt->len;
  ::memcpy( key, rt->value, keylen );
  return true;
}

void
EvRvService::send( void *hdr,  size_t off,   const void *data,
                   size_t data_len )
{
#if 0
  if ( off + data_len < 8 * 1024 ) {
    uint8_t buf[ 8 * 1024 ];
    MDOutput out;
    ::memcpy( buf, hdr, off );
    ::memcpy( &buf[ off ], data, data_len );
    printf( "-> %lu ->\n", off + data_len );
    out.print_hex( buf, off + data_len );
  }
#endif
  this->append2( hdr, off, data, data_len );
  this->bs += off + data_len;
  this->ms++;
}

bool
EvRvService::fwd_msg( EvPublish &pub,  const void *,  size_t )
{
  uint8_t      buf[ 8 * 1024 ];
  RvMsgWriter  rvmsg( buf, sizeof( buf ) ),
               submsg( NULL, 0 );
  size_t       off, msg_len;
  const void * msg;

  rvmsg.append_string( "mtype", 6, "D", 2 );
  /* some subjects may not encode */
  if ( rvmsg.append_subject( "sub", 4, pub.subject ) == 0 ) {
    uint8_t msg_enc = pub.msg_enc;
    switch ( msg_enc ) {
      case (uint8_t) RVMSG_TYPE_ID:
      do_rvmsg:;
        rvmsg.append_msg( "data", 5, submsg );
        off = rvmsg.off + submsg.off;
        submsg.off += pub.msg_len - 8;
        msg     = &((const uint8_t *) pub.msg)[ 8 ];
        msg_len = pub.msg_len - 8;
        rvmsg.update_hdr( submsg );
        break;

      case MD_OPAQUE:
      case (uint8_t) RAIMSG_TYPE_ID:
      case (uint8_t) TIB_SASS_TYPE_ID:
      case (uint8_t) TIB_SASS_FORM_TYPE_ID:
      do_tibmsg:;
        msg     = pub.msg;
        msg_len = pub.msg_len;

        static const char data_hdr[] = "\005data";
        ::memcpy( &buf[ rvmsg.off ], data_hdr, sizeof( data_hdr ) );
        rvmsg.off += sizeof( data_hdr );
        buf[ rvmsg.off++ ] = 7/*RV_OPAQUE*/;
        if ( msg_len <= 120 )
          buf[ rvmsg.off++ ] = (uint8_t) msg_len;
        else if ( msg_len + 2 < 30000 ) {
          buf[ rvmsg.off++ ] = 121;
          buf[ rvmsg.off++ ] = ( ( msg_len + 2 ) >> 8 ) & 0xff;
          buf[ rvmsg.off++ ] = ( msg_len + 2 ) & 0xff;
        }
        else {
          buf[ rvmsg.off++ ] = 122;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 24 ) & 0xff;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 16 ) & 0xff;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 8 ) & 0xff;
          buf[ rvmsg.off++ ] = ( msg_len + 4 ) & 0xff;
        }
        off = rvmsg.off;
        rvmsg.off += msg_len;
        rvmsg.update_hdr();
        break;

      case MD_MESSAGE:
        if ( RvMsg::is_rvmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_rvmsg;
        if ( TibMsg::is_tibmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_tibmsg;
        if ( TibSassMsg::is_tibsassmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_tibmsg;
        /* FALLTHRU */
      default:
        off     = 0;
        msg     = NULL;
        msg_len = 0;
        break;
    }
    if ( off > 0 ) {
      this->send( buf, off, msg, msg_len );
      bool flow_good = ( this->pending() <= this->send_highwater );
      this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
      return flow_good;
    }
  }
  return true;
}

void
RvPatternMap::release( void )
{
  RvPatternRoutePos ppos;

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
EvRvService::release( void )
{
  printf( "rv release fd=%d\n", this->fd );
  this->rem_all_sub();
  this->sub_tab.release();
  this->pat_tab.release();
  this->EvConnection::release_buffers();
  this->push_free_list();
}

void
EvRvService::push_free_list( void )
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "capr sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_rv.push_hd( this );
  }
}

void
EvRvService::pop_free_list( void )
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_rv.pop( this );
  }
}

void
EvRvService::pub_session( uint8_t )
{
}

static inline uint32_t
copy_subj_in( const uint8_t *buf,  char *subj,  bool &is_wild )
{
  uint8_t segs = buf[ 0 ];
  uint32_t i, j = 1, k = 0;

  is_wild = false;
  if ( segs > 0 ) {
    for (;;) {
      i  = j + 1;
      j += buf[ j ];
      if ( k + j - i >= RV_MAX_SUBJ_LEN - 2 )
        break;
      if ( i + 2 == j ) {
        if ( buf[ i ] == '*' ||
             ( buf[ i ] == '>' && segs == 1 ) )
          is_wild = true;
      }
      while ( i + 1 < j )
        subj[ k++ ] = (char) buf[ i++ ];
      if ( --segs > 0 )
        subj[ k++ ] = '.';
      else
        break;
    } 
  } 
  subj[ k ] = '\0';
  return k;
}

int
RvMsgIn::unpack( void *msgbuf,  size_t msglen )
{
  MDFieldIter * it;
  MDName        nm;
  MDReference   mref;
  int           cnt = 0,
                status = 0;

  this->mem.reuse();
  this->msg = RvMsg::unpack_rv( msgbuf, 0, msglen, 0, NULL, &this->mem );
  if ( this->msg == NULL || this->msg->get_field_iter( it ) != 0 )
    status = ERR_RV_MSG;
  else {
#if 0
    MDOutput out;
    printf( "<< %lu >>\n", msglen );
    this->msg->print( &out );
#endif
    this->iter = (RvFieldIter *) it;
    if ( this->iter->first() == 0 ) {
      do {
        if ( this->iter->get_name( nm ) != 0 ||
             this->iter->get_reference( mref ) != 0 )
          return ERR_RV_REF;
        switch ( nm.fnamelen ) {
          case 4:
            if ( ::memcmp( nm.fname, "sub", 4 ) == 0 ) {
              this->sublen = copy_subj_in( mref.fptr, this->sub, this->is_wild );
              cnt |= 1;
            }
            break;
          case 5:
            if ( ::memcmp( nm.fname, "data", 5 ) == 0 ) {
              this->data = mref;
              cnt |= 8;
            }
            break;
          case 6:
            if ( ::memcmp( nm.fname, "mtype", 6 ) == 0 ) {
              if ( mref.ftype == MD_STRING && mref.fsize == 2 ) {
                this->mtype = mref.fptr[ 0 ];
                if ( this->mtype >= 'C' && this->mtype <= 'L' ) {
                  static const uint32_t valid =
                    1 /* C */ | 2 /* D */ | 64 /* I */ | 512 /* L */;
                  if ( ( ( 1 << ( this->mtype - 'C' ) ) & valid ) != 0 )
                    cnt |= 2;
                }
              }
            }
            break;
          case 7:
            if ( ::memcmp( nm.fname, "return", 7 ) == 0 ) {
              this->reply    = (char *) mref.fptr;
              this->replylen = mref.fsize;
              if ( this->replylen > 0 && this->reply[ this->replylen ] == '\0' )
                this->replylen--;
              cnt |= 4;
            }
            break;
          default:
            break;
        }
      } while ( cnt < 15 && this->iter->next() == 0 );
    }
    if ( ( cnt & 2 ) == 0 )
      status = ERR_RV_MTYPE; /* no mtype */
  }
  if ( ( cnt & 1 ) == 0 ) {
    this->sub[ 0 ] = '\0';
    this->sublen = 0; /* no subject */
  }
  if ( ( cnt & 2 ) == 0 )
    this->mtype = 0;
  if ( ( cnt & 4 ) == 0 ) {
    this->reply    = NULL;
    this->replylen = 0;
  }
  if ( ( cnt & 8 ) == 0 )
    this->data.zero();
  return status;
}

#if 0
static inline uint32_t
copy_subj_out( const char *subj,  uint8_t *buf,  uint32_t &hash )
{
  uint32_t i = 2, j = 1, segs = 1;
  for ( ; *subj != '\0'; subj++ ) {
    if ( *subj == '.' || i - j == 0xff ) {
      buf[ i++ ] = 0;
      buf[ j ]   = (uint8_t) ( i - j );
      j = i++;
      if ( ++segs == 0xff )
        break;
    }
    else {
      buf[ i++ ] = *subj;
    }
    if ( i > 1029 )
      break;
  }
  buf[ i++ ] = 0;
  buf[ j ]   = (uint8_t) ( i - j );
  buf[ 0 ]   = (uint8_t) segs;
  hash       = kv_crc_c( buf, i, 0 );
  return i;
}

static inline uint32_t
copy_subj_in2( const uint8_t *buf,  char *subj )
{
  uint8_t segs = buf[ 0 ];
  uint32_t i, j = 1, k = 0;

  if ( segs > 0 ) {
    for (;;) {
      i  = j + 1;
      j += buf[ j ];
      if ( k + j - i >= CAPR_MAX_SUBJ_LEN - 2 )
        break;
      while ( i + 1 < j )
        subj[ k++ ] = (char) buf[ i++ ];
      if ( --segs > 0 )
        subj[ k++ ] = '.';
      else
        break;
    } 
  } 
  subj[ k ] = '\0';
  return k;
}
#endif
