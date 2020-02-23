#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <raids/ev_nats.h>
#include <raids/int_str.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raids/ev_publish.h>
#include <raids/kv_pubsub.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raids/pattern_cvt.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

/*
 * NATS protocol:
 *
 * 1. session init
 * server -> INFO { (below) } \r\n
 * (optional)
 * client -> CONNECT { verbose, pedantic, tls_required, auth_token, user,
 *                     pass, name, lang, protocol, echo } \r\n
 * 2. subscribe
 * client -> SUB <subject> [queue group] <sid> \r\n
 *        -> UNSUB <sid> [max-msgs] \r\n
 *
 * ping/pong, pong responds to ping, used for keepalive:
 * client/server -> PING \r\n
 *               -> PONG \r\n
 *
 * 3. publish
 * client -> PUB <subject> [reply-to] <#bytes> \r\n [payload] \r\n
 * server -> MSG <subject> <sid> [reply-to] <#bytes> \r\n [payload] \r\n
 *
 * 4. error / ok status (ok turned off by verbose=false)
 * server -> +OK \r\n
 * server -> -ERR (opt msg) \r\n
 */

/* ID = 22 chars base62 string dependent on the hash of the database = 255 */
static char nats_server_info[] =
"INFO {\"server_id\":\"______________________\","
      "\"version\":\"1.1.1\",\"go\":\"go1.5.0\","
      "\"host\":\"255.255.255.255\",\"port\":65535,"
      "\"auth_required\":false,\"ssl_required\":false,"
      "\"tls_required\":false,\"tls_verify\":false,"
      "\"max_payload\":1048576}\r\n";
bool is_server_info_init;

static void
init_server_info( uint64_t h1,  uint64_t h2,  uint16_t port )
{
  char host[ 256 ];
  struct addrinfo *res = NULL, *p;
  uint64_t r = 0;
  int i;
  rand::xorshift1024star prng;
  uint64_t svid[ 2 ] = { h1, h2 };

  prng.init( svid, sizeof( svid ) ); /* same server id until shm destroyed */

  for ( i = 0; i < 22; i++ ) {
    if ( ( i % 10 ) == 0 )
      r = prng.next();
    char c = r % 62;
    c = ( c < 10 ) ? ( c + '0' ) : (
        ( c < 36 ) ? ( ( c - 10 ) + 'A' ) : ( ( c - 36 ) + 'a' ) );
    nats_server_info[ 19 + i ] = c;
    r >>= 6;
  }

  if ( ::gethostname( host, sizeof( host ) ) == 0 &&
       ::getaddrinfo( host, NULL, NULL, &res ) == 0 ) {
    for ( p = res; p != NULL; p = p->ai_next ) {
      if ( p->ai_family == AF_INET && p->ai_addr != NULL ) {
        char *ip = ::inet_ntoa( ((struct sockaddr_in *) p->ai_addr)->sin_addr );
        size_t len = ::strlen( ip );
        ::memcpy( &nats_server_info[ 84 ], ip, len );
        nats_server_info[ 84 + len++ ] = '\"';
        nats_server_info[ 84 + len++ ] = ',';
        while ( len < 17 )
          nats_server_info[ 84 + len++ ] = ' ';
        break;
      }
    }
    ::freeaddrinfo( res );
  }

  for ( i = 0; port > 0; port /= 10 ) 
    nats_server_info[ 112 - i++ ] = ( port % 10 ) + '0';
  while ( i < 5 )
    nats_server_info[ 112 - i++ ] = ' ';
  is_server_info_init = true;
}

bool
EvNatsListen::accept( void ) noexcept
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
    return false;
  }
  EvNatsService *c =
    this->poll.get_free_list<EvNatsService>( this->poll.free_nats );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
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

  if ( ! is_server_info_init ) {
    uint16_t port = 42222;
    uint64_t h1, h2;
    struct sockaddr_storage myaddr;
    socklen_t myaddrlen = sizeof( myaddr );
    if ( ::getsockname( sock, (sockaddr *) &myaddr, &myaddrlen ) == 0 ) {
      if ( myaddr.ss_family == AF_INET )
        port = ntohs( ((sockaddr_in *) &myaddr)->sin_port );
      else if ( myaddr.ss_family == AF_INET6 )
        port = ntohs( ((sockaddr_in6 *) &myaddr)->sin6_port );
    }
    this->poll.map->hdr.get_hash_seed( KV_DB_COUNT-1, h1, h2 );
    init_server_info( h1, h2, port );
  }
  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "nats" );
  c->initialize_state();
  c->idle_push( EV_WRITE_HI );
  if ( this->poll.add_sock( c ) < 0 ) {
    printf( "failed to add sock %d\n", sock );
    ::close( sock );
    c->push_free_list();
    return false;
  }
  c->append_iov( nats_server_info, sizeof( nats_server_info ) );
  return true;
}

static char *
parse_end_size( char *start,  char *end,  size_t &sz,  size_t &digits )
{
  while ( end > start ) {
    if ( *--end >= '0' && *end <= '9' ) {
      char *last_digit = end;
      sz = (size_t) ( *end - '0' );
      if ( *--end >= '0' && *end <= '9' ) {
        sz += (size_t) ( *end - '0' ) * 10;
        if ( *--end >= '0' && *end <= '9' ) {
          sz += (size_t) ( *end - '0' ) * 100;
          if ( *--end >= '0' && *end <= '9' ) {
            sz += (size_t) ( *end - '0' ) * 1000;
            if ( *--end >= '0' && *end <= '9' ) {
              size_t p = 10000;
              do {
                sz += (size_t) ( *end - '0' ) * p;
                p  *= 10;
              } while ( *--end >= '0' && *end <= '9' );
            }
          }
        }
      }
      digits = (size_t) ( last_digit - end );
      return end;
    }
  }
  sz = 0;
  digits = 0;
  return NULL;
}

static const size_t MAX_NATS_ARGS = 3; /* <subject> <sid> <reply> */

static size_t
parse_args( char *start,  char *end,  char **args,  size_t *len )
{
  char *p;
  size_t n;
  for ( p = start; ; p++ ) {
    if ( p >= end )
      return 0;
    if ( *p > ' ' )
      break;
  }
  n = 0;
  args[ 0 ] = p;
  for (;;) {
    if ( ++p == end || *p <= ' ' ) {
      len[ n ] = p - args[ n ];
      if ( ++n == MAX_NATS_ARGS )
        return n;
      while ( p < end && *p <= ' ' )
        p++;
      if ( p == end )
        break;
      args[ n ] = p;
    }
  }
  return n;
}

void
EvNatsService::process( void ) noexcept
{
  enum { DO_OK = 1, DO_ERR = 2, NEED_MORE = 4, FLOW_BACKPRESSURE = 8,
         HAS_PING = 16 };
  static const char ok[]   = "+OK\r\n",
                    err[]  = "-ERR\r\n",
                    pong[] = "PONG\r\n";
  size_t            buflen, used, sz, nargs, size_len, max_msgs;
  char            * p, * eol, * start, * end, * size_start;
  char            * args[ MAX_NATS_ARGS ];
  size_t            argslen[ MAX_NATS_ARGS ];
  int               fl, verb_ok, cmd_cnt, msg_cnt;

  for (;;) { 
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    used    = 0;
    fl      = 0;
    cmd_cnt = 0;
    msg_cnt = 0;
    verb_ok = ( this->verbose ? DO_OK : 0 );
    /* decode nats hdrs */
    for ( p = start; p < end; ) {
      if ( this->msg_state == NATS_HDR_STATE ) {
        eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
        if ( eol != NULL ) {
          sz = &eol[ 1 ] - p;
          if ( sz > 3 ) {
            switch ( unaligned<uint32_t>( p ) & 0xdfdfdfdf ) { /* 4 toupper */
            /*case NATS_KW_OK1:     // client side only
              case NATS_KW_OK2:     break;
              case NATS_KW_MSG1:   // MSG <subject> <sid> [reply] <size>
              case NATS_KW_MSG2:    break;
              case NATS_KW_ERR:     break; */
              case NATS_KW_SUB1:   /* SUB <subject> [queue group] <sid> */
              case NATS_KW_SUB2:
                nargs = parse_args( &p[ 4 ], eol, args, argslen );
                if ( nargs != 2 ) {
                  fl |= DO_ERR;
                  break;
                }
                this->subject     = args[ 0 ];
                this->sid         = args[ 1 ];
                this->subject_len = argslen[ 0 ];
                this->sid_len     = argslen[ 1 ];
                this->add_sub();
                fl |= verb_ok;
                break;
              case NATS_KW_PUB1:   /* PUB <subject> [reply] <size> */
              case NATS_KW_PUB2:
                size_start = parse_end_size( p, eol - 1, this->msg_len,
                                             size_len );
                if ( size_start == NULL ) {
                  fl |= DO_ERR;
                  break;
                }
                if ( sz <= this->tmp_size ) {
                  ::memcpy( this->buffer, p, sz );
                  size_start = &this->buffer[ size_start - p ];
                  p = this->buffer;
                }
                else {
                  char *tmp = this->alloc( sz );
                  ::memcpy( tmp, p, sz );
                  size_start = &tmp[ size_start - p ];
                  p = tmp;
                }
                nargs = parse_args( &p[ 4 ], size_start, args, argslen );
                if ( nargs < 1 || nargs > 2 ) {
                  fl |= DO_ERR;
                  break;
                }
                this->subject     = args[ 0 ];
                this->subject_len = argslen[ 0 ];
                if ( nargs > 1 ) {
                  this->reply     = args[ 1 ];
                  this->reply_len = argslen[ 1 ];
                }
                else {
                  this->reply     = NULL;
                  this->reply_len = 0;
                }
                this->msg_len_ptr    = &size_start[ 1 ];
                this->msg_len_digits = size_len;
                this->msg_state = NATS_PUB_STATE;
                break;
              case NATS_KW_PING:
                this->append( pong, sizeof( pong ) - 1 );
                fl |= HAS_PING;
                break;
            /*case NATS_KW_PONG:    break;
              case NATS_KW_INFO:    break;*/
              case NATS_KW_UNSUB: /* UNSUB <sid> [max-msgs] */
                nargs = parse_args( &p[ 6 ], eol, args, argslen );
                if ( nargs != 1 ) {
                  if ( nargs != 2 ) {
                    fl |= DO_ERR;
                    break;
                  }
                  parse_end_size( args[ 1 ], &args[ 1 ][ argslen[ 1 ] ],
                                  max_msgs, size_len );
                }
                else {
                  max_msgs = 0;
                }
                this->sid         = args[ 0 ];
                this->sid_len     = argslen[ 0 ];
                this->subject     = NULL;
                this->subject_len = 0;
                this->rem_sid( max_msgs );
                fl |= verb_ok;
                break;
              case NATS_KW_CONNECT:
                this->parse_connect( p, sz );
                break;
              default:
                break;
            }
          }
          p = &eol[ 1 ];
          used += sz;
          cmd_cnt++;
        }
        else { /* no end of line */
          fl |= NEED_MORE;
        }
      }
      else { /* msg_state == NATS_PUB_STATE */
        if ( (size_t) ( end - p ) >= this->msg_len ) {
          this->msg_ptr = p;
          p = &p[ this->msg_len ];
          used += this->msg_len;
          this->msg_state = NATS_HDR_STATE;
          /* eat trailing crlf */
          while ( p < end && ( *p == '\r' || *p == '\n' ) ) {
            p++;
            used++;
          }
          if ( ! this->fwd_pub() )
            fl |= FLOW_BACKPRESSURE;
          fl |= verb_ok;
          msg_cnt++;
        }
        else { /* not enough to consume message */
          fl |= NEED_MORE;
        }
      }
      if ( ( fl & NEED_MORE ) != 0 ) {
        this->pushpop( EV_READ, EV_READ_LO );
        break;
      }
      if ( ( fl & DO_OK ) != 0 )
        this->append( ok, sizeof( ok ) - 1 );
      if ( ( fl & DO_ERR ) != 0 )
        this->append( err, sizeof( err ) - 1 );
      if ( ( fl & ( FLOW_BACKPRESSURE | HAS_PING ) ) != 0 ) {
        this->off += used;
        if ( this->pending() > 0 )
          this->push( EV_WRITE_HI );
        if ( this->test( EV_READ ) )
          this->pushpop( EV_READ_LO, EV_READ );
        return;
      }
      fl = 0;
    }
    this->off += used;
    if ( used == 0 || ( fl & NEED_MORE ) != 0 )
      goto break_loop;
  }
break_loop:;
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
  return;
}

void
EvNatsService::add_sub( void ) noexcept
{
  NatsStr xsid( this->sid, this->sid_len );
  NatsStr xsubj( this->subject, this->subject_len );

  if ( this->map.put( xsubj, xsid ) == NATS_IS_NEW ) {
    if ( xsubj.is_wild() ) {
      this->add_wild( xsubj );
    }
    else {
      uint32_t rcnt = this->poll.sub_route.add_route( xsubj.hash(), this->fd );
      this->poll.notify_sub( xsubj.hash(), xsubj.str, xsubj.len,
                             this->fd, rcnt, 'N' );
    }
  }
#if 0
  printf( "add_sub:\n" );
  this->map.print();
#endif
}

void
EvNatsService::add_wild( NatsStr &xsubj ) noexcept
{
  NatsWildRec * rt;
  char          buf[ 1024 ];
  PatternCvt    cvt( buf, sizeof( buf ) );
  uint32_t      h, rcnt;
  size_t        erroff;
  int           error;

  if ( cvt.convert_rv( xsubj.str, xsubj.len ) == 0 ) {
    h = kv_crc_c( xsubj.str, cvt.prefixlen,
                  this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
    rt = this->map.add_wild( h, xsubj );
    if ( rt == NULL || rt->re != NULL )
      return;
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
    if ( rt->re == NULL ) {
      this->map.rem_wild( h, xsubj );
      return;
    }
    rcnt = this->poll.sub_route.add_pattern_route( h, this->fd, cvt.prefixlen );
    this->poll.notify_psub( h, buf, cvt.off, xsubj.str, cvt.prefixlen,
                            this->fd, rcnt, 'N' );
  }
}

void
NatsSubMap::rem_wild( uint32_t h,  NatsStr &subj ) noexcept
{
  RouteLoc      loc;
  NatsWildRec * rt;
  if ( (rt = this->wild_map.find( h, subj.str, subj.len, loc )) != NULL ) {
    if ( rt->md != NULL ) {
      pcre2_match_data_free( rt->md );
      rt->md = NULL;
    }
    if ( rt->re != NULL ) {
      pcre2_code_free( rt->re );
      rt->re = NULL;
    }
    this->wild_map.remove( loc );
  }
}

void
EvNatsService::rem_wild( NatsStr &xsubj ) noexcept
{
  char       buf[ 1024 ];
  PatternCvt cvt( buf, sizeof( buf ) );
  uint32_t   h, rcnt;

  if ( cvt.convert_rv( xsubj.str, xsubj.len ) == 0 ) {
    h = kv_crc_c( xsubj.str, cvt.prefixlen,
                  this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
    rcnt = this->poll.sub_route.del_pattern_route( h, this->fd,
                                                   cvt.prefixlen );
    this->poll.notify_punsub( h, buf, cvt.off, xsubj.str, cvt.prefixlen,
                              this->fd, rcnt, 'N' );
    this->map.rem_wild( h, xsubj );
  }
}

void
EvNatsService::rem_sid( uint32_t max_msgs ) noexcept
{
  NatsStr       xsid( this->sid, this->sid_len ),
                xsubj;
  NatsLookup    look;
  uint32_t      exp_cnt  = 0,
                subj_cnt = 0;
  NatsSubStatus status;

  if ( this->map.lookup_sid( xsid, look ) == NATS_OK ) {
    while ( look.iter.get( xsubj ) ) {
      uint32_t h = xsubj.hash();
      status = this->map.expire_subj( xsubj, xsid, max_msgs );
      if ( status == NATS_EXPIRED ) {
        if ( xsubj.is_wild() )
          this->rem_wild( xsubj );
        else {
          uint32_t rcnt = 0;
          if ( this->map.sub_map.find_by_hash( h ) == NULL )
            rcnt = this->poll.sub_route.del_route( h, this->fd );
          this->poll.notify_unsub( h, xsubj.str, xsubj.len,
                                   this->fd, rcnt, 'N' );
        }
      }
      if ( status != NATS_OK ) {
        exp_cnt++;
        look.iter.remove();
      }
      else {
        look.iter.incr();
      }
      subj_cnt++;
    }
    /* no more subs left */
    if ( max_msgs == 0 || exp_cnt == subj_cnt )
      this->map.sid_map.remove( look.loc );
    else { /* wait for max msgs */
      look.rt->max_msgs = max_msgs;
      if ( exp_cnt > 0 ) {
        NatsPair val( xsid );
        uint16_t new_len = look.rt->len - look.iter.rmlen;
        this->map.sid_map.resize( xsid.hash(), &val, look.rt->len, new_len,
                                  look.loc );
      }
    }
  }
#if 0
  printf( "rem_sid(%u):\n", max_msgs );
  this->map.print();
#endif
}

void
EvNatsService::rem_all_sub( void ) noexcept
{
  NatsMapRec * rt;
  uint32_t     i;
  uint16_t     off;
  NatsStr      xsubj;

  for ( rt = this->map.sub_map.first( i, off ); rt != NULL;
        rt = this->map.sub_map.next( i, off ) ) {
    xsubj.read( rt->value );
    if ( xsubj.is_wild() )
      this->rem_wild( xsubj );
    else {
      uint32_t rcnt = this->poll.sub_route.del_route( rt->hash, this->fd );
      this->poll.notify_unsub( rt->hash, xsubj.str, xsubj.len,
                               this->fd, rcnt, 'N' );
    }
  }
}

bool
EvNatsService::fwd_pub( void ) noexcept
{
  NatsStr   xsub( this->subject, this->subject_len );
  EvPublish pub( this->subject, this->subject_len,
                 this->reply, this->reply_len,
                 this->msg_ptr, this->msg_len,
                 this->fd, xsub.hash(),
                 this->msg_len_ptr, this->msg_len_digits,
                 MD_STRING, 'p' );
  return this->poll.forward_msg( pub, NULL, 0, NULL );
}

bool
EvNatsService::on_msg( EvPublish &pub ) noexcept
{
  bool flow_good = true;

  if ( this->echo || (uint32_t) this->fd != pub.src_route ) {
    for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
      NatsStr       xsid;
      NatsLookup    look;
      NatsSubStatus status;
      
      if ( pub.subj_hash == pub.hash[ cnt ] ) {
        NatsStr xsubj( pub.subject, pub.subject_len, pub.hash[ cnt ] );
        status = this->map.lookup_publish( xsubj, look );
        if ( status != NATS_NOT_FOUND ) {
          for ( ; look.iter.get( xsid ); look.iter.incr() ) {
            flow_good &= this->fwd_msg( pub, xsid.str, xsid.len );
          }
        }
        if ( status == NATS_EXPIRED ) {
          uint32_t h = pub.subj_hash;
          if ( this->map.expire_publish( xsubj, look ) == NATS_EXPIRED ) {
            uint32_t rcnt = 0;
            /* check for duplicate hashes */
            if ( this->map.sub_map.find_by_hash( h ) == NULL )
              rcnt = this->poll.sub_route.del_route( h, this->fd );
            this->poll.notify_unsub( h, xsubj.str, xsubj.len,
                                     this->fd, rcnt, 'N' );
          }
        }
      }
      else {
        NatsWildRec * rt = NULL;
        RouteLoc      loc;
        uint32_t      h  = pub.hash[ cnt ];
        rt = this->map.wild_map.find_by_hash( h, loc );
        for (;;) {
          if ( rt == NULL )
            break;
          if ( pcre2_match( rt->re, (const uint8_t *) pub.subject,
                            pub.subject_len, 0, 0, rt->md, 0 ) == 1 ) {
            NatsStr xsubj( rt->value, rt->len, rt->subj_hash );
            status = this->map.lookup_publish( xsubj, look );
            if ( status != NATS_NOT_FOUND ) {
              for ( ; look.iter.get( xsid ); look.iter.incr() ) {
                flow_good &= this->fwd_msg( pub, xsid.str, xsid.len );
              }
            }
            if ( status == NATS_EXPIRED ) {
              if ( this->map.expire_publish( xsubj, look ) == NATS_EXPIRED )
                this->rem_wild( xsubj );
            }
          }
          rt = this->map.wild_map.find_next_by_hash( h, loc );
        }
      }
    }
  }
#if 0
  printf( "publish:\n" );
  this->map.print();
#endif
  return flow_good;
}

bool
EvNatsService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  NatsMapRec * rt;
  if ( (rt = this->map.sub_map.find_by_hash( h )) != NULL ) {
    NatsStr xsub;
    xsub.read( rt->value );
    ::memcpy( key, xsub.str, xsub.len );
    keylen = rt->len;
    return true;
  }
  return false;
}

static inline char *
concat_hdr( char *p,  const char *q,  size_t sz )
{
  do { *p++ = *q++; } while ( --sz > 0 );
  return p;
}

bool
EvNatsService::fwd_msg( EvPublish &pub,  const void *sid,
                        size_t sid_len ) noexcept
{
  size_t msg_len_digits =
           ( pub.msg_len_digits > 0 ? pub.msg_len_digits :
             uint_digits( pub.msg_len ) );
  size_t len = 4 +                                  /* MSG */
               pub.subject_len + 1 +                /* <subject> */
               sid_len + 1 +                        /* <sid> */
    ( pub.reply_len > 0 ? pub.reply_len + 1 : 0 ) + /* [reply] */
               msg_len_digits + 2 +             /* <size> \r\n */
               pub.msg_len + 2;                     /* <blob> \r\n */
  char *p = this->alloc( len );

  p = concat_hdr( p, "MSG ", 4 );
  p = concat_hdr( p, pub.subject, pub.subject_len );
  *p++ = ' ';
  p = concat_hdr( p, (const char *) sid, sid_len );
  *p++ = ' ';
  if ( pub.reply_len > 0 ) {
    p = concat_hdr( p, (const char *) pub.reply, pub.reply_len );
    *p++ = ' ';
  }
  if ( pub.msg_len_digits == 0 ) {
    uint_to_str( pub.msg_len, p, msg_len_digits );
    p = &p[ msg_len_digits ];
  }
  else {
    p = concat_hdr( p, pub.msg_len_buf, msg_len_digits );
  }

  *p++ = '\r'; *p++ = '\n';
  ::memcpy( p, pub.msg, pub.msg_len );
  p += pub.msg_len;
  *p++ = '\r'; *p++ = '\n';

  this->sz += len;
  bool flow_good = ( this->pending() <= this->send_highwater );
  this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
  return flow_good;
}

#if 0
  if ( this->reply_len > 0 )
    printf( "fwd [%.*s] [reply=%.*s] nbytes %ld\n",
            (int) this->subject_len, this->subject,
            (int) this->reply_len, this->reply, this->msg_len );
  else
    printf( "fwd [%.*s] nbytes %ld\n", (int) this->subject_len, this->subject,
            this->msg_len );
#endif

static bool connect_parse_bool( const char *b ) { return *b == 't'; }
static size_t connect_parse_string( const char *b, const char *e,  char *s,
                                    size_t max_sz ) {
  /* b = start of string, e = comma or bracket after end of string */
  char * start = s;
  if ( max_sz == 0 )
    return 0;
  if ( max_sz > 1 ) {
    if ( *b++ == '\"' ) {
      while ( e > b )
        if ( *--e == '\"' )
          break;
      while ( b < e ) {
        *s++ = *b++;
        if ( --max_sz == 1 )
          break;
      }
    }
  }
  *s++ = '\0';
  return s - start;
}
static int connect_parse_int( const char *b ) {
  return ( *b >= '0' && *b <= '9' ) ? ( *b - '0' ) : 0;
}

void
EvNatsService::parse_connect( const char *buf,  size_t sz ) noexcept
{
  const char * start, * end, * p, * v, * comma;
  char * strbuf = this->buffer, **ptr = NULL;
  size_t used, buflen = sizeof( this->buffer );

  /* find start and end {} */
  if ( (start = (const char *) ::memchr( buf, '{', sz )) == NULL )
    return;
  sz -= start - buf;
  for ( end = &start[ sz ]; end > start; )
    if ( *--end == '}' )
      break;
  if ( end - start < 6 )
    return;
  /* CONNECT {\"user\":\"derek\",\"pass\":\"foo\",\"name\":\"router\"}\r\n */
  for ( p = start; p < end; p = comma ) {
    while ( *++p != '\"' ) /* field name */
      if ( p == end )
        goto finished;
    /* p = start of field name, v = start of value, comma = end of value
     * "user":"derek",
     * ^     ^       ^
     * |     |       |
     * p     v       comma */
    v = (const char *) ::memchr( &p[ 1 ], ':', end - &p[ 1 ] ); /* value */
    if ( v == NULL || v - p < 6 ) /* field name must be at least 4 chars */
      goto finished;
    v++;
    /* should scan string, ',' may occur in the value and split it */
    comma = (const char *) ::memchr( v, ',', end - v );
    if ( comma == NULL )
      comma = end;
    used = 0;
    switch ( ( unaligned<uint32_t>( &p[ 1 ] ) ) & 0xdfdfdfdf ) {
      case NATS_JS_VERBOSE:
        this->verbose     = connect_parse_bool( v ); break;
      case NATS_JS_PEDANTIC:
        this->pedantic    = connect_parse_bool( v ); break;
      case NATS_JS_TLS_REQUIRE:
        this->tls_require = connect_parse_bool( v ); break;
      case NATS_JS_ECHO:
        this->echo        = connect_parse_bool( v ); break;
      case NATS_JS_PROTOCOL:
        this->protocol    = connect_parse_int( v ); break;
      case NATS_JS_NAME:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->name;
        break;
      case NATS_JS_LANG:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->lang;
        break;
      case NATS_JS_VERSION:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->version;
        break;
      case NATS_JS_USER:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->user;
        break;
      case NATS_JS_PASS:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->pass;
        break;
      case NATS_JS_AUTH_TOKEN:
        used = connect_parse_string( v, comma, strbuf, buflen );
        ptr  = &this->auth_token;
        break;
      default: break;
    }
    if ( used > 0 ) {
      buflen -= used; /* put these strings at the end of buffer[] */
      ::memmove( &strbuf[ buflen ], strbuf, used );
      *ptr    = &strbuf[ buflen ];
    }
  }
finished:; /* update amount of buffer available */
  /*printf( "verbose %s pedantic %s echo %s\n",
           this->verbose ? "t" : "f", this->pedantic ? "t" : "f",
           this->echo ? "t" : "f" );*/
  this->tmp_size = buflen;
}

void
EvNatsService::release( void ) noexcept
{
  //printf( "nats release fd=%d\n", this->fd );
  this->rem_all_sub();
  this->map.release();
  this->EvConnection::release_buffers();
  this->push_free_list();
}

void
EvNatsService::push_free_list( void ) noexcept
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "nats sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_nats.push_hd( this );
  }
}

void
EvNatsService::pop_free_list( void ) noexcept
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_nats.pop( this );
  }
}

