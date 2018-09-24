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
#include <raids/redis_msg.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raids/ev_publish.h>

using namespace rai;
using namespace ds;
using namespace kv;
#define fallthrough __attribute__ ((fallthrough))

/* ID = 22*base62, 12 chars random prefix, 10 chars incrementing ID */
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

void
EvNatsListen::accept( void )
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
  EvNatsService * c = this->poll.free_nats.hd;
  if ( c != NULL )
    c->pop_free_list();
  else {
    void * m = aligned_malloc( sizeof( EvNatsService ) * EvPoll::ALLOC_INCR );
    if ( m == NULL ) {
      perror( "accept: no memory" );
      ::close( sock );
      return;
    }
    c = new ( m ) EvNatsService( this->poll );
    for ( int i = EvPoll::ALLOC_INCR - 1; i >= 1; i-- ) {
      new ( (void *) &c[ i ] ) EvNatsService( this->poll );
      c[ i ].push_free_list();
    }
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
    if ( ::getsockname( sock, (sockaddr *) &myaddr, &myaddrlen ) == 0 )
      port = ntohs( ((sockaddr_in *) &myaddr)->sin_port );
    this->poll.map->hdr.get_hash_seed( KV_DB_COUNT-1, h1, h2 );
    init_server_info( h1, h2, port );
  }
  c->fd = sock;
  c->initialize_state();
  c->idle_push( EV_WRITE_HI );
  if ( this->poll.add_sock( c ) < 0 ) {
    printf( "failed to add sock %d\n", sock );
    ::close( sock );
    c->push_free_list();
    return;
  }
  c->append_iov( nats_server_info, sizeof( nats_server_info ) );
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
EvNatsService::process( bool /*use_prefetch*/ )
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
EvNatsService::add_sub( void )
{
  /* case1:  sid doesn't exist, add sid to sid tab & sid to subject tab
   *   sid -> null / sid -> sid tab [ sid ],
   *                        sid -> subj tab [ subj ] */
  StrHashRec & sidrec = StrHashRec::make_rec( this->sid, this->sid_len ),
             & subrec = StrHashRec::make_rec( this->subject, this->subject_len);
  StrHashKey   sidkey( sidrec ),
               subkey( subrec );
  SidMsgCount  cnt( 0 );

  this->sid_tab.put( sidkey, cnt, subrec, true );
  if ( this->sub_tab.put( subkey, cnt, sidrec ) == NATS_IS_NEW )
    this->poll.sub_route.add_route( subkey.pos.h, this->fd );
#if 0
  printf( "add_sub:\n" );
  this->sid_tab.print();
  this->sub_tab.print();
#endif
}

void
EvNatsService::rem_sid( uint32_t max_msgs )
{
  StrHashRec & sidrec = StrHashRec::make_rec( this->sid, this->sid_len );
  StrHashKey   sidkey( sidrec );

  if ( max_msgs != 0 ) {
    SidMsgCount cnt( max_msgs );
    SidList     sub_list;
    /* update the max_msgs foreach sid -> subj */
    if ( this->sid_tab.updcnt( sidkey, cnt, NULL ) != NATS_IS_NEW ) {
      if ( this->sid_tab.lookup( sidkey, sub_list ) ) {
        if ( sub_list.first() ) {
          do {
            StrHashKey subkey( sub_list.sid );
            if ( this->sub_tab.updcnt( subkey, cnt,
                                       &sidrec ) == NATS_IS_EXPIRED ) {
              this->rem_sid_key( sidkey );
              break;
            }
          } while ( sub_list.next() );
        }
      }
    }
  }
  else {
    this->rem_sid_key( sidkey );
  }
#if 0
  printf( "rem_sid(%u):\n", max_msgs );
  this->sid_tab.print();
  this->sub_tab.print();
#endif
}

void
EvNatsService::rem_sid_key( StrHashKey &sidkey )
{
  SidList sub_list;
  /*printf( "rem_sid_key(%.*s)\n", (int) sidkey.rec.len, sidkey.rec.str );*/
  if ( this->sid_tab.lookup( sidkey, sub_list ) ) {
    if ( sub_list.first() ) {
      do {
        StrHashKey subkey( sub_list.sid );
        if ( this->sub_tab.deref( subkey, sidkey.rec ) ) {
        /*printf( "rem_route(%.*s)\n", (int) subkey.rec.len, subkey.rec.str );*/
          this->poll.sub_route.del_route( subkey.pos.h, this->fd );
        }
      } while ( sub_list.next() );
    }
    this->sid_tab.rem( sidkey );
  }
}

void
EvNatsService::rem_all_sub( void )
{
  HashPos pos;
  if ( this->sub_tab.first( pos ) ) {
    do {
      this->poll.sub_route.del_route( pos.h, this->fd );
    } while ( this->sub_tab.next( pos ) );
  }
}

bool
EvNatsService::fwd_pub( void )
{
  HashPos sub_pos;
  uint32_t * routes, rcnt;
  sub_pos.init( this->subject, this->subject_len );
  rcnt = this->poll.sub_route.get_route( sub_pos.h, routes );
  if ( rcnt > 0 ) {
    EvPublish pub( this->subject, this->subject_len,
                   this->reply, this->reply_len,
                   this->msg_ptr, this->msg_len,
                   routes, rcnt, this->fd, sub_pos.h,
                   this->msg_len_ptr, this->msg_len_digits );
    return this->poll.publish( pub );
  }
  return true;
}

bool
EvNatsService::publish( EvPublish &pub )
{
  bool flow_good   = true;
  int  sub_expired = 0;

  if ( this->echo || (uint32_t) this->fd != pub.src_route ) {
    SidList sid_list;
    HashPos sub_pos( pub.subj_hash );
    if ( this->sub_tab.lookup( pub.subject, pub.subject_len, sub_pos,
                               sid_list ) ) {
      if ( sid_list.first() ) {
        do {
          flow_good &= this->fwd_msg( pub, sid_list.sid.str, sid_list.sid.len );
          if ( ! sid_list.incr_msg_count() )
            sub_expired++;
        } while ( sid_list.next() );
      }
      if ( sub_expired > 0 ) {
        StrHashRec sidrec;
        for ( ; sub_expired > 0; sub_expired-- ) {
          if ( this->sub_tab.get_expired( pub.subject, pub.subject_len, sub_pos,
                                          sidrec ) ) {
            StrHashKey sidkey( sidrec );
            this->rem_sid_key( sidkey );
          }
        }
      }
    }
  }
#if 0
  printf( "publish:\n" );
  this->sid_tab.print();
  this->sub_tab.print();
#endif
  return flow_good;
}

static inline char *
concat_hdr( char *p,  const char *q,  size_t sz )
{
  do { *p++ = *q++; } while ( --sz > 0 );
  return p;
}

bool
EvNatsService::fwd_msg( EvPublish &pub,  const void *sid,  size_t sid_len )
{
  size_t len = 4 +                                  /* MSG */
               pub.subject_len + 1 +                /* <subject> */
               sid_len + 1 +                        /* <sid> */
    ( pub.reply_len > 0 ? pub.reply_len + 1 : 0 ) + /* [reply] */
               pub.msg_len_digits + 2 +             /* <size> \r\n */
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
  p = concat_hdr( p, pub.msg_len_buf, pub.msg_len_digits );

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
EvNatsService::parse_connect( const char *buf,  size_t sz )
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
EvNatsService::release( void )
{
  //printf( "nats release fd=%d\n", this->fd );
  this->rem_all_sub();
  this->sub_tab.release();
  this->sid_tab.release();
  this->EvConnection::release();
  this->push_free_list();
}

void
EvNatsService::push_free_list( void )
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "nats sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_nats.push_hd( this );
  }
}

void
EvNatsService::pop_free_list( void )
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_nats.pop( this );
  }
}


