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
#include <raids/redis_hash.h>
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
        ::strncpy( &nats_server_info[ 84 ], ip, len );
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
      this->pop( EV_READ );
    }
    return;
  }
  EvNatsService * c;
  if ( (c = (EvNatsService *) this->poll.free_nats) != NULL )
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
  c->fd = sock;

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
  if ( c->add_poll() < 0 ) {
    ::close( sock );
    c->push_free_list();
  }
  c->initialize_state();
  c->append_iov( nats_server_info, sizeof( nats_server_info ) );
  c->push( EV_WRITE );
  printf( "nats accept fd=%d\n", c->fd );
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
  enum { DO_OK = 1, DO_ERR = 2, NEED_MORE = 4, FLOW_BACKPRESSURE = 8 };
  static const char ok[]   = "+OK\r\n",
                    err[]  = "-ERR\r\n",
                    pong[] = "PONG\r\n";
  size_t            buflen, used, sz, nargs, size_len;
  char            * p, * eol, * start, * end, * size_start;
  char            * args[ MAX_NATS_ARGS ];
  size_t            argslen[ MAX_NATS_ARGS ];
  int               fl, verb_ok, cmd_cnt, msg_cnt;

  for (;;) { 
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    if ( this->idx + this->vlen / 4 >= this->vlen ) {
      if ( this->try_write() == 0 || this->idx + 8 >= this->vlen )
        goto need_write;
    }
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
                break;
            /*case NATS_KW_PONG:    break;
              case NATS_KW_INFO:    break;*/
              case NATS_KW_UNSUB: /* UNSUB <sid> [max-msgs] */
                nargs = parse_args( &p[ 6 ], eol, args, argslen );
                if ( nargs != 1 ) {
                  fl |= DO_ERR;
                  break;
                }
                this->sid     = args[ 0 ];
                this->sid_len = argslen[ 0 ];
                this->rem_sub();
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
      if ( ( fl & NEED_MORE ) != 0 )
        break;
      if ( ( fl & DO_OK ) != 0 )
        this->append( ok, sizeof( ok ) - 1 );
      if ( ( fl & DO_ERR ) != 0 )
        this->append( err, sizeof( err ) - 1 );
      if ( ( fl & FLOW_BACKPRESSURE ) != 0 ) {
        this->off += used;
        goto back_pressure;
      }
      fl = 0;
    }
    this->off += used;
    if ( ( fl & NEED_MORE ) != 0 ) {
      if ( cmd_cnt + msg_cnt != 0 || ! this->try_read() )
        goto break_loop;
    }
    else if ( used == 0 )
      goto break_loop;
  }
break_loop:;
  this->pop( EV_PROCESS );
back_pressure:;
  if ( this->pending() > 0 ) {
need_write:;
    this->push( EV_WRITE );
  }
  return;
}

HashData *
EvNatsService::resize_tab( HashData *curr,  size_t add_len )
{
  size_t count    = ( add_len >> 3 ) | 1,
         data_len = add_len + 1;
  if ( curr != NULL ) {
    data_len  = add_len + curr->data_len();
    data_len += data_len / 2 + 2;
    count     = curr->count();
    count    += count / 2 + 2;
  }
  size_t asize = HashData::alloc_size( count, data_len );
  void * m     = ::malloc( sizeof( HashData ) + asize );
  void * p     = &((char *) m)[ sizeof( HashData ) ];
  HashData *newbe = new ( m ) HashData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    curr->copy( *newbe );
    delete curr;
  }
  return newbe;
}

void
EvNatsService::add_sub( void )
{
  HashPos    sub_pos, sid_pos;
  size_t     sz    = this->sid_len + this->subject_len,
             retry = 1;
  HashStatus hstat = HASH_OK;
  /* SUB <subject> [queue group] <sid> */
  sid_pos.init( this->sid, this->sid_len );
  for (;;) {
    if ( this->sid_tab != NULL ) {
      hstat = this->sid_tab->hset( this->sid, this->sid_len,
                                   this->subject, this->subject_len, sid_pos );
      if ( hstat != HASH_FULL )
        break;
    }
    if ( this->sid_tab == NULL || hstat == HASH_FULL ) {
      this->sid_tab = this->resize_tab( this->sid_tab, sz + retry );
      retry += 9;
    }
  }

  sub_pos.init( this->subject, this->subject_len );
  for (;;) {
    if ( this->sub_tab != NULL ) {
      hstat = this->sub_tab->hset( this->subject, this->subject_len,
                                   this->sid, this->sid_len, sub_pos );
      if ( hstat != HASH_FULL )
        break;
    }
    if ( this->sub_tab == NULL || hstat == HASH_FULL ) {
      this->sub_tab = this->resize_tab( this->sub_tab, sz + retry );
      retry += 9;
    }
  }
  this->poll.sub_route.add_route( sub_pos.h, this->fd );
}

void
EvNatsService::rem_sub( void )
{
  HashPos    sub_pos, sid_pos;
  ListVal    lv;
  char       buf[ 256 ];
  void     * subj;
  size_t     subj_len;
  HashStatus hstat;
  bool       is_alloced = false;
  /* UNSUB <sid> [max-msgs] */
  if ( this->sid_tab != NULL ) {
    sid_pos.init( this->sid, this->sid_len );
    hstat = this->sid_tab->hget( this->sid, this->sid_len, lv, sid_pos );
    if ( hstat == HASH_OK ) {
      if ( this->sub_tab != NULL ) {
        subj_len = lv.unitary( subj, buf, sizeof( buf ), is_alloced );
        sub_pos.init( subj, subj_len );
        this->sub_tab->hdel( subj, subj_len, sub_pos );
        if ( is_alloced )
          ::free( subj );
        this->poll.sub_route.del_route( sub_pos.h, this->fd );
      }
      this->sid_tab->hrem( sid_pos.i );
    }
  }
}

void
EvNatsService::rem_all_sub( void )
{
  if ( this->sub_tab != NULL ) {
    HashVal kv;
    HashPos sub_pos;
    size_t count = this->sub_tab->hcount();
    for ( size_t i = 1; i <= count; i++ ) {
      HashStatus hstat = this->sub_tab->hindex( i, kv );
      if ( hstat == HASH_OK ) {
        sub_pos.init( kv.key, kv.keylen );
        this->poll.sub_route.del_route( sub_pos.h, this->fd );
      }
    }
    this->sub_tab->hremall();
  }
  if ( this->sid_tab != NULL )
    this->sid_tab->hremall();
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
  ListVal    lv;
  char       buf[ 256 ];
  void     * sid;
  size_t     sid_len;
  HashStatus hstat;
  bool       is_alloced = false,
             flow_good = true;

  if ( ( this->echo || (uint32_t) this->fd != pub.src_route ) &&
       this->sub_tab != NULL ) {
    HashPos sub_pos( pub.subj_hash );
    hstat = this->sub_tab->hget( pub.subject, pub.subject_len, lv, sub_pos );
    if ( hstat == HASH_OK ) {
      sid_len = lv.unitary( sid, buf, sizeof( buf ), is_alloced );
      flow_good = this->fwd_msg( pub, sid, sid_len );
      if ( is_alloced )
        ::free( sid );
    }
  }
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
  this->push( EV_WRITE );
  return this->pending() < 80 * 1024;
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
  printf( "verbose %s pedantic %s echo %s\n",
           this->verbose ? "t" : "f", this->pedantic ? "t" : "f",
           this->echo ? "t" : "f" );
  this->tmp_size = buflen;
}

void
EvNatsService::release( void )
{
  printf( "nats release fd=%d\n", this->fd );
  this->rem_all_sub();
  if ( this->sub_tab != NULL ) {
    ::free( this->sub_tab );
    this->sub_tab = NULL;
  }
  if ( this->sid_tab != NULL ) {
    ::free( this->sid_tab );
    this->sid_tab = NULL;
  }
  this->EvConnection::release();
  this->push_free_list();
}

void
EvNatsService::push_free_list( void )
{
  if ( this->state != 0 )
    this->popall();
  this->next[ 0 ] = this->poll.free_nats;
  this->poll.free_nats = this;
}

void
EvNatsService::pop_free_list( void )
{
  this->poll.free_nats = this->next[ 0 ];
  this->next[ 0 ] = NULL;
}


