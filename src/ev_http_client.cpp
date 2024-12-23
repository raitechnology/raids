#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/int_str.h>
#include <raids/ev_http_client.h>

using namespace rai;
using namespace kv;
using namespace ds;
using namespace md;

uint32_t rai::ds::ws_debug = 0;

HttpClient::HttpClient( kv::EvPoll &p,  kv::RoutePublish &sr,
                        kv::EvConnectionNotify *n ) noexcept
    : ds::SSL_Connection( p, p.register_type( "httpclient" ), n ),
      RouteNotify( sr ), sub_route( sr ), cb( 0 ), host( 0 ), host_len( 0 ),
      ws_mask( 0 ), ws_bytes_sent( 0 ), ht_state( HT_INIT ),
      ht_target( HT_DATA ), args_count( 0 )
{
  this->rand.init();
}

HttpClient::HttpClient( kv::EvPoll &p ) noexcept
    : ds::SSL_Connection( p, p.register_type( "httpclient" ) ),
      RouteNotify( p.sub_route ), sub_route( p.sub_route ), cb( 0 ),
      host( 0 ), host_len( 0 ), ws_mask( 0 ), ws_bytes_sent( 0 ),
      ht_state( HT_INIT ), ht_target( HT_DATA ), args_count( 0 )
{
  this->rand.init();
}
/* restart the protocol parser */
void
HttpClient::initialize_state( void ) noexcept
{
  this->cb            = NULL;
  this->ws_mask       = 0;
  this->ws_bytes_sent = 0;
  this->ht_state      = HT_INIT;
  this->ht_target     = HT_DATA;
}

int
HttpClient::connect( EvConnectParam &param ) noexcept
{
  HttpClientParameters parm2;
  SSL_Context  ctx;
  const char * crt_file = NULL,
             * key_file = NULL,
             * ca_file  = NULL,
             * ca_dir   = NULL;
  bool         ssl      = false;
  parm2.ai     = param.ai;
  parm2.k      = param.k;
  parm2.opts   = param.opts;
  parm2.rte_id = param.rte_id;

  for ( int i = 0; i + 1 < param.argc; i += 2 ) {
    if ( ::strcmp( param.argv[ i ], "daemon" ) == 0 ||
         ::strcmp( param.argv[ i ], "connect" ) == 0 ||
         ::strcmp( param.argv[ i ], "host" ) == 0 ||
         ::strcmp( param.argv[ i ], "url" ) == 0 )
      parm2.addr = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "port" ) == 0 )
      parm2.port = atoi( param.argv[ i + 1 ] );
    else if ( ::strcmp( param.argv[ i ], "crt_file" ) == 0 )
      crt_file = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "key_file" ) == 0 )
      key_file = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "ca_file" ) == 0 )
      ca_file = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "ca_dir" ) == 0 )
      ca_dir = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "ssl" ) == 0 ) {
      const char * str = param.argv[ i + 1 ];
      ssl = parse_bool( str, strlen( str ) );
    }
    else if ( ::strcmp( param.argv[ i ], "websock" ) == 0 ||
              ::strcmp( param.argv[ i ], "websocket" ) == 0 ) {
      const char * str = param.argv[ i + 1 ];
      parm2.is_websock = parse_bool( str, strlen( str ) );
    }
  }
  if ( parm2.addr != NULL ) {
    if ( ::strncmp( parm2.addr, "wss:", 4 ) == 0 ||
         ::strncmp( parm2.addr, "https:", 6 ) == 0 )
      ssl = true;
  }
  static const bool is_client = true;
  if ( crt_file != NULL || key_file != NULL || ca_file  != NULL ||
       ca_dir   != NULL ) {
    SSL_Config cfg( crt_file, key_file, ca_file, ca_dir, is_client, false );
    if ( ! ctx.init_config( cfg ) )
      return -1;
    parm2.ctx = &ctx;
  }
  else if ( ssl ) {
    SSL_Config cfg( NULL, NULL, NULL, NULL, is_client, false );
    cfg.no_verify = true;
    if ( ! ctx.init_config( cfg ) )
      return -1;
    parm2.ctx = &ctx;
  }
  if ( this->ht_connect( parm2, param.n, NULL ) )
    return 0;
  return -1;
}

static bool
is_digits_str( const char *s ) noexcept
{
  while ( *s >= '0' && *s <= '9' )
    s++;
  return *s == '\0';
}

bool
HttpClient::ht_connect( HttpClientParameters &p,
                        EvConnectionNotify *n,
                        HttpClientCB *c ) noexcept
{
  char * conn = NULL, buf[ 256 ];
  size_t addr_len = 0;
  int port = p.port;
  if ( this->fd != -1 )
    return false;
  this->initialize_state();
  if ( p.addr != NULL ) {
    addr_len = ::strlen( p.addr );
    size_t len = addr_len >= sizeof( buf ) ? sizeof( buf ) - 1 : addr_len;
    ::memcpy( buf, p.addr, len );
    buf[ len ] = '\0';
    conn = buf;
  }
  if ( conn != NULL ) {
    char * pt;
    if ( (pt = ::strrchr( conn, ':' )) != NULL && is_digits_str( &pt[ 1 ] ) ) {
      port = atoi( pt + 1 );
      *pt = '\0';
    }
    else if ( is_digits_str( conn ) ) {
      port = atoi( conn );
      conn = NULL;
    }
    if ( conn != NULL ) { /* strip tcp: prefix */
      if ( ::strncmp( conn, "tcp:", 4 ) == 0 )
        conn += 4;
      else if ( ::strncmp( conn, "http:", 5 ) == 0 ) {
        conn += 5;
        this->ht_target = HT_DATA;
      }
      else if ( ::strncmp( conn, "https:", 6 ) == 0 ) {
        conn += 6;
        this->ht_target = HT_DATA;
      }
      else if ( ::strncmp( conn, "ws:", 3 ) == 0 ) {
        conn += 3;
        this->ht_target = HT_WEBSOCK;
      }
      else if ( ::strncmp( conn, "wss:", 4 ) == 0 ) {
        conn += 4;
        this->ht_target = HT_WEBSOCK;
      }
      else if ( ::strcmp( conn, "tcp" ) == 0 )
        conn += 3;
      if ( conn[ 0 ] == '/' ) {
        conn++;
        if ( conn[ 0 ] == '/' )
          conn++;
      }
      if ( conn[ 0 ] == '\0' )
        conn = NULL;
    }
  }
  if ( p.is_websock )
    this->ht_target = HT_WEBSOCK;
  if ( p.ai == NULL ) {
    if ( port == 0 )
      port = ( p.ctx == NULL ? 80 : 443 );
    if ( EvTcpConnection::connect( *this, conn, port, p.opts ) != 0 ) {
      this->ht_state = HT_CLOSE;
      return false;
    }
  }
  else {
    EvConnectParam param( p.ai, p.opts, p.k, p.rte_id );
    if ( EvTcpConnection::connect3( *this, param ) != 0 ) {
      this->ht_state = HT_CLOSE;
      return false;
    }
  }
  size_t len;
  if ( conn != NULL )
    len = ::strlen( conn );
  else
    len = this->peer_address.len();
  this->host = (char *) ::malloc( len + 1 );
  ::memcpy( this->host, conn ? conn : 
            this->peer_address.buf, len );
  this->host[ len ] = '\0';
  this->host_len = len;

  if ( p.ctx != NULL ) {
    if ( ! this->init_ssl_connect( *p.ctx ) ) {
      this->ht_state = HT_CLOSE;
      return false;
    }
    this->ht_state = HT_SSL;
  }
  else {
    this->ht_state = HT_DATA;
    if ( this->ht_target == HT_WEBSOCK )
      this->send_websocket_upgrade();
  }
  if ( n != NULL )
    this->notify = n;
  if ( c != NULL )
    this->cb = c;
  if ( this->ht_state == this->ht_target && this->notify != NULL )
    this->notify->on_connect( *this );
  return true;
}


void
HttpClient::ssl_init_finished( void ) noexcept
{
  this->ht_state = HT_DATA;
  if ( this->ht_target == HT_WEBSOCK )
    this->send_websocket_upgrade();
  if ( this->ht_state == this->ht_target && this->notify != NULL )
    this->notify->on_connect( *this );
}

void
HttpClient::send_websocket_upgrade( void ) noexcept
{
  this->send_request2( a(
    "GET / HTTP/1.1\r\n"
    "Host: " ), a( this->host, this->host_len ), a( "\r\n"
    "Upgrade: websocket\r\n"
    "Connection: Upgrade\r\n"
    "Sec-WebSocket-Key: " ), a( ws_key, sizeof( ws_key ) - 1 ), a( "\r\n"
    "Sec-WebSocket-Version: 13\r\n"
    /* Sec-WebSocket-Protocol: proto\r\n */
    "\r\n" ), NULL );
}

void
HttpClient::process( void ) noexcept
{
#if 0
  size_t buflen = this->len - this->off;
  printf( "%.*s", (int) buflen, &this->recv[ this->off ] );
  this->msgs_recv++;
#endif
  if ( this->ht_state == HT_WEBSOCK ) {
    if ( this->process_websock() )
      goto is_closed;
  }   
  else {
    if ( this->process_http() )
      goto is_closed;
  }
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push_write();
  return;
      
is_closed:;
  this->pushpop( EV_SHUTDOWN, EV_PROCESS );
  return;
}

bool
HttpClient::process_http( void ) noexcept
{
  static const size_t MAX_HTTP_REQUEST_LEN = 65 * 1024;
  for (;;) {
    char   * start,
           * end;
    size_t   buflen = this->len - this->off;

    if ( buflen == 0 )
      return false;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    size_t  used         = 0;    /* amount eaten */
    char  * http_request = NULL; /* the first line: GET / HTTP/1.1 */
    size_t  request_len  = 0;    /* len of request line */
    HttpRsp hrsp;        /* the parameters after the first line */
    /* decode http hdrs */
    for ( char *p = start; p < end; ) {
      char * eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
      if ( eol == NULL ) {
        /* if request is bigger than MAX, close the connection */
        if ( (size_t) ( end - p ) > MAX_HTTP_REQUEST_LEN )
          return true;
        return false; /* didn't see the empty line */
      }
      size_t size = &eol[ 1 ] - p;
      if ( size <= 2 ) { /* found the empty line following hdr (\r\n) */
        if ( http_request == NULL ) /* no request */
          return true;
        used = &eol[ 1 ] - start;
        break;
      }
      if ( p == start ) {
        http_request = p;
        request_len  = size;
        if ( ! hrsp.parse_version( http_request, request_len ) )
          return true;
      }
      else {
        hrsp.parse_header( p, size );
      }
      p = &eol[ 1 ];
    }
    if ( ws_debug )
      printf( "<- [%.*s]\n", (int) used, start );
    hrsp.hdr     = start;
    hrsp.hdr_len = used;
    hrsp.data    = &start[ used ];
    if ( &start[ used + hrsp.content_length ] > end )
      return false;
    this->off += (uint32_t) ( used + hrsp.content_length );
    this->msgs_recv++;
    if ( ( hrsp.opts & HttpRsp::UPGRADE ) != 0 ) {
      this->ht_state = HT_WEBSOCK;
      this->ws_bytes_sent = this->bytes_sent;
      if ( this->ht_state == this->ht_target && this->notify != NULL )
        this->notify->on_connect( *this );
      if ( this->cb != NULL )
        this->cb->on_switch( hrsp );
      return false;
    }
    if ( this->cb != NULL )
      this->cb->on_http_msg( hrsp );
    if ( ( hrsp.opts & HttpRsp::KEEP_ALIVE ) == 0 )
      return true;
  }
}

void HttpClientCB::on_http_msg( HttpRsp & ) noexcept {}
void HttpClientCB::on_switch( HttpRsp & ) noexcept {}
void HttpClientCB::on_ws_msg( WSClientMsg & ) noexcept {}

bool
HttpClient::process_websock( void ) noexcept
{
  WSClientMsg ws;
  for (;;) {
    size_t buflen = this->len - this->off;
    char * start = &this->recv[ this->off ],
         * end   = &start[ buflen ];
    if ( buflen == 0 )
      return false;

    size_t hdrsize = ws.frame.decode( start, buflen );
    if ( hdrsize <= 1 ) {
      if ( hdrsize == 0 )
        return false;
      if ( ws_debug )
        printf( "ws_close\n" );
      return true;
    }
    char * p = &start[ hdrsize ];
    if ( &p[ ws.frame.payload_len ] > end )
      return false;
    if ( ws.frame.mask != 0 )
      ws.frame.apply_mask( p );
    ws.data = p;
    ws.len  = ws.frame.payload_len;
    if ( ws_debug )
      printf( "<- [%.*s]\n", (int) ws.len, p );
    size_t frame_size = ws.len + hdrsize;

    switch ( ws.frame.opcode ) {
      case WebSocketFrame::WS_PING:
        this->send_ws_pong( ws );
        break;
      case WebSocketFrame::WS_PONG:
        break;
      default: /* WS_TEXT, WS_BINARY */ 
        if ( this->cb != NULL )
          this->cb->on_ws_msg( ws );
        break;
    }
    this->off += frame_size;
  }
}

void
HttpClient::send_ws_pong( WSClientMsg &ping ) noexcept
{
  WebSocketFrame ws;

  if ( this->ws_mask == 0 )
    this->ws_mask = this->rand.next();
  ws.set( ping.len, this->ws_mask & 0xffffffffU, WebSocketFrame::WS_PONG, true );
  this->ws_mask >>= 32;
  char * p = this->alloc( WebSocketFrame::MAX_HEADER_SIZE + ping.len );
  size_t off = ws.encode( p );
  ::memcpy( &p[ off ], ping.data, ping.len );
  ws.apply_mask( &p[ off ] );
  this->sz += off + ping.len;
}

bool
HttpRsp::parse_version( const char *line,  size_t len ) noexcept
{
  size_t i;
  if ( len > 0 && line[ len - 1 ] == '\n' ) {
    len -= 1;
    if ( len > 0 && line[ len - 1 ] == '\r' )
      len -= 1;
  }
  if ( kv_strncasecmp( line, "HTTP", 4 ) != 0 )
    return false;
  if ( ::memcmp( &line[ 4 ], "/1.1 ", 5 ) == 0 ) {
    this->opts |= HTTP_1_1;
    i = 9;
  }
  else if ( ::memcmp( &line[ 4 ], "/2 ", 3 ) == 0 ) {
    i = 7;
    this->opts |= HTTP_1_1;
  }
  else if ( ::memcmp( &line[ 4 ], "/1.0 ", 5 ) == 0 ) {
    i = 9;
  }
  else {
    const char * p;
    if ( (p = (const char *) ::memchr( &line[ 4 ], ' ', len - 4 )) == NULL )
      return false;
    i = &p[ 1 ] - line;
  }
  for ( this->http_code = 0; i < len; i++ ) {
    if ( line[ i ] >= '0' && line[ i ] <= '9' )
      this->http_code = this->http_code * 10 + ( line[ i ] - '0' );
    else if ( line[ i ] != ' ' )
      break;
  }
  this->http_code_string = &line[ i ];
  this->http_code_length = &line[ len ] - &line[ i ];
  return true;
}

void
HttpRsp::parse_header( const char *line,  size_t len ) noexcept
{
  size_t i;
  if ( len > 0 && line[ len - 1 ] == '\n' ) {
    len -= 1;
    if ( len > 0 && line[ len - 1 ] == '\r' )
      len -= 1;
  }
  switch ( line[ 0 ] ) {
    case 'C': case 'c':  { /* Connection: */
      static const char   conn[]   = "Connection: ";
      static const size_t conn_len = sizeof( conn ) - 1;
      static const char   clen[]   = "Content-Length: ";
      static const size_t clen_len = sizeof( clen ) - 1;
      static const char   ctyp[]   = "Content-Type: ";
      static const size_t ctyp_len = sizeof( ctyp ) - 1;

      /* Connection: Close */
      if ( kv_strncasecmp( line, conn, conn_len ) == 0 ) {
        size_t k = conn_len;
        for (;;) {
          while ( k < len && line[ k ] == ' ' )
            k++;
          if ( k >= len )
            break;
          /* upgrade */
          if ( line[ k ] == 'U' || line[ k ] == 'u' ) {
            static const char upgrade[]   = "upgrade";
            static size_t     upgrade_len = sizeof( upgrade ) - 1;
            if ( len - k >= upgrade_len &&
                 ::kv_strncasecmp( &line[ k ], upgrade, upgrade_len ) == 0 )
              this->opts |= UPGRADE;
          }
          /* keep-alive */
          else if ( line[ k ] == 'K' || line[ k ] == 'k' ) {
            static const char keep[]   = "keep-alive";
            static size_t     keep_len = sizeof( keep ) - 1;
            if ( len - k >= keep_len &&
                 ::kv_strncasecmp( &line[ k ], keep, keep_len ) == 0 )
              this->opts |= KEEP_ALIVE;
          }
          /* close */
          else if ( line[ k ] == 'C' || line[ k ] == 'c' ) {
            static const char close[]   = "close";
            static size_t     close_len = sizeof( close ) - 1;
            if ( len - k >= close_len &&
                 ::kv_strncasecmp( &line[ k ], close, close_len ) == 0 )
              this->opts |= CLOSE;
          }
          const void * p;
          if ( (p = ::memchr( &line[ k ], ',', len - k )) == NULL )
            break;
          k = &((const char *) p)[ 1 ] - line;
        }
      }
      /* Content-Lenth: 1234 */
      else if ( kv_strncasecmp( line, clen, clen_len ) == 0 ) {
        for ( i = clen_len; line[ i ] >= '0' && line[ i ] <= '9'; i++ )
          ;
        string_to_uint( &line[ clen_len ], i - clen_len, this->content_length );
      }
      /* Content-Type: text/plain; charset=UTF-8 */
      else if ( kv_strncasecmp( line, ctyp, ctyp_len ) == 0 ) {
        len -= ctyp_len;
        for ( i = 0; i < len; i++ ) {
          if ( i == STR_SZ - 1 )
            break;
          if ( line[ i + ctyp_len ] <= ' ' || line[ i + ctyp_len ] == ';' )
            break;
          this->content_type[ i ] = line[ i + ctyp_len ];
        }
        this->content_type[ i ] = '\0';
      }
      break;
    }
    case 'U': case 'u':  { /* Upgrade: websocket */
      static const char   uwsk[]   = "Upgrade: websocket";
      static const size_t uwsk_len = sizeof( uwsk ) - 1;
      if ( kv_strncasecmp( line, uwsk, uwsk_len ) == 0 )
        this->opts |= WEBSOCKET;
      break;
    }
    case 'S': case 's':  { /* Sec-WebSocket-[Version,Key,Protocol] */
      static const char   secw[]   = "Sec-WebSocket-";
      static const size_t secw_len = sizeof( secw ) - 1;
      static const char   acc[]    = "Accept: ";
      static const size_t acc_len  = sizeof( acc ) - 1;
      static const char   prot[]   = "Protocol: ";
      static const size_t prot_len = sizeof( prot ) - 1;

      if ( kv_strncasecmp( line, secw, secw_len ) == 0 ) {
        const char * ptr   = &line[ secw_len ];
        char       * str   = NULL;
        size_t       start = 0;
        if ( kv_strncasecmp( ptr, acc, acc_len ) == 0 ) {
          str   = this->wsacc;
          start = secw_len + acc_len;
        }
        else if ( kv_strncasecmp( ptr, prot, prot_len ) == 0 ) {
          str   = this->wspro;
          start = secw_len + prot_len;
        }
        if ( str != NULL ) {
          len -= start;
          for ( i = 0; i < len; i++ ) {
            if ( i == STR_SZ - 1 )
              break;
            if ( line[ i + start ] <= ' ' )
              break;
            str[ i ] = line[ i + start ];
          }
          str[ i ] = '\0';
          if ( str == this->wsacc )
            this->wsacclen = i;
        }
      }
      break;
    }
    default:
      break;
  }
}

void
HttpClient::process_shutdown( void ) noexcept
{
  if ( ws_debug )
    printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
            this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
HttpClient::release( void ) noexcept
{
  if ( ws_debug )
    printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
            this->peer_address.buf );
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
  this->SSL_Connection::release_ssl();
  this->EvConnection::release_buffers();
  this->ht_state = HT_CLOSE;
  if ( this->host != NULL ) {
    ::free( this->host );
    this->host = NULL;
    this->host_len = 0;
  }
}

void
HttpClient::process_close( void ) noexcept
{
  if ( ws_debug )
    printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
            this->peer_address.buf );
  /*if ( this->poll.quit == 0 )
    this->poll.quit = 1;*/
  this->EvSocket::process_close();
}

bool
HttpClient::timer_expire( uint64_t, uint64_t ) noexcept
{
  /*this->send_ping();*/
  return true;
}

void
HttpClient::send_request2( const Arg *a,  ... ) noexcept
{
  size_t  nbytes = a->len;
  va_list va;
  const Arg * b;

  this->args_count = 0;
  va_start( va, a );
  for (;;) {
    if ( (b = va_arg( va, const Arg * )) == NULL )
      break;
    nbytes += b->len;
  }
  va_end( va );

  if ( this->ht_state == HT_WEBSOCK ) {
    if ( this->ws_mask == 0 )
      this->ws_mask = this->rand.next();

    uint32_t mask = this->ws_mask & 0xffffffffU;
    this->ws_mask >>= 32;

    WebSocketFrame ws;
    ws.set( nbytes, mask, WebSocketFrame::WS_TEXT, true );

    size_t hdrsz = ws.hdr_size();
    char * frame = this->alloc( hdrsz + nbytes );
    ws.encode( frame );

    char *p = &frame[ hdrsz ];
    ::memcpy( p, a->value, a->len );
    p += a->len;
    va_start( va, a );
    for (;;) {
      if ( (b = va_arg( va, const Arg * )) == NULL )
        break;
      ::memcpy( p, b->value, b->len );
      p += b->len;
    }
    va_end( va );
    if ( ws_debug )
      printf( "-> [%.*s]\n", (int) nbytes, &frame[ hdrsz ] );
    ws.apply_mask2( &frame[ hdrsz ], 0, nbytes );
    this->ws_bytes_sent += nbytes + hdrsz;
    this->sz += nbytes + hdrsz;
  }
  else {
    char * frame = this->alloc( nbytes ), * p = frame;
    ::memcpy( p, a->value, a->len );
    p += a->len;
    va_start( va, a );
    for (;;) {
      if ( (b = va_arg( va, const Arg * )) == NULL )
        break;
      ::memcpy( p, b->value, b->len );
      p += b->len;
    }
    va_end( va );
    if ( ws_debug )
      printf( "-> [%.*s]\n", (int) nbytes, frame );
    this->sz += nbytes;
  }
  this->msgs_sent++;
  this->idle_push( EV_WRITE );
}

void
HttpClient::send_request( const char *tmplate,  VarHT &ht ) noexcept
{
  const char   open = '(',
               clos = ')';
  const char * m    = tmplate,
             * e    = &m[ ::strlen( tmplate ) ];
  if ( ws_debug )
    printf( "-> [" );
  for (;;) {
    const char * p = (const char *) ::memchr( m, '@', e - m );
    if ( p == NULL ) {
      if ( ws_debug )
        printf( "%.*s]\n", (int) ( e - m ), m );
      this->append( m, e - m );
      break;
    }
    if ( &p[ 2 ] < e && p[ 1 ] == open ) {
      const char * s = (const char *) ::memchr( &p[ 2 ], clos, e - &p[ 2 ] );
      if ( s != NULL ) {
        Val var( &p[ 2 ], s - &p[ 2 ] ), val;
        ht.get( var, val );
        if ( ws_debug ) {
          printf( "%.*s", (int) ( p - m ), m );
          printf( "%.*s", (int) val.len, val.str );
        }
        this->append2( m, p - m, val.str, val.len );
        m = &s[ 1 ];
        continue;
      }
    }
    if ( ws_debug )
      printf( "%.*s", (int) ( &p[ 1 ] - m ), m );
    this->append( m, &p[ 1 ] - m );
    m = &p[ 1 ];
  }
  if ( this->ht_state == HT_WEBSOCK ) {
    size_t         nbytes,
                   off,
                   j, hdrsz;
    char         * frame;
    WebSocketFrame ws;

    this->flush();
    off    = this->idx;
    nbytes = this->iov[ --off ].iov_len;
    while ( this->ws_bytes_sent < this->bytes_sent + nbytes && off > 0 )
      nbytes += this->iov[ --off ].iov_len;

    if ( this->ws_mask == 0 )
      this->ws_mask = this->rand.next();
    ws.set( nbytes, this->ws_mask & 0xffffffffU, WebSocketFrame::WS_TEXT, true );
    this->ws_mask >>= 32;

    hdrsz = ws.hdr_size();
    frame = this->alloc_temp( hdrsz );
    ws.encode( frame );
    this->insert_iov( off++, frame, hdrsz );
    for ( j = 0; off < this->idx; off++ ) {
      j = ws.apply_mask2( this->iov[ off ].iov_base, j,
                          this->iov[ off ].iov_len );
    }
    this->ws_bytes_sent += nbytes + hdrsz;
  }
  this->msgs_sent++;
  this->idle_push( EV_WRITE );
}

size_t
HtReqArgs::template_size( const char *m,  const char *e ) noexcept
{
  size_t nbytes = 0;
  for (;;) {
    const char * p = (const char *) ::memchr( m, '@', e - m );
    if ( p == NULL ) {
      nbytes += ( e - m );
      return nbytes;
    }
    if ( &p[ 3 ] < e && p[ 1 ] == '(' && p[ 2 ] >= '0' && p[ 2 ] <= '9' &&
         p[ 3 ] == ')' ) {
      int i = p[ 2 ] - '0';
      nbytes += ( p - m );
      nbytes += this->arg[ i ].len;
      m = &p[ 4 ];
    }
    else {
      nbytes += &p[ 1 ] - m;
      m = &p[ 1 ];
    }
  }
}

void
HtReqArgs::template_copy( const char *m,  const char *e,  char *o ) noexcept
{
  for (;;) {
    size_t x = ( e - m );
    const char * p = (const char *) ::memchr( m, '@', x );
    if ( p == NULL ) {
      ::memcpy( o, m, x );
      return;
    }
    if ( &p[ 3 ] < e && p[ 1 ] == '(' && p[ 2 ] >= '0' && p[ 2 ] <= '9' &&
         p[ 3 ] == ')' ) {
      int i = p[ 2 ] - '0';
      x = ( p - m );
      ::memcpy( o, m, x );
      o += x;
      ::memcpy( o, this->arg[ i ].str, this->arg[ i ].len );
      o += this->arg[ i ].len;
      m = &p[ 4 ];
    }
    else {
      x = ( &p[ 1 ] - m );
      ::memcpy( o, m, x );
      o += x;
      m = &p[ 1 ];
    }
  }
}

void
HttpClient::send_request3( const char *tmplate,  HtReqArgs &args ) noexcept
{
  const char * m = tmplate,
             * e = &m[ ::strlen( tmplate ) ];
  size_t nbytes  = args.template_size( m, e );

  if ( this->ht_state == HT_WEBSOCK ) {
    if ( this->ws_mask == 0 )
      this->ws_mask = this->rand.next();

    uint32_t mask = this->ws_mask & 0xffffffffU;
    this->ws_mask >>= 32;

    WebSocketFrame ws;
    ws.set( nbytes, mask, WebSocketFrame::WS_TEXT, true );

    size_t hdrsz = ws.hdr_size();
    char * frame = this->alloc( hdrsz + nbytes );
    ws.encode( frame );

    args.template_copy( m, e, &frame[ hdrsz ] );
    if ( ws_debug )
      printf( "-> [%.*s]\n", (int) nbytes, &frame[ hdrsz ] );

    ws.apply_mask2( &frame[ hdrsz ], 0, nbytes );
    this->ws_bytes_sent += nbytes + hdrsz;
    this->sz += nbytes + hdrsz;
  }
  else {
    char * frame = this->alloc( nbytes );
    args.template_copy( m, e, frame );

    if ( ws_debug )
      printf( "-> [%.*s]\n", (int) nbytes, frame );
    this->sz += nbytes;
  }
  this->msgs_sent++;
  this->idle_push( EV_WRITE );
}

VarHT &
VarHT::add( const Pair &pair ) noexcept
{
  const Val & x = pair.x,
            & y = pair.y;
  uint32_t h = x.hash();
  size_t   m = this->sz - 1;

  if ( this->used >= this->sz / 2 ) {
    this->resize();
    m = this->sz - 1;
  }
  uint32_t k = h & m;
  for ( size_t i = 0; i < this->sz; i++ ) {
    if ( this->p[ k ].x.equals( x ) ) {
      this->p[ k ].y.copy( y );
      break;
    }
    if ( this->p[ k ].x.is_empty() ) {
      this->p[ k ].x.copy( x );
      this->p[ k ].y.copy( y );
      this->used++;
      break;
    }
    k = ( k + 1 ) & m;
  }
  return *this;
}

void
VarHT::resize( void ) noexcept
{
  size_t sz2    = ( this->sz == 0 ? 8 : this->sz * 2 ),
         mask   = sz2 - 1,
         old_sz = this->sz * sizeof( this->p[ 0 ] ),
         new_sz = sz2 * sizeof( this->p[ 0 ] );
  this->mem.extend( old_sz, new_sz, &this->p );

  for ( size_t i = this->sz; i < sz2; i++ )
    new ( &this->p[ i ] ) Pair();

  for ( size_t i = 0; i < sz2; i++ ) {
    if ( this->p[ i ].is_empty() ) {
      if ( i > this->sz )
        break;
      continue;
    }
    uint32_t h = this->p[ i ].hash();
    for ( size_t j = h & mask; ; ) {
      if ( j == i )
        break;
      if ( this->p[ j ].is_empty() ) {
        this->p[ j ].move( this->p[ i ] );
        break;
      }
      j = ( j + 1 ) & mask;
    }
  }
  this->sz = sz2;
}

bool
VarHT::get( const Val &x,  Val &y ) const noexcept
{
  uint32_t h = x.hash();
  size_t   m = this->sz - 1;
  uint32_t k = h & m;

  for ( size_t i = 0; i < this->sz; i++ ) {
    if ( this->p[ k ].x.equals( x ) ) {
      y.copy( this->p[ k ].y );
      return true;
    }
    if ( this->p[ k ].x.is_empty() )
      break;
    k = ( k + 1 ) & m;
  }
  y.zero();
  return false;
}
