#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <ctype.h>
#include <raikv/util.h>
#include <raikv/os_file.h>
#include <raids/ev_http.h>
#include <raids/http_auth.h>

using namespace rai;
using namespace ds;
using namespace kv;

static void SHA1( const void *data,  size_t len,
                  uint8_t digest[20] ) noexcept;

EvHttpListen::EvHttpListen( EvPoll &p ) noexcept
  : EvTcpListen( p, "http_listen", "http_sock" ),
    sub_route( p.sub_route ) {}

EvHttpListen::EvHttpListen( EvPoll &p,  RoutePublish &sr ) noexcept
  : EvTcpListen( p, "http_listen", "http_sock" ),
    sub_route( sr ) {}

EvSocket *
EvHttpListen::accept( void ) noexcept
{
#if 0
  static HtDigestDB * db;
  static HttpServerNonce * svr;

  if ( svr == NULL ) {
    char hostname[ 256 ];
    ::gethostname( hostname, sizeof( hostname ) );
    db = new ( ::malloc( sizeof( HtDigestDB ) ) ) HtDigestDB();
    db->set_realm( "raids", hostname );
    db->add_user_pass( "danger", "high", NULL );
    svr = new ( ::malloc( sizeof( HttpServerNonce ) ) ) HttpServerNonce();
    svr->regenerate();
  }
#endif
  EvHttpService *c =
    this->poll.get_free_list<EvHttpService, RoutePublish &>
      ( this->accept_sock_type, this->sub_route );
  if ( c == NULL )
    return NULL;
  if ( ! this->accept2( *c, "http" ) )
    return NULL;
  c->setup_ids( c->fd, ++this->timer_id );
  c->initialize_state( /*svr, db*/ );
  return c;
}

void
EvHttpConnection::process( void ) noexcept
{
  if ( this->websock_off > 0 ) {
    if ( this->process_websock() )
      goto is_closed;
    goto break_loop;
  }
  else if ( this->process_http() )
    goto is_closed;

break_loop:;
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push_write();
  return;

is_closed:;
  this->pushpop( EV_SHUTDOWN, EV_PROCESS );
  return;
}

bool
EvHttpConnection::process_http( void ) noexcept
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

    size_t   used         = 0;    /* amount eaten */
    char   * http_request = NULL; /* the first line: GET / HTTP/1.1 */
    size_t   request_len  = 0;    /* len of request line */
    HttpReq  hreq;        /* the parameters after the first line */
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
        if ( ! hreq.parse_version( http_request, request_len ) )
          return true;
      }
      else {
        hreq.parse_header( p, size );
      }
      p = &eol[ 1 ];
    }
    hreq.data = &start[ used ];
    if ( &start[ used + hreq.content_length ] > end )
      return false;
    this->off += (uint32_t) ( used + hreq.content_length );
    this->msgs_recv++;

    if ( this->digest_db != NULL ) {
      HttpDigestAuth auth( this->svr_nonce->nonce, this->digest_db );
      if ( hreq.authorize_len == 0 ) {
        this->send_401_unauthorized( hreq, auth );
        return false;
      }
      else {
        if ( ! auth.parse_auth( hreq.authorize, hreq.authorize_len, true ) ) {
          /*printf( "parse auth failed %s\n", auth.error() );*/
          this->send_401_unauthorized( hreq, auth );
          return false;
        }
        if ( ! auth.check_auth( hreq.method, hreq.method_len ) ) {
          /*printf( "check auth failed %s\n", auth.error() );*/
          this->send_401_unauthorized( hreq, auth );
          return false;
        }
      }
    }
    /* GET path HTTP/1.1 */
    switch ( http_request[ 0 ] ) {
      case 'g': /* GET */
      case 'G': {
        /* check for websock upgrade */
        if ( ( hreq.opts & HttpReq::UPGRADE )   != 0 &&
             ( hreq.opts & HttpReq::WEBSOCKET ) != 0 &&
             hreq.wsver[ 0 ] && hreq.wskey[ 0 ] /*&& wspro[ 0 ]*/ ) {
          if ( this->send_ws_upgrade( hreq ) ) {
            this->websock_off = this->bytes_sent + this->pending();
            if ( ::strncmp( hreq.wspro, "term", 4 ) == 0 ) {
              this->flush();
              this->term.tty_init();
              this->term.tty_prompt();
              this->is_using_term = true;
              this->flush_term();
            }
            return false; /* process websock frames now */
          }
          this->send_404_not_found( hreq, 0 );
        }
        else {
          /* do redis GET path */
          if ( ! this->process_get( hreq ) )
            this->send_404_not_found( hreq, 0 );
        }
        break;
      }
      case 'p':
      case 'P': /* POST, PUT */
        /* do POST path */
        if ( ! this->process_post( hreq ) )
          this->send_404_not_found( hreq, 0 );
        else
          this->send_201_created( hreq );
        break;

      default:
        this->send_404_not_found( hreq, HttpReq::CLOSE );
        return false;
    }
  }
}

static const char http_hdr11[] = "HTTP/1.1 ",
                  http_hdr10[] = "HTTP/1.0 ",
                  code_200[]   = "200 OK\r\n",
                  code_201[]   = "201 Created\r\n",
                  code_401[]   = "401 Unauthorized\r\n",
                  code_404[]   = "404 Not Found\r\n",
                  no_cache[]   = "Cache-Control: no-cache\r\n",
                  conn_close[] = "Connection: close\r\n",
                  keep_alive[] = "Connection: keep-alive\r\n",
                  ctype_resp[] = "Content-Type: application/x-resp\r\n",
                  ctype_json[] = "Content-Type: application/json\r\n",
                  ctype_html[] = "Content-Type: text/html\r\n",
                  ctype[]      = "Content-Type: ",
                  gzenc[]      = "Content-Encoding: gzip\r\n",
                  clength[]    = "Content-Length: ",
                  clength_40[] = "Content-Length: 40\r\n",
                  /*clength_0[]  = "Content-Length: 0\r\n",*/
                  location[]   = "Location: ";

/* initialize http response */
void
EvHttpConnection::init_http_response( const HttpReq &hreq,  HttpOut &hout,
                                   int opts,  int code ) noexcept
{
  hout.off = hout.size = 0;
  if ( ( hreq.opts & HttpReq::HTTP_1_1 ) != 0 )
    hout.push( http_hdr11, sizeof( http_hdr11 ) - 1 );
  else
    hout.push( http_hdr10, sizeof( http_hdr10 ) - 1 );
  if ( code == 200 )
    hout.push( code_200, sizeof( code_200 ) - 1 );
  else if ( code == 201 )
    hout.push( code_201, sizeof( code_201 ) - 1 );
  else if ( code == 401 )
    hout.push( code_401, sizeof( code_401 ) - 1 );
  else
    hout.push( code_404, sizeof( code_404 ) - 1 );

  /* if either Close specified or http/1.0 and ! Connection: Keep-Alive */
  if ( ( ( hreq.opts | opts ) & HttpReq::CLOSE ) != 0 ||
       ( ( hreq.opts & HttpReq::HTTP_1_1 ) == 0 &&
         ( hreq.opts & HttpReq::KEEP_ALIVE ) == 0 ) ) {
    hout.push( conn_close, sizeof( conn_close ) - 1 );
    this->push( EV_SHUTDOWN ); /* close after data sent */
  }
  else {
    hout.push( keep_alive, sizeof( keep_alive ) - 1 );
  }
  if ( code != 201 )
    hout.push( no_cache, sizeof( no_cache ) - 1 );
}

void
EvHttpConnection::send_404_not_found( const HttpReq &hreq,  int opts ) noexcept
{
  static const char not_found_html[] =
  "\r\n" /* text must be 40 chars */
  "<html><body> Not  Found </body></html>\r\n";
  HttpOut hout;
  char  * s;
  this->init_http_response( hreq, hout, opts, 404 );
  hout.push( ctype_html, sizeof( ctype_html ) - 1 );
  hout.push( clength_40, sizeof( clength_40 ) - 1 );
  hout.push( not_found_html, sizeof( not_found_html ) - 1 );
  if ( (s = this->alloc( hout.size )) != NULL )
    this->sz = hout.cat( s );
}

void
EvHttpConnection::send_401_unauthorized( const HttpReq &hreq,
                                         HttpDigestAuth &auth ) noexcept
{
  static const char unauthorized_html[] =
  "\r\n" /* text must be 40 chars */
  "<html><body>Unauthorized</body></html>\r\n";
  HttpOut hout;
  char  * s;
  this->init_http_response( hreq, hout, 0, 401 );
  bool   stale = ( auth.errcode == HT_AUTH_STALE );
  size_t len   = auth.gen_server( *this->svr_nonce, stale );
  hout.push( auth.out_buf, len );
  hout.push( ctype_html, sizeof( ctype_html ) - 1 );
  hout.push( clength_40, sizeof( clength_40 ) - 1 );
  hout.push( unauthorized_html, sizeof( unauthorized_html ) - 1 );
  if ( (s = this->alloc( hout.size )) != NULL )
    this->sz = hout.cat( s );
}

void
EvHttpConnection::send_404_bad_type( const HttpReq &hreq ) noexcept
{
  static const char bad_type_html[] =
  "\r\n" /* text must be 40 chars */
  "<html><body> Bad  Type  </body></html>\r\n";
  HttpOut hout;
  char  * s;
  this->init_http_response( hreq, hout, 0, 404 );
  hout.push( ctype_html, sizeof( ctype_html ) - 1 );
  hout.push( clength_40, sizeof( clength_40 ) - 1 );
  hout.push( bad_type_html, sizeof( bad_type_html ) - 1 );
  if ( (s = this->alloc( hout.size )) != NULL )
    this->sz = hout.cat( s );
}

void
EvHttpConnection::send_201_created( const HttpReq &hreq ) noexcept
{
  static const char created_html[] =
  "\r\n" /* text must be 40 chars */
  "<html><body>  Created   </body></html>\r\n";
  HttpOut hout;
  char  * s;
  this->init_http_response( hreq, hout, 0, 201 );
  hout.push( ctype_html, sizeof( ctype_html ) - 1 );
  hout.push( clength_40, sizeof( clength_40 ) - 1 );
  if ( hreq.path_len > 0 ) {
    hout.push( location, sizeof( location ) - 1 );
    hout.push( hreq.path, hreq.path_len );
    hout.push( "\r\n", 2 );
  }
  hout.push( created_html, sizeof( created_html ) - 1 );
  if ( (s = this->alloc( hout.size )) != NULL )
    this->sz = hout.cat( s );
}

bool
HttpReq::parse_version( const char *line,  size_t len ) noexcept
{
  size_t i, j;
  if ( len > 0 && line[ len - 1 ] == '\n' ) {
    len -= 1;
    if ( len > 0 && line[ len - 1 ] == '\r' )
      len -= 1;
  }
  if ( len <= 9 )
    return false;
  for ( i = len; i > 0 && line[ i - 1 ] != ' '; i-- )
    ;
  if ( kv_strncasecmp( &line[ i ], "HTTP/1.1", 8 ) == 0 ||
       kv_strncasecmp( &line[ i ], "HTTP/2", 6 ) == 0 )
    this->opts |= HTTP_1_1;

  if ( i > 0 && line[ i - 1 ] == ' ' )
    len = i - 1;
  /* parse GET /path */
  for ( i = 0; i < len; i++ ) {
    if ( line[ i ] != ' ' ) {
      this->method = &line[ i ];
      break;
    }
  }
  if ( this->method == NULL )
    return false;
  for ( j = i; j < len; j++ ) {
    if ( line[ j ] == ' ' ) {
      this->method_len = j - i;
      break;
    }
  }
  if ( this->method_len == 0 )
    return false;
  for ( i = j; i < len; i++ ) {
    if ( line[ i ] == '/' ) {
      this->path = &line[ i ];
      this->path_len = len - i;
      return true;
    }
  }
  return false;
}

void
HttpReq::parse_header( const char *line,  size_t len ) noexcept
{
  size_t i;
  if ( len > 0 && line[ len - 1 ] == '\n' ) {
    len -= 1;
    if ( len > 0 && line[ len - 1 ] == '\r' )
      len -= 1;
  }
  /* find websock stuff */
  switch ( line[ 0 ] ) {
    case 'A': case 'a': {
      static const char   auth[]   = "Authorization: ";
      static const size_t auth_len = sizeof( auth ) - 1;
      /*HttpDigestAuth auth( this->svc.nonce.nonce, &this->svc.htDigestDb );*/

      if ( kv_strncasecmp( line, auth, auth_len ) == 0 ) {
        this->authorize     = line;
        this->authorize_len = len;
      }
      break;
    }
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
        for ( i = 0; isdigit( line[ i + clen_len ] ); i++ )
          ;
        string_to_uint( &line[ clen_len ], i, this->content_length );
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
      static const char   vers[]   = "Version: ";
      static const size_t vers_len = sizeof( vers ) - 1;
      static const char   key[]    = "Key: ";
      static const size_t key_len  = sizeof( key ) - 1;
      static const char   prot[]   = "Protocol: ";
      static const size_t prot_len = sizeof( prot ) - 1;

      if ( kv_strncasecmp( line, secw, secw_len ) == 0 ) {
        const char * ptr   = &line[ secw_len ];
        char       * str   = NULL;
        size_t       start = 0;
        if ( kv_strncasecmp( ptr, vers, vers_len ) == 0 ) {
          str   = this->wsver;
          start = secw_len + vers_len;
        }
        else if ( kv_strncasecmp( ptr, key, key_len ) == 0 ) {
          str   = this->wskey;
          start = secw_len + key_len;
        }
        else if ( kv_strncasecmp( ptr, prot, prot_len ) == 0 ) {
          str   = this->wspro;
          start = secw_len + prot_len;
        }
        if ( str != NULL ) {
          static const char wsguid[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
          len -= start;
          for ( i = 0; i < len; i++ ) {
            if ( i == STR_SZ - 1 )
              break;
            if ( line[ i + start ] <= ' ' )
              break;
            str[ i ] = line[ i + start ];
          }
          str[ i ] = '\0';
          /* concat guid to the wskey, SHA1 digest is returned */
          if ( str == this->wskey && i + sizeof( wsguid ) < STR_SZ ) {
            ::strcpy( &this->wskey[ i ], wsguid );
            this->wskeylen = i + sizeof( wsguid ) - 1;
          }
        }
      }
      break;
    }
    default:
      break;
  }
}

bool
EvHttpConnection::process_websock( void ) noexcept
{
  for (;;) {
    char * start, * end;
    size_t buflen = this->len - this->off;
    if ( buflen == 0 )
      return false;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    /* decode websock frame */
    size_t used = this->recv_wsframe( start, end );
    if ( used <= 1 ) { /* 0 == not enough data for hdr, 1 == closed */
      if ( used == 0 )
        return false;
      return true;
    }
    this->off += (uint32_t) used;
    WSMsg wmsg;
    wmsg.msgcnt = 0;
    wmsg.nlcnt  = 0;

    if ( this->is_using_term ) {
      this->term_int = this->term.interrupt + this->term.suspend;
      this->term.tty_input( &this->wsbuf[ this->wsoff ],
                            this->wslen - this->wsoff );
      this->wsoff = this->wslen;
      this->flush_term();
      wmsg.inptr = this->term.line_buf;
      wmsg.inoff = this->term.line_off;
      wmsg.inlen = this->term.line_len;
    }
    else {
      wmsg.inptr = this->wsbuf;
      wmsg.inoff = this->wsoff;
      wmsg.inlen = this->wslen;
    }
    this->process_wsmsg( wmsg );
    if ( this->is_using_term ) {
      if ( wmsg.msgcnt == 0 && wmsg.nlcnt != 0 ) {
        if ( this->term.tty_prompt() )
          this->flush_term();
      }
      this->term.line_off = wmsg.inoff;
    }
    else
      this->wsoff = wmsg.inoff;
  }
}

void
EvHttpService::process_wsmsg( WSMsg &wmsg ) noexcept
{
  while ( wmsg.inoff < wmsg.inlen ) {
    char * p = &wmsg.inptr[ wmsg.inoff ];
    size_t size = wmsg.inlen - wmsg.inoff;
    int    status;
    switch ( p[ 0 ] ) {
      default:
      case DS_SIMPLE_STRING: /* + */
      case DS_ERROR_STRING:  /* - */
      case DS_INTEGER_VALUE: /* : */
      case DS_BULK_STRING:   /* $ */
      case DS_BULK_ARRAY:    /* * */
        status = this->msg.unpack( p, size, this->tmp );
        break;
      case '\n':
        wmsg.nlcnt++;
        /* FALLTHRU */
      case ' ':
      case '\t':
      case '\r':
        size = 1; /* eat the whitespace */
        status = -1;
        break;
      case '"': /* possible json */
      case '\'':
      case '[':
      case '0': case '1': case '2': case '3': case '4':
      case '5': case '6': case '7': case '8': case '9':
        status = this->msg.unpack_json( p, size, this->tmp );
        break;
    }
    if ( status != DS_MSG_STATUS_OK ) {
      if ( status < 0 ) {
        wmsg.inoff += size;
        continue;
      }
      if ( status == DS_MSG_STATUS_PARTIAL ) {
        /*printf( "partial [%.*s]\n", (int)size, p );*/
        break;
      }
      this->mstatus = (RedisMsgStatus) status;
      this->send_err_string( ERR_MSG_STATUS, KEY_OK );
/*        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
               status, ds_msg_status_string( (RedisMsgStatus) status ),
               inlen - inoff );*/
      wmsg.inoff = wmsg.inlen;
      break;
    }
    this->msgs_recv++;
    wmsg.msgcnt++;
    wmsg.inoff += size;
    if ( (status = this->exec( this, NULL )) == EXEC_OK )
      if ( this->alloc_fail )
        status = ERR_ALLOC_FAIL;
    switch ( status ) {
      case EXEC_SETUP_OK:
        /*if ( q != NULL )
          return;*/
        this->exec_run_to_completion();
        if ( ! this->alloc_fail ) {
          this->msgs_sent++;
          break;
        }
        status = ERR_ALLOC_FAIL;
        /* FALLTHRU */
      case EXEC_QUIT:
        if ( status == EXEC_QUIT ) {
          this->push( EV_SHUTDOWN );
          this->poll.quit++;
        }
        /* FALLTHRU */
      default:
        this->msgs_sent++;
        this->send_status( (ExecStatus) status, KEY_OK );
        break;
      case EXEC_DEBUG:
        break;
    }
  }
}

bool
EvHttpConnection::flush_term( void ) noexcept
{
  const char * buf    = this->term.out_buf;
  size_t       buflen = this->term.out_len;
  if ( buflen == 0 )
    return false;

  uint8_t msg[ 2 + 255 ];
  for ( size_t i = 0; i < buflen; i += 255 ) {
    msg[ 0 ] = '@';
    msg[ 1 ] = (uint8_t) kv::min_int<size_t>( 255, buflen - i );
    ::memcpy( &msg[ 2 ], &buf[ i ], msg[ 1 ] );
    this->append( msg, (size_t) msg[ 1 ] + 2 );
  }
  this->term.tty_out_reset();
  return true;
}

bool
EvHttpService::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  bool flow_good = true;
  int  status    = this->RedisExec::do_pub( pub, cm );
  if ( ( status & RPUB_FORWARD_MSG ) != 0 ) {
    flow_good = this->idle_push_write();
  }
  if ( ( status & RPUB_CONTINUE_MSG ) != 0 ) {
    this->push_continue_list( cm );
    this->idle_push( EV_PROCESS );
  }
  return flow_good;
}

uint8_t
EvHttpService::is_subscribed( const NotifySub &sub ) noexcept
{
  return this->RedisExec::test_subscribed( sub );
}

uint8_t
EvHttpService::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  return this->RedisExec::test_psubscribed( pat );
}

bool
EvHttpService::timer_expire( uint64_t tid,  uint64_t event_id ) noexcept
{
  if ( tid == this->timer_id ) {
    RedisContinueMsg *cm = NULL;
    if ( this->continue_expire( event_id, cm ) ) {
      this->push_continue_list( cm );
      this->idle_push( EV_PROCESS );
    }
  }
  return false;
}

void
EvHttpService::key_prefetch( EvKeyCtx &ctx ) noexcept
{
  this->RedisExec::exec_key_prefetch( ctx );
}

int
EvHttpService::key_continue( EvKeyCtx &ctx ) noexcept
{
  return this->RedisExec::exec_key_continue( ctx );
}

bool
EvHttpService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  return this->RedisExec::do_hash_to_sub( h, key, keylen );
}

void
EvHttpConnection::write( void ) noexcept
{
  if ( this->websock_off != 0 &&
       this->websock_off < this->bytes_sent + this->pending() )
    if ( ! this->frame_websock() )
      return;
  return this->EvConnection::write();
}

bool
EvHttpConnection::frame_websock( void ) noexcept
{
  size_t msgcnt = this->wsmsgcnt;
  bool b = this->frame_websock2();
  /* if a message was output, push another prompt out */
  if ( this->is_using_term ) {
    if ( msgcnt != this->wsmsgcnt ||
         this->term_int != this->term.interrupt + this->term.suspend ) {
      this->term_int = this->term.interrupt + this->term.suspend;
      if ( this->term.tty_prompt() ) {
        this->flush_term();
        this->frame_websock2();
      }
    }
  }
  return b;
}

bool
EvHttpConnection::frame_websock2( void ) noexcept
{
  size_t         nbytes = this->bytes_sent,
                 off    = 0,
                 i, fbytes;
  char         * frame;
  WebSocketFrame ws;

  if ( this->sz > 0 )
    this->flush();
  /* find websock stream offset */
  for ( ; off < this->idx; off++ ) {
    nbytes += this->iov[ off ].iov_len;
    if ( this->websock_off < nbytes )
      break;
  }
  if ( off == this->idx )
    return true;

  nbytes = this->iov[ off ].iov_len;
  for ( i = off + 1; i < this->idx; i++ )
    nbytes += this->iov[ i ].iov_len;

  ws.set( nbytes, 0, WebSocketFrame::WS_TEXT, true );
  fbytes = ws.hdr_size();
  frame = this->alloc_temp( fbytes );
  ws.encode( frame );
  this->insert_iov( off, frame, fbytes );
  this->websock_off += nbytes + fbytes;
  return true;
}

bool
EvHttpService::frame_websock2( void ) noexcept
{
  static const char eol[]    = "\r\n";
  static size_t     eol_size = sizeof( eol ) - 1;
  size_t      nbytes = this->bytes_sent,
              off    = 0,
              i;
  char      * newbuf;

  if ( this->sz > 0 )
    this->flush();
  /* find websock stream offset */
  for ( ; off < this->idx; off++ ) {
    nbytes += this->iov[ off ].iov_len;
    if ( this->websock_off < nbytes )
      break;
  }
  if ( off == this->idx )
    return true;
  /* concat buffers */
  if ( off + 1 < this->idx ) {
    nbytes = this->iov[ off ].iov_len;
    for ( i = off + 1; i < this->idx; i++ )
      nbytes += this->iov[ i ].iov_len;
    newbuf = this->alloc_temp( nbytes );
    if ( newbuf == NULL )
      return false;
    nbytes = this->iov[ off ].iov_len;
    ::memcpy( newbuf, this->iov[ off ].iov_base, nbytes );
    for ( i = off + 1; i < this->idx; i++ ) {
      size_t iov_len = this->iov[ i ].iov_len;
      ::memcpy( &newbuf[ nbytes ], this->iov[ i ].iov_base, iov_len );
      nbytes += iov_len;
    }
    this->iov[ off ].iov_base = newbuf;
    this->iov[ off ].iov_len  = nbytes;
    this->idx = off + 1;
  }
  /* frame the new data */
  RedisMsg       msg;
  WebSocketFrame ws;
  char         * buf    = (char *) this->iov[ off ].iov_base,
               * wsmsg;
  const size_t   buflen = this->iov[ off ].iov_len;
  size_t         bufoff,
                 msgsize,
                 hsize,
                 size,
                 totsize = 0;
  RedisMsgStatus mstatus;
  bool           is_literal;
  /* determine the size of each framed json msg */
  for ( bufoff = 0; ; ) {
    msgsize = buflen - bufoff;
    /* the '@'[len] is a literal, not a msg, pass through */
    if ( msgsize > 2 && buf[ bufoff ] == '@' ) {
      size = (uint8_t) buf[ bufoff + 1 ];
      msgsize = size + 2;
      if ( bufoff + msgsize > buflen )
        return false;
      is_literal = true;
    }
    else {
      mstatus = msg.unpack( &buf[ bufoff ], msgsize, this->tmp );
      if ( mstatus != DS_MSG_STATUS_OK )
        return false;
      size = msg.to_almost_json_size( false );
      size += eol_size;
      is_literal = false;
      this->wsmsgcnt++;
    }
    ws.set( size, 0, WebSocketFrame::WS_TEXT, true );
    hsize = ws.hdr_size();
    totsize += size + hsize;
    if ( (bufoff += msgsize) == buflen )
      break;
  }
  /* frame and convert to json */
  if ( totsize > 0 ) {
    newbuf = this->alloc_temp( totsize );
    /* if only one msg, then it is already decoded */
    if ( totsize == hsize + size ) {
      ws.encode( newbuf );
      if ( is_literal )
        ::memcpy( &newbuf[ hsize ], &buf[ 2 ], size );
      else {
        msg.to_almost_json( &newbuf[ hsize ], false );
        ::memcpy( &newbuf[ hsize + size - eol_size ], eol, eol_size );
      }
      /*printf( "frame: %.*s\n", (int) size, &newbuf[ hsize ] );*/
    }
    else {
      wsmsg = newbuf;
      for ( bufoff = 0; ; ) {
        msgsize = buflen - bufoff;
        if ( msgsize > 2 && buf[ bufoff ] == '@' ) {
          size = (uint8_t) buf[ bufoff + 1 ];
          msgsize = size + 2;
          ws.set( size, 0, WebSocketFrame::WS_TEXT, true );
          hsize = ws.hdr_size();
          ws.encode( wsmsg );
          ::memcpy( &wsmsg[ hsize ], &buf[ bufoff + 2 ], size );
        }
        else {
          msg.unpack( &buf[ bufoff ], msgsize, this->tmp );
          size = msg.to_almost_json_size( false );
          size += eol_size;
          ws.set( size, 0, WebSocketFrame::WS_TEXT, true );
          hsize = ws.hdr_size();
          ws.encode( wsmsg );
          msg.to_almost_json( &wsmsg[ hsize ], false );
          ::memcpy( &wsmsg[ hsize + size - eol_size ], eol, eol_size );
        }
        bufoff += msgsize;
        /*printf( "frame2: %.*s\n", (int) size, &wsmsg[ hsize ] );*/
        wsmsg = &wsmsg[ hsize + size ];
        if ( wsmsg == &newbuf[ totsize ] )
          break;
      }
    }
    if ( totsize >= buflen )
      this->wr_pending += totsize - buflen;
    else
      this->wr_pending -= buflen - totsize;
    this->iov[ off ].iov_base = newbuf;
    this->iov[ off ].iov_len  = totsize;
    this->websock_off += totsize;
  }
  return true;
}

const char *
EvHttpConnection::get_mime_type( const char *path,  size_t len,
                                 size_t &mlen,  bool &is_gzip ) noexcept
{
#define RET( s ) { mlen = sizeof( s ) - 1; return s; }
  if ( len >= 3 ) {
    const char *p = &path[ len-3 ];
    is_gzip = ( ::memcmp( p, ".gz", 3 ) == 0 );
    if ( is_gzip ) {
      if ( len < 6 )
        goto octet_stream;
      p   -= 3;
      len -= 3;
    }
    /* the third char from the end */
    switch ( p[ 0 ] ) {
      case '.':
        if ( p[ 1 ] == 'j' && p[ 2 ] == 's' )                       /* [.]js */
          RET( "text/javascript" );
        if ( p[ 1 ] == 'm' && p[ 2 ] == 'd' )                       /* [.]md */
          RET( "text/markdown" );
        break;
      case 'c':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".css", 4 ) == 0 )    /* .[c]ss */
          RET( "text/css" );
        if ( len >= 5 && ::memcmp( &p[ -2 ], ".scss", 5 ) == 0 )    /* .s[c]ss */
          RET( "text/x-scss" );
        break;
      case 'd':
        if ( ( len >= 5 &&
               ::memcmp( &p[ -2 ], ".adoc", 5 ) == 0 ) ||         /* .a[d]oc */
             ( len >= 9 &&
               ::memcmp( &p[ -6 ], ".asciidoc", 9 ) == 0 ) )  /* .ascii[d]oc */
          RET( "text/asciidoc" );
        break;
      case 'h':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".htm", 4 ) == 0 )    /* .[h]tm */
          RET( "text/html" );
        break;
      case 'i':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".ico", 4 ) == 0 )    /* .[h]tm */
          RET( "image/png" );
        break;
      case 'j':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".jpg", 4 ) == 0 )    /* .[j]pg */
          RET( "image/jpeg" );
        break;
      case 'p':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".png", 4 ) == 0 )    /* .[p]ng */
          RET( "image/png" );
        break;
      case 's':
        if ( len >= 5 && ::memcmp( &p[ -2 ], ".json", 5 ) == 0 )  /* .j[s]on */
          RET( "application/json" );
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".svg", 4 ) == 0 )    /* .[s]vg */
          RET( "image/svg+xml" );
        break;
      case 't':
        if ( len >= 5 && ::memcmp( &p[ -2 ], ".html", 5 ) == 0 )  /* .h[t]ml */
          RET( "text/html" );
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".txt", 4 ) == 0 )    /* .[t]xt */
          RET( "text/plain" );
        break;
      case 'x':
        if ( len >= 4 && ::memcmp( &p[ -1 ], ".xml", 4 ) == 0 )    /* .[x]ml */
          RET( "text/xml" );
        break;
      default:
        break;
    }
  }
octet_stream:;
  RET( "application/octet-stream" );
#undef RET
}

static uint8_t
hexval( char c ) noexcept
{
  if ( isdigit( c ) ) return (uint8_t) c - '0';
  if ( c >= 'A' ) return (uint8_t) c - 'A' + 10;
  return (uint8_t) c - 'a' + 10;
}

size_t
HttpReq::decode_uri( const char *s,  const char *e,  char *q,
                     size_t qlen ) noexcept
{
  static const char * amp_str[ 5 ] = {"apos;", "quot;", "amp;", "lt;", "gt;" };
  static const size_t amp_len[ 5 ] = { 5, 5, 4, 3, 3 };
  static const char   amp_chr[ 5 ] = { '&', '"', '&', '<', '>' };
  char * start = q, * qe = &q[ qlen - 1 ];
  size_t i;
  while ( s < e && q < qe ) {
    switch ( s[ 0 ] ) {
      case '%':
        /* check if the two chars following % are hex chars */
        if ( isxdigit( s[ 1 ] ) && isxdigit( s[ 2 ] ) ) {
          *q = (char) ( ( hexval( s[ 1 ] ) << 4 ) | hexval( s[ 2 ] ) );
          s = &s[ 3 ];
          break;
        }
        *q = '%'; s++;
        break;
      case '+':
        *q = ' '; s++;
        break;
      case '&':
        for ( i = 0; i < 5; i++ ) {
          if ( kv_strncasecmp( &s[ 1 ], amp_str[ i ], amp_len[ i ] ) == 0 ) {
            *q = amp_chr[ i ]; s = &s[ amp_len[ i ] + 1 ];
            break;
          }
        }
        if ( i == 5 ) {
          *q = '&'; s++;
        }
        break;
      default:
        *q = *s++;
        break;
    }
    q++;
  }
  *q = '\0';
  return q - start;
}

bool
EvHttpConnection::process_get( const HttpReq &hreq ) noexcept
{
  char         buf[ 1024 ];
  const char * obj = hreq.path,
             * end = &hreq.path[ hreq.path_len ];

  if ( obj == NULL )
    return false;
  size_t path_len = (size_t) ( end - &obj[ 1 ] );
  if ( path_len > sizeof( buf ) - 11 ) /* space for index.html */
    return false;
  path_len = HttpReq::decode_uri( &obj[ 1 ], end, buf, sizeof( buf ) );
  if ( path_len == 0 ) {
    ::strcpy( buf, "index.html" );
    path_len = 10;
  }
  return this->process_get_file( buf, path_len );
}

bool
EvHttpService::process_get( const HttpReq &hreq ) noexcept
{
  /* GET /somefile HTTP/1.1 */
  ds_msg_t     ar[ 2 ];
  char         buf[ 1024 ];
  const char * obj = hreq.path,
             * end = &hreq.path[ hreq.path_len ];
  HttpOut      hout;
  size_t       off, i, j, d, n,
               len, mlen, qoff;
  char       * s;
  const char * mtype;
  RedisMsg     m;
  ExecStatus   status;
  bool         use_json = false,
               use_resp = false;

  if ( obj == NULL )
    return false;
  len = (size_t) ( end - &obj[ 1 ] );
  if ( len > sizeof( buf ) - 11 ) /* space for index.html */
    return false;
  /* skip /? */
  if ( obj[ 1 ] == '?' ) {
    qoff = 2; /* resp format */
    use_resp = true;
  }
  /* skip /js? */
  else if ( obj[ 1 ] == 'j' && obj[ 2 ] == 's' && obj[ 3 ] == '?' ) {
    qoff = 4;
    use_json = true;
  }
  else {
    qoff = 1; /* skip the leading '/' */
  }
/*  if ( ::strncmp( path, "console/", 8 ) == 0 )
    return this->send_file( path, len );*/

  len = HttpReq::decode_uri( &obj[ qoff ], end, buf, sizeof( buf ) );
  if ( len == 0 ) {
    ::strcpy( buf, "index.html" );
    len = 10;
  }
  mlen = len;

  if ( qoff > 1 ) {
    len = crlf( buf, len );
    if ( this->msg.unpack( buf, len, this->tmp ) != DS_MSG_STATUS_OK )
      return false;
  }
  else {
    /* make array: [ "get", buf ] */
    this->msg.type  = DS_BULK_ARRAY;
    this->msg.len   = 2;
    this->msg.array = ar;
    ar[ 0 ].type    = DS_INTEGER_VALUE;
    ar[ 0 ].len     = 0;
    ar[ 0 ].ival    = GET_CMD;
    ar[ 1 ].type    = DS_BULK_STRING;
    ar[ 1 ].len     = (int32_t) len;
    ar[ 1 ].strval  = buf;
  }
  if ( this->sz > 0 )
    this->flush();
  off = this->pending(); /* record location at start of list */
  i   = this->idx;
  if ( (status = this->exec( NULL, NULL )) == EXEC_OK )
    if ( this->alloc_fail )
      status = ERR_ALLOC_FAIL;
  switch ( status ) {
    case EXEC_SETUP_OK:
      this->exec_run_to_completion();
      if ( ! this->alloc_fail )
        break;
      status = ERR_ALLOC_FAIL;
      /* fall through */
    case EXEC_QUIT:
      if ( status == EXEC_QUIT ) {
        this->push( EV_SHUTDOWN );
        this->poll.quit++;
      }
      /* fall through */
    default:
      this->send_status( status, KEY_OK );
      if ( ! use_json && ! use_resp ) {
        this->truncate( off );
        return false;
      }
      break;
    case EXEC_DEBUG:
      return false;
  }
  if ( this->pending() == off )
    return false;

  this->init_http_response( hreq, hout, 0, 200 );

  if ( use_json ) {
    s = this->trunc_copy( off, n );
    if ( n == 0 )
      return false;
    if ( m.unpack( s, n, this->tmp ) != DS_MSG_STATUS_OK )
      return false;
    hout.push( ctype_json, sizeof( ctype_json ) - 1 );
    hout.push( clength, sizeof( clength ) - 1 );

    n = m.to_almost_json_size( false ) + 2;
    d = uint64_digits( n );
    s = this->alloc( n + hout.size + d + 4 );
    j = hout.cat( s );
    j += uint64_to_string( n - off, &s[ j ], d );
    j  = crlf( s, j );
    j  = crlf( s, j );
    j += m.to_almost_json( &s[ j ], false );
    this->sz = crlf( s, j );
    return true;
  }
  if ( use_resp ) {
    hout.push( ctype_resp, sizeof( ctype_resp ) - 1 );
    hout.push( clength, sizeof( clength ) - 1 );

    this->flush();
    n = this->pending();
    d = uint64_digits( n - off );
    s = this->alloc( hout.size + d + 4 );
    j = hout.cat( s );
    j += uint64_to_string( n - off, &s[ j ], d );
    j  = crlf( s, j );
    this->sz = crlf( s, j );
    this->prepend_flush( i );
    return true;
  }
  s = this->trunc_copy( off, n );
  if ( n == 0 )
    return false;
  if ( m.unpack( s, n, this->tmp ) != DS_MSG_STATUS_OK )
    return false;
  if ( m.type == DS_BULK_STRING && m.len >= 0 ) {
    bool is_gzip;
    mtype = get_mime_type( buf, mlen, mlen, is_gzip );
    hout.push( ctype, sizeof( ctype ) - 1 );
    if ( is_gzip )
      hout.push( gzenc, sizeof( gzenc ) - 1 );
    hout.push( mtype, mlen );
    hout.push( "\r\n", 2 );
    hout.push( clength, sizeof( clength ) - 1 );

    n = m.len;
    d = uint64_digits( n );
    s = this->alloc( hout.size + d + 4 );
    j = hout.cat( s );
    j += uint64_to_string( n, &s[ j ], d );
    j  = crlf( s, j );
    this->sz = crlf( s, j );
    this->append_iov( m.strval, n );
    return true;
  }
  return false;
  /*return this->send_file( path );*/
}

bool
EvHttpConnection::process_post( const HttpReq & ) noexcept
{
  return false;
}

bool
EvHttpService::process_post( const HttpReq &hreq ) noexcept
{
  const char * obj = hreq.path,
             * end = &hreq.path[ hreq.path_len ];
  ds_msg_t     ar[ 3 ];
  char         buf[ 1024 ];
  size_t       off, len;
  ExecStatus   status;
  bool         ret = true;

  /* skip the leading '/' */
  len = HttpReq::decode_uri( &obj[ 1 ], end, buf, sizeof( buf ) );
  if ( len == 0 ) {
    ::strcpy( buf, "index.html" );
    len = 10;
  }
  /* make array: [ "set", buf, data ] */
  this->msg.type  = DS_BULK_ARRAY;
  this->msg.len   = 3;
  this->msg.array = ar;
  ar[ 0 ].type    = DS_INTEGER_VALUE;
  ar[ 0 ].len     = 0;
  ar[ 0 ].ival    = SET_CMD;
  ar[ 1 ].type    = DS_BULK_STRING;
  ar[ 1 ].len     = (int32_t) len;
  ar[ 1 ].strval  = buf;
  ar[ 2 ].type    = DS_BULK_STRING;
  ar[ 2 ].len     = (int32_t) hreq.content_length;
  ar[ 2 ].strval  = (char *) hreq.data;

  if ( this->sz > 0 )
    this->flush();
  off = this->pending(); /* record location at start of list */
  if ( (status = this->exec( NULL, NULL )) == EXEC_OK )
    if ( this->alloc_fail )
      status = ERR_ALLOC_FAIL;
  switch ( status ) {
    case EXEC_SETUP_OK:
      this->exec_run_to_completion();
      if ( ! this->alloc_fail )
        break;
      /* fall through */
    default:
      ret = false;
      break;
  }
  if ( this->pending() == off )
    ret = false;
  this->truncate( off );
  return ret;
}


bool
EvHttpConnection::process_get_file( const char *,  size_t ) noexcept
{
  return false;
#if 0
  RedisBufQueue q( *this );
  int  fd;
  bool res = false,
       toolarge = false;
  os_stat statbuf;
  if ( (fd = os_open( path, O_RDONLY, 0 )) < 0 ||
       os_fstat( fd, &statbuf ) < 0 )
    return false;

  if ( (size_t) statbuf.st_size > (size_t) ( 10 * 1024 * 1024 ) ) {
    fprintf( stderr, "File %s too large: %" PRIu64 "\n", path,
             statbuf.st_size );
    toolarge = true;
  }
  else {
    StreamBuf::BufList * p = q.get_buf( 256 );
    size_t mlen;
    int n = ::snprintf( p->buf( 0 ), 256,
                        "HTTP/1.1 200 OK\r\n"
                        "Connection: keep-alive\r\n"
                        "Cache-Control: no-cache\r\n"
                        "Content-Type: %s\r\n"
                        "Content-Length: %u\r\n"
                        "\r\n",
        get_mime_type( path, path_len, mlen, is_gzip ), (int) statbuf.st_size );
    if ( n > 0 && n < 256 ) {
      p->used += n;
      p = q.get_buf( statbuf.st_size );
      ssize_t rd_len = os_read( fd, p->buf( 0 ), statbuf.st_size );
      if ( (size_t) rd_len == (size_t) statbuf.st_size ) {
        p->used += statbuf.st_size;
        this->append_iov( q );
        res = true;
      }
    }
  }
  if ( toolarge )
    perror( path );
  os_close( fd );
  return res;
#endif
}

bool
EvHttpConnection::send_ws_upgrade( const HttpReq &hreq ) noexcept
{
  static const char b64[] =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  uint8_t digest[ 160 / 8 ];
  uint32_t val, i, j = 0;
  char wsacc[ 32 ];
  bool res = false;
  /* websock switch hashes wskey with SHA1 and base64 encodes the result */
  SHA1( hreq.wskey, hreq.wskeylen, digest );
  /* base64 encode it */
  for ( i = 0; i < 18; i += 3 ) {
    val = ( (uint32_t) digest[ i ] << 16 ) | ( (uint32_t) digest[ i+1 ] << 8 ) |
          (uint32_t) digest[ i+2 ];
    wsacc[ j ]     = b64[ ( val >> 18 ) & 63U ];
    wsacc[ j + 1 ] = b64[ ( val >> 12 ) & 63U ];
    wsacc[ j + 2 ] = b64[ ( val >> 6 ) & 63U ];
    wsacc[ j + 3 ] = b64[ val & 63U ];
    j += 4;
  }
  val = (uint32_t) ( digest[ i ] << 16 ) | (uint32_t) ( digest[ i+1 ] << 8 );
  wsacc[ j ] = b64[ ( val >> 18 ) & 63U ];
  wsacc[ j + 1 ] = b64[ ( val >> 12 ) & 63U ];
  wsacc[ j + 2 ] = b64[ ( val >> 6 ) & 63U ];
  wsacc[ j + 3 ] = '=';
  wsacc[ j + 4 ] = '\0';

  char * p = this->alloc( 256 );
  if ( p != NULL ) {
    int n = ::snprintf( p, 256,
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Connection: upgrade\r\n"
      "Upgrade: websocket\r\n"
      "Sec-WebSocket-Version: %s\r\n"
      "%s%s%s"
      "Sec-WebSocket-Accept: %s\r\n"
      "Content-Length: 0\r\n"
      "\r\n",
      hreq.wsver,
      hreq.wspro[0]?"Sec-WebSocket-Protocol: ":"", hreq.wspro,
      hreq.wspro[0]?"\r\n":"",
      wsacc );
    /*printf( "%.*s", n, p );*/
    if ( n > 0 && n < 256 ) {
      this->sz += n;
      res = true;
    }
  }
  return res;
}

bool
EvHttpConnection::send_ws_pong( const char *payload,  size_t pay_len ) noexcept
{
  WebSocketFrame ws;

  ws.set( pay_len, 0, WebSocketFrame::WS_PONG, true );
  char * p = this->alloc( WebSocketFrame::MAX_HEADER_SIZE + pay_len );
  if ( p != NULL ) {
    size_t off = ws.encode( p );
    ::memcpy( &p[ off ], payload, pay_len );
    this->sz += off + pay_len;
    return true;
  }
  return false;
}

size_t
EvHttpConnection::recv_wsframe( char *start,  char *end ) noexcept
{
  WebSocketFrame ws;
  size_t hdrsize = ws.decode( start, end - start );
  if ( hdrsize <= 1 ) /* 0 == not enough data for hdr, 1 == closed */
    return hdrsize;
  if ( ws.payload_len > (uint64_t) 10 * 1024 * 1024 ) {
    fprintf( stderr, "Websocket payload too large: %" PRIu64 "\n",
             ws.payload_len );
    return 1; /* close */
  }
  char *p = &start[ hdrsize ];
  if ( &p[ ws.payload_len ] > end ) { /* if still need more data */
    printf( "need more data\n" );
    return 0;
  }
  //printf( "ws opcode %x\n", ws.opcode );
  switch ( ws.opcode ) {
    case WebSocketFrame::WS_PING:
      if ( ws.mask != 0 )
        ws.apply_mask( p );
      this->send_ws_pong( p, ws.payload_len );
      break;
    case WebSocketFrame::WS_PONG:
      break;
    default: { /* WS_TEXT, WS_BINARY */
#if 0
      printf( "ws%s%s[%.*s](len=%d p[0]=%d)\n",
              ( ws.opcode & WebSocketFrame::WS_TEXT ) ? "text" : "",
              ( ws.opcode & WebSocketFrame::WS_BINARY ) ? "bin" : "",
              (int) ws.payload_len, p, (int)ws.payload_len, p[ 0 ] );
#endif
      for (;;) {
        if ( this->wslen + ws.payload_len <= this->wsalloc ) {
          ::memcpy( &this->wsbuf[ this->wslen ], p, ws.payload_len );
          if ( ws.mask != 0 )
            ws.apply_mask( &this->wsbuf[ this->wslen ] );
          this->wslen += ws.payload_len;
          break;
        }
        if ( this->wsoff > 0 ) {
          this->wslen -= this->wsoff;
#if 0
          if ( this->term_cooked )
            this->wsecho -= this->wsoff;
#endif
          ::memmove( this->wsbuf, &this->wsbuf[ this->wsoff ],
                     this->wslen );
          this->wsoff = 0;
        }
        else {
          size_t size = kv::align<size_t>( this->wslen + ws.payload_len, 1024 );
          char * tmp = (char *) ::realloc( this->wsbuf, size );
          if ( tmp == NULL )
            return 1; /* close */
          this->wsbuf   = tmp;
          this->wsalloc = size;
        }
      }
      break;
    }
  }
  //printf( "wspayload: %ld\n", ws.payload_len );
  return hdrsize + ws.payload_len;
}

void
EvHttpConnection::release( void ) noexcept
{
  this->term.tty_release();
  if ( this->wsbuf != NULL )
    ::free( this->wsbuf );
  this->EvConnection::release_buffers();
}

void
EvHttpService::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
EvHttpService::release( void ) noexcept
{
  this->RedisExec::release();
  this->EvHttpConnection::release();
}

bool
EvHttpService::match( PeerMatchArgs &ka ) noexcept
{
  if ( this->sub_tab.sub_count + this->pat_tab.sub_count() != 0 ) {
    if ( EvSocket::client_match( *this, &ka, MARG( "pubsub" ),
                                             MARG( "http" ), NULL ) )
      return true;
  }
  else {
    if ( EvSocket::client_match( *this, &ka, MARG( "normal" ),
                                             MARG( "http" ), NULL ) )
      return true;
  }
  return this->EvConnection::match( ka );
}

int
EvHttpService::client_list( char *buf,  size_t buflen ) noexcept
{
  int i = this->EvConnection::client_list( buf, buflen );
  if ( i >= 0 )
    i += this->exec_client_list( &buf[ i ], buflen - i );
  return i;
}


/* ================ sha1.c ================ */
/*
SHA-1 in C
By Steve Reid <steve@edmweb.com>
100% Public Domain

Test Vectors (from FIPS PUB 180-1)
"abc"
  A9993E36 4706816A BA3E2571 7850C26C 9CD0D89D
"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
  84983E44 1C3BD26E BAAE4AA1 F95129E5 E54670F1
A million repetitions of "a"
  34AA973C D4C4DAA4 F61EEB2B DBAD2731 6534016F
*/

typedef struct {
    uint32_t state[5];
    uint32_t count[2];
    uint8_t buffer[64];
} SHA1_CTX;

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

/* blk0() and blk() perform the initial expand. */
/* I got the idea of expanding during the round function from SSLeay */
#if BYTE_ORDER == LITTLE_ENDIAN
#define blk0(i) (block->l[i] = (rol(block->l[i],24)&0xFF00FF00) \
    |(rol(block->l[i],8)&0x00FF00FF))

#elif BYTE_ORDER == BIG_ENDIAN
#define blk0(i) block->l[i]

#else
#error "Endianness not defined!"
#endif

#define blk(i) (block->l[i&15] = rol(block->l[(i+13)&15]^block->l[(i+8)&15] \
    ^block->l[(i+2)&15]^block->l[i&15],1))

/* (R0+R1), R2, R3, R4 are the different operations used in SHA1 */
#define R0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define R3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define R4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);

/* Hash a single 512-bit block. This is the core of the algorithm. */
static void
SHA1Transform(uint32_t state[5], const uint8_t buffer[64]) noexcept
{
    uint32_t a, b, c, d, e;
    typedef union {
        uint8_t c[64];
        uint32_t l[16];
    } CHAR64LONG16;
    CHAR64LONG16 block[1];  /* use array to appear as a pointer */
    memcpy(block, buffer, 64);
    /* Copy context->state[] to working vars */
    a = state[0];
    b = state[1];
    c = state[2];
    d = state[3];
    e = state[4];
    /* 4 rounds of 20 operations each. Loop unrolled. */
    R0(a,b,c,d,e, 0); R0(e,a,b,c,d, 1); R0(d,e,a,b,c, 2); R0(c,d,e,a,b, 3);
    R0(b,c,d,e,a, 4); R0(a,b,c,d,e, 5); R0(e,a,b,c,d, 6); R0(d,e,a,b,c, 7);
    R0(c,d,e,a,b, 8); R0(b,c,d,e,a, 9); R0(a,b,c,d,e,10); R0(e,a,b,c,d,11);
    R0(d,e,a,b,c,12); R0(c,d,e,a,b,13); R0(b,c,d,e,a,14); R0(a,b,c,d,e,15);
    R1(e,a,b,c,d,16); R1(d,e,a,b,c,17); R1(c,d,e,a,b,18); R1(b,c,d,e,a,19);
    R2(a,b,c,d,e,20); R2(e,a,b,c,d,21); R2(d,e,a,b,c,22); R2(c,d,e,a,b,23);
    R2(b,c,d,e,a,24); R2(a,b,c,d,e,25); R2(e,a,b,c,d,26); R2(d,e,a,b,c,27);
    R2(c,d,e,a,b,28); R2(b,c,d,e,a,29); R2(a,b,c,d,e,30); R2(e,a,b,c,d,31);
    R2(d,e,a,b,c,32); R2(c,d,e,a,b,33); R2(b,c,d,e,a,34); R2(a,b,c,d,e,35);
    R2(e,a,b,c,d,36); R2(d,e,a,b,c,37); R2(c,d,e,a,b,38); R2(b,c,d,e,a,39);
    R3(a,b,c,d,e,40); R3(e,a,b,c,d,41); R3(d,e,a,b,c,42); R3(c,d,e,a,b,43);
    R3(b,c,d,e,a,44); R3(a,b,c,d,e,45); R3(e,a,b,c,d,46); R3(d,e,a,b,c,47);
    R3(c,d,e,a,b,48); R3(b,c,d,e,a,49); R3(a,b,c,d,e,50); R3(e,a,b,c,d,51);
    R3(d,e,a,b,c,52); R3(c,d,e,a,b,53); R3(b,c,d,e,a,54); R3(a,b,c,d,e,55);
    R3(e,a,b,c,d,56); R3(d,e,a,b,c,57); R3(c,d,e,a,b,58); R3(b,c,d,e,a,59);
    R4(a,b,c,d,e,60); R4(e,a,b,c,d,61); R4(d,e,a,b,c,62); R4(c,d,e,a,b,63);
    R4(b,c,d,e,a,64); R4(a,b,c,d,e,65); R4(e,a,b,c,d,66); R4(d,e,a,b,c,67);
    R4(c,d,e,a,b,68); R4(b,c,d,e,a,69); R4(a,b,c,d,e,70); R4(e,a,b,c,d,71);
    R4(d,e,a,b,c,72); R4(c,d,e,a,b,73); R4(b,c,d,e,a,74); R4(a,b,c,d,e,75);
    R4(e,a,b,c,d,76); R4(d,e,a,b,c,77); R4(c,d,e,a,b,78); R4(b,c,d,e,a,79);
    /* Add the working vars back into context.state[] */
    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
}
/* SHA1Init - Initialize new context */
static void
SHA1Init(SHA1_CTX* context) noexcept
{
    /* SHA1 initialization constants */
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
    context->state[4] = 0xC3D2E1F0;
    context->count[0] = context->count[1] = 0;
}
/* Run your data through this. */
static void
SHA1Update(SHA1_CTX* context, const uint8_t* data, uint32_t len) noexcept
{
    uint32_t i, j;

    j = context->count[0];
    if ((context->count[0] += len << 3) < j)
        context->count[1]++;
    context->count[1] += (len>>29);
    j = (j >> 3) & 63;
    if ((j + len) > 63) {
        memcpy(&context->buffer[j], data, (i = 64-j));
        SHA1Transform(context->state, context->buffer);
        for ( ; i + 63 < len; i += 64) {
            SHA1Transform(context->state, &data[i]);
        }
        j = 0;
    }
    else i = 0;
    memcpy(&context->buffer[j], &data[i], len - i);
}
/* Add padding and return the message digest. */
static void
SHA1Final(uint8_t digest[20], SHA1_CTX* context) noexcept
{
    unsigned i;
    uint8_t finalcount[8];
    uint8_t c;

    for (i = 0; i < 8; i++) {
        finalcount[i] = (uint8_t)((context->count[(i >= 4 ? 0 : 1)]
         >> ((3-(i & 3)) * 8) ) & 255);  /* Endian independent */
    }
    c = 0200;
    SHA1Update(context, &c, 1);
    while ((context->count[0] & 504) != 448) {
	c = 0000;
        SHA1Update(context, &c, 1);
    }
    SHA1Update(context, finalcount, 8);  /* Should cause a SHA1Transform() */
    for (i = 0; i < 20; i++) {
        digest[i] = (uint8_t)
         ((context->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
    }
    /* Wipe variables */
    memset(context, '\0', sizeof(*context));
    memset(&finalcount, '\0', sizeof(finalcount));
}
/* ================ end of sha1.c ================ */

static void
SHA1( const void *data, size_t len, uint8_t digest[20] ) noexcept
{
  const uint8_t *ptr = (const uint8_t *) data;
  SHA1_CTX ctx;
  SHA1Init( &ctx );
  for (;;) {
    uint32_t sz = len & (size_t) 0xffffffffU;
    SHA1Update( &ctx, ptr, sz );
    if ( len == (size_t) sz )
      break;
    len -= (size_t) sz;
    ptr = &ptr[ sz ];
  }
  SHA1Final( digest, &ctx );
}

#ifdef TEST_SHA1
#define BUFSIZE 4096

#define UNUSED(x) (void)(x)
int sha1Test(int argc, char **argv)
{
    SHA1_CTX ctx;
    uint8_t hash[20], buf[BUFSIZE];
    int i;

    UNUSED(argc);
    UNUSED(argv);

    for(i=0;i<BUFSIZE;i++)
        buf[i] = i;

    SHA1Init(&ctx);
    for(i=0;i<1000;i++)
        SHA1Update(&ctx, buf, BUFSIZE);
    SHA1Final(hash, &ctx);

    printf("SHA1=");
    for(i=0;i<20;i++)
        printf("%02x", hash[i]);
    printf("\n");
    return 0;
}
#endif
