#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <openssl/sha.h>
#include <raids/ev_http.h>

using namespace rai;
using namespace ds;

void
EvHttpListen::accept( void )
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
  EvHttpService * c = this->poll.free_http.hd;
  if ( c != NULL )
    c->pop_free_list();
  else {
    void * m = aligned_malloc( sizeof( EvHttpService ) * EvPoll::ALLOC_INCR );
    if ( m == NULL ) {
      perror( "accept: no memory" );
      ::close( sock );
      return;
    }
    c = new ( m ) EvHttpService( this->poll );
    for ( int i = EvPoll::ALLOC_INCR - 1; i >= 1; i-- ) {
      new ( (void *) &c[ i ] ) EvHttpService( this->poll );
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
  c->sub_id = sock;
  c->initialize_state();
  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    c->push_free_list();
  }
}

static char page404[] =
"HTTP/1.1 404 Not Found\r\n"
"Connection: close\r\n"
"Content-Type: text/html\r\n"
"Content-Length: 40\r\n"
"\r\n"
"<html><body> Not  Found </body></html>\r\n";

void
EvHttpService::process( bool /*use_prefetch*/ )
{
  StreamBuf       & strm = *this;
  //EvPrefetchQueue * q    = ( use_prefetch ? this->poll.prefetch_queue : NULL );
  size_t            buflen, used, i, j, k, sz;
  char            * p, * eol, * start, * end;
  char            * line[ 64 ];
  size_t            llen[ 64 ];

  if ( this->is_not_found )
    goto is_closed;
  for (;;) { 
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    /* decode websock frame */
    if ( this->websock_off > 0 ) {
      char * inptr;
      size_t inoff,
             inlen;
      used = this->recv_wsframe( start, end );
      if ( used <= 1 ) { /* 0 == not enough data for hdr, 1 == closed */
        if ( used == 0 )
          goto break_loop;
        goto is_closed;
      }
      this->off += used;
      if ( this->is_using_term ) {
        this->term.tty_input( &this->wsbuf[ this->wsoff ],
                              this->wslen - this->wsoff );
        this->wsoff = this->wslen;
        this->flush_term();
        inptr = this->term.line_buf;
        inoff = this->term.line_off;
        inlen = this->term.line_len;
      }
      else {
        inptr = this->wsbuf;
        inoff = this->wsoff;
        inlen = this->wslen;
      }
      while ( inoff < inlen ) {
        sz = inlen - inoff;
        p  = &inptr[ inoff ];
        RedisMsgStatus mstatus = this->msg.unpack( p, sz, strm.tmp );
        if ( mstatus != REDIS_MSG_OK ) {
          if ( mstatus == REDIS_MSG_PARTIAL ) {
            /*printf( "partial [%.*s]\n", (int)sz, p );*/
            break;
          }
          fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                   mstatus, redis_msg_status_string( mstatus ),
                   inlen - inoff );
          inoff = inlen;
          break;
        }
        inoff += sz;
        ExecStatus status;
        if ( (status = this->exec( this, NULL )) == EXEC_OK )
          if ( strm.alloc_fail )
            status = ERR_ALLOC_FAIL;
        switch ( status ) {
          case EXEC_SETUP_OK:
            /*if ( q != NULL )
              return;*/
            this->exec_run_to_completion();
            if ( ! strm.alloc_fail )
              break;
            status = ERR_ALLOC_FAIL;
            /* FALLTHRU */
          default:
            this->send_err( status );
            break;
          case EXEC_QUIT:
            this->poll.quit++;
            break;
          case EXEC_DEBUG:
            break;
        }
      }
      if ( this->is_using_term )
        this->term.line_off = inoff;
      else
        this->wsoff = inoff;
      continue;
    }

    i     = 0;
    used  = 0;
    /* decode http hdrs */
    for ( p = start; p < end; ) {
      eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
      if ( eol == NULL )
        break;
      sz = &eol[ 1 ] - p;
      if ( sz <= 2 ) {
        used = &eol[ 1 ] - start;
        break;
      }
      line[ i ] = p;
      llen[ i ] = sz;
      p = &eol[ 1 ];
      i++;
      if ( i == 64 ) {
        fprintf( stderr, "http header has too many lines (%ld)\n", i );
        goto not_found;
      }
    }
    if ( used == 0 ) /* if didn't find line break which terminates hdrs */
      goto break_loop;
    this->off += used;

    /* GET path HTTP/1.1 */
    if ( toupper( line[ 0 ][ 0 ] ) == 'G' && llen[ 0 ] > 12 ) {
      char wsver[ 128 ], wskey[ 128 ], wspro[ 128 ], *str;
      bool upgrade = false, websock = false;
      size_t start, wskeylen = 0;
      wsver[ 0 ] = wskey[ 0 ] = wspro[ 0 ] = '\0';
      /* find websock stuff */
      for ( j = 1; j < i; j++ ) {
        switch ( line[ j ][ 0 ] ) {
          case 'c': case 'C':  /* Connection: upgrade */
            if ( ::strncasecmp( line[ j ], "Connection: upgrade", 19 ) == 0 )
              upgrade = true;
            break;
          case 'u': case 'U':  /* Upgrade: websocket */
            if ( ::strncasecmp( line[ j ], "Upgrade: websocket", 18 ) == 0 )
              websock = true;
            break;
          case 's': case 'S':  /* Sec-WebSocket-[Version,Key,Protocol] */
            if ( ::strncasecmp( line[ j ], "Sec-WebSocket-", 14 ) == 0 ) {
              const char *ptr = &line[ j ][ 14 ];
              if ( ::strncasecmp( ptr, "Version: ", 9 ) == 0 ) {
                str = wsver;
                start = 23;
              }
              else if ( ::strncasecmp( ptr, "Key: ", 5 ) == 0 ) {
                str = wskey;
                start = 19;
              }
              else if ( ::strncasecmp( ptr, "Protocol: ", 10 ) == 0 ) {
                str = wspro;
                start = 24;
              }
              else {
                str = NULL;
                start = 0;
              }
              if ( str != NULL ) {
                static const char wsguid[] =
                  "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
                for ( k = start; line[ j ][ k ] > ' ' && k - start < 128; k++ )
                  str[ k - start ] = line[ j ][ k ];
                k -= start;
                str[ k ] = '\0';
                if ( str == wskey && k + sizeof( wsguid ) < 128 ) {
                  ::strcpy( &wskey[ k ], wsguid );
                  wskeylen = k + sizeof( wsguid ) - 1;
                }
              }
            }
            break;
          default:
            break;
        }
        /*printf( "[%.*s]\n", (int) llen[ j ] - 2, line[ j ] );*/
      }
      if ( upgrade && websock && wsver[ 0 ] && wskey[ 0 ] /*&& wspro[ 0 ]*/ ) {
        if ( this->send_ws_upgrade( wsver, wskey, wskeylen, wspro ) ) {
          this->websock_off = this->nbytes_sent + this->strm.pending();
          if ( ::strncmp( wspro, "term", 4 ) == 0 ) {
            this->flush();
            this->term.tty_init();
            this->term.tty_prompt();
            this->is_using_term = true;
            this->flush_term();
          }
        }
        else {
          goto not_found;
        }
      }
      else {
        /*printf( "-> %.*s\n", (int) llen[ 0 ], line[ 0 ] );*/
        if ( ! this->send_file( line[ 0 ], llen[ 0 ] ) )
          goto not_found;
      }
    }
    else
      goto not_found;
  }
break_loop:;
  this->pop( EV_PROCESS );
  if ( strm.pending() > 0 )
    this->push( EV_WRITE );
  return;

is_closed:;
  this->pushpop( EV_CLOSE, EV_PROCESS );
  return;

not_found:;
  printf( "-> not found\n" );
  strm.append( page404, sizeof( page404 ) - 1 );
  this->push( EV_WRITE_HI );
  this->is_not_found = true;
}

bool
EvHttpService::flush_term( void )
{
  const char * buf    = this->term.out_buf;
  size_t       buflen = this->term.out_len;
  if ( buflen == 0 )
    return false;

  StreamBuf & strm = *this;
  uint8_t msg[ 2 + 255 ];
  for ( size_t i = 0; i < buflen; i += 255 ) {
    msg[ 0 ] = '@';
    msg[ 1 ] = (uint8_t) kv::min<size_t>( 255, buflen - i );
    ::memcpy( &msg[ 2 ], &buf[ i ], msg[ 1 ] );
    strm.append( msg, (size_t) msg[ 1 ] + 2 );
  }
  this->term.tty_out_reset();
  return true;
}

bool
EvHttpService::publish( EvPublish &pub )
{
  bool flow_good = true;
  if ( this->RedisExec::do_pub( pub ) ) {
    flow_good = ( this->strm.pending() <= this->send_highwater );
    this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
  }
  return flow_good;
}

bool
EvHttpService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  return this->RedisExec::do_hash_to_sub( h, key, keylen );
}
#if 0
void
EvHttpService::cook_string( char *s,  size_t len )
{
  StreamBuf & strm = *this;
  char  * end = &s[ len ],
        * buf;
  size_t  sz,
          off;
  for (;;) {
    sz = ( end - s ) + 1; /* +1 for '\r' to '\r\n' expansion */
    if ( sz > 255 ) /* must fit in a byte */
      sz = 255;
    buf = strm.alloc( sz + 2 );
    if ( buf == NULL )
      return;
    buf[ 0 ] = '@';
    for ( off = 0; s < end; s++ ) {
      if ( *s == '\r' && ( &s[ 1 ] == end || s[ 1 ] != '\n' ) ) {
        if ( off + 2 > sz )
          break;
        *s = '\n'; /* change input '\r' into '\n' so redis msg parses line */
        buf[ 2 + off++ ] = '\r';
        buf[ 2 + off++ ] = '\n';
      }
      else {
        if ( off + 1 > sz )
          break;
        buf[ 2 + off++ ] = *s;
      }
    }
    buf[ 1 ] = (uint8_t) off;
    strm.sz += off + 2;
    if ( s == end )
      return;
  }
}
#endif

size_t
EvHttpService::try_write( void )
{
  if ( this->websock_off != 0 &&
       this->websock_off < this->nbytes_sent + this->pending() )
    if ( ! this->frame_websock() )
      return 0;
  return this->EvConnection::try_write();
}

size_t
EvHttpService::write( void )
{
  if ( this->websock_off != 0 &&
       this->websock_off < this->nbytes_sent + this->pending() )
    if ( ! this->frame_websock() )
      return 0;
  return this->EvConnection::write();
}

bool
EvHttpService::frame_websock( void )
{
  size_t msgcnt = this->wsmsgcnt;
  bool b = this->frame_websock2();
  /* if a message was output, push another prompt out */
  if ( msgcnt != this->wsmsgcnt && this->is_using_term ) {
    if ( this->term.tty_prompt() ) {
      this->flush_term();
      this->frame_websock2();
    }
  }
  return b;
}

bool
EvHttpService::frame_websock2( void )
{
  static const char eol[]    = "\r\n";
  static size_t     eol_size = sizeof( eol ) - 1;
  StreamBuf & strm   = *this;
  size_t      nbytes = this->nbytes_sent,
              off    = strm.woff,
              i;
  char      * newbuf;

  if ( strm.sz > 0 )
    strm.flush();
  /* find websock stream offset */
  for ( ; off < strm.idx; off++ ) {
    nbytes += strm.iov[ off ].iov_len;
    if ( this->websock_off < nbytes )
      break;
  }
  if ( off == strm.idx )
    return true;
  /* concat buffers */
  if ( off + 1 < strm.idx ) {
    nbytes = strm.iov[ off ].iov_len;
    for ( i = off + 1; i < strm.idx; i++ )
      nbytes += strm.iov[ i ].iov_len;
    newbuf = strm.alloc_temp( nbytes );
    if ( newbuf == NULL )
      return false;
    nbytes = strm.iov[ off ].iov_len;
    ::memcpy( newbuf, strm.iov[ off ].iov_base, nbytes );
    for ( i = off + 1; i < strm.idx; i++ ) {
      size_t len = strm.iov[ i ].iov_len;
      ::memcpy( &newbuf[ nbytes ], strm.iov[ i ].iov_base, len );
      nbytes += len;
    }
    strm.iov[ off ].iov_base = newbuf;
    strm.iov[ off ].iov_len  = nbytes;
    strm.idx = off + 1;
  }
  /* frame the new data */
  RedisMsg       msg;
  WebSocketFrame ws;
  char         * buf    = (char *) strm.iov[ off ].iov_base,
               * wsmsg;
  const size_t   buflen = strm.iov[ off ].iov_len;
  size_t         bufoff,
                 msgsize,
                 hsz,
                 sz,
                 totsz = 0;
  RedisMsgStatus mstatus;
  bool           is_literal;
  /* determine the size of each framed json msg */
  for ( bufoff = 0; ; ) {
    msgsize = buflen - bufoff;
    /* the '@'[len] is a literal, not a msg, pass through */
    if ( msgsize > 2 && buf[ bufoff ] == '@' ) {
      sz = (uint8_t) buf[ bufoff + 1 ];
      msgsize = sz + 2;
      if ( bufoff + msgsize > buflen )
        return false;
      is_literal = true;
    }
    else {
      mstatus = msg.unpack( &buf[ bufoff ], msgsize, this->strm.tmp );
      if ( mstatus != REDIS_MSG_OK )
        return false;
      sz = msg.to_almost_json_size( false );
      sz += eol_size;
      is_literal = false;
      this->wsmsgcnt++;
    }
    ws.set( sz, 0, WebSocketFrame::WS_TEXT, true );
    hsz = ws.hdr_size();
    totsz += sz + hsz;
    if ( (bufoff += msgsize) == buflen )
      break;
  }
  /* frame and convert to json */
  if ( totsz > 0 ) {
    newbuf = strm.alloc_temp( totsz );
    /* if only one msg, then it is already decoded */
    if ( totsz == hsz + sz ) {
      ws.encode( newbuf );
      if ( is_literal )
        ::memcpy( &newbuf[ hsz ], &buf[ 2 ], sz );
      else {
        msg.to_almost_json( &newbuf[ hsz ], false );
        ::memcpy( &newbuf[ hsz + sz - eol_size ], eol, eol_size );
      }
      /*printf( "frame: %.*s\n", (int) sz, &newbuf[ hsz ] );*/
    }
    else {
      wsmsg = newbuf;
      for ( bufoff = 0; ; ) {
        msgsize = buflen - bufoff;
        if ( msgsize > 2 && buf[ bufoff ] == '@' ) {
          sz = (uint8_t) buf[ bufoff + 1 ];
          msgsize = sz + 2;
          ws.set( sz, 0, WebSocketFrame::WS_TEXT, true );
          hsz = ws.hdr_size();
          ws.encode( wsmsg );
          ::memcpy( &wsmsg[ hsz ], &buf[ bufoff + 2 ], sz );
        }
        else {
          msg.unpack( &buf[ bufoff ], msgsize, this->strm.tmp );
          sz = msg.to_almost_json_size( false );
          sz += eol_size;
          ws.set( sz, 0, WebSocketFrame::WS_TEXT, true );
          hsz = ws.hdr_size();
          ws.encode( wsmsg );
          msg.to_almost_json( &wsmsg[ hsz ], false );
          ::memcpy( &wsmsg[ hsz + sz - eol_size ], eol, eol_size );
        }
        bufoff += msgsize;
        /*printf( "frame2: %.*s\n", (int) sz, &wsmsg[ hsz ] );*/
        wsmsg = &wsmsg[ hsz + sz ];
        if ( wsmsg == &newbuf[ totsz ] )
          break;
      }
    }
    if ( totsz >= buflen )
      strm.wr_pending += totsz - buflen;
    else
      strm.wr_pending -= buflen - totsz;
    strm.iov[ off ].iov_base = newbuf;
    strm.iov[ off ].iov_len  = totsz;
    this->websock_off += totsz;
  }
  return true;
}

static const char *
get_mime_type( const char *path,  size_t len )
{
  if ( len >= 3 ) {
    const char *p = &path[ len-3 ];
    switch ( p[ 0 ] ) {
#define CASE( c, d, e, s ) case c: if ( p[ 1 ] == d && p[ 2 ] == e ) return s
      /* check for [.]js */
      CASE( '.', 'j', 's', "application/x-javascript" ); break;
      case 't': /* check for .h[t]ml */
        if ( len >= 5 &&
            p[ -2 ] == '.' && p[ -1 ] == 'h' && p[ 1 ] == 'm' && p[ 2 ] == 'l' )
          return "text/html";
        /* FALLTHRU */
        /* fall through, could be .[t]xt */
      default:
        if ( len < 4 || path[ len-4 ] != '.' )
          break;
        switch ( p[ 0 ] ) {
          CASE( 'c', 's', 's', "text/css" ); break;      /* .css */
          CASE( 'h', 't', 'm', "text/html" ); break;     /* .htm */
          CASE( 'j', 'p', 'g', "image/jpeg" ); break;    /* .jpg */
          CASE( 'p', 'n', 'g', "image/png" ); break;     /* .png */
          CASE( 's', 'v', 'g', "image/svg+xml" ); break; /* .svg */
          CASE( 't', 'x', 't', "text/plain" ); break;    /* .txt */
          CASE( 'x', 'm', 'l', "text/xml" ); break;      /* .xml */
#undef CASE
        }
        break;
    }
  }
  return "application/octet-stream";
}

bool
EvHttpService::send_file( const char *get,  size_t getlen )
{
  /* GET /somefile HTTP/1.1 */
  const char *obj = (const char *) ::memchr( get, '/', getlen ),
             *end = (const char *)
                    ( obj ? ::memchr( obj, ' ', &get[ getlen ] - obj ) : NULL );
  struct stat statbuf;
  char   path[ 1024 ];
  size_t len;
  int    fd;
  bool   res;

  if ( obj == NULL || end == NULL )
    return false;
  len = (size_t) ( end - &obj[ 1 ] );
  if ( len > sizeof( path ) - 11 )
    return false;
  if ( len > 0 ) {
    ::memcpy( path, &obj[ 1 ], len );
    path[ len ] = '\0';
  }
  if ( len == 0 || path[ len - 1 ] == '/' ) {
    ::strcpy( &path[ len ], "index.html" );
    len += 10;
  }

  if ( (fd = ::open( path, O_RDONLY )) < 0 )
    return false;
  res = false;
  if ( ::fstat( fd, &statbuf ) == 0 && statbuf.st_size != 0 ) {
    if ( statbuf.st_size <= 10 * 1024 * 1024 ) {
      char * p = this->strm.alloc_temp( 256 + statbuf.st_size );
      if ( p != NULL &&
           ::read( fd, &p[ 256 ], statbuf.st_size ) == statbuf.st_size ) {
        int n = ::snprintf( p, 256,
          "HTTP/1.1 200 OK\r\n"
          "Connection: keep-alive\r\n"
          "Cache-Control: no-cache\r\n"
          "Content-Type: %s\r\n"
          "Content-Length: %u\r\n"
          "\r\n", get_mime_type( path, len ), (int) statbuf.st_size );
        if ( n > 0 && n < 256 ) {
          ::memmove( &p[ 256 - n ], p, n );
          p = &p[ 256 - n ];
          this->strm.append_iov( p, statbuf.st_size + (size_t) n );
          res = true;
        }
      }
    }
    else {
      fprintf( stderr, "File too large: %lu\n", statbuf.st_size );
    }
  }
  ::close( fd );
  return res;
}

bool
EvHttpService::send_ws_upgrade( const char *wsver, const char *wskey,
                                size_t wskeylen,  const char *wspro )
{
  static const char b64[] =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  uint8_t digest[ 160 / 8 ];
  uint32_t val, i, j = 0;
  char wsacc[ 32 ];
  bool res = false;
  /* websock switch hashes wskey with SHA1 and base64 encodes the result */
  SHA1( (const uint8_t *) wskey, wskeylen, digest );
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

  char * p = this->strm.alloc( 256 );
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
      wsver,
      wspro[0]?"Sec-WebSocket-Protocol: ":"", wspro, wspro[0]?"\r\n":"",
      wsacc );
    /*printf( "%.*s", n, p );*/
    if ( n > 0 && n < 256 ) {
      this->strm.sz += n;
      res = true;
    }
  }
  return res;
}

bool
EvHttpService::send_ws_pong( const char *payload,  size_t len )
{
  WebSocketFrame ws;

  ws.set( len, 0, WebSocketFrame::WS_PONG, true );
  char * p = this->strm.alloc( WebSocketFrame::MAX_HEADER_SIZE + len );
  if ( p != NULL ) {
    size_t off = ws.encode( p );
    ::memcpy( &p[ off ], payload, len );
    this->strm.sz += off + len;
    return true;
  }
  return false;
}

size_t
EvHttpService::recv_wsframe( char *start,  char *end )
{
  WebSocketFrame ws;
  size_t hdrsz = ws.decode( start, end - start );
  if ( hdrsz <= 1 ) /* 0 == not enough data for hdr, 1 == closed */
    return hdrsz;
  if ( ws.payload_len > (uint64_t) 10 * 1024 * 1024 ) {
    fprintf( stderr, "Websocket payload too large: %lu\n", ws.payload_len );
    return 1; /* close */
  }
  char *p = &start[ hdrsz ];
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
          size_t sz  = kv::align<size_t>( this->wslen + ws.payload_len, 1024 );
          char * tmp = (char *) ::realloc( this->wsbuf, sz );
          if ( tmp == NULL )
            return 1; /* close */
          this->wsbuf   = tmp;
          this->wsalloc = sz;
        }
      }
      break;
    }
  }
  //printf( "wspayload: %ld\n", ws.payload_len );
  return hdrsz + ws.payload_len;
}

void
EvHttpService::release( void )
{
  this->term.tty_release();
  if ( this->wsbuf != NULL )
    ::free( this->wsbuf );
  this->RedisExec::release();
  this->EvConnection::release_buffers();
  this->push_free_list();
}

void
EvHttpService::push_free_list( void )
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "redis sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_http.push_hd( this );
  }
}

void
EvHttpService::pop_free_list( void )
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_http.pop( this );
  }
}


