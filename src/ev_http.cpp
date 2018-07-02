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
  static int on = 1, off = 0;
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
  EvHttpService * c;
  if ( (c = (EvHttpService *) this->poll.free_http) != NULL )
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
  if ( ::setsockopt( sock, IPPROTO_TCP, TCP_NODELAY, &off, sizeof( off ) ) != 0)
    perror( "warning: TCP_NODELAY" );
  if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof( on ) ) != 0 )
    perror( "warning: SO_KEEPALIVE" );
  if ( ::setsockopt( sock, SOL_SOCKET, SO_LINGER, &lin, sizeof( lin ) ) != 0 )
    perror( "warning: SO_LINGER" );
  if ( ::setsockopt( sock, SOL_TCP, TCP_NODELAY, &on, sizeof( on ) ) != 0 )
    perror( "warning: TCP_NODELAY" );
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  c->fd = sock;
  if ( c->add_poll() < 0 ) {
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
EvHttpService::process( bool use_prefetch )
{
  StreamBuf       & strm = *this;
  //EvPrefetchQueue * q    = ( use_prefetch ? this->poll.prefetch_queue : NULL );
  size_t            buflen, used, i, j, k, sz;
  char            * p, * eol, * start, * end;
  char            * line[ 64 ];
  size_t            llen[ 64 ];

  for (;;) { 
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    if ( strm.idx + strm.vlen / 4 >= strm.vlen ) {
      if ( ! this->try_write() || strm.idx + 8 >= strm.vlen )
        goto need_write;
    }
    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    /* decode websock frame */
    if ( this->websock_off > 0 ) {
      WebSocketFrame ws;
      used = ws.decode( start, buflen );
      if ( used <= 1 ) { /* 0 == not enough data for hdr, 1 == closed */
        if ( used == 0 )
          goto break_loop;
        goto is_closed;
      }
      p = &start[ used ];
      if ( ws.payload_len > (uint64_t) 10 * 1024 * 1024 ) {
        fprintf( stderr, "Websocket payload too large: %lu\n", ws.payload_len );
        goto is_closed;
      }
      if ( &p[ ws.payload_len ] > end ) { /* if still need more data */
        printf( "need more data\n" );
        goto break_loop;
      }
      if ( ws.mask != 0 ) {
        for ( i = 0; i < ws.payload_len; i += j ) {
          uint32_t bits[ 64 / 4 ];
          j = sizeof( bits );
          if ( i + j > ws.payload_len )
            j = ws.payload_len - i;
          ::memcpy( bits, &p[ i ], j );
          for ( k = 0; k * sizeof( bits[ 0 ] ) < j; k++ )
            bits[ k ] ^= ws.mask;
          ::memcpy( &p[ i ], bits, j );
        }
      }
      //printf( "ws opcode %x\n", ws.opcode );
      switch ( ws.opcode ) {
        case WebSocketFrame::WS_PING: this->send_ws_pong( p, ws.payload_len );
        case WebSocketFrame::WS_PONG: break;
        default: { /* WS_TEXT, WS_BINARY */
          printf( "ws%s%s[%.*s]\n",
                  ( ws.opcode & WebSocketFrame::WS_TEXT ) ? "text" : "",
                  ( ws.opcode & WebSocketFrame::WS_BINARY ) ? "bin" : "",
                  (int) ws.payload_len, p );
          for ( size_t poff = 0; poff < ws.payload_len; ) {
            sz = ws.payload_len - poff;
            RedisMsgStatus mstatus = this->msg.unpack( p, sz, strm.tmp );
            if ( mstatus != REDIS_MSG_OK ) {
              fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                       mstatus, redis_msg_status_string( mstatus ),
                       ws.payload_len - poff );
              poff = ws.payload_len;
              break;
            }
            poff += sz;
            ExecStatus status;
            if ( (status = this->exec( this, NULL )) == EXEC_OK )
              if ( strm.alloc_fail )
                status = EXEC_ALLOC_FAIL;
            switch ( status ) {
              case EXEC_SETUP_OK:
                /*if ( q != NULL )
                  return;*/
                this->exec_run_to_completion();
                if ( ! strm.alloc_fail )
                  break;
                status = EXEC_ALLOC_FAIL;
                /* fall through */
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
          break;
        }
      }
      //printf( "wspayload: %ld\n", ws.payload_len );
      this->off += used + ws.payload_len;
      continue;
    }

    i     = 0;
    used  = 0;
    /* decode http hdrs */
    for ( p = start; p < end; ) {
      eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
      if ( eol != NULL ) {
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
      else {
        if ( ! this->try_read() )
          goto break_loop;
        break;
      }
    }
    if ( used == 0 )
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
            //printf( "[%.*s]\n", (int) llen[ j ] - 2, line[ j ] );
            break;
        }
      }
      if ( upgrade && websock && wsver[ 0 ] && wskey[ 0 ] && wspro[ 0 ] ) {
        if ( this->send_ws_upgrade( wsver, wskey, wskeylen, wspro ) )
          this->websock_off = this->nbytes_sent + this->strm.pending();
        else
          goto not_found;
      }
      else if ( ! this->send_file( line[ 0 ], llen[ 0 ] ) )
        goto not_found;
      //printf( "-> ok\n" );
    }
    else
      goto not_found;
    //strm.append( indexhtml, sizeof( indexhtml ) - 1 );
  }
not_found:;
  printf( "-> not found\n" );
  strm.append( page404, sizeof( page404 ) - 1 );
is_closed:;
  this->push( EV_CLOSE );
break_loop:;
  this->pop( EV_PROCESS );
  if ( strm.pending() > 0 ) {
need_write:;
    this->push( EV_WRITE );
  }
  return;
}

bool
EvHttpService::try_write( void )
{
  if ( this->websock_off != 0 &&
       this->websock_off < this->nbytes_sent + this->pending() )
    if ( ! this->frame_websock() )
      return false;
  return this->EvConnection::try_write();
}

bool
EvHttpService::write( void )
{
  if ( this->websock_off != 0 &&
       this->websock_off < this->nbytes_sent + this->pending() )
    if ( ! this->frame_websock() )
      return false;
  return this->EvConnection::write();
}

bool
EvHttpService::frame_websock( void )
{
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
  /* determine the size of each framed json msg */
  for ( bufoff = 0; ; ) {
    msgsize = buflen - bufoff;
    mstatus = msg.unpack( &buf[ bufoff ], msgsize, this->strm.tmp );
    if ( mstatus != REDIS_MSG_OK )
      return false;
    sz = msg.to_almost_json_size( false );
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
      msg.to_almost_json( &newbuf[ hsz ], false );
      printf( "frame: %.*s\n", (int) sz, &newbuf[ hsz ] );
    }
    else {
      wsmsg = newbuf;
      for ( bufoff = 0; ; ) {
        msgsize = buflen - bufoff;
        msg.unpack( &buf[ bufoff ], msgsize, this->strm.tmp );
        bufoff += msgsize;
        sz = msg.to_almost_json_size( false );
        ws.set( sz, 0, WebSocketFrame::WS_TEXT, true );
        hsz = ws.hdr_size();
        ws.encode( wsmsg );
        msg.to_almost_json( &wsmsg[ hsz ], false );
        printf( "frame2: %.*s\n", (int) sz, &wsmsg[ hsz ] );
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
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  uint8_t digest[ 160 / 8 ];
  uint32_t val, i, j = 0;
  char out[ 32 ];
  bool res = false;
  /* websock switch hashes wskey with SHA1 and base64 encodes the result */
  SHA1( (const uint8_t *) wskey, wskeylen, digest );
  /* base64 encode it */
  for ( i = 0; i < 18; i += 3 ) {
    val = ( (uint32_t) digest[ i ] << 16 ) | ( (uint32_t) digest[ i+1 ] << 8 ) |
          (uint32_t) digest[ i+2 ];
    out[ j ]     = b64[ ( val >> 18 ) & 63U ];
    out[ j + 1 ] = b64[ ( val >> 12 ) & 63U ];
    out[ j + 2 ] = b64[ ( val >> 6 ) & 63U ];
    out[ j + 3 ] = b64[ val & 63U ];
    j += 4;
  }
  val = (uint32_t) ( digest[ i ] << 16 ) | (uint32_t) ( digest[ i+1 ] << 8 );
  out[ j ] = b64[ ( val >> 18 ) & 63U ];
  out[ j + 1 ] = b64[ ( val >> 12 ) & 63U ];
  out[ j + 2 ] = b64[ ( val >> 6 ) & 63U ];
  out[ j + 3 ] = '=';
  out[ j + 4 ] = '\0';

  char * p = this->strm.alloc( 256 );
  if ( p != NULL ) {
    int n = ::snprintf( p, 256,
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Connection: upgrade\r\n"
      "Upgrade: websocket\r\n"
      "Sec-WebSocket-Version: %s\r\n"
      "Sec-WebSocket-Protocol: %s\r\n"
      "Sec-WebSocket-Accept: %s\r\n"
      "\r\n", wsver, wspro, out );
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

void
EvHttpService::release( void )
{
  this->websock_off = 0; /* this structure will be reused w/different fd */
  this->RedisExec::release();
  this->EvConnection::release();
  this->push_free_list();
}

void
EvHttpService::push_free_list( void )
{
  if ( this->state != 0 )
    this->popall();
  this->next[ 0 ] = this->poll.free_http;
  this->poll.free_http = this;
}

void
EvHttpService::pop_free_list( void )
{
  this->poll.free_http = this->next[ 0 ];
  this->next[ 0 ] = NULL;
}


