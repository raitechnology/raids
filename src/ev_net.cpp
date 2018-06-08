#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raids/ev_net.h>
#include <raids/ev_service.h>
#include <raids/ev_client.h>

using namespace rai;
using namespace ds;
using namespace kv;

/* for setsockopt() */
static int on = 1, off = 0;
static const size_t poll_alloc_incr = 64;

int
EvPoll::init( int numfds,  bool prefetch,  bool single )
{
  size_t sz = sizeof( this->ev[ 0 ] ) * numfds;

  if ( prefetch )
    this->prefetch_queue = EvPrefetchQueue::create();
  this->single_thread = single;

  if ( (this->efd = ::epoll_create( numfds )) < 0 ) {
    perror( "epoll" );
    return -1;
  }
  this->nfds = numfds;
  this->ev   = (struct epoll_event *) aligned_malloc( sz );
  if ( this->ev == NULL ) {
    perror( "malloc" );
    return -1;
  }
  return 0;
}

int
EvPoll::wait( int ms )
{
  int n = ::epoll_wait( this->efd, this->ev, this->nfds, ms );
  if ( n < 0 ) {
    if ( errno == EINTR )
      return 0;
    perror( "epoll_wait" );
    return -1;
  }
  for ( int i = 0; i < n; i++ ) {
    int fd = this->ev[ i ].data.fd;
    if ( ( this->ev[ i ].events & ( EPOLLIN | EPOLLRDHUP ) ) != 0 )
      this->sock[ fd ]->push( EV_READ );
    if ( ( this->ev[ i ].events & ( EPOLLOUT ) ) != 0 )
      this->sock[ fd ]->push( EV_WRITE );
  }
  return n;
}

void
EvPoll::dispatch( void )
{
  EvSocket *s, *next;
  bool use_pref;
  int cnt;
  for (;;) {
    cnt = 0;
    use_pref = ( this->queue[ EV_PROCESS ].cnt > 1 );
    for ( s = this->queue[ EV_PROCESS ].hd; s != NULL; s = next ) {
      next = s->next[ EV_PROCESS ];
      switch ( s->type ) {
	case EV_LISTEN_SOCK:  break;
        case EV_CLIENT_SOCK:  ((EvClient *) s)->process(); break;
        case EV_SERVICE_SOCK: ((EvService *) s)->process( use_pref ); break;
        case EV_TERMINAL:     ((EvTerminal *) s)->process(); break;
      }
      cnt++;
    }
    if ( this->prefetch_queue != NULL && ! this->prefetch_queue->is_empty() )
      this->drain_prefetch( *this->prefetch_queue );
    for ( s = this->queue[ EV_WRITE ].hd; s != NULL; s = next ) {
      next = s->next[ EV_WRITE ];
      switch ( s->type ) {
	case EV_LISTEN_SOCK:  break;
        case EV_CLIENT_SOCK:  ((EvClient *) s)->write(); break;
        case EV_SERVICE_SOCK: ((EvService *) s)->write(); break;
        case EV_TERMINAL:     ((EvTerminal *) s)->write(); break;
      }
      cnt++;
    }
    for ( s = this->queue[ EV_READ ].hd; s != NULL; s = next ) {
      next = s->next[ EV_READ ];
      switch ( s->type ) {
	case EV_LISTEN_SOCK:  ((EvListen *) s)->accept(); break;
        case EV_CLIENT_SOCK:  ((EvClient *) s)->read(); break;
        case EV_SERVICE_SOCK: ((EvService *) s)->read(); break;
        case EV_TERMINAL:     ((EvTerminal *) s)->read(); break;
      }
      cnt++;
    }
    if ( cnt == 0 )
      break;
  }
  if ( this->quit || this->queue[ EV_CLOSE ].hd != NULL )
    this->process_close();
}

void
EvPoll::drain_prefetch( EvPrefetchQueue &q )
{
  RedisKeyCtx *ctx[ PREFETCH_SIZE ];
  EvService   *svc;
  size_t i, j, sz, cnt = 0;

  sz = PREFETCH_SIZE;
  if ( sz > q.count() )
    sz = q.count();
  this->prefetch_cnt[ sz ]++;
  for ( i = 0; i < sz; i++ ) {
    ctx[ i ] = q.pop();
    ctx[ i ]->prefetch();
  }
  i &= ( PREFETCH_SIZE - 1 );
  for ( j = 0; ; ) {
    if ( ctx[ j ]->run( svc ) )
      svc->process( true );
    cnt++;
    if ( --sz == 0 && q.is_empty() ) {
      this->prefetch_cnt[ 0 ] += cnt;
      return;
    }
    j = ( j + 1 ) & ( PREFETCH_SIZE - 1 );
    if ( ! q.is_empty() ) {
      do {
        ctx[ i ] = q.pop();
        ctx[ i ]->prefetch();
        i = ( i + 1 ) & ( PREFETCH_SIZE - 1 );
      } while ( ++sz < PREFETCH_SIZE && ! q.is_empty() );
    }
  }
}

void
EvPoll::dispatch_service( void )
{
  EvSocket *s, *next;
  size_t cnt;

  for (;;) {
    cnt = 0;
    if ( this->prefetch_queue != NULL && this->queue[ EV_PROCESS ].cnt > 1 ) {
      for ( s = this->queue[ EV_PROCESS ].hd; s != NULL; s = next ) {
        next = s->next[ EV_PROCESS ];
        ((EvService *) s)->process( true );
        cnt++;
      }
      if ( ! this->prefetch_queue->is_empty() )
        this->drain_prefetch( *this->prefetch_queue );
    }
    else {
      for ( s = this->queue[ EV_PROCESS ].hd; s != NULL; s = next ) {
        next = s->next[ EV_PROCESS ];
        ((EvService *) s)->process( false );
        cnt++;
      }
    }
    this->prefetch_cnt[ 1 ] += cnt;
    for ( s = this->queue[ EV_WRITE ].hd; s != NULL; s = next ) {
      next = s->next[ EV_WRITE ];
      ((EvService *) s)->write();
      cnt++;
    }
    for ( s = this->queue[ EV_READ ].hd; s != NULL; s = next ) {
      next = s->next[ EV_READ ];
      if ( s->type == EV_SERVICE_SOCK ) {
	((EvService *) s)->read();
      }
      else {
        ((EvListen *) s)->accept();
      }
      cnt++;
    }
    if ( cnt == 0 )
      break;
  }
  if ( this->quit || this->queue[ EV_CLOSE ].hd != NULL )
    this->process_close();
}

void
EvPoll::process_close( void )
{
  EvSocket *s, *next;
  if ( this->quit ) {
    if ( this->queue[ EV_WAIT ].hd == NULL )
      this->quit = 5;
    else {
      for ( s = this->queue[ EV_WAIT ].hd; s != NULL; s = next ) {
        next = s->next[ EV_WAIT ];
        if ( ! s->test( EV_WRITE ) || this->quit >= 5 ) {
          s->popall();
          s->push( EV_CLOSE );
        }
      }
      this->quit++;
    }
  }
  for ( s = this->queue[ EV_CLOSE ].hd; s != NULL; s = next ) {
    next = s->next[ EV_CLOSE ];
    s->close();
    switch ( s->type ) {
      case EV_LISTEN_SOCK:  break;
      case EV_CLIENT_SOCK:  ((EvClient *) s)->process_close(); break;
      case EV_SERVICE_SOCK: ((EvService *) s)->process_close(); break;
      case EV_TERMINAL:     ((EvTerminal *) s)->process_close(); break;
    }
  }
}

int
EvSocket::add_poll( void )
{
  if ( this->fd > this->poll.maxfd ) {
    int xfd = this->fd + poll_alloc_incr;
    EvSocket **tmp;
    if ( xfd < this->poll.nfds )
      xfd = this->poll.nfds;
  try_again:;
    tmp = (EvSocket **) ::realloc( this->poll.sock,
                                   xfd * sizeof( this->poll.sock[ 0 ] ) );
    if ( tmp == NULL ) {
      perror( "realloc" );
      xfd /= 2;
      if ( xfd > this->fd )
        goto try_again;
      return -1;
    }
    for ( int i = this->poll.maxfd + 1; i < xfd; i++ )
      tmp[ i ] = NULL;
    this->poll.sock  = tmp;
    this->poll.maxfd = xfd - 1;
  }
  struct epoll_event event;
  ::memset( &event, 0, sizeof( struct epoll_event ) );
  event.data.fd = this->fd;
  event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
  if ( ::epoll_ctl( this->poll.efd, EPOLL_CTL_ADD, this->fd, &event ) < 0 ) {
    perror( "epoll_ctl" );
    return -1;
  }
  this->poll.sock[ this->fd ] = this;
  this->push( EV_WAIT );
  return 0;
}

void
EvSocket::remove_poll( void )
{
  struct epoll_event event;
  if ( this->fd >= this->poll.maxfd &&
       this->poll.sock[ this->fd ] == this ) {
    ::memset( &event, 0, sizeof( struct epoll_event ) );
    event.data.fd = this->fd;
    event.events  = 0;
    if ( ::epoll_ctl( this->poll.efd, EPOLL_CTL_DEL, this->fd, &event ) < 0 )
      perror( "epoll_ctl" );
    this->poll.sock[ this->fd ] = NULL;
  }
  this->popall();
  if ( this->type != EV_LISTEN_SOCK )
    ((EvConnection *) this)->release();
  if ( this->type == EV_SERVICE_SOCK )
    this->push( EV_DEAD );
  /* other socks are not recycled */
}

void
EvSocket::close( void )
{
  this->remove_poll();
  ::close( this->fd );
}

int
EvListen::listen( const char *ip,  int port )
{
  int  status = 0,
       sock;
  char svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p;

  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags    = AI_PASSIVE;

  status = ::getaddrinfo( ip, svc, &hints, &ai );
  if ( status != 0 ) {
    perror( "getaddrinfo" );
    return -1;
  }
  sock = -1;
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; ) {
    for ( p = ai; p != NULL; p = p->ai_next ) {
      if ( fam == p->ai_family ) {
	sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
	if ( sock < 0 )
	  continue;
        if ( fam == AF_INET6 ) {
	  if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, &off,
	                     sizeof( off ) ) != 0 )
	    perror( "warning: IPV6_V6ONLY" );
        }
	if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &on,
                           sizeof( on ) ) != 0 )
          perror( "warning: SO_REUSEADDR" );
	if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEPORT, &on,
                           sizeof( on ) ) != 0 )
          perror( "warning: SO_REUSEPORT" );
	status = ::bind( sock, p->ai_addr, p->ai_addrlen );
	if ( status == 0 )
	  goto break_loop;
	::close( sock );
        sock = -1;
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
    fam = AF_INET;
  }
break_loop:;
  if ( status != 0 ) {
    perror( "error: bind" );
    goto fail;
  }
  if ( sock == -1 ) {
    fprintf( stderr, "error: failed to create a socket\n" );
    status = -1;
    goto fail;
  }
  status = ::listen( sock, 128 );
  if ( status != 0 ) {
    perror( "error: listen" );
    goto fail;
  }
  this->fd = sock;
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( (status = this->add_poll()) < 0 ) {
    this->fd = -1;
    goto fail;
  }
  if ( 0 ) {
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

int
EvClient::connect( const char *ip,  int port )
{
  int  status = 0,
       sock;
  char svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p;

  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  status = ::getaddrinfo( ip, svc, &hints, &ai );
  if ( status != 0 ) {
    perror( "getaddrinfo" );
    return -1;
  }
  sock = -1;
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; ) {
    for ( p = ai; p != NULL; p = p->ai_next ) {
      if ( fam == p->ai_family ) {
	sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
	if ( sock < 0 )
	  continue;
        if ( fam == AF_INET6 ) {
	  if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, &off,
	                     sizeof( off ) ) != 0 )
	    perror( "warning: IPV6_V6ONLY" );
        }
	if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &on,
                           sizeof( on ) ) != 0 )
          perror( "warning: SO_REUSEADDR" );
	if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEPORT, &on,
                           sizeof( on ) ) != 0 )
          perror( "warning: SO_REUSEPORT" );
	if ( ::setsockopt( sock, SOL_TCP, TCP_NODELAY, &on,
                           sizeof( on ) ) != 0 )
          perror( "warning: TCP_NODELAY" );
	status = ::connect( sock, p->ai_addr, p->ai_addrlen );
	if ( status == 0 )
	  goto break_loop;
	::close( sock );
        sock = -1;
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
    fam = AF_INET;
  }
break_loop:;
  if ( status != 0 ) {
    perror( "error: connect" );
    goto fail;
  }
  if ( sock == -1 ) {
    fprintf( stderr, "error: failed to create a socket\n" );
    status = -1;
    goto fail;
  }
  this->fd = sock;
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( (status = this->add_poll()) < 0 ) {
    this->fd = -1;
    goto fail;
  }
  if ( 0 ) {
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

int
EvTerminal::start( int sock )
{
  this->fd = sock;
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  return this->add_poll();
}

void
EvListen::accept( void )
{
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
  EvService * c;
  if ( (c = (EvService *) this->poll.queue[ EV_DEAD ].hd) != NULL )
    c->pop( EV_DEAD );
  else {
    void * m = aligned_malloc( sizeof( EvService ) * poll_alloc_incr );
    if ( m == NULL ) {
      perror( "accept: no memory" );
      ::close( sock );
      return;
    }
    c = new ( m ) EvService( this->poll );
    for ( int i = poll_alloc_incr - 1; i >= 1; i-- ) {
      new ( (void *) &c[ i ] ) EvService( this->poll );
      c[ i ].push( EV_DEAD );
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
    c->push( EV_DEAD );
  }
}

bool
EvConnection::read( void )
{
  this->adjust_recv();
  if ( &this->recv[ this->len ] < this->recv_end ) {
    ssize_t nbytes = ::read( this->fd, &this->recv[ this->len ],
                             this->recv_end - &this->recv[ this->len ] );
    if ( nbytes > 0 ) {
      this->len += nbytes;
      this->push( EV_PROCESS );
      return true;
    }
    else {
      this->pop( EV_READ );
      if ( nbytes < 0 ) {
        if ( errno != EINTR ) {
          if ( errno != EAGAIN ) {
            if ( errno != ECONNRESET )
              perror( "read" );
            this->popall();
            this->push( EV_CLOSE );
          }
        }
      }
      else if ( nbytes == 0 )
        this->push( EV_CLOSE );
    }
  }
  return false;
}

bool
EvConnection::try_read( void )
{
  /* XXX: check of write side is full and return false */
  this->adjust_recv();
  if ( &this->recv[ this->len + 1024 ] >= this->recv_end ) {
    size_t newsz = ( this->recv_end - this->recv ) * 2;
    void * ex_recv_buf = aligned_malloc( newsz );
    if ( ex_recv_buf == NULL )
      return false;
    ::memcpy( ex_recv_buf, this->recv, this->len );
    if ( this->recv != this->recv_buf )
      ::free( this->recv );
    this->recv = (char *) ex_recv_buf;
    this->recv_end = &this->recv[ newsz ];
  }
  return this->read();
}

bool
EvConnection::write( void )
{
  struct msghdr h;
  StreamBuf & strm = *this;
  if ( strm.sz > 0 )
    strm.flush();
  ::memset( &h, 0, sizeof( h ) );
  h.msg_iov    = &strm.iov[ strm.woff ];
  h.msg_iovlen = strm.idx - strm.woff;
  ssize_t nbytes = ::sendmsg( this->fd, &h, 0 );
  if ( nbytes > 0 ) {
    strm.wr_pending -= nbytes;
    if ( strm.wr_pending == 0 ) {
      strm.idx = strm.woff = 0;
      strm.tmp.reset();
      this->pop( EV_WRITE );
    }
    else {
      for (;;) {
        if ( (size_t) nbytes >= strm.iov[ strm.woff ].iov_len ) {
	  nbytes -= strm.iov[ strm.woff ].iov_len;
	  strm.woff++;
	  if ( nbytes == 0 )
	    break;
	}
	else {
	  char *base = (char *) strm.iov[ strm.woff ].iov_base;
	  strm.iov[ strm.woff ].iov_len -= nbytes;
	  strm.iov[ strm.woff ].iov_base = &base[ nbytes ];
	  break;
	}
      }
    }
    return true;
  }
  if ( errno != EAGAIN && errno != EINTR ) {
    this->popall();
    this->push( EV_CLOSE );
    if ( errno != ECONNRESET )
      perror( "sendmsg" );
  }
  return false;
}

bool
EvConnection::try_write( void )
{
  StreamBuf & strm = *this;
  if ( strm.woff < strm.idx )
    if ( ! this->write() )
      return false;
  if ( strm.woff > strm.vlen / 2 ) {
    uint32_t i = 0;
    while ( strm.woff < strm.vlen )
      strm.iov[ i++ ] = strm.iov[ strm.woff++ ];
    strm.woff = 0;
    strm.idx  = i;
  }
  return true;
}

void
EvConnection::close_alloc_error( void )
{
  fprintf( stderr, "Allocation failed! Closing connection\n" );
  this->popall();
  this->push( EV_CLOSE );
}

