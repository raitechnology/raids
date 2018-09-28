#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raids/ev_tcp.h>
#include <raids/ev_service.h>

using namespace rai;
using namespace ds;

int
EvTcpListen::listen( const char *ip,  int port )
{
  static int on = 1, off = 0;
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
  status = ::listen( sock, 256 );
  if ( status != 0 ) {
    perror( "error: listen" );
    goto fail;
  }
  this->fd = sock;
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( (status = this->poll.add_sock( this )) < 0 ) {
    this->fd = -1;
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

void
EvRedisListen::accept( void )
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
  EvRedisService * c = this->poll.free_redis.hd;
  if ( c != NULL )
    c->pop_free_list();
  else {
    void * m = aligned_malloc( sizeof( EvRedisService ) * EvPoll::ALLOC_INCR );
    if ( m == NULL ) {
      perror( "accept: no memory" );
      ::close( sock );
      return;
    }
    c = new ( m ) EvRedisService( this->poll );
    for ( int i = EvPoll::ALLOC_INCR - 1; i >= 1; i-- ) {
      new ( (void *) &c[ i ] ) EvRedisService( this->poll );
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
  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    c->push_free_list();
  }
}

int
EvTcpClient::connect( const char *ip,  int port )
{
  /* for setsockopt() */
  static int on = 1, off = 0;
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

  if ( this->poll.add_sock( this ) < 0 ) {
    this->fd = -1;
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

