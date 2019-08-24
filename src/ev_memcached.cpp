#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raids/ev_memcached.h>

using namespace rai;
using namespace ds;
using namespace kv;

void
EvMemcachedListen::accept( void )
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
  EvMemcachedService * c = this->poll.free_memcached.hd;
  if ( c != NULL )
    c->pop_free_list();
  else {
    void * m = aligned_malloc( sizeof( EvMemcachedService ) *
                               EvPoll::ALLOC_INCR );
    if ( m == NULL ) {
      perror( "accept: no memory" );
      ::close( sock );
      return;
    }
    c = new ( m ) EvMemcachedService( this->poll );
    for ( int i = EvPoll::ALLOC_INCR - 1; i >= 1; i-- ) {
      new ( (void *) &c[ i ] ) EvMemcachedService( this->poll );
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
  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    c->push_free_list();
  }
}

const char *
rai::ds::memcached_status_string( MemcachedStatus status )
{
  switch ( status ) {
    case MEMCACHED_OK: return "OK";
    case MEMCACHED_MSG_PARTIAL: return "Partial";
    case MEMCACHED_EMPTY: return "Empty";
    case MEMCACHED_SETUP_OK: return "Setup OK";
    case MEMCACHED_SUCCESS: return "Success";
    case MEMCACHED_DEPENDS: return "Depends";
    case MEMCACHED_CONTINUE: return "Continue";
    case MEMCACHED_QUIT: return "Quit";
    case MEMCACHED_VERSION: return "Version";
    case MEMCACHED_ALLOC_FAIL: return "Alloc fail";
    case MEMCACHED_BAD_CMD: return "Bad cmd";
    case MEMCACHED_BAD_ARGS: return "Bad args";
    case MEMCACHED_INT_OVERFLOW: return "Integer overflow";
    case MEMCACHED_BAD_INT: return "Bad integer";
    case MEMCACHED_ERR_KV: return "Err KV";
    case MEMCACHED_BAD_TYPE: return "Bad type";
    case MEMCACHED_NOT_IMPL: return "Not impl";
    case MEMCACHED_BAD_PAD: return "Bad pad";
    case MEMCACHED_BAD_BIN_ARGS: return "Bad binary args";
    case MEMCACHED_BAD_BIN_CMD: return "Bad binary cmd";
  }
  return "Unknown";
}

void
EvMemcachedService::process( bool use_prefetch )
{
  static const char   error[]   = "ERROR";
  static const size_t error_len = sizeof( error ) - 1;
  StreamBuf       & strm = *this;
  EvPrefetchQueue * q    = ( use_prefetch ? this->poll.prefetch_queue : NULL );
  size_t            buflen;
  MemcachedStatus   mstatus,
                    status;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    mstatus = this->unpack( &this->recv[ this->off ], buflen );
    if ( mstatus != MEMCACHED_OK ) {
      if ( mstatus != MEMCACHED_MSG_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, memcached_status_string( mstatus ), buflen );
        this->off = this->len;
        strm.sz += this->send_string( error, error_len );
        /*this->pushpop( EV_CLOSE, EV_PROCESS );
        this->pop( EV_PROCESS );
        break;*/
      }
      /* need more data, switch to read (wait for poll)*/
      /*this->pushpop( EV_READ, EV_READ_LO );*/
      this->pop( EV_PROCESS );
      break;
    }
    this->off += buflen;

    if ( (status = this->exec( this, q )) == MEMCACHED_OK )
      if ( strm.alloc_fail )
        status = MEMCACHED_ALLOC_FAIL;
    switch ( status ) {
      case MEMCACHED_SETUP_OK:
        if ( q != NULL ) {
          this->pushpop( EV_PREFETCH, EV_PROCESS ); /* prefetch keys */
          return;
        }
        this->exec_run_to_completion();
        if ( ! strm.alloc_fail )
          break;
        status = MEMCACHED_ALLOC_FAIL;
        /* fall through */
      default:
        this->send_err( status );
        break;
      case MEMCACHED_QUIT:
        this->push( EV_SHUTDOWN );
        break;
    }
  }
  if ( strm.pending() > 0 )
    this->push( EV_WRITE );
}

void
EvMemcachedService::release( void )
{
  this->MemcachedExec::release();
  this->EvConnection::release_buffers();
  this->push_free_list();
}

void
EvMemcachedService::push_free_list( void )
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "memcached sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_memcached.push_hd( this );
  }
}

void
EvMemcachedService::pop_free_list( void )
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_memcached.pop( this );
  }
}

