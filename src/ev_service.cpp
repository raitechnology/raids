#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <raids/ev_service.h>
#include <raids/ev_tcp.h>

using namespace rai;
using namespace ds;
using namespace kv;

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

void
EvRedisService::process( bool use_prefetch )
{
  StreamBuf       & strm = *this;
  EvPrefetchQueue * q    = ( use_prefetch ? this->poll.prefetch_queue : NULL );
  size_t            buflen;
  RedisMsgStatus    mstatus;
  ExecStatus        status;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    mstatus = this->msg.unpack( &this->recv[ this->off ], buflen, strm.tmp );
    if ( mstatus != REDIS_MSG_OK ) {
      if ( mstatus != REDIS_MSG_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, redis_msg_status_string( mstatus ), buflen );
        this->off = this->len;
        /*this->pushpop( EV_CLOSE, EV_PROCESS );*/
        this->pop( EV_PROCESS );
        break;
      }
      /* need more data, switch to read (wait for poll)*/
      /*this->pushpop( EV_READ, EV_READ_LO );*/
      this->pop( EV_PROCESS );
      break;
    }
    this->off += buflen;

    if ( (status = this->exec( this, q )) == EXEC_OK )
      if ( strm.alloc_fail )
        status = ERR_ALLOC_FAIL;
    switch ( status ) {
      case EXEC_SETUP_OK:
        if ( q != NULL ) {
          this->pushpop( EV_PREFETCH, EV_PROCESS ); /* prefetch keys */
          return;
        }
        this->exec_run_to_completion();
        if ( ! strm.alloc_fail )
          break;
        status = ERR_ALLOC_FAIL;
        /* FALLTHRU */
      case EXEC_QUIT:
        if ( status == EXEC_QUIT )
          this->push( EV_SHUTDOWN );
        /* FALLTHRU */
      default:
        this->send_err( status );
        break;
      case EXEC_DEBUG:
        this->debug();
        break;
    }
  }
  if ( strm.pending() > 0 )
    this->push( EV_WRITE );
}

bool
EvRedisService::on_msg( EvPublish &pub )
{
  bool flow_good = true;
  if ( this->RedisExec::do_pub( pub ) ) {
    flow_good = ( this->strm.pending() <= this->send_highwater );
    this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
  }
  return flow_good;
}

bool
EvRedisService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  return this->RedisExec::do_hash_to_sub( h, key, keylen );
}

void
EvRedisService::release( void )
{
  this->RedisExec::release();
  this->EvConnection::release_buffers();
  this->push_free_list();
}

void
EvRedisService::push_free_list( void )
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "redis sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_redis.push_hd( this );
  }
}

void
EvRedisService::pop_free_list( void )
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_redis.pop( this );
  }
}

void
EvRedisService::debug( void )
{
  struct sockaddr_storage addr;
  socklen_t addrlen;
  char buf[ 128 ], svc[ 32 ];
  EvSocket *s;
  size_t i;
  for ( i = 0; i < EvPoll::PREFETCH_SIZE; i++ ) {
    if ( this->poll.prefetch_cnt[ i ] != 0 )
      printf( "[%ld]: %lu\n", i, this->poll.prefetch_cnt[ i ] );
  }
  printf( "heap: " );
  for ( i = 0; i < this->poll.queue.num_elems; i++ ) {
    s = this->poll.queue.heap[ i ];
    if ( s->type != EV_LISTEN_SOCK ) {
      addrlen = sizeof( addr );
      getpeername( s->fd, (struct sockaddr*) &addr, &addrlen );
      getnameinfo( (struct sockaddr*) &addr, addrlen, buf, sizeof( buf ),
                   svc, sizeof( svc ), NI_NUMERICHOST | NI_NUMERICSERV );
    }
    else {
      buf[ 0 ] = 'L'; buf[ 1 ] = '\0';
      svc[ 0 ] = 0;
    }
    printf( "%d/%s:%s ", s->fd, buf, svc );
  }
  printf( "\n" );
  if ( this->poll.prefetch_queue == NULL ||
       this->poll.prefetch_queue->is_empty() )
    printf( "prefetch empty\n" );
  else
    printf( "prefetch count %lu\n",
	    this->poll.prefetch_queue->count() );
}

