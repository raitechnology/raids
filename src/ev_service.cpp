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
#include <sys/types.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <raids/ev_service.h>

using namespace rai;
using namespace ds;
using namespace kv;

EvRedisListen::EvRedisListen( EvPoll &p ) noexcept
  : EvTcpListen( p, "redis_tcp_listen", "redis_sock" ),
    sub_route( p.sub_route ) {}

EvRedisUnixListen::EvRedisUnixListen( EvPoll &p ) noexcept
  : EvUnixListen( p, "redis_unix_listen", "redis_sock" ),
    sub_route( p.sub_route ) {}

EvRedisListen::EvRedisListen( EvPoll &p,  RoutePublish &sr ) noexcept
  : EvTcpListen( p, "redis_tcp_listen", "redis_sock" ),
    sub_route( sr ) {}

EvRedisUnixListen::EvRedisUnixListen( EvPoll &p,  RoutePublish &sr ) noexcept
  : EvUnixListen( p, "redis_unix_listen", "redis_sock" ),
    sub_route( sr ) {}

bool
EvRedisListen::accept( void ) noexcept
{
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  int sock = ::accept( this->fd, (struct sockaddr *) &addr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
	perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    return false;
  }
  /*char buf[ 80 ];*/
  /*printf( "accept from %s\n", get_ip_str( &addr, buf, sizeof( buf ) ) );*/
  EvRedisService *c =
    this->poll.get_free_list2<EvRedisService, RoutePublish>(
      this->accept_sock_type, this->sub_route );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
  }
  EvTcpListen::set_sock_opts( this->poll, sock, this->sock_opts );
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "redis" );
  c->setup_ids( sock, ++this->timer_id );

  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    this->poll.push_free_list( c );
    return false;
  }
  return true;
}

bool
EvRedisUnixListen::accept( void ) noexcept
{
  struct sockaddr_un sunaddr;
  socklen_t addrlen = sizeof( sunaddr );
  int sock = ::accept( this->fd, (struct sockaddr *) &sunaddr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
	perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    return false;
  }
  EvRedisService *c =
    this->poll.get_free_list2<EvRedisService, RoutePublish>(
       this->accept_sock_type, this->sub_route );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  this->PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, "redis" );
  c->setup_ids( sock, ++this->timer_id );

  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    this->poll.push_free_list( c );
    return false;
  }
  return true;
}


void
EvRedisService::process( void ) noexcept
{
  if ( ! this->cont_list.is_empty() )
    this->drain_continuations( this );

  StreamBuf       & strm = *this;
  EvPrefetchQueue * q    = this->poll.prefetch_queue;
  RedisMsgStatus    mstatus;
  ExecStatus        status;

  for (;;) {
    size_t buflen = this->len - this->off;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    mstatus = this->msg.unpack( &this->recv[ this->off ], buflen, strm.tmp );
    if ( mstatus != DS_MSG_STATUS_OK ) {
      if ( mstatus != DS_MSG_STATUS_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, ds_msg_status_string( mstatus ), buflen );
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
    this->msgs_recv++;

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
        if ( ! strm.alloc_fail ) {
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
        this->send_status( status, KEY_OK );
        break;
      case EXEC_DEBUG:
        this->debug();
        break;
    }
  }
  if ( ! this->push_write() )
    strm.reset();
}

bool
EvRedisService::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  bool flow_good = true;
  int  status    = this->RedisExec::do_pub( pub, cm );

  if ( ( status & RPUB_FORWARD_MSG ) != 0 ) {
    flow_good = ( this->strm.pending() <= this->send_highwater );
    this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
  }
  if ( ( status & RPUB_CONTINUE_MSG ) != 0 ) {
    this->push_continue_list( cm );
    this->idle_push( EV_PROCESS );
  }
  return flow_good;
}

uint8_t
EvRedisService::is_subscribed( const NotifySub &sub ) noexcept
{
  return this->RedisExec::test_subscribed( sub );
}

uint8_t
EvRedisService::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  return this->RedisExec::test_psubscribed( pat );
}

bool
EvRedisService::timer_expire( uint64_t tid,  uint64_t event_id ) noexcept
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

bool
EvRedisService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  return this->RedisExec::do_hash_to_sub( h, key, keylen );
}

void
EvRedisService::release( void ) noexcept
{
  this->RedisExec::release();
  this->EvConnection::release_buffers();
  this->poll.push_free_list( this );
}

bool
EvRedisService::match( PeerMatchArgs &ka ) noexcept
{
  if ( this->sub_tab.sub_count + this->pat_tab.sub_count() != 0 ) {
    if ( EvSocket::client_match( *this, &ka, MARG( "pubsub" ), NULL ) )
      return true;
  }
  else {
    if ( EvSocket::client_match( *this, &ka, MARG( "normal" ), NULL ) )
      return true;
  }
  return this->EvConnection::match( ka );
}

int
EvRedisService::client_list( char *buf,  size_t buflen ) noexcept
{
  int i = this->EvConnection::client_list( buf, buflen );
  if ( i >= 0 )
    i += this->exec_client_list( &buf[ i ], buflen - i );
  return i;
}

void
EvRedisService::debug( void ) noexcept
{
  char buf[ 1024 ];
  EvSocket *s;
  size_t i;
  /*for ( i = 0; i < EvPoll::PREFETCH_SIZE; i++ ) {
    if ( this->poll.prefetch_cnt[ i ] != 0 )
      printf( "[%ld]: %lu\n", i, this->poll.prefetch_cnt[ i ] );
  }*/
  printf( "heap: " );
  for ( i = 0; i < this->poll.ev_queue.num_elems; i++ ) {
    s = this->poll.ev_queue.heap[ i ];
    int sz = (int) s->client_list( buf, sizeof( buf ) );
    printf( "%d/%.*s ", s->fd, sz, buf );
  }
  printf( "\n" );
  if ( this->poll.prefetch_queue == NULL ||
       this->poll.prefetch_queue->is_empty() )
    printf( "prefetch empty\n" );
  else
    printf( "prefetch count %lu\n",
	    this->poll.prefetch_queue->count() );
}


void
EvRedisService::key_prefetch( EvKeyCtx &ctx ) noexcept
{
  this->RedisExec::exec_key_prefetch( ctx );
}

int
EvRedisService::key_continue( EvKeyCtx &ctx ) noexcept
{
  return this->RedisExec::exec_key_continue( ctx );
}


