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
#include <raids/ev_unix.h>

using namespace rai;
using namespace ds;
using namespace kv;

EvRedisListen::EvRedisListen( EvPoll &p ) noexcept
             : EvTcpListen( p, this->ops ),
               timer_id( (uint64_t) EV_REDIS_SOCK << 56 )
{
}

EvRedisUnixListen::EvRedisUnixListen( EvPoll &p ) noexcept
                 : EvUnixListen( p, this->ops ),
                   timer_id( (uint64_t) EV_REDIS_SOCK << 48 )
{
}

bool
EvRedisListen::accept( void ) noexcept
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
    return false;
  }
  /*char buf[ 80 ];*/
  /*printf( "accept from %s\n", get_ip_str( &addr, buf, sizeof( buf ) ) );*/
  EvRedisService *c =
    this->poll.get_free_list<EvRedisService>( this->poll.free_redis );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
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
  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "redis" );
  c->setup_ids( sock, ++this->timer_id );

  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    c->push_free_list();
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
  if ( strm.pending() > 0 )
    this->push( EV_WRITE );
  else
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
  this->push_free_list();
}

void
EvRedisService::push_free_list( void ) noexcept
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "redis sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_redis.push_hd( this );
  }
}

void
EvRedisService::pop_free_list( void ) noexcept
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_redis.pop( this );
  }
}

bool
EvRedisServiceOps::match( PeerData &pd,  PeerMatchArgs &ka ) noexcept
{
  EvRedisService & svc = (EvRedisService &) pd;
  if ( svc.sub_tab.sub_count() + svc.pat_tab.sub_count() != 0 ) {
    if ( this->EvSocketOps::client_match( pd, ka, MARG( "pubsub" ), NULL ) )
      return true;
  }
  else {
    if ( this->EvSocketOps::client_match( pd, ka, MARG( "normal" ), NULL ) )
      return true;
  }
  return this->EvConnectionOps::match( pd, ka );
}

int
EvRedisServiceOps::client_list( PeerData &pd,  char *buf,
                                size_t buflen ) noexcept
{
  int i = this->EvConnectionOps::client_list( pd, buf, buflen );
  if ( i >= 0 )
    i += ((EvRedisService &) pd).client_list( &buf[ i ], buflen - i );
  return i;
}

void
EvRedisService::debug( void ) noexcept
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
  for ( i = 0; i < this->poll.ev_queue.num_elems; i++ ) {
    s = this->poll.ev_queue.heap[ i ];
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
