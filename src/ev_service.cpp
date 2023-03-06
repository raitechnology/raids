#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
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

EvSocket *
EvRedisListen::accept( void ) noexcept
{
  EvRedisService *c =
    this->poll.get_free_list<EvRedisService, RoutePublish &, EvListen &>(
      this->accept_sock_type, this->sub_route, *this );
  if ( c == NULL )
    return NULL;
  if ( ! this->accept2( *c, "redis" ) )
    return NULL;
  c->setup_ids( c->fd, ++this->timer_id );
  return c;
}

EvSocket *
EvRedisUnixListen::accept( void ) noexcept
{
  EvRedisService *c =
    this->poll.get_free_list<EvRedisService, RoutePublish &, EvListen &>(
      this->accept_sock_type, this->sub_route, *this );
  if ( c == NULL )
    return NULL;
  if ( ! this->accept2( *c, "redis" ) )
    return NULL;
  c->setup_ids( c->fd, ++this->timer_id );
  return c;
}

void
EvRedisService::process( void ) noexcept
{
  if ( this->stamp == 0 ) {
    this->stamp = this->active_ns;
    if ( this->notify != NULL )
      this->notify->on_connect( *this );
  }
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
        fprintf( stderr, "protocol error(%d/%s), ignoring %u bytes\n",
                 mstatus, ds_msg_status_string( mstatus ), (uint32_t) buflen );
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
    this->off += (uint32_t) buflen;
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

void
EvRedisService::pub_data_loss( EvPublish &pub ) noexcept
{
  if ( this->notify != NULL )
    this->notify->on_data_loss( *this, pub );
}

bool
EvRedisService::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  bool flow_good = true;
  int  status    = this->RedisExec::do_pub( pub, cm, this );

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
EvRedisService::get_service( void *host,  uint16_t &svc ) noexcept
{
  return this->listen.get_service( host, svc );
}

bool
EvRedisService::set_session( const char session[ MAX_SESSION_LEN ] ) noexcept
{
  size_t len = ::strlen( session );
  return this->RedisExec::set_session( session, len );
}

size_t
EvRedisService::get_userid( char userid[ MAX_USERID_LEN ] ) noexcept
{
  ::memcpy( userid, "nobody", 7 );
  return 7;
}

size_t
EvRedisService::get_session( uint16_t svc,
                             char session[ MAX_SESSION_LEN ] ) noexcept
{
  if ( this->session_len > 0 ) {
    uint16_t tmp = 0;
    if ( this->listen.get_service( NULL, tmp ) && svc == tmp ) {
      ::memcpy( session, this->session, this->session_len );
      session[ this->session_len ] = '\0';
      return this->session_len;
    }
  }
  session[ 0 ] = '\0';
  return 0;
}

size_t
EvRedisService::get_subscriptions( uint16_t svc,  SubRouteDB &subs ) noexcept
{
  uint16_t tmp = 0;
  if ( ! this->listen.get_service( NULL, tmp ) || svc != tmp )
    return 0;
  return this->RedisExec::do_get_subscriptions( subs );
}

size_t
EvRedisService::get_patterns( uint16_t svc,  int pat_fmt,
                              SubRouteDB &pats ) noexcept
{
  uint16_t tmp = 0;
  if ( ! this->listen.get_service( NULL, tmp ) || svc != tmp )
    return 0;
  if ( pat_fmt != GLOB_PATTERN_FMT )
    return 0;
  return this->RedisExec::do_get_patterns( pats );
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
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
}

void
EvRedisService::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
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
    printf( "prefetch count %u\n",
	    (uint32_t) this->poll.prefetch_queue->count() );
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


