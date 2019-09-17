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
#include <raids/ev_http.h>
#include <raids/ev_nats.h>
#include <raids/ev_capr.h>
#include <raids/ev_rv.h>
#include <raids/ev_publish.h>
#include <raids/kv_pubsub.h>
#include <raids/ev_memcached.h>
#include <raids/bit_iter.h>
#include <raids/timer_queue.h>

using namespace rai;
using namespace ds;
using namespace kv;

int
EvPoll::init( int numfds,  bool prefetch/*,  bool single*/ )
{
  size_t sz = sizeof( this->ev[ 0 ] ) * numfds;

  if ( prefetch )
    this->prefetch_queue = EvPrefetchQueue::create();
  /*this->single_thread = single;*/

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
  this->timer_queue = EvTimerQueue::create_timer_queue( *this );
  if ( this->timer_queue == NULL )
    return -1;
  return 0;
}

int
EvPoll::init_shm( EvShm &shm )
{
  this->map    = shm.map;
  this->ctx_id = shm.ctx_id;
  if ( (this->pubsub = KvPubSub::create( *this )) == NULL ) {
    fprintf( stderr, "unable to open unix kv dgram socket\n" );
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
    EvSocket *s = this->sock[ this->ev[ i ].data.fd ];
    if ( ( this->ev[ i ].events & ( EPOLLIN | EPOLLRDHUP ) ) != 0 )
      s->idle_push( s->type != EV_LISTEN_SOCK ? EV_READ : EV_READ_HI );
    if ( ( this->ev[ i ].events & ( EPOLLOUT ) ) != 0 )
      s->idle_push( EV_WRITE );
  }
  return n;
}

const char *
sock_type_string( EvSockType t )
{
  switch ( t ) {
    case EV_REDIS_SOCK:     return "redis";
    case EV_HTTP_SOCK:      return "http";
    case EV_LISTEN_SOCK:    return "listen"; /* virtual accept */
    case EV_CLIENT_SOCK:    return "client";
    case EV_TERMINAL:       return "term";
    case EV_NATS_SOCK:      return "nats";
    case EV_CAPR_SOCK:      return "capr";
    case EV_RV_SOCK:        return "rv";
    case EV_KV_PUBSUB:      return "kv_pubsub";
    case EV_SHM_SOCK:       return "shm";
    case EV_TIMER_QUEUE:    return "timer";
    case EV_SHM_SVC:        return "svc";
    case EV_MEMCACHED_SOCK: return "memcached";
    case EV_MEMUDP_SOCK:    return "memcached_udp";
    case EV_CLIENTUDP_SOCK: return "client_udp";
  }
  return "unknown";
}

void
EvPoll::drain_prefetch( void )
{
  EvPrefetchQueue & pq = *this->prefetch_queue;
  EvKeyCtx * ctx[ PREFETCH_SIZE ];
  EvSocket * s;
  size_t i, j, sz, cnt = 0;

  sz = PREFETCH_SIZE;
  if ( sz > pq.count() )
    sz = pq.count();
  for ( i = 0; i < sz; i++ ) {
    ctx[ i ] = pq.pop();
    EvKeyCtx & k = *ctx[ i ];
    s = k.owner;
    switch ( s->type ) {
      case EV_REDIS_SOCK:
        ((EvRedisService *) s)->exec_key_prefetch( k ); break;
      case EV_HTTP_SOCK:
        ((EvHttpService *) s)->exec_key_prefetch( k ); break;
      case EV_MEMCACHED_SOCK:
        ((EvMemcachedService *) s)->exec_key_prefetch( k ); break;
      case EV_MEMUDP_SOCK:
        ((EvMemcachedUdp *) s)->exec->exec_key_prefetch( k ); break;
      case EV_SHM_SOCK:
        /*((EvShmClient *) s)->exec->exec_key_prefetch( k );*/ break;
      case EV_NATS_SOCK:      break;
      case EV_CAPR_SOCK:      break;
      case EV_RV_SOCK:        break;
      case EV_KV_PUBSUB:      break;
      case EV_SHM_SVC:        break;
      case EV_LISTEN_SOCK:    break;
      case EV_CLIENT_SOCK:    break;
      case EV_TERMINAL:       break;
      case EV_TIMER_QUEUE:    break;
      case EV_CLIENTUDP_SOCK: break;
    }
    /*ctx[ i ]->prefetch();*/
  }
  this->prefetch_cnt[ sz ]++;
  i &= ( PREFETCH_SIZE - 1 );
  for ( j = 0; ; ) {
    int status = 0;
    EvKeyCtx & k = *ctx[ j ];
    s = k.owner;
    switch ( s->type ) {
      case EV_REDIS_SOCK:
        status = ((EvRedisService *) s)->exec_key_continue( k ); break;
      case EV_HTTP_SOCK:
        status = ((EvHttpService *) s)->exec_key_continue( k ); break;
      case EV_MEMCACHED_SOCK:
        status = ((EvMemcachedService *) s)->exec_key_continue( k ); break;
      case EV_MEMUDP_SOCK:
        status = ((EvMemcachedUdp *) s)->exec->exec_key_continue( k ); break;
      case EV_SHM_SOCK:
        /*status = ((EvShmClient *) s)->exec->exec_key_continue( k );*/ break;
      case EV_NATS_SOCK:      break;
      case EV_CAPR_SOCK:      break;
      case EV_RV_SOCK:        break;
      case EV_KV_PUBSUB:      break;
      case EV_SHM_SVC:        break;
      case EV_LISTEN_SOCK:    break;
      case EV_CLIENT_SOCK:    break;
      case EV_TERMINAL:       break;
      case EV_TIMER_QUEUE:    break;
      case EV_CLIENTUDP_SOCK: break;
    }
    switch ( status ) {
      default:
      case EK_SUCCESS:
        switch ( s->type ) {
          case EV_REDIS_SOCK:
            ((EvRedisService *) s)->process( true ); break;
          case EV_HTTP_SOCK:
            ((EvHttpService *) s)->process( true ); break;
          case EV_MEMCACHED_SOCK:
            ((EvMemcachedService *) s)->process( true ); break;
          case EV_MEMUDP_SOCK:
            ((EvMemcachedUdp *) s)->process( true ); break;
          case EV_CLIENT_SOCK:    break;
          case EV_TERMINAL:       break;
          case EV_NATS_SOCK:      break;
          case EV_CAPR_SOCK:      break;
          case EV_RV_SOCK:        break;
          case EV_KV_PUBSUB:      break;
          case EV_TIMER_QUEUE:    break;
          case EV_SHM_SVC:        break;
          case EV_LISTEN_SOCK:    break;
          case EV_SHM_SOCK:       break;
          case EV_CLIENTUDP_SOCK: break;
        }
        if ( s->test( EV_PREFETCH ) != 0 ) {
          s->pop( EV_PREFETCH ); /* continue prefetching */
        }
        else { /* push back into queue if has an event for read or write */
          if ( s->state != 0 )
            this->queue.push( s );
        }
        break;
      case EK_DEPENDS:   /* incomplete, depends on another key */
        pq.push( &k );
        break;
      case EK_CONTINUE:  /* key complete, more keys to go */
        break;
    }
    cnt++;
    if ( --sz == 0 && pq.is_empty() ) {
      this->prefetch_cnt[ 0 ] += cnt;
      return;
    }
    j = ( j + 1 ) & ( PREFETCH_SIZE - 1 );
    if ( ! pq.is_empty() ) {
      do {
        ctx[ i ] = pq.pop();
        EvKeyCtx & k = *ctx[ i ];
        s = k.owner;
        switch ( s->type ) {
          case EV_REDIS_SOCK:
            ((EvRedisService *) s)->exec_key_prefetch( k ); break;
          case EV_HTTP_SOCK:
            ((EvHttpService *) s)->exec_key_prefetch( k ); break;
          case EV_MEMCACHED_SOCK:
            ((EvMemcachedService *) s)->exec_key_prefetch( k ); break;
          case EV_MEMUDP_SOCK:
            ((EvMemcachedUdp *) s)->exec->exec_key_prefetch( k ); break;
          case EV_SHM_SOCK:
            /*((EvShmClient *) s)->exec->exec_key_prefetch( k );*/ break;
          case EV_NATS_SOCK:      break;
          case EV_CAPR_SOCK:      break;
          case EV_RV_SOCK:        break;
          case EV_KV_PUBSUB:      break;
          case EV_SHM_SVC:        break;
          case EV_LISTEN_SOCK:    break;
          case EV_CLIENT_SOCK:    break;
          case EV_TERMINAL:       break;
          case EV_TIMER_QUEUE:    break;
          case EV_CLIENTUDP_SOCK: break;
        }
        /*ctx[ i ]->prefetch();*/
        i = ( i + 1 ) & ( PREFETCH_SIZE - 1 );
      } while ( ++sz < PREFETCH_SIZE && ! pq.is_empty() );
      this->prefetch_cnt[ sz ]++;
    }
  }
}

bool
EvPoll::dispatch( void )
{
  EvSocket * s;
  uint64_t busy_ns = this->timer_queue->busy_delta();
  uint64_t start   = this->prio_tick;
  int      state;

  if ( this->quit )
    this->process_quit();
  for (;;) {
  next_tick:;
    if ( start + 1000 < this->prio_tick ) /* run poll() at least every 1000 */
      break;
    if ( busy_ns == 0 ) /* if a timer may expire, run poll() */
      break;
    if ( this->queue.is_empty() ) {
      if ( this->prefetch_pending > 0 ) {
      do_prefetch:;
        this->prefetch_pending = 0;
        this->drain_prefetch(); /* run prefetch */
        if ( ! this->queue.is_empty() )
          goto next_tick;
      }
      break;
    }
    s     = this->queue.heap[ 0 ];
    state = __builtin_ffs( s->state ) - 1;
    this->prio_tick++;
    if ( state > EV_PREFETCH && this->prefetch_pending > 0 )
      goto do_prefetch;
    this->queue.pop();
    /*printf( "dispatch %u %u (%x)\n", s->type, state, s->state );*/
    switch ( state ) {
      case EV_READ:
      case EV_READ_LO:
      case EV_READ_HI:
        switch ( s->type ) {
          case EV_REDIS_SOCK:     ((EvRedisService *) s)->read(); break;
          case EV_HTTP_SOCK:      ((EvHttpService *) s)->read(); break;
          case EV_LISTEN_SOCK:    ((EvListen *) s)->accept(); break;
          case EV_CLIENT_SOCK:    ((EvNetClient *) s)->read(); break;
          case EV_TERMINAL:       ((EvTerminal *) s)->read(); break;
          case EV_NATS_SOCK:      ((EvNatsService *) s)->read(); break;
          case EV_CAPR_SOCK:      ((EvCaprService *) s)->read(); break;
          case EV_RV_SOCK:        ((EvRvService *) s)->read(); break;
          case EV_KV_PUBSUB:      ((KvPubSub *) s)->read(); break;
          case EV_TIMER_QUEUE:    ((EvTimerQueue *) s)->read(); break;
          case EV_SHM_SVC:        ((EvShmSvc *) s)->read(); break;
          case EV_MEMCACHED_SOCK: ((EvMemcachedService *) s)->read(); break;
          case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) s)->read(); break;
          case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->read(); break;
          case EV_SHM_SOCK:       break;
        }
        break;
      case EV_PROCESS:
        switch ( s->type ) {
          case EV_REDIS_SOCK:     ((EvRedisService *) s)->process( true ); break;
          case EV_HTTP_SOCK:      ((EvHttpService *) s)->process( false ); break;
          case EV_CLIENT_SOCK:    ((EvNetClient *) s)->process(); break;
          case EV_TERMINAL:       ((EvTerminal *) s)->process(); break;
          case EV_NATS_SOCK:      ((EvNatsService *) s)->process( false ); break;
          case EV_CAPR_SOCK:      ((EvCaprService *) s)->process( false ); break;
          case EV_RV_SOCK:        ((EvRvService *) s)->process( false ); break;
          case EV_KV_PUBSUB:      ((KvPubSub *) s)->process( false ); break;
          case EV_TIMER_QUEUE:    ((EvTimerQueue *) s)->process(); break;
          case EV_SHM_SVC:        ((EvShmSvc *) s)->process( false ); break;
          case EV_MEMCACHED_SOCK: ((EvMemcachedService *) s)->process( true ); break;
          case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) s)->process( true ); break;
          case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->process(); break;
          case EV_LISTEN_SOCK:    break;
          case EV_SHM_SOCK:       break;
        }
        break;
      case EV_PREFETCH:
        s->pop( EV_PREFETCH );
        this->prefetch_pending++;
        goto next_tick; /* skip putting s back into event queue */
      case EV_WRITE:
      case EV_WRITE_HI:
        switch ( s->type ) {
          case EV_REDIS_SOCK:     ((EvRedisService *) s)->write(); break;
          case EV_HTTP_SOCK:      ((EvHttpService *) s)->write(); break;
          case EV_CLIENT_SOCK:    ((EvNetClient *) s)->write(); break;
          case EV_TERMINAL:       ((EvTerminal *) s)->write(); break;
          case EV_NATS_SOCK:      ((EvNatsService *) s)->write(); break;
          case EV_CAPR_SOCK:      ((EvCaprService *) s)->write(); break;
          case EV_RV_SOCK:        ((EvRvService *) s)->write(); break;
          case EV_KV_PUBSUB:      ((KvPubSub *) s)->write(); break;
          case EV_SHM_SVC:        ((EvShmSvc *) s)->write(); break;
          case EV_MEMCACHED_SOCK: ((EvMemcachedService *) s)->write(); break;
          case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) s)->write(); break;
          case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->write(); break;
          case EV_LISTEN_SOCK:    break;
          case EV_SHM_SOCK:       break;
          case EV_TIMER_QUEUE:    break;
        }
        break;
      case EV_SHUTDOWN:
        switch ( s->type ) {
          case EV_REDIS_SOCK:     ((EvRedisService *) s)->process_shutdown(); break;
          case EV_HTTP_SOCK:      ((EvHttpService *) s)->process_shutdown(); break;
          case EV_LISTEN_SOCK:    ((EvListen *) s)->process_shutdown(); break;
          case EV_CLIENT_SOCK:    ((EvNetClient *) s)->process_shutdown(); break;
          case EV_TERMINAL:       ((EvTerminal *) s)->process_shutdown(); break;
          case EV_NATS_SOCK:      ((EvNatsService *) s)->process_shutdown(); break;
          case EV_CAPR_SOCK:      ((EvCaprService *) s)->process_shutdown(); break;
          case EV_RV_SOCK:        ((EvRvService *) s)->process_shutdown(); break;
          case EV_KV_PUBSUB:      ((KvPubSub *) s)->process_shutdown(); break;
          case EV_SHM_SOCK:       ((EvShmClient *) s)->process_shutdown(); break;
          case EV_TIMER_QUEUE:    ((EvTimerQueue *) s)->process_shutdown(); break;
          case EV_SHM_SVC:        ((EvShmSvc *) s)->process_shutdown(); break;
          case EV_MEMCACHED_SOCK: ((EvMemcachedService *) s)->process_shutdown(); break;
          case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) s)->process_shutdown(); break;
          case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->process_shutdown(); break;
        }
        s->pop( EV_SHUTDOWN );
        break;
      case EV_CLOSE:
        s->popall();
        this->remove_sock( s );
        switch ( s->type ) {
          case EV_REDIS_SOCK:     break;
          case EV_HTTP_SOCK:      break;
          case EV_LISTEN_SOCK:    break;
          case EV_CLIENT_SOCK:    ((EvNetClient *) s)->process_close(); break;
          case EV_TERMINAL:       ((EvTerminal *) s)->process_close(); break;
          case EV_NATS_SOCK:      break;
          case EV_CAPR_SOCK:      break;
          case EV_RV_SOCK:        break;
          case EV_KV_PUBSUB:      ((KvPubSub *) s)->process_close(); break;
          case EV_SHM_SOCK:       break;
          case EV_TIMER_QUEUE:    break;
          case EV_SHM_SVC:        ((EvShmSvc *) s)->process_close(); break;
          case EV_MEMCACHED_SOCK: break;
          case EV_MEMUDP_SOCK:    break;
          case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->process_close(); break;
        }
        break;
      case EV_BUSY_POLL:
        switch ( s->type ) {
          case EV_REDIS_SOCK:
          case EV_HTTP_SOCK:
          case EV_LISTEN_SOCK:
          case EV_CLIENT_SOCK:
          case EV_TERMINAL:
          case EV_NATS_SOCK:
          case EV_CAPR_SOCK:
          case EV_RV_SOCK:
          case EV_SHM_SOCK:
          case EV_TIMER_QUEUE:
          case EV_SHM_SVC:
          case EV_MEMCACHED_SOCK:
          case EV_MEMUDP_SOCK:
          case EV_CLIENTUDP_SOCK: break;
          case EV_KV_PUBSUB: {
            uint64_t ns = ( busy_ns > 300 ? 300 : busy_ns );
            if ( ! ((KvPubSub *) s)->busy_poll( ns ) )
              busy_ns -= ns;
            else
              busy_ns = 0;
            break;
          }
        }
        break;
    }
    if ( s->state != 0 ) {
      s->prio_cnt = this->prio_tick;
      this->queue.push( s );
    }
  }
  return start == this->prio_tick;
#if 0
    use_pref = ( this->prefetch_queue != NULL &&
                 this->queue[ EV_PROCESS ].cnt > 1 );
    if ( this->prefetch_queue != NULL && ! this->prefetch_queue->is_empty() )
      this->drain_prefetch( *this->prefetch_queue );
#endif
}

bool
EvPoll::publish_one( EvPublish &pub,  uint32_t *rcount_total,
                     RoutePublishData &rpd )
{
  uint32_t * routes    = rpd.routes;
  uint32_t   rcount    = rpd.rcount;
  uint32_t   hash[ 1 ];
  uint8_t    prefix[ 1 ];
  bool       flow_good = true;

  if ( rcount_total != NULL )
    *rcount_total += rcount;
  pub.hash       = hash;
  pub.prefix     = prefix;
  pub.prefix_cnt = 1;
  hash[ 0 ]      = rpd.hash;
  prefix[ 0 ]    = rpd.prefix;
  for ( uint32_t i = 0; i < rcount; i++ ) {
    EvSocket * s;
    if ( routes[ i ] <= (uint32_t) this->maxfd &&
         (s = this->sock[ routes[ i ] ]) != NULL ) {
      switch ( s->type ) {
        case EV_REDIS_SOCK:
          flow_good &= ((EvRedisService *) s)->on_msg( pub );
          break;
        case EV_HTTP_SOCK:
          flow_good &= ((EvHttpService *) s)->on_msg( pub );
          break;
        case EV_NATS_SOCK:
          flow_good &= ((EvNatsService *) s)->on_msg( pub );
          break;
        case EV_CAPR_SOCK:
          flow_good &= ((EvCaprService *) s)->on_msg( pub );
          break;
        case EV_RV_SOCK:
          flow_good &= ((EvRvService *) s)->on_msg( pub );
          break;
        case EV_KV_PUBSUB:
          flow_good &= ((KvPubSub *) s)->on_msg( pub );
          break;
        case EV_SHM_SOCK: 
          flow_good &= ((EvShmClient *) s)->on_msg( pub );
          break;
        case EV_SHM_SVC:
          flow_good &= ((EvShmSvc *) s)->on_msg( pub );
          break;
        case EV_TERMINAL:       break;
        case EV_CLIENT_SOCK:    break;
        case EV_LISTEN_SOCK:    break;
        case EV_TIMER_QUEUE:    break;
        case EV_MEMCACHED_SOCK: break;
        case EV_MEMUDP_SOCK:    break;
        case EV_CLIENTUDP_SOCK: break;
      }
    }
  }
  return flow_good;
}

template<uint8_t N>
bool
EvPoll::publish_multi( EvPublish &pub,  uint32_t *rcount_total,
                       RoutePublishData *rpd )
{
  EvSocket * s;
  uint32_t   min_route,
             rcount    = 0,
             hash[ 2 ];
  uint8_t    prefix[ 2 ],
             i, cnt;
  bool       flow_good = true;

  pub.hash   = hash;
  pub.prefix = prefix;
  for (;;) {
    for ( i = 0; i < N; ) {
      if ( rpd[ i++ ].rcount > 0 ) {
        min_route = rpd[ i - 1 ].routes[ 0 ];
        goto have_one_route;
      }
    }
    break; /* if no routes left */
  have_one_route:; /* if at least one route, find minimum route number */
    for ( ; i < N; i++ ) {
      if ( rpd[ i ].rcount > 0 && rpd[ i ].routes[ 0 ] < min_route )
        min_route = rpd[ i ].routes[ 0 ];
    }
    /* accumulate hashes going to min_route */
    cnt = 0;
    for ( i = 0; i < N; i++ ) {
      if ( rpd[ i ].rcount > 0 && rpd[ i ].routes[ 0 ] == min_route ) {
        rpd[ i ].routes++;
        rpd[ i ].rcount--;
        hash[ cnt ]   = rpd[ i ].hash;
        prefix[ cnt ] = rpd[ i ].prefix;
        cnt++;
      }
    }
    /* send hashes to min_route */
    if ( (s = this->sock[ min_route ]) != NULL ) {
      rcount++;
      pub.prefix_cnt = cnt;
      switch ( s->type ) {
        case EV_REDIS_SOCK:
          flow_good &= ((EvRedisService *) s)->on_msg( pub );
          break;
        case EV_HTTP_SOCK:
          flow_good &= ((EvHttpService *) s)->on_msg( pub );
          break;
        case EV_NATS_SOCK:
          flow_good &= ((EvNatsService *) s)->on_msg( pub );
          break;
        case EV_CAPR_SOCK:
          flow_good &= ((EvCaprService *) s)->on_msg( pub );
          break;
        case EV_RV_SOCK:
          flow_good &= ((EvRvService *) s)->on_msg( pub );
          break;
        case EV_KV_PUBSUB:
          flow_good &= ((KvPubSub *) s)->on_msg( pub );
          break;
        case EV_SHM_SOCK: 
          flow_good &= ((EvShmClient *) s)->on_msg( pub );
          break;
        case EV_SHM_SVC:
          flow_good &= ((EvShmSvc *) s)->on_msg( pub );
          break;
        case EV_TERMINAL:       break;
        case EV_CLIENT_SOCK:    break;
        case EV_LISTEN_SOCK:    break;
        case EV_TIMER_QUEUE:    break;
        case EV_MEMCACHED_SOCK: break;
        case EV_MEMUDP_SOCK:    break;
        case EV_CLIENTUDP_SOCK: break;
      }
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}

bool
EvPoll::publish_queue( EvPublish &pub,  uint32_t *rcount_total )
{
  RoutePublishQueue & queue     = this->pub_queue;
  RoutePublishData  * rpd       = queue.pop();
  EvSocket          * s;
  uint32_t            min_route;
  uint32_t            rcount    = 0;
  bool                flow_good = true;
  uint8_t             cnt;
  uint8_t             prefix[ 65 ];
  uint32_t            hash[ 65 ];

  pub.hash   = hash;
  pub.prefix = prefix;
  while ( rpd != NULL ) {
    min_route = rpd->routes[ 0 ];
    rpd->routes++;
    rpd->rcount--;
    cnt = 1;
    hash[ 0 ]   = rpd->hash;
    prefix[ 0 ] = rpd->prefix;
    if ( rpd->rcount > 0 )
      queue.push( rpd );
    for (;;) {
      if ( queue.is_empty() ) {
        rpd = NULL;
        break;
      }
      rpd = queue.pop();
      if ( min_route != rpd->routes[ 0 ] )
        break;
      rpd->routes++;
      rpd->rcount--;
      hash[ cnt ]   = rpd->hash;
      prefix[ cnt ] = rpd->prefix;
      cnt++;
      if ( rpd->rcount > 0 )
        queue.push( rpd );
    }
    if ( (s = this->sock[ min_route ]) != NULL ) {
      rcount++;
      pub.prefix_cnt = cnt;
      switch ( s->type ) {
        case EV_REDIS_SOCK:
          flow_good &= ((EvRedisService *) s)->on_msg( pub );
          break;
        case EV_HTTP_SOCK:
          flow_good &= ((EvHttpService *) s)->on_msg( pub );
          break;
        case EV_NATS_SOCK:
          flow_good &= ((EvNatsService *) s)->on_msg( pub );
          break;
        case EV_CAPR_SOCK:
          flow_good &= ((EvCaprService *) s)->on_msg( pub );
          break;
        case EV_RV_SOCK:
          flow_good &= ((EvRvService *) s)->on_msg( pub );
          break;
        case EV_KV_PUBSUB:
          flow_good &= ((KvPubSub *) s)->on_msg( pub );
          break;
        case EV_SHM_SOCK: 
          flow_good &= ((EvShmClient *) s)->on_msg( pub );
          break;
        case EV_SHM_SVC:
          flow_good &= ((EvShmSvc *) s)->on_msg( pub );
          break;
        case EV_TERMINAL:       break;
        case EV_CLIENT_SOCK:    break;
        case EV_LISTEN_SOCK:    break;
        case EV_TIMER_QUEUE:    break;
        case EV_MEMCACHED_SOCK: break;
        case EV_MEMUDP_SOCK:    break;
        case EV_CLIENTUDP_SOCK: break;
      }
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}

bool
RoutePublish::forward_msg( EvPublish &pub,  uint32_t *rcount_total,
                           uint8_t pref_cnt,  KvPrefHash *ph )
{
  EvPoll   & poll      = static_cast<EvPoll &>( *this );
  uint32_t * routes    = NULL;
  uint32_t   rcount    = poll.sub_route.get_route( pub.subj_hash, routes ),
             hash;
  uint8_t    n         = 0;
  bool       flow_good = true;
  RoutePublishData rpd[ 65 ];

  if ( rcount_total != NULL )
    *rcount_total = 0;
  if ( rcount > 0 ) {
    rpd[ 0 ].prefix = 64;
    rpd[ 0 ].hash   = pub.subj_hash;
    rpd[ 0 ].rcount = rcount;
    rpd[ 0 ].routes = routes;
    n = 1;
  }

  BitIter64 bi( poll.sub_route.pat_mask );
  if ( bi.first() ) {
    uint8_t j = 0;
    do {
      while ( j < pref_cnt ) {
        if ( ph[ j++ ].pref == bi.i ) {
          hash = ph[ j - 1 ].get_hash();
          goto found_hash;
        }
      }
      hash = kv_crc_c( pub.subject, bi.i, poll.sub_route.prefix_seed( bi.i ) );
    found_hash:;
      rcount = poll.sub_route.push_get_route( n, hash, routes );
      if ( rcount > 0 ) {
        rpd[ n ].hash   = hash;
        rpd[ n ].prefix = bi.i;
        rpd[ n ].rcount = rcount;
        rpd[ n ].routes = routes;
        n++;
      }
    } while ( bi.next() );
  }
  /* likely cases <= 3 wilcard matches, most likely just 1 match */
  if ( n > 0 ) {
    if ( n == 1 ) {
      flow_good &= poll.publish_one( pub, rcount_total, rpd[ 0 ] );
    }
    else {
      switch ( n ) {
        case 2:
          flow_good &= poll.publish_multi<2>( pub, rcount_total, rpd );
          break;
        default: {
          for ( uint8_t i = 0; i < n; i++ )
            poll.pub_queue.push( &rpd[ i ] );
          flow_good &= poll.publish_queue( pub, rcount_total );
          break;
        }
      }
    }
  }
  return flow_good;
}

bool
RoutePublish::hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                           size_t &keylen )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  EvSocket *s;
  if ( r <= (uint32_t) poll.maxfd && (s = poll.sock[ r ]) != NULL ) {
    switch ( s->type ) {
      case EV_REDIS_SOCK:
        return ((EvRedisService *) s)->hash_to_sub( h, key, keylen );
      case EV_HTTP_SOCK:
        return ((EvHttpService *) s)->hash_to_sub( h, key, keylen );
      case EV_NATS_SOCK:
        return ((EvNatsService *) s)->hash_to_sub( h, key, keylen );
      case EV_CAPR_SOCK:
        return ((EvCaprService *) s)->hash_to_sub( h, key, keylen );
      case EV_RV_SOCK:
        return ((EvRvService *) s)->hash_to_sub( h, key, keylen );
      case EV_KV_PUBSUB:
        return ((KvPubSub *) s)->hash_to_sub( h, key, keylen );
      case EV_SHM_SOCK:
        return ((EvShmClient *) s)->hash_to_sub( h, key, keylen );
      case EV_SHM_SVC:
        return ((EvShmSvc *) s)->hash_to_sub( h, key, keylen );
      case EV_TERMINAL:       break;
      case EV_CLIENT_SOCK:    break;
      case EV_LISTEN_SOCK:    break;
      case EV_TIMER_QUEUE:    break;
      case EV_MEMCACHED_SOCK: break;
      case EV_MEMUDP_SOCK:    break;
      case EV_CLIENTUDP_SOCK: break;
    }
  }
  return false;
}
#if 0
bool
EvSocket::publish( EvPublish & )
{
  fprintf( stderr, "no publish defined for type %d\n", this->type );
  return false;
}

bool
EvSocket::hash_to_sub( uint32_t,  char *,  size_t & )
{
  fprintf( stderr, "no hash_to_sub defined for type %d\n", this->type );
  return false;
}
#endif
void
EvPoll::process_quit( void )
{
  if ( this->quit ) {
    EvSocket *s = this->active_list.hd;
    if ( s == NULL ) { /* no more sockets open */
      this->quit = 5;
      return;
    }
    /* wait for socks to flush data for up to 5 interations */
    do {
      if ( this->quit >= 5 ) {
        if ( s->state != 0 ) {
          this->queue.remove( s ); /* close state */
          s->popall();
        }
        s->push( EV_CLOSE );
        this->queue.push( s );
      }
      else if ( ! s->test( EV_SHUTDOWN | EV_CLOSE ) ) {
        s->idle_push( EV_SHUTDOWN );
      }
    } while ( (s = s->next) != NULL );
    this->quit++;
  }
}

int
EvPoll::add_sock( EvSocket *s )
{
  /* make enough space for fd */
  if ( s->fd > this->maxfd ) {
    int xfd = align<int>( s->fd + 1, EvPoll::ALLOC_INCR );
    EvSocket **tmp;
    if ( xfd < this->nfds )
      xfd = this->nfds;
  try_again:;
    tmp = (EvSocket **)
          ::realloc( this->sock, xfd * sizeof( this->sock[ 0 ] ) );
    if ( tmp == NULL ) {
      perror( "realloc" );
      xfd /= 2;
      if ( xfd > s->fd )
        goto try_again;
      return -1;
    }
    for ( int i = this->maxfd + 1; i < xfd; i++ )
      tmp[ i ] = NULL;
    this->sock  = tmp;
    this->maxfd = xfd - 1;
  }
  if ( s->type != EV_SHM_SVC ) { /* shm svc doesn't use the fd */
    /* add to poll set */
    struct epoll_event event;
    ::memset( &event, 0, sizeof( struct epoll_event ) );
    event.data.fd = s->fd;
    event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if ( ::epoll_ctl( this->efd, EPOLL_CTL_ADD, s->fd, &event ) < 0 ) {
      perror( "epoll_ctl" );
      return -1;
    }
  }
  this->sock[ s->fd ] = s;
  this->fdcnt++;
  /* add to active list */
  s->listfl = IN_ACTIVE_LIST;
  this->active_list.push_tl( s );
  /* if sock starts in write mode, add it to the queue */
  s->prio_cnt = this->prio_tick;
  if ( s->state != 0 )
    this->queue.push( s );
  return 0;
}

bool
EvPoll::timer_expire( EvTimerEvent &ev )
{
  EvSocket *s;

  if ( ev.id <= this->maxfd && (s = this->sock[ ev.id ]) != NULL ) {
    switch ( s->type ) {
      case EV_REDIS_SOCK:     break;
      case EV_HTTP_SOCK:      break;
      case EV_CLIENT_SOCK:    break;
      case EV_TERMINAL:       break;
      case EV_NATS_SOCK:      break;
      case EV_SHM_SOCK:       break;
      case EV_LISTEN_SOCK:    break;
      case EV_KV_PUBSUB:      break;
      case EV_TIMER_QUEUE:    break;
      case EV_MEMCACHED_SOCK: break;
      case EV_MEMUDP_SOCK:    break;
      case EV_CLIENTUDP_SOCK: break;
      case EV_CAPR_SOCK:
        return ((EvCaprService *) s)->timer_expire( ev.timer_id );
      case EV_RV_SOCK:
        return ((EvRvService *) s)->timer_expire( ev.timer_id );
      case EV_SHM_SVC:
        return ((EvShmSvc *) s)->timer_expire( ev.timer_id );
    }
  }
  return false;
}

void
EvPoll::remove_sock( EvSocket *s )
{
  struct epoll_event event;
  if ( s->fd < 0 )
    return;
  /* remove poll set */
  if ( s->fd <= this->maxfd && this->sock[ s->fd ] == s ) {
    if ( s->type != EV_SHM_SVC ) {
      ::memset( &event, 0, sizeof( struct epoll_event ) );
      event.data.fd = s->fd;
      event.events  = 0;
      if ( ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event ) < 0 )
        perror( "epoll_ctl" );
    }
    this->sock[ s->fd ] = NULL;
    this->fdcnt--;
  }
  /* terms are stdin, stdout */
  if ( s->type != EV_TERMINAL && s->type != EV_SHM_SVC ) {
    if ( ::close( s->fd ) != 0 ) {
      fprintf( stderr, "close: errno %d/%s, fd %d type %d\n",
               errno, strerror( errno ), s->fd, s->type );
    }
  }
  if ( s->listfl == IN_ACTIVE_LIST ) {
    s->listfl = IN_NO_LIST;
    this->active_list.pop( s );
  }
  /* release memory buffers */
  switch ( s->type ) {
    case EV_REDIS_SOCK:     ((EvRedisService *) s)->release(); break;
    case EV_HTTP_SOCK:      ((EvHttpService *) s)->release(); break;
    case EV_CLIENT_SOCK:    ((EvNetClient *) s)->release(); break;
    case EV_TERMINAL:       ((EvTerminal *) s)->release(); break;
    case EV_NATS_SOCK:      ((EvNatsService *) s)->release(); break;
    case EV_CAPR_SOCK:      ((EvCaprService *) s)->release(); break;
    case EV_RV_SOCK:        ((EvRvService *) s)->release(); break;
    case EV_SHM_SOCK:       ((EvShmClient *) s)->release(); break;
    case EV_SHM_SVC:        ((EvShmSvc *) s)->release(); break;
    case EV_MEMCACHED_SOCK: ((EvMemcachedService *) s)->release(); break;
    case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) s)->release(); break;
    case EV_CLIENTUDP_SOCK: ((EvUdpClient *) s)->release(); break;
    case EV_LISTEN_SOCK:    break;
    case EV_KV_PUBSUB:      break;
    case EV_TIMER_QUEUE:    break;
  }
  s->fd = -1;
}

bool
EvConnection::read( void )
{
  this->adjust_recv();
  for (;;) {
    if ( this->len < this->recv_size ) {
      ssize_t nbytes = ::read( this->fd, &this->recv[ this->len ],
                               this->recv_size - this->len );
      if ( nbytes > 0 ) {
        this->len += nbytes;
        this->nbytes_recv += nbytes;
        this->push( EV_PROCESS );
        /* if buf almost full, switch to low priority read */
        if ( this->len >= this->recv_highwater )
          this->pushpop( EV_READ_LO, EV_READ );
        else
          this->pushpop( EV_READ, EV_READ_LO );
        return true;
      }
      /* wait for epoll() to set EV_READ again */
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
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
        this->push( EV_SHUTDOWN ); /* close after process and writes */
      /*else if ( this->test( EV_WRITE ) )
        this->pushpop( EV_WRITE_HI, EV_WRITE );*/
      return false;
    }
    /* allow draining of existing buf before resizing */
    if ( this->test( EV_READ ) ) {
      this->pushpop( EV_READ_LO, EV_READ );
      return false;
    }
    /* process was not able to drain read buf */
    if ( ! this->resize_recv_buf() )
      return false;
  }
}

bool
EvConnection::resize_recv_buf( void )
{
  size_t newsz = this->recv_size * 2;
  if ( newsz != (size_t) (uint32_t) newsz )
    return false;
  void * ex_recv_buf = aligned_malloc( newsz );
  if ( ex_recv_buf == NULL )
    return false;
  ::memcpy( ex_recv_buf, &this->recv[ this->off ], this->len );
  this->len -= this->off;
  this->off  = 0;
  if ( this->recv != this->recv_buf )
    ::free( this->recv );
  this->recv = (char *) ex_recv_buf;
  this->recv_size = newsz;
  return true;
}

void
EvConnection::write( void )
{
  struct msghdr h;
  ssize_t nbytes;
  size_t nb = 0;
  StreamBuf & strm = *this;
  if ( strm.sz > 0 )
    strm.flush();
  ::memset( &h, 0, sizeof( h ) );
  h.msg_iov    = &strm.iov[ strm.woff ];
  h.msg_iovlen = strm.idx - strm.woff;
  if ( h.msg_iovlen == 1 ) {
    nbytes = ::send( this->fd, h.msg_iov[ 0 ].iov_base,
                     h.msg_iov[ 0 ].iov_len, MSG_NOSIGNAL );
  }
  else {
    nbytes = ::sendmsg( this->fd, &h, MSG_NOSIGNAL );
    while ( nbytes < 0 && errno == EMSGSIZE ) {
      if ( (h.msg_iovlen /= 2) == 0 )
        break;
      nbytes = ::sendmsg( this->fd, &h, MSG_NOSIGNAL );
    }
  }
  if ( nbytes > 0 ) {
    strm.wr_pending -= nbytes;
    this->nbytes_sent += nbytes;
    nb += nbytes;
    if ( strm.wr_pending == 0 ) {
      strm.reset();
      this->pop2( EV_WRITE, EV_WRITE_HI );
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
    return;
  }
  if ( nbytes == 0 || ( nbytes < 0 && errno != EAGAIN && errno != EINTR ) ) {
    if ( nbytes < 0 && errno != ECONNRESET && errno != EPIPE ) {
      fprintf( stderr, "sendmsg: errno %d/%s, fd %d, state %d\n",
               errno, strerror( errno ), this->fd, this->state );
    }
    this->popall();
    this->push( EV_CLOSE );
  }
}

bool
EvUdp::alloc_mmsg( void )
{
  static const size_t gsz = sizeof( struct sockaddr_storage ) +
                            sizeof( struct iovec ),
                      psz = 64 * 1024 + gsz;
  StreamBuf      & strm     = *this;
  uint32_t         i,
                   new_size = this->in_nsize - this->in_size;
  struct mmsghdr * sav      = this->in_mhdr;

  if ( this->in_nsize <= this->in_size )
    return false;
  /* allocate new_size buffers, and in_nsize headers */
  this->in_mhdr = (struct mmsghdr *) strm.alloc_temp( psz * new_size +
                                    sizeof( struct mmsghdr ) * this->in_nsize );
  if ( this->in_mhdr == NULL )
    return false;
  i = this->in_size;
  /* if any existing buffers exist, the pointers will be copied to the head */
  if ( i > 0 )
    ::memcpy( this->in_mhdr, sav, sizeof( sav[ 0 ] ) * i );
  /* initialize the rest of the buffers at the tail */
  void *p = (void *) &this->in_mhdr[ this->in_nsize ]/*,
       *g = (void *) &((uint8_t *) p)[ gsz * this->in_size ]*/ ;
  for ( ; i < this->in_nsize; i++ ) {
    this->in_mhdr[ i ].msg_hdr.msg_name    = (struct sockaddr *) p;
    this->in_mhdr[ i ].msg_hdr.msg_namelen = sizeof( struct sockaddr_storage );
    p = &((uint8_t *) p)[ sizeof( struct sockaddr_storage ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov    = (struct iovec *) p;
    this->in_mhdr[ i ].msg_hdr.msg_iovlen = 1;
    p = &((uint8_t *) p)[ sizeof( struct iovec ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_base = p;
    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_len  = 64 * 1024;
    p = &((uint8_t *) p)[ 64 * 1024 ];

    this->in_mhdr[ i ].msg_hdr.msg_control    = NULL;
    this->in_mhdr[ i ].msg_hdr.msg_controllen = 0;
    this->in_mhdr[ i ].msg_hdr.msg_flags      = 0;

    this->in_mhdr[ i ].msg_len = 0;
  }
  /* in_nsize can expand if we need more udp frames */
  this->in_size = this->in_nsize;
  return true;
}

bool
EvUdp::read( void )
{
  int nmsgs = 0;
  if ( this->in_moff == this->in_size ) {
    if ( ! this->alloc_mmsg() ) {
      perror( "alloc" );
      this->popall();
      this->push( EV_CLOSE );
      return false;
    }
  }
  if ( this->in_moff + 1 < this->in_size ) {
    nmsgs = ::recvmmsg( this->fd, &this->in_mhdr[ this->in_moff ],
                        this->in_size - this->in_moff, 0, NULL );
  }
  else {
    ssize_t nbytes = ::recvmsg( this->fd,
                                &this->in_mhdr[ this->in_moff ].msg_hdr, 0 );
    if ( nbytes > 0 ) {
      this->in_mhdr[ this->in_moff ].msg_len = nbytes;
      nmsgs = 1;
    }
  }
  if ( nmsgs > 0 ) {
    this->in_nmsgs += nmsgs;
    for ( int i = 0; i < nmsgs; i++ )
      this->nbytes_recv += this->in_mhdr[ this->in_moff + i ].msg_len;
    this->in_nsize = ( ( this->in_nmsgs < 8 ) ? this->in_nmsgs + 1 : 8 );
    this->push( EV_PROCESS );
    this->pushpop( EV_READ_LO, EV_READ );
    return true;
  }
  this->in_nsize = 1;
  /* wait for epoll() to set EV_READ again */
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  if ( nmsgs < 0 && errno != EINTR ) {
    if ( errno != EAGAIN ) {
      if ( errno != ECONNRESET )
        perror( "recvmmsg" );
      this->popall();
      this->push( EV_CLOSE );
    }
  }
  return false;
}

void
EvUdp::write( void )
{
  int nmsgs = 0;
  if ( this->out_nmsgs > 1 ) {
    nmsgs = ::sendmmsg( this->fd, this->out_mhdr, this->out_nmsgs, 0 );
    if ( nmsgs > 0 ) {
      for ( uint32_t i = 0; i < this->out_nmsgs; i++ )
        this->nbytes_sent += this->out_mhdr[ i ].msg_len;
      this->clear_buffers();
      this->pop2( EV_WRITE, EV_WRITE_HI );
      return;
    }
  }
  else {
    ssize_t nbytes = ::sendmsg( this->fd, &this->out_mhdr[ 0 ].msg_hdr, 0 );
    if ( nbytes > 0 ) {
      this->nbytes_sent += nbytes;
      this->clear_buffers();
      this->pop2( EV_WRITE, EV_WRITE_HI );
      return;
    }
    if ( nbytes < 0 )
      nmsgs = -1;
  }
  if ( nmsgs < 0 && errno != EAGAIN && errno != EINTR ) {
    if ( errno != ECONNRESET && errno != EPIPE ) {
      fprintf( stderr, "sendmsg: errno %d/%s, fd %d, state %d\n",
               errno, strerror( errno ), this->fd, this->state );
    }
    this->popall();
    this->push( EV_CLOSE );
  }
}

void
EvConnection::close_alloc_error( void )
{
  fprintf( stderr, "Allocation failed! Closing connection\n" );
  this->popall();
  this->push( EV_CLOSE );
}

void
EvSocket::idle_push( EvState s )
{
  if ( this->state == 0 ) {
  do_push:;
    this->push( s );
    /*printf( "idle_push %d %x\n", this->type, this->state );*/
    this->prio_cnt = this->poll.prio_tick;
    this->poll.queue.push( this );
  }
  else { /* check if added state requires queue to be rearranged */
    int x1 = __builtin_ffs( this->state ),
        x2 = __builtin_ffs( this->state | ( 1 << s ) );
    if ( x1 > x2 ) {
      /*printf( "remove %d\n", this->type );*/
      this->poll.queue.remove( this );
      goto do_push;
    }
    else {
      this->push( s );
      /*printf( "idle_push2 %d %x\n", this->type, this->state );*/
    }
  }
}
