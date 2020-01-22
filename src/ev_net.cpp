#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/un.h>
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
/* vtable dispatch */
inline void EvSocket::v_write( void ) {
  SOCK_CALL( this, write() );
}
inline void EvSocket::v_read( void ) {
  SOCK_CALL( this, read() );
}
inline void EvSocket::v_process( void ) {
  SOCK_CALL( this, process() );
}
inline void EvSocket::v_release( void ) {
  SOCK_CALL( this, release() );
}
inline bool EvSocket::v_timer_expire( uint64_t tid, uint64_t eid ) {
  bool b = false;
  SOCK_CALL2( b, this, timer_expire( tid, eid ) );
  return b;
}
inline bool EvSocket::v_hash_to_sub( uint32_t h, char *k, size_t &klen ) {
  bool b = false;
  SOCK_CALL2( b, this, hash_to_sub( h, k, klen ) );
  return b;
}
inline bool EvSocket::v_on_msg( EvPublish &pub ) {
  bool b = true;
  SOCK_CALL2( b, this, on_msg( pub ) );
  return b;
}
inline void EvSocket::v_exec_key_prefetch( EvKeyCtx &ctx ) {
  SOCK_CALL( this, exec_key_prefetch( ctx ) );
}
inline int  EvSocket::v_exec_key_continue( EvKeyCtx &ctx ) {
  int status = 0;
  SOCK_CALL2( status, this, exec_key_continue( ctx ) );
  return status;
}
inline void EvSocket::v_process_shutdown( void ) {
  SOCK_CALL( this, process_shutdown() );
}
inline void EvSocket::v_process_close( void ) {
  SOCK_CALL( this, process_close() );
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
    s->v_exec_key_prefetch( k );
  }
  this->prefetch_cnt[ sz ]++;
  i &= ( PREFETCH_SIZE - 1 );
  for ( j = 0; ; ) {
    EvKeyCtx & k = *ctx[ j ];
    s = k.owner;
    switch( s->v_exec_key_continue( k ) ) {
      default:
      case EK_SUCCESS:
        s->msgs_sent++;
        s->v_process();
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
        s->v_exec_key_prefetch( k );
        /*ctx[ i ]->prefetch();*/
        i = ( i + 1 ) & ( PREFETCH_SIZE - 1 );
      } while ( ++sz < PREFETCH_SIZE && ! pq.is_empty() );
      this->prefetch_cnt[ sz ]++;
    }
  }
}

uint64_t
EvPoll::current_coarse_ns( void ) const
{
  if ( this->map != NULL )
    return this->map->hdr.current_stamp;
  return current_realtime_coarse_ns();
}

bool
EvPoll::dispatch( void )
{
  EvSocket * s;
  uint64_t busy_ns = this->timer_queue->busy_delta(),
           curr_ns = this->current_coarse_ns();
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
        s->active_ns = curr_ns;
        s->v_read();
        break;
      case EV_PROCESS:
        s->v_process();
        break;
      case EV_PREFETCH:
        s->pop( EV_PREFETCH );
        this->prefetch_pending++;
        goto next_tick; /* skip putting s back into event queue */
      case EV_WRITE:
      case EV_WRITE_HI:
        s->v_write();
        break;
      case EV_SHUTDOWN:
        s->v_process_shutdown();
        s->pop( EV_SHUTDOWN );
        break;
      case EV_CLOSE:
        s->popall();
        this->remove_sock( s );
        s->v_process_close();
        break;
      case EV_BUSY_POLL:
        if ( s->type == EV_KV_PUBSUB ) {
          uint64_t ns = ( busy_ns > 300 ? 300 : busy_ns );
          if ( ! ((KvPubSub *) s)->busy_poll( ns ) )
            busy_ns -= ns;
          else
            busy_ns = 0;
        }
        break;
    }
    if ( s->state != 0 ) {
      s->prio_cnt = this->prio_tick;
      this->queue.push( s );
    }
  }
  return start == this->prio_tick;
}

/* different publishers for different size route matches, one() is the most
 * common, but multi / queue need to be used with multiple routes */
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
      flow_good &= s->v_on_msg( pub );
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
      flow_good &= s->v_on_msg( pub );
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}
/* same as above with a prio queue heap instead of linear search */
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
      flow_good &= s->v_on_msg( pub );
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}
/* match subject against route db and forward msg to fds subscribed, route
 * db contains both exact matches and wildcard prefix matches */
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
/* convert a hash into a subject string, this may have collisions */
bool
RoutePublish::hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                           size_t &keylen )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  EvSocket *s;
  bool b = false;
  if ( r <= (uint32_t) poll.maxfd && (s = poll.sock[ r ]) != NULL )
    b = s->v_hash_to_sub( h, key, keylen );
  return b;
}
/* track number of subscribes to keyspace subjects to enable them */
inline void
RoutePublish::update_keyspace_count( const char *sub,  size_t len,  int add )
{
  /* keyspace subjects are special, since subscribing to them can create
   * some overhead */
  static const char kspc[] = "__keyspace@",
                    kevt[] = "__keyevent@",
                    lblk[] = "__listblkd@",
                    zblk[] = "__zsetblkd@",
                    sblk[] = "__strmblkd@",
                    moni[] = "__monitor_@";

  if ( ::memcmp( kspc, sub, len ) == 0 ) /* len <= 11, could match multiple */
    this->keyspace_cnt += add;
  if ( ::memcmp( kevt, sub, len ) == 0 )
    this->keyevent_cnt += add;
  if ( ::memcmp( lblk, sub, len ) == 0 )
    this->listblkd_cnt += add;
  if ( ::memcmp( zblk, sub, len ) == 0 )
    this->zsetblkd_cnt += add;
  if ( ::memcmp( sblk, sub, len ) == 0 )
    this->strmblkd_cnt += add;
  if ( ::memcmp( moni, sub, len ) == 0 )
    this->monitor__cnt += add;

  this->key_flags = ( ( this->keyspace_cnt == 0 ? 0 : EKF_KEYSPACE_FWD ) |
                      ( this->keyevent_cnt == 0 ? 0 : EKF_KEYEVENT_FWD ) |
                      ( this->listblkd_cnt == 0 ? 0 : EKF_LISTBLKD_NOT ) |
                      ( this->zsetblkd_cnt == 0 ? 0 : EKF_ZSETBLKD_NOT ) |
                      ( this->strmblkd_cnt == 0 ? 0 : EKF_STRMBLKD_NOT ) |
                      ( this->monitor__cnt == 0 ? 0 : EKF_MONITOR      ) );
  /*printf( "%.*s %d key_flags %x\n", (int) len, sub, add, this->key_flags );*/
}
/* external patterns from kv pubsub */
void
EvPoll::add_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                           uint32_t fd )
{
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  /* if first route added for hash */
  if ( this->sub_route.add_pattern_route( hash, fd, prefix_len ) == 1 ) {
    /*printf( "add_pattern %.*s\n", (int) prefix_len, sub );*/
    this->update_keyspace_count( sub, pre_len, 1 );
  }
}

void
EvPoll::del_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                           uint32_t fd )
{
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  /* if last route deleted */
  if ( this->sub_route.del_pattern_route( hash, fd, prefix_len ) == 0 ) {
    /*printf( "del_pattern %.*s\n", (int) prefix_len, sub );*/
    this->update_keyspace_count( sub, pre_len, -1 );
  }
}
/* external routes from kv pubsub */
void
EvPoll::add_route( const char *sub,  size_t sub_len,  uint32_t hash,
                   uint32_t fd )
{
  /* if first route added for hash */
  if ( this->sub_route.add_route( hash, fd ) == 1 ) {
    if ( sub_len > 11 ) {
      /*printf( "add_route %.*s\n", (int) sub_len, sub );*/
      this->update_keyspace_count( sub, 11, 1 );
    }
  }
}

void
EvPoll::del_route( const char *sub,  size_t sub_len,  uint32_t hash,
                   uint32_t fd )
{
  /* if last route deleted */
  if ( this->sub_route.del_route( hash, fd ) == 0 ) {
    if ( sub_len > 11 ) {
      /*printf( "del_route %.*s\n", (int) sub_len, sub );*/
      this->update_keyspace_count( sub, 11, -1 );
    }
  }
}
/* client subscribe, notify to kv pubsub */
void
RoutePublish::notify_sub( uint32_t h,  const char *sub,  size_t len,
                          uint32_t sub_id,  uint32_t rcnt,  char src_type,
                          const char *rep,  size_t rlen )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  if ( len > 11 )
    this->update_keyspace_count( sub, 11, 1 );
  poll.pubsub->do_sub( h, sub, len, sub_id, rcnt, src_type, rep, rlen );
}

void
RoutePublish::notify_unsub( uint32_t h,  const char *sub,  size_t len,
                            uint32_t sub_id,  uint32_t rcnt,  char src_type )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  if ( len > 11 )
    this->update_keyspace_count( sub, 11, -1 );
  poll.pubsub->do_unsub( h, sub, len, sub_id, rcnt, src_type );
}
/* client pattern subscribe, notify to kv pubsub */
void
RoutePublish::notify_psub( uint32_t h,  const char *pattern,  size_t len,
                           const char *prefix,  uint8_t prefix_len,
                           uint32_t sub_id,  uint32_t rcnt,  char src_type )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  this->update_keyspace_count( prefix, pre_len, 1 );
  poll.pubsub->do_psub( h, pattern, len, prefix, prefix_len,
                        sub_id, rcnt, src_type );
}

void
RoutePublish::notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                             const char *prefix,  uint8_t prefix_len,
                             uint32_t sub_id,  uint32_t rcnt,  char src_type )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  this->update_keyspace_count( prefix, pre_len, -1 );
  poll.pubsub->do_punsub( h, pattern, len, prefix, prefix_len,
                          sub_id, rcnt, src_type );
}
/* shutdown and close all open socks */
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
    } while ( (s = (EvSocket *) s->next) != NULL );
    this->quit++;
  }
}
/* convert sockaddr into a string and set peer_address[] */
void
PeerData::set_addr( const sockaddr *sa )
{
  const size_t maxlen = sizeof( this->peer_address );
  char         buf[ maxlen ],
             * s = buf,
             * t = NULL;
  const char * p;
  size_t       len;
  in_addr    * in;
  in6_addr   * in6;
  uint16_t     in_port;

  if ( sa != NULL ) {
    switch ( sa->sa_family ) {
      case AF_INET6:
        in6     = &((struct sockaddr_in6 *) sa)->sin6_addr;
        in_port = ((struct sockaddr_in6 *) sa)->sin6_port;
        /* check if ::ffff: prefix */
        if ( ((uint64_t *) in6)[ 0 ] == 0 &&
             ((uint16_t *) in6)[ 4 ] == 0 &&
             ((uint16_t *) in6)[ 5 ] == 0xffffU ) {
          in = &((in_addr *) in6)[ 3 ];
          goto do_af_inet;
        }
        p = inet_ntop( AF_INET6, in6, &s[ 1 ], maxlen - 9 );
        if ( p == NULL )
          break;
        /* make [ip6]:port */
        len = ::strlen( &s[ 1 ] ) + 1;
        t   = &s[ len ];
        s[ 0 ] = '[';
        t[ 0 ] = ']';
        t[ 1 ] = ':';
        len = uint_to_str( ntohs( in_port ), &t[ 2 ] );
        t   = &t[ 2 + len ];
        break;

      case AF_INET:
        in      = &((struct sockaddr_in *) sa)->sin_addr;
        in_port = ((struct sockaddr_in *) sa)->sin_port;
      do_af_inet:;
        p = inet_ntop( AF_INET, in, s, maxlen - 7 );
        if ( p == NULL )
          break;
        /* make ip4:port */
        len = ::strlen( s );
        t   = &s[ len ];
        t[ 0 ] = ':';
        len = uint_to_str( ntohs( in_port ), &t[ 1 ] );
        t   = &t[ 1 + len ];
        break;

      case AF_LOCAL:
        len = ::strnlen( ((struct sockaddr_un *) sa)->sun_path,
                         sizeof( ((struct sockaddr_un *) sa)->sun_path ) );
        if ( len > maxlen - 1 )
          len = maxlen - 1;
        ::memcpy( s, ((struct sockaddr_un *) sa)->sun_path, len );
        t = &s[ len ];
        break;

      default:
        break;
    }
  }
  if ( t != NULL ) {
    /* set strlen */
    this->set_peer_address( buf, t - s );
  }
  else {
    this->set_peer_address( NULL, 0 );
  }
}

bool
EvSocketOps::match( PeerData &pd,  PeerMatchArgs &ka )
{
  return this->client_match( pd, ka, NULL );
}

int
EvSocketOps::client_list( PeerData &pd,  char *buf,  size_t buflen )
{
  /* id=1082 addr=[::1]:43362 fd=8 name= age=1 idle=0 flags=N */
  static const uint64_t ONE_NS = 1000000000;
  uint64_t cur_time_ns = ((EvSocket &) pd).poll.current_coarse_ns();
  /* list: 'id=1082 addr=[::1]:43362 fd=8 name= age=1 idle=0 flags=N db=0
   * sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0
   * events=r cmd=client\n'
   * id=unique id, addr=peer addr, fd=sock, age=time
   * conn, idle=time idle, flags=mode, db=cur db, sub=channel subs,
   * psub=pattern subs, multi=cmds qbuf=query buf size, qbuf-free=free
   * qbuf, obl=output buf len, oll=outut list len, omem=output mem usage,
   * events=sock rd/wr, cmd=last cmd issued */
  return ::snprintf( buf, buflen,
    "id=%lu addr=%.*s fd=%d name=%.*s kind=%s age=%ld idle=%ld ",
    pd.id,
    (int) pd.get_peer_address_strlen(), pd.peer_address,
    pd.fd,
    (int) pd.get_name_strlen(), pd.name,
    pd.kind,
    ( cur_time_ns - pd.start_ns ) / ONE_NS,
    ( cur_time_ns - pd.active_ns ) / ONE_NS );
}

bool
EvSocketOps::client_match( PeerData &pd,  PeerMatchArgs &ka,  ... )
{
  /* match filters, if any don't match return false */
  if ( ka.id != 0 )
    if ( (uint64_t) ka.id != pd.id ) /* match id */
      return false;
  if ( ka.ip_len != 0 ) /* match ip address string */
    if ( ka.ip_len != pd.get_peer_address_strlen() ||
         ::memcmp( ka.ip, pd.peer_address, ka.ip_len ) != 0 )
      return false;
  if ( ka.type_len != 0 ) {
    va_list    args;
    size_t     k, sz;
    const char * str;
    va_start( args, ka );
    for ( k = 1; ; k++ ) {
      str = va_arg( args, const char * );
      if ( str == NULL ) {
        k = 0; /* no match */
        break;
      }
      sz = va_arg( args, size_t );
      if ( sz == ka.type_len &&
           ::strncasecmp( ka.type, str, sz ) == 0 )
        break; /* match */
    }
    va_end( args );
    if ( k != 0 )
      return true;
    /* match the kind */
    if ( pd.kind != NULL && ka.type_len == ::strlen( pd.kind ) &&
         ::strncasecmp( ka.type, pd.kind, ka.type_len ) == 0 )
      return true;
    return false;
  }
  return true;
}

bool
EvSocketOps::client_kill( PeerData &pd )
{
  EvSocket &s = (EvSocket &) pd;
  /* if already shutdown, close up immediately */
  if ( s.test( EV_SHUTDOWN ) != 0 ) {
    if ( s.state != 0 ) {
      s.poll.queue.remove( &s ); /* close state */
      s.popall();
    }
    s.idle_push( EV_CLOSE );
  }
  else { /* close after writing pending data */
    s.idle_push( EV_SHUTDOWN );
  }
  return true;
}

bool
EvConnectionOps::match( PeerData &pd,  PeerMatchArgs &ka )
{
  return this->client_match( pd, ka, MARG( "tcp" ), NULL );
}

int
EvConnectionOps::client_list( PeerData &pd,  char *buf,  size_t buflen )
{
  EvConnection & c = (EvConnection &) pd;
  int i = this->EvSocketOps::client_list( pd, buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "rbuf=%u rsz=%u imsg=%lu br=%lu "
                     "wbuf=%lu wsz=%lu omsg=%lu bs=%lu ",
                     c.len - c.off, c.recv_size, c.msgs_recv, c.bytes_recv,
                     c.wr_pending,
                     c.tmp.fast_len + c.tmp.block_cnt * c.tmp.alloc_size,
                     c.msgs_sent, c.bytes_sent );
  }
  return i;
}

void
EvSocketOps::client_stats( PeerData &pd,  PeerStats &ps )
{
  EvSocket & s = (EvSocket &) pd;
  ps.bytes_recv += s.bytes_recv;
  ps.bytes_sent += s.bytes_sent;
  ps.msgs_recv  += s.msgs_recv;
  ps.msgs_sent  += s.msgs_sent;
}

void
EvSocketOps::retired_stats( PeerData &pd,  PeerStats &ps )
{
  EvSocket & s = (EvSocket &) pd;
  ps.bytes_recv += s.poll.peer_stats.bytes_recv;
  ps.bytes_sent += s.poll.peer_stats.bytes_sent;
  ps.msgs_recv  += s.poll.peer_stats.msgs_recv;
  ps.msgs_sent  += s.poll.peer_stats.msgs_sent;
  ps.accept_cnt += s.poll.peer_stats.accept_cnt;
}

bool
EvListenOps::match( PeerData &pd,  PeerMatchArgs &ka )
{
  return this->client_match( pd, ka, MARG( "listen" ), NULL );
}

int
EvListenOps::client_list( PeerData &pd,  char *buf,  size_t buflen )
{
  EvListen & l = (EvListen &) pd;
  int i = this->EvSocketOps::client_list( pd, buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "acpt=%lu ",
                     l.accept_cnt );
  }
  return i;
}

void
EvListenOps::client_stats( PeerData &pd,  PeerStats &ps )
{
  EvListen & l = (EvListen &) pd;
  ps.accept_cnt += l.accept_cnt;
}

bool
EvUdpOps::match( PeerData &pd,  PeerMatchArgs &ka )
{
  return this->client_match( pd, ka, MARG( "udp" ), NULL );
}

int
EvUdpOps::client_list( PeerData &pd,  char *buf,  size_t buflen )
{
  EvUdp & u = (EvUdp &) pd;
  int i = this->EvSocketOps::client_list( pd, buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "imsg=%lu omsg=%lu br=%lu bs=%lu ",
                     u.msgs_recv, u.msgs_sent,
                     u.bytes_recv, u.bytes_sent );
  }
  return i;
}

/* enable epolling of sock fd */
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
  uint64_t ns = this->current_coarse_ns();
  s->start_ns   = ns;
  s->active_ns  = ns;
  s->id         = ++this->next_id;
  s->bytes_recv = 0;
  s->bytes_sent = 0;
  s->msgs_recv  = 0;
  s->msgs_sent  = 0;
  return 0;
}
/* start a timer event */
bool
RoutePublish::add_timer_seconds( int id,  uint32_t ival,  uint64_t timer_id,
                                 uint64_t event_id )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->add_timer_seconds( id, ival, timer_id, event_id );
}

bool
RoutePublish::add_timer_millis( int id,  uint32_t ival,  uint64_t timer_id,
                                uint64_t event_id )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->add_timer_units( id, ival, IVAL_MILLIS, timer_id,
                                            event_id );
}

bool
RoutePublish::remove_timer( int id,  uint64_t timer_id,  uint64_t event_id )
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->remove_timer( id, timer_id, event_id );
}
/* dispatch a timer firing */
bool
EvPoll::timer_expire( EvTimerEvent &ev )
{
  EvSocket *s;
  bool b = false;
  if ( ev.id <= this->maxfd && (s = this->sock[ ev.id ]) != NULL )
    b = s->v_timer_expire( ev.timer_id, ev.event_id );
  return b;
}
/* remove a sock fd from epolling */
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
    s->op.client_stats( *s, this->peer_stats );
    s->listfl = IN_NO_LIST;
    this->active_list.pop( s );
  }
  /* release memory buffers */
  s->v_release();
  s->fd = -1;
}
/* fill up recv buffers */
void
EvConnection::read( void )
{
  this->adjust_recv();
  for (;;) {
    if ( this->len < this->recv_size ) {
      ssize_t nbytes = ::read( this->fd, &this->recv[ this->len ],
                               this->recv_size - this->len );
      if ( nbytes > 0 ) {
        this->len += nbytes;
        this->bytes_recv += nbytes;
        this->push( EV_PROCESS );
        /* if buf almost full, switch to low priority read */
        if ( this->len >= this->recv_highwater )
          this->pushpop( EV_READ_LO, EV_READ );
        else
          this->pushpop( EV_READ, EV_READ_LO );
        return;
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
      return;
    }
    /* allow draining of existing buf before resizing */
    if ( this->test( EV_READ ) ) {
      this->pushpop( EV_READ_LO, EV_READ );
      return;
    }
    /* process was not able to drain read buf */
    if ( ! this->resize_recv_buf() )
      return;
  }
}
/* if msg is too large for existing buffers, resize it */
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
/* write data to sock fd */
void
EvConnection::write( void )
{
  struct msghdr h;
  ssize_t nbytes;
  size_t nb = 0;
  StreamBuf & strm = *this;
  if ( strm.sz > 0 )
    strm.flush();
  else if ( strm.wr_pending == 0 ) {
    this->pop2( EV_WRITE, EV_WRITE_HI );
    return;
  }
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
    this->bytes_sent += nbytes;
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
/* use mmsg for udp sockets */
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
/* read udp packets */
void
EvUdp::read( void )
{
  int nmsgs = 0;
  if ( this->in_moff == this->in_size ) {
    if ( ! this->alloc_mmsg() ) {
      perror( "alloc" );
      this->popall();
      this->push( EV_CLOSE );
      return;
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
      this->bytes_recv += this->in_mhdr[ this->in_moff + i ].msg_len;
    this->in_nsize = ( ( this->in_nmsgs < 8 ) ? this->in_nmsgs + 1 : 8 );
    this->push( EV_PROCESS );
    this->pushpop( EV_READ_LO, EV_READ );
    return;
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
}
/* write udp packets */
void
EvUdp::write( void )
{
  int nmsgs = 0;
  if ( this->out_nmsgs > 1 ) {
    nmsgs = ::sendmmsg( this->fd, this->out_mhdr, this->out_nmsgs, 0 );
    if ( nmsgs > 0 ) {
      for ( uint32_t i = 0; i < this->out_nmsgs; i++ )
        this->bytes_sent += this->out_mhdr[ i ].msg_len;
      this->clear_buffers();
      this->pop2( EV_WRITE, EV_WRITE_HI );
      return;
    }
  }
  else {
    ssize_t nbytes = ::sendmsg( this->fd, &this->out_mhdr[ 0 ].msg_hdr, 0 );
    if ( nbytes > 0 ) {
      this->bytes_sent += nbytes;
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
/* if some alloc failed, kill the client */
void
EvConnection::close_alloc_error( void )
{
  fprintf( stderr, "Allocation failed! Closing connection\n" );
  this->popall();
  this->push( EV_CLOSE );
}
/* when a sock is not dispatch()ed, it may need to be rearranged in the queue
 * for correct priority */
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
