#ifndef __rai_raids__ev_net_h__
#define __rai_raids__ev_net_h__

#include <raikv/shm_ht.h>
#include <raikv/prio_queue.h>
#include <raikv/dlinklist.h>
#include <raids/stream_buf.h>
#include <raids/route_db.h>

namespace rai {
namespace ds {

struct EvSocket;           /* base class for anything with a fd */
struct EvPrefetchQueue;    /* queue for prefetching key memory */
struct EvPublish;          /* data for publishing, key + msg */
struct EvPoll;             /* manages events with epoll() */
struct KvPubSub;           /* manages pubsub through kv shm */
struct EvRedisService;     /* service redis protocol */
struct EvHttpService;      /* service http + websock protocol */
struct EvNatsService;      /* service nats protocol */
struct EvCaprService;      /* service capr protocol */
struct EvRvService;        /* service rv protocol */
struct EvMemcachedService; /* service memcached protocol */
struct EvMemcachedUdp;     /* service memcached udp protocol */
struct EvUdpClient;        /* client for memcached udp protocol */
struct EvShm;              /* kv shm + ctx_id */
struct EvShmSvc;           /* shm direct service */
struct EvShmClient;        /* shm direct client */
struct EvTimerQueue;       /* timerfd with heap queue of events */
struct EvTimerEvent;       /* a timer event signal */
struct EvNetClient;        /* a redis client using tcp/unix */
struct EvTerminal;         /* terminal line editor */
struct EvKeyCtx;           /* a key operand, an expr may have multiple keys */

enum EvSockType {
  EV_REDIS_SOCK     = 0, /* redis protocol */
  EV_HTTP_SOCK      = 1, /* http / websock protocol */
  EV_LISTEN_SOCK    = 2, /* any type of listener (tcp or unix sock stream) */
  EV_CLIENT_SOCK    = 3, /* redis client protocol */
  EV_TERMINAL       = 4, /* redis terminal (converts redis proto to json) */
  EV_NATS_SOCK      = 5, /* nats pub/sub protocol */
  EV_CAPR_SOCK      = 6, /* capr pub/sub protocol */
  EV_RV_SOCK        = 7, /* rv pub/sub protocol */
  EV_KV_PUBSUB      = 8, /* route between processes */
  EV_SHM_SOCK       = 9, /* local shm client */
  EV_TIMER_QUEUE    = 10,/* event timers */
  EV_SHM_SVC        = 11,/* pubsub service */
  EV_MEMCACHED_SOCK = 12,/* memcached protocol */
  EV_MEMUDP_SOCK    = 13,/* memcached udp protocol */
  EV_CLIENTUDP_SOCK = 14 /* udp client */
};

/* vtable dispatch, without vtable, this allows compiler to inline the
 * functions and discard the empty ones (how a final qualifier should work) */
#define SOCK_CALL( S, F ) \
  switch ( S->type ) { \
    case EV_REDIS_SOCK:     ((EvRedisService *) S)->F; break; \
    case EV_HTTP_SOCK:      ((EvHttpService *) S)->F; break; \
    case EV_LISTEN_SOCK:    ((EvListen *) S)->F; break; \
    case EV_CLIENT_SOCK:    ((EvNetClient *) S)->F; break; \
    case EV_TERMINAL:       ((EvTerminal *) S)->F; break; \
    case EV_NATS_SOCK:      ((EvNatsService *) S)->F; break; \
    case EV_CAPR_SOCK:      ((EvCaprService *) S)->F; break; \
    case EV_RV_SOCK:        ((EvRvService *) S)->F; break; \
    case EV_KV_PUBSUB:      ((KvPubSub *) S)->F; break; \
    case EV_SHM_SOCK:       ((EvShmClient *) S)->F; break; \
    case EV_TIMER_QUEUE:    ((EvTimerQueue *) S)->F; break; \
    case EV_SHM_SVC:        ((EvShmSvc *) S)->F; break; \
    case EV_MEMCACHED_SOCK: ((EvMemcachedService *) S)->F; break; \
    case EV_MEMUDP_SOCK:    ((EvMemcachedUdp *) S)->F; break; \
    case EV_CLIENTUDP_SOCK: ((EvUdpClient *) S)->F; break; \
  }
#define SOCK_CALL2( R, S, F ) \
  switch ( S->type ) { \
    case EV_REDIS_SOCK:     R = ((EvRedisService *) S)->F; break; \
    case EV_HTTP_SOCK:      R = ((EvHttpService *) S)->F; break; \
    case EV_LISTEN_SOCK:    R = ((EvListen *) S)->F; break; \
    case EV_CLIENT_SOCK:    R = ((EvNetClient *) S)->F; break; \
    case EV_TERMINAL:       R = ((EvTerminal *) S)->F; break; \
    case EV_NATS_SOCK:      R = ((EvNatsService *) S)->F; break; \
    case EV_CAPR_SOCK:      R = ((EvCaprService *) S)->F; break; \
    case EV_RV_SOCK:        R = ((EvRvService *) S)->F; break; \
    case EV_KV_PUBSUB:      R = ((KvPubSub *) S)->F; break; \
    case EV_SHM_SOCK:       R = ((EvShmClient *) S)->F; break; \
    case EV_TIMER_QUEUE:    R = ((EvTimerQueue *) S)->F; break; \
    case EV_SHM_SVC:        R = ((EvShmSvc *) S)->F; break; \
    case EV_MEMCACHED_SOCK: R = ((EvMemcachedService *) S)->F; break; \
    case EV_MEMUDP_SOCK:    R = ((EvMemcachedUdp *) S)->F; break; \
    case EV_CLIENTUDP_SOCK: R = ((EvUdpClient *) S)->F; break; \
  }

enum EvState {
  EV_READ_HI   = 0, /* listen port accept */
  EV_CLOSE     = 1, /* if close set, do that before write/read */
  EV_WRITE_HI  = 2, /* when send buf full at send_highwater or read pressure */
  EV_READ      = 3, /* use read to fill until no more data or recv_highwater */
  EV_PROCESS   = 4, /* process read buffers */
  EV_PREFETCH  = 5, /* process key prefetch */
  EV_WRITE     = 6, /* write at low priority, suboptimal send of small buf */
  EV_SHUTDOWN  = 7, /* showdown after writes */
  EV_READ_LO   = 8, /* read at low priority, back pressure from full write buf */
  EV_BUSY_POLL = 9  /* busy poll, loop and keep checking for new data */
};

enum EvListFlag {
  IN_NO_LIST     = 0, /* init, invalid */
  IN_ACTIVE_LIST = 1, /* in the active list */
  IN_FREE_LIST   = 2  /* in a free list */
};

struct EvSocket : public PeerData /* fd and address of peer */ {
  EvPoll   & poll;     /* the parent container */
  uint64_t   prio_cnt; /* timeslice each socket for a slot to run */
  uint32_t   state;    /* bit mask of states, the queues the sock is in */
  EvSockType type;     /* listen or cnnection */
  EvListFlag listfl;   /* in active list or free list */
  uint64_t   bytes_recv, /* stat counters for bytes and msgs */
             bytes_sent,
             msgs_recv,
             msgs_sent;

  EvSocket( EvPoll &p,  EvSockType t,  PeerOps &o )
    : PeerData( o ), poll( p ), prio_cnt( 0 ),
      state( 0 ), type( t ), listfl( IN_NO_LIST ),
      bytes_recv( 0 ), bytes_sent( 0 ), msgs_recv( 0 ), msgs_sent( 0 ) {}

  /* priority queue states */
  int test( int s ) const { return this->state & ( 1U << s ); }
  void push( int s )      { this->state |= ( 1U << s ); }
  void pop( int s )       { this->state &= ~( 1U << s ); }
  void pop2( int s, int t ) {
    this->state &= ~( ( 1U << s ) | ( 1U << t ) ); }
  void pop3( int s, int t, int u ) {
    this->state &= ~( ( 1U << s ) | ( 1U << t ) | ( 1U << u ) ); }
  void popall( void )     { this->state = 0; }
  void pushpop( int s, int t ) {
    this->state = ( this->state | ( 1U << s ) ) & ~( 1U << t ); }
  void idle_push( EvState s );
  /* these should be overridden by subclass */
  /*bool publish( EvPublish &pub );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );*/
  /* priority queue test, ordered by first bit set (EV_WRITE > EV_READ).
   * a sock with EV_READ bit set will have a higher priority than one with
   * EV_WRITE */
  static bool is_greater( EvSocket *s1,  EvSocket *s2 ) {
    int x1 = __builtin_ffs( s1->state ),
        x2 = __builtin_ffs( s2->state );
    return x1 > x2 || ( x1 == x2 && s1->prio_cnt > s2->prio_cnt );
  }
  /* the "virtual" calls, dispatched based on this->type, inlined ev_net.cpp */
  void v_write( void );
  void v_read( void );
  void v_process( void );
  void v_release( void );
  bool v_timer_expire( uint64_t tid, uint64_t eid );
  bool v_hash_to_sub( uint32_t h, char *k, size_t &klen );
  bool v_on_msg( EvPublish &pub );
  void v_exec_key_prefetch( EvKeyCtx &ctx );
  int  v_exec_key_continue( EvKeyCtx &ctx );
  void v_process_shutdown( void );
  void v_process_close( void );
};

#if __cplusplus >= 201103L
  /* 64b align */
  static_assert( 256 == sizeof( EvSocket ), "socket size" );
#endif

struct EvSocketOps : public PeerOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen );
  virtual bool client_kill( PeerData &pd );
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka );
  virtual void client_stats( PeerData &pd,  PeerStats &ps );
  virtual void retired_stats( PeerData &pd,  PeerStats &ps );
  static bool client_match( PeerData &pd,  PeerMatchArgs &ka,  ... );
};

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( kv::BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( kv::BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

/* route_db.h has RoutePublish which contains the function for publishing -
 *   bool publish( pub, rcount, pref_cnt, ph )
 *   publishers may not need to see EvPoll, only RoutePublish, that is why it
 *   is a sepearate structure */
struct EvPoll : public RoutePublish {
  kv::PrioQueue<EvSocket *, EvSocket::is_greater> queue;
  EvSocket          ** sock;            /* sock array indexed by fd */
  struct epoll_event * ev;              /* event array used by epoll() */
  kv::HashTab        * map;             /* the data store */
  EvPrefetchQueue    * prefetch_queue;  /* ordering keys */
  KvPubSub           * pubsub;          /* cross process pubsub */
  EvTimerQueue       * timer_queue;     /* timer events */
  uint64_t             prio_tick,       /* priority queue ticker */
                       next_id;         /* unique id for connection */
  uint32_t             ctx_id,          /* this thread context */
                       fdcnt;           /* num fds in poll set */
  int                  efd,             /* epoll fd */
                       nfds,            /* max epoll() fds, array sz this->ev */
                       maxfd,           /* current maximum fd number */
                       quit;            /* when > 0, wants to exit */
  static const size_t  ALLOC_INCR    = 64, /* alloc size of poll socket ar */
                       PREFETCH_SIZE = 4;  /* pipe size of number of pref */
  size_t               prefetch_pending,
                       prefetch_cnt[ PREFETCH_SIZE + 1 ];
  RouteDB              sub_route;       /* subscriptions */
  RoutePublishQueue    pub_queue;       /* temp routing queue: */
  PeerStats            peer_stats;      /* accumulator after sock closes */
     /* this causes a message matching multiple wildcards to be sent once */

  /* socket lists, active and free lists, multiple socks are allocated at a
   * time to speed up accept and connection setup */
  kv::DLinkList<EvSocket>           active_list;    /* active socks in poll */
  kv::DLinkList<EvRedisService>     free_redis;     /* EvRedisService free */
  kv::DLinkList<EvHttpService>      free_http;      /* EvHttpService free */
  kv::DLinkList<EvNatsService>      free_nats;      /* EvNatsService free */
  kv::DLinkList<EvCaprService>      free_capr;      /* EvCaprService free */
  kv::DLinkList<EvRvService>        free_rv;        /* EvRvService free */
  kv::DLinkList<EvMemcachedService> free_memcached; /* EvMemcached free */
  /*bool single_thread; (if kv single threaded) */
  /* alloc ALLOC_INCR(64) elems of the above list elems at a time, aligned 64 */
  template<class T>
  T *get_free_list( kv::DLinkList<T> &free_list ) {
    T *c = free_list.hd;
    if ( c == NULL ) {
      size_t sz  = kv::align<size_t>( sizeof( T ), 64 );
      void * m   = aligned_malloc( sz * EvPoll::ALLOC_INCR );
      char * end = &((char *) m)[ sz * EvPoll::ALLOC_INCR ];
      if ( m == NULL )
        return NULL;
      while ( (char *) m < end ) {
        end = &end[ -sz ];
        c = new ( end ) T( *this );
        c->push_free_list();
      }
    }
    c->pop_free_list();
    return c;
  }
  template<class T, class S>
  T *get_free_list2( kv::DLinkList<T> &free_list, S &stats ) {
    T *c = free_list.hd;
    if ( c == NULL ) {
      size_t sz  = kv::align<size_t>( sizeof( T ), 64 );
      void * m   = aligned_malloc( sz * EvPoll::ALLOC_INCR );
      char * end = &((char *) m)[ sz * EvPoll::ALLOC_INCR ];
      if ( m == NULL )
        return NULL;
      while ( (char *) m < end ) {
        end = &end[ -sz ];
        c = new ( end ) T( *this, stats );
        c->push_free_list();
      }
    }
    c->pop_free_list();
    return c;
  }

  EvPoll()
    : sock( 0 ), ev( 0 ), map( 0 ), prefetch_queue( 0 ), pubsub( 0 ),
      timer_queue( 0 ), prio_tick( 0 ), next_id( 0 ), ctx_id( 0 ), fdcnt( 0 ),
      efd( -1 ), nfds( -1 ), maxfd( -1 ), quit( 0 ), prefetch_pending( 0 ),
      sub_route( *this ) /*, single_thread( false )*/ {
    ::memset( this->prefetch_cnt, 0, sizeof( this->prefetch_cnt ) );
  }

  int init( int numfds,  bool prefetch/*,  bool single*/ );
  int init_shm( EvShm &shm );    /* open shm pubsub */
  void add_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                          uint32_t fd );
  void del_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                          uint32_t fd );
  void add_route( const char *sub,  size_t sub_len,  uint32_t hash,
                  uint32_t fd );
  void del_route( const char *sub,  size_t sub_len,  uint32_t hash,
                  uint32_t fd );
  int wait( int ms );            /* call epoll() with ms timeout */
  bool dispatch( void );         /* process any sock in the queues */
  void drain_prefetch( void );   /* process prefetches */
  bool publish_one( EvPublish &pub,  uint32_t *rcount_total,
                    RoutePublishData &rpd );
  template<uint8_t N>
  bool publish_multi( EvPublish &pub,  uint32_t *rcount_total,
                      RoutePublishData *rpd );
  bool publish_queue( EvPublish &pub,  uint32_t *rcount_total );
  uint64_t current_coarse_ns( void ) const;
  /* add to poll set, name is a peer, alt is if listening or shm direct */
  int add_sock( EvSocket *s );
  void remove_sock( EvSocket *s ); /* remove from poll set */
  bool timer_expire( EvTimerEvent &ev ); /* process timer event fired */
  void process_quit( void );     /* quit state close socks */
};

struct EvListen : public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  uint64_t accept_cnt;
  EvListen( EvPoll &p,  PeerOps &o )
    : EvSocket( p, EV_LISTEN_SOCK, o ), accept_cnt( 0 ) {}

  virtual bool accept( void ) { return false; }
  void write( void ) {}
  void read( void ) { if ( this->accept() ) this->accept_cnt++; }
  void process( void ) {}
  void release( void ) {}
  bool timer_expire( uint64_t, uint64_t ) { return false; }
  bool hash_to_sub( uint32_t, char *, size_t & ) { return false; }
  bool on_msg( EvPublish & ) { return true; }
  void exec_key_prefetch( EvKeyCtx & ) {}
  int exec_key_continue( EvKeyCtx & ) { return 0; }
  void process_shutdown( void ) { this->pushpop( EV_CLOSE, EV_SHUTDOWN ); }
  void process_close( void ) {}
};

struct EvListenOps : public EvSocketOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen );
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka );
  virtual void client_stats( PeerData &pd,  PeerStats &ps );
};

struct EvConnection : public EvSocket, public StreamBuf {
  char   * recv;           /* initially recv_buf, but may realloc */
  uint32_t off,            /* offset of recv_buf consumed */
           len,            /* length of data in recv_buf */
           recv_size,      /* recv buf size */
           recv_highwater, /* recv_highwater: switch to low priority read */
           send_highwater, /* send_highwater: switch to high priority write */
           pad;
  char     recv_buf[ 16 * 1024 ] __attribute__((__aligned__( 64 )));

  EvConnection( EvPoll &p, EvSockType t,  PeerOps &o ) : EvSocket( p, t, o ) {
    this->recv           = this->recv_buf;
    this->off            = 0;
    this->len            = 0;
    this->recv_size      = sizeof( this->recv_buf );
    this->recv_highwater = this->recv_size - this->recv_size / 8;
    this->send_highwater = this->recv_size * 2;
    this->pad            = 0xaa99bb88U;
  }
  void release_buffers( void ) { /* release all buffs */
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {   /* clear any allocations and counters */
    this->StreamBuf::reset();
    this->off = this->len = 0;
    if ( this->recv != this->recv_buf ) {
      ::free( this->recv );
      this->recv = this->recv_buf;
      this->recv_size = sizeof( this->recv_buf );
      this->recv_highwater = this->recv_size - this->recv_size / 8;
      this->send_highwater = this->recv_size * 2;
    }
  }
  void adjust_recv( void ) {     /* data is read at this->recv[ this->len ] */
    if ( this->off > 0 ) {
      this->len -= this->off;
      if ( this->len > 0 )
        ::memmove( this->recv, &this->recv[ this->off ], this->len );
      else if ( this->recv != this->recv_buf ) {
        ::free( this->recv );
        this->recv = this->recv_buf;
        this->recv_size = sizeof( this->recv_buf );
      }
      this->off = 0;
    }
  }
  bool resize_recv_buf( void );   /* need more buffer space */
  void read( void );              /* fill recv buf */
  void write( void );             /* flush stream buffer */
  void close_alloc_error( void ); /* if stream buf alloc failed or similar */
  void process_shutdown( void ) { this->pushpop( EV_CLOSE, EV_SHUTDOWN ); }
};

struct EvConnectionOps : public EvSocketOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen );
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka );
};

struct EvUdp : public EvSocket, public StreamBuf {
  struct    mmsghdr * in_mhdr,
                    * out_mhdr;
  uint32_t  in_moff,
            in_nmsgs,
            in_size,
            in_nsize,
            out_nmsgs;

  EvUdp( EvPoll &p, EvSockType t, PeerOps &o ) : EvSocket( p, t, o ),
    in_mhdr( 0 ), out_mhdr( 0 ), in_moff( 0 ), in_nmsgs( 0 ), in_size( 0 ),
    in_nsize( 1 ), out_nmsgs( 0 ) {}
  void zero( void ) {
    this->in_mhdr = this->out_mhdr = NULL;
    this->in_moff = this->in_nmsgs = 0;
    this->out_nmsgs = this->in_size = 0;
  }
  bool alloc_mmsg( void );
  int listen( const char *ip,  int port,  const char *k );
  int connect( const char *ip,  int port );

  void release_buffers( void ) { /* release all buffs */
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {   /* clear any allocations and counters */
    this->zero();
    this->StreamBuf::reset();
  }
  void read( void );             /* fill recv buf, return true if read some */
  void write( void );            /* flush stream buffer */
  void process_shutdown( void ) { this->pushpop( EV_CLOSE, EV_SHUTDOWN ); }
};

struct EvUdpOps : public EvSocketOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen );
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka );
};

}
}
#endif
