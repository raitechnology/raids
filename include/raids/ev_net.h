#ifndef __rai_raids__ev_net_h__
#define __rai_raids__ev_net_h__

#include <raikv/shm_ht.h>
#include <raikv/prio_queue.h>
#include <raikv/dlinklist.h>
#include <raids/stream_buf.h>
#include <raids/route_db.h>

namespace rai {
namespace ds {

enum EvSockType {
  EV_REDIS_SOCK  = 0, /* redis protocol */
  EV_HTTP_SOCK   = 1, /* http / websock protocol */
  EV_LISTEN_SOCK = 2, /* any type of listener (tcp or unix sock stream) */
  EV_CLIENT_SOCK = 3, /* redis client protocol */
  EV_TERMINAL    = 4, /* redis terminal (converts redis proto to/from json) */
  EV_NATS_SOCK   = 5, /* nats pub/sub protocol */
  EV_CAPR_SOCK   = 6, /* capr pub/sub protocol */
  EV_RV_SOCK     = 7, /* rv pub/sub protocol */
  EV_KV_PUBSUB   = 8, /* route between processes */
  EV_SHM_SOCK    = 9, /* local shm client */
  EV_TIMER_QUEUE = 10,/* event timers */
  EV_SHM_SVC     = 11 /* pubsub service */
};

enum EvState {
  EV_READ_HI   = 0, /* listen port accept */
  EV_CLOSE     = 1, /* if close set, do that before write/read */
  EV_WRITE_HI  = 2, /* when send buf full at send_highwater or read pressure */
  EV_READ      = 3, /* use read to fill until no more data or recv_highwater */
  EV_PROCESS   = 4, /* process read buffers */
  EV_WRITE     = 5, /* write at low priority, suboptimal send of small buf */
  EV_SHUTDOWN  = 6, /* showdown after writes */
  EV_READ_LO   = 7, /* read at low priority, back pressure from full write buf */
  EV_BUSY_POLL = 8  /* busy poll, loop and keep checking for new data */
};

enum EvListFlag {
  IN_NO_LIST     = 0, /* init, invalid */
  IN_ACTIVE_LIST = 1, /* in the active list */
  IN_FREE_LIST   = 2  /* in a free list */
};

struct EvSocket;
struct EvPrefetchQueue; /* queue for prefetching key memory */
struct EvPublish;
struct EvPoll;
struct KvPubSub;

struct EvSocket {
  EvSocket * next,     /* link for sock lists */
           * back;
  EvPoll   & poll;     /* the parent container */
  uint64_t   prio_cnt; /* timeslice each socket for a slot to run */
  int        fd;       /* the socket fd */
  uint16_t   state;    /* bit mask of states, the queues the sock is in */
  EvSockType type;     /* listen or cnnection */
  EvListFlag listfl;   /* in active list or free list */

  EvSocket( EvPoll &p,  EvSockType t )
    : next( 0 ), back( 0 ), poll( p ), prio_cnt( 0 ), fd( -1 ),
      state( 0 ), type( t ), listfl( IN_NO_LIST ) {}

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
  bool publish( EvPublish &pub );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  /* priority queue test, ordered by first bit set (EV_WRITE > EV_READ).
   * a sock with EV_READ bit set will have a higher priority than one with
   * EV_WRITE */
  static bool is_greater( EvSocket *s1,  EvSocket *s2 ) {
    int x1 = __builtin_ffs( s1->state ),
        x2 = __builtin_ffs( s2->state );
    return x1 > x2 || ( x1 == x2 && s1->prio_cnt > s2->prio_cnt );
  }
};

struct EvRedisService;
struct EvHttpService;
struct EvNatsService;
struct EvCaprService;
struct EvRvService;
struct KvPubSub;
struct EvShm;
struct EvTimerQueue;
struct EvTimerEvent;

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
  uint64_t             prio_tick;       /* priority queue ticker */
  uint32_t             ctx_id,          /* this thread context */
                       fdcnt;           /* num fds in poll set */
  int                  efd,             /* epoll fd */
                       nfds,            /* max epoll() fds, array sz this->ev */
                       maxfd,           /* current maximum fd number */
                       quit;            /* when > 0, wants to exit */
  static const size_t  ALLOC_INCR    = 64, /* alloc size of poll socket ar */
                       PREFETCH_SIZE = 8;  /* pipe size of number of pref */
  size_t               prefetch_cnt[ PREFETCH_SIZE + 1 ];
  RouteDB              sub_route;       /* subscriptions */
  RoutePublishQueue    pub_queue;       /* temp routing queue: */
     /* this causes a message matching multiple wildcards to be sent once */

  /* socket lists, active and free lists, multiple socks are allocated at a
   * time to speed up accept and connection setup */
  kv::DLinkList<EvSocket>       active_list;/* active socks in poll */
  kv::DLinkList<EvRedisService> free_redis; /* EvRedisService free */
  kv::DLinkList<EvHttpService>  free_http;  /* EvHttpService free */
  kv::DLinkList<EvNatsService>  free_nats;  /* EvNatsService free */
  kv::DLinkList<EvCaprService>  free_capr;  /* EvCaprService free */
  kv::DLinkList<EvRvService>    free_rv;    /* EvRvService free */
  /*bool single_thread; (if kv single threaded) */

  EvPoll()
    : sock( 0 ), ev( 0 ), map( 0 ), prefetch_queue( 0 ), pubsub( 0 ),
      timer_queue( 0 ), prio_tick( 0 ), ctx_id( 0 ), fdcnt( 0 ), efd( -1 ),
      nfds( -1 ), maxfd( -1 ), quit( 0 ), sub_route( *this )
      /*, single_thread( false )*/ {
    ::memset( this->prefetch_cnt, 0, sizeof( this->prefetch_cnt ) );
  }

  int init( int numfds,  bool prefetch/*,  bool single*/ );
  int init_shm( EvShm &shm );    /* open shm pubsub */
  int wait( int ms );            /* call epoll() with ms timeout */
  bool dispatch( void );         /* process any sock in the queues */
  void drain_prefetch( EvPrefetchQueue &q ); /* process prefetches */
  bool publish_one( EvPublish &pub,  uint32_t *rcount_total,
                    RoutePublishData &rpd );
  template<uint8_t N>
  bool publish_multi( EvPublish &pub,  uint32_t *rcount_total,
                      RoutePublishData *rpd );
  bool publish_queue( EvPublish &pub,  uint32_t *rcount_total );

  int add_sock( EvSocket *s );     /* add to poll set */
  void remove_sock( EvSocket *s ); /* remove from poll set */
  bool timer_expire( EvTimerEvent &ev ); /* process timer event fired */
  void process_quit( void );     /* quit state close socks */
};

inline void
EvSocket::idle_push( EvState s )
{
  bool mt = ( this->state == 0 );
  this->push( s );
  if ( mt ) { /* add to queue if already there */
    this->prio_cnt = this->poll.prio_tick;
    this->poll.queue.push( this );
  }
}

struct EvListen : public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvListen( EvPoll &p ) : EvSocket( p, EV_LISTEN_SOCK ) {}

  virtual void accept( void ) {}
  void process_close( void );
  void process_shutdown( void ) { this->pushpop( EV_CLOSE, EV_SHUTDOWN ); }
};

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( kv::BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( kv::BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

struct EvConnection : public EvSocket, public StreamBuf {
  char   * recv;           /* initially recv_buf, but may realloc */
  uint32_t off,            /* offset of recv_buf consumed */
           len,            /* length of data in recv_buf */
           recv_size,      /* recv buf size */
           recv_highwater, /* recv_highwater: switch to low priority read */
           send_highwater, /* send_highwater: switch to high priority write */
           pad;
  uint64_t nbytes_recv,
           nbytes_sent;
  char     recv_buf[ 4 * 4096 ] __attribute__((__aligned__( 64 )));

  EvConnection( EvPoll &p, EvSockType t ) : EvSocket( p, t ) {
    this->recv           = this->recv_buf;
    this->off            = 0;
    this->len            = 0;
    this->recv_size      = sizeof( this->recv_buf );
    this->recv_highwater = this->recv_size - this->recv_size / 8;
    this->send_highwater = this->recv_size * 2;
    this->pad            = 0xaa99bb88U;
    this->nbytes_recv    = 0;
    this->nbytes_sent    = 0;
  }
  void release_buffers( void ) { /* release all buffs */
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {   /* clear any allocations and counters */
    this->StreamBuf::reset();
    this->off = this->len = 0;
    this->nbytes_recv = 0;
    this->nbytes_sent = 0;
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
      this->off = 0;
    }
  }
  bool resize_recv_buf( void );   /* need more buffer space */
  bool read( void );              /* fill recv buf, return true if read some */
  bool try_read( void );          /* try to read and create space */
  size_t write( void );           /* flush stream buffer */
  size_t try_write( void );       /* try to flush and create space */
  void close_alloc_error( void ); /* if stream buf alloc failed or similar */
  void process_shutdown( void ) { this->pushpop( EV_CLOSE, EV_SHUTDOWN ); }
};

}
}
#endif
