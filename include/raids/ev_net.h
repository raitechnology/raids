#ifndef __rai_raids__ev_net_h__
#define __rai_raids__ev_net_h__

#include <raikv/shm_ht.h>
#include <raids/stream_buf.h>

namespace rai {
namespace ds {

enum EvSockType {
  EV_LISTEN_SOCK  = 0,
  EV_SERVICE_SOCK = 1,
  EV_CLIENT_SOCK  = 2,
  EV_TERMINAL     = 3
};

enum EvState {
  EV_DEAD    = 0,  /* a socket may be in multiple of these states */
  EV_WAIT    = 1,
  EV_READ    = 2,
  EV_PROCESS = 3,
  EV_WRITE   = 4,
  EV_CLOSE   = 5,
  EV_MAX     = 6
};

struct EvSocket;

struct EvQueue {
  uint16_t   idx,   /* state, queue[ idx ] */
             state; /* state bit, 1 << idx */
  uint32_t   cnt;
  EvSocket * hd,    /* list of socks in the queue */
           * tl;
  void init( EvState ev ) {
    this->idx   = ev;
    this->state = 1U << ev;
    this->cnt   = 0;
    this->hd    = NULL;
    this->tl    = NULL;
  }
  void push( EvSocket *p );
  void pop( EvSocket *p );
};

struct EvPrefetchQueue;

struct EvPoll {
  EvQueue              queue[ EV_MAX ]; /* EvState queues */
  EvSocket          ** sock;            /* sock array indexed by fd */
  struct epoll_event * ev;              /* event array used by epoll() */
  kv::HashTab        * map;             /* the data store */
  EvPrefetchQueue    * prefetch_queue;  /* ordering keys */
  const uint32_t       ctx_id;          /* this thread context */
  int                  efd,             /* epoll fd */
                       nfds,            /* max epoll() fds, array sz this->ev */
                       maxfd,           /* current maximum fd number */
                       quit;            /* when > 0, wants to exit */
  static const size_t  PREFETCH_SIZE = 8;
  size_t               prefetch_cnt[ PREFETCH_SIZE + 1 ];
  bool                 single_thread;

  EvPoll( kv::HashTab *m,  uint32_t id )
    : sock( 0 ), ev( 0 ), map( m ), prefetch_queue( 0 ), ctx_id( id ),
      efd( -1 ), nfds( -1 ), maxfd( -1 ), quit( 0 ), single_thread( false ) {
    for ( int i = EV_DEAD; i < EV_MAX; i++ )
      this->queue[ i ].init( (EvState) i );
    ::memset( this->prefetch_cnt, 0, sizeof( this->prefetch_cnt ) );
  }

  int init( int numfds,  bool prefetch,  bool single );
  int wait( int ms );            /* call epoll() with ms timeout */
  void dispatch( void );         /* process any sock in the queues */
  void drain_prefetch( EvPrefetchQueue &q ); /* process prefetches */
  void dispatch_service( void ); /* process service and listen socks */
  void process_close( void );    /* close socks or quit state */
};

struct EvSocket {
  EvSocket * next[ EV_MAX ], /* a link for the queue for each state bit */
           * back[ EV_MAX ];
  EvPoll   & poll;           /* the parent container */
  EvSockType type;           /* listen or cnnection */
  uint16_t   state;          /* bit mask of states, the queues the sock is in */
  int        fd;             /* the socket fd */

  EvSocket( EvPoll &p,  EvSockType t )
    : poll( p ), type( t ), state( 0 ), fd( -1 ) {
    ::memset( this->next, 0, sizeof( this->next ) );
    ::memset( this->back, 0, sizeof( this->back ) );
  }
  int add_poll( void );
  void remove_poll( void );
  void close( void );
  int test( EvState s ) const {
    return this->state & ( 1U << (int) s );
  }
  void push( EvState s ) {
    if ( ! this->test( s ) ) {
      this->state |= ( 1U << (int) s );
      this->poll.queue[ s ].push( this );
    }
  }
  void pop( EvState s ) {
    if ( this->test( s ) ) {
      this->state &= ~( 1U << (int) s );
      this->poll.queue[ s ].pop( this );
    }
  }
  void popall( void ) {
    for ( int i = 0; i < EV_MAX; i++ ) {
      if ( this->test( (EvState) i ) ) {
	this->state &= ~( 1U << i );
	this->poll.queue[ i ].pop( this );
      }
    }
  }
};

struct EvListen : public EvSocket {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvListen( EvPoll &p ) : EvSocket( p, EV_LISTEN_SOCK ) {}

  int listen( const char *ip,  int port );
  void accept( void );
};

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( kv::BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( kv::BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

struct EvConnection : public EvSocket, public StreamBuf {
  char   * recv,        /* initially recv_buf, but may realloc */
         * recv_end;    /* alloc size of current buffer */
  uint32_t off,         /* offset of recv_buf consumed */
           len;         /* length of data in recv_buf */
  char     recv_buf[ 8192 ] __attribute__((__aligned__( 64 )));

  EvConnection( EvPoll &p, EvSockType t ) : EvSocket( p, t ) {
    this->recv     = this->recv_buf;
    this->recv_end = &this->recv_buf[ sizeof( this->recv_buf ) ];
    this->off      = 0;
    this->len      = 0;
  }
  void release( void ) {
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {
    this->StreamBuf::reset();
    this->off = this->len = 0;
    if ( this->recv != this->recv_buf ) {
      ::free( this->recv );
      this->recv = this->recv_buf;
      this->recv_end = &this->recv_buf[ sizeof( this->recv_buf ) ];
    }
  }
  void adjust_recv( void ) {
    if ( this->off > 0 ) {
      this->len -= this->off;
      if ( this->len > 0 )
        ::memmove( this->recv, &this->recv[ this->off ], this->len );
      this->off = 0;
    }
  }
  bool read( void );
  bool try_read( void );
  bool write( void );
  bool try_write( void );
  void close_alloc_error( void );
};

inline void EvQueue::push( EvSocket *p ) {
  const uint16_t i = this->idx;
  this->cnt++;
  p->state |= this->state;
  p->next[ i ] = this->hd;
  p->back[ i ] = NULL;
  if ( this->hd == NULL )
    this->tl = p;
  else
    this->hd->back[ i ] = p;
  this->hd = p;
}

inline void EvQueue::pop( EvSocket *p ) {
  const uint16_t i = this->idx;
  this->cnt--;
  p->state &= ~this->state;
  if ( p->back[ i ] == NULL )
    this->hd = p->next[ i ];
  else
    p->back[ i ]->next[ i ] = p->next[ i ];
  if ( p->next[ i ] == NULL )
    this->tl = p->back[ i ];
  else
    p->next[ i ]->back[ i ] = p->back[ i ];
  p->next[ i ] = p->back[ i ] = NULL;
}

}
}
#endif
