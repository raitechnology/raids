#ifndef __rai_raids__timer_queue_h__
#define __rai_raids__timer_queue_h__

#include <raids/ev_net.h>

namespace rai {
namespace ds {

enum TimerUnits {
  IVAL_SECS   = 0,
  IVAL_MILLIS = 1,
  IVAL_MICROS = 2,
  IVAL_NANOS  = 3
};

struct EvTimerEvent {
  int      id;          /* owner of event (fd) */
  uint32_t ival;        /* interval with lower 2 bits containing units */
  uint64_t timer_id,    /* if multiple timer events for each owner */
           next_expire; /* next expiration time */

  static bool is_greater( EvTimerEvent e1,  EvTimerEvent e2 ) {
    return e1.next_expire > e2.next_expire; /* expires later */
  }
  bool operator==( const EvTimerEvent &el ) const {
    return this->id == el.id && this->timer_id == el.timer_id;
  }
};

struct EvTimerQueue : public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  kv::PrioQueue<EvTimerEvent, EvTimerEvent::is_greater> queue;
  uint64_t last, now;

  EvTimerQueue( EvPoll &p )
    : EvSocket( p, EV_TIMER_QUEUE ), last( 0 ), now( 0 ) {}

  bool add_timer( int id,  uint32_t ival,  uint64_t timer_id,  TimerUnits u );
  bool remove_timer( int id,  uint64_t timer_id );
  void repost( void );
  bool read( void );
  bool set_timer( uint64_t ns );
  void process( void );
  void process_close( void ) {}
  void process_shutdown( void ) {}

  static EvTimerQueue *create_timer_queue( EvPoll &p );
};

}
}
#endif
