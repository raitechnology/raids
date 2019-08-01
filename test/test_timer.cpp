#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <raikv/prio_queue.h>

using namespace rai;
using namespace kv;

int
set_timer( int tfd,  uint64_t nsecs )
{
  struct itimerspec ts;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 0;
  ts.it_value.tv_sec  = nsecs / (uint64_t) 1000000000;
  ts.it_value.tv_nsec = nsecs % (uint64_t) 1000000000;

  if ( timerfd_settime( tfd, 0, &ts, NULL ) < 0 ) {
    printf("timerfd_settime() failed: errno=%d\n", errno);
    close(tfd);
    return 1;
  }
  return 0;
}

uint64_t
current_ns( void )
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

struct TimeElem {
  int      id;
  uint32_t ival;
  uint64_t next;

  static bool is_greater( TimeElem e1,  TimeElem e2 ) {
    return e1.next > e2.next;
  }
  bool operator==( const TimeElem &el ) const {
    return this->id == el.id;
  }
};

enum TimerUnits {
  IVAL_SECS   = 0,
  IVAL_MILLIS = 1,
  IVAL_MICROS = 2,
  IVAL_NANOS  = 3
};

static uint32_t ival_ns[] = { 1000 * 1000 * 1000, 1000 * 1000, 1000, 0 };

struct TimerQueue : public PrioQueue<TimeElem, TimeElem::is_greater> {
  int      epfd,
           tfd;
  uint64_t last,
           now;

  TimerQueue( int epf,  int tf ) : epfd( epf ), tfd( tf ),
                                   last( current_ns() ), now( 0 ) {}

  void add_timer( int id,  uint32_t ival,  TimerUnits u ) {
    TimeElem el;
    el.id   = id;
    el.ival = ( ival << 2 ) | (uint32_t) u;
    el.next = current_ns() + ( (uint64_t) ival * (uint64_t) ival_ns[ u ] );
    this->push( el );
  }
  int wait( void );

  int dispatch( void );

  void repost( void ) {
    TimeElem el = this->pop();
    el.next += (uint64_t) ( el.ival >> 2 ) * (uint64_t) ival_ns[ el.ival & 3 ];
    this->push( el );
  }
};

int
TimerQueue::wait( void )
{
  int cnt = 0;
  epoll_event ev;
  for (;;) {
    this->now = current_ns();
    if ( this->heap[ 0 ].next <= this->now )
      break;
    set_timer( this->tfd, this->heap[ 0 ].next - this->now );
    int n = epoll_wait( this->epfd, &ev, 1, 1000 );
    if ( n > 0 ) {
      uint64_t res;
      if ( ::read( this->tfd, &res, 8 ) > 0 )
        cnt++;
    }
  }
  return cnt;
}

int
TimerQueue::dispatch( void )
{
  int id = this->heap[ 0 ].id;
  this->last = this->now;
  this->repost();
  return id;
}

int
main( int, char ** )
{
  int tfd, epfd;
  epoll_event ev;

  tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  if (tfd == -1) {
    printf("timerfd_create() failed: errno=%d\n", errno);
    return 1;
  }

  epfd = epoll_create(1);
  if (epfd == -1) {
    printf("epoll_create() failed: errno=%d\n", errno);
    close(tfd);
    return 1;
  }

  ev.events = EPOLLIN;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &ev) == -1) {
    printf("epoll_ctl(ADD) failed: errno=%d\n", errno);
    close(epfd);
    close(tfd);
    return 1;
  }

  struct timeval s;
  gettimeofday( &s, NULL );
  TimerQueue q( epfd, tfd );
  q.add_timer( 0, 10, IVAL_SECS );
  q.add_timer( 1, 6500, IVAL_MILLIS );
  q.add_timer( 2, 9750000, IVAL_MICROS );
  double ival[ 3 ] = { 10.0, 6.5, 9.75 };
  int count[ 3 ] = { 0, 0, 0 };
  for (;;) {
    struct tm x;
    struct timeval t;
    q.wait();
    int id = q.dispatch();

    gettimeofday( &t, NULL );
    if ( t.tv_usec < s.tv_usec ) {
      t.tv_usec += 1000000;
      t.tv_sec--;
    }
    t.tv_sec -= s.tv_sec;
    t.tv_usec -= s.tv_usec;
    localtime_r( &t.tv_sec, &x );
    count[ id ]++;
    printf( "id=%u times=%d ival=%.3f, %02u:%02u.%06u\n\n",
            id, count[ id ], (double) count[ id ] * ival[ id ],
            x.tm_min, x.tm_sec, (uint32_t) t.tv_usec );
  }
#if 0
  for (;;) {
    uint64_t t1 = current_ns(), t2;
    set_timer( tfd, 30000000 );
    int n = epoll_wait( epfd, &ev, 1, 1000 );
    if ( n > 0 )
      ::read( tfd, &res, 8 );
    t2 = current_ns();
    printf( "%d = %lu (%lu)\n", n, t2 - t1, res );
  }
#endif
  return 0;
}
