#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <raids/ev_client.h>
#include <raids/ev_publish.h>
#include <raids/kv_pubsub.h>
#include <raids/timer_queue.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ ) 
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
      return argv[ i + b ];
  return def; /* default value */
}

struct PingTest : public EvShmSvc {
  const char * sub,
             * pub,
             * ibx;
  size_t       len,
               plen,
               ilen;
  uint32_t     h,
               ph,
               ih,
               per_sec,
               ns_ival;
  uint64_t     sum,
               count,
               print_count,
               last_time;
  bool         active_ping, /* initiates the round trip or one way */
               round_trip;

  PingTest( EvPoll &poll,  const char *s,  const char *p,  const char *i,
            bool act,  bool round,  uint32_t ps_rate )
    : EvShmSvc( poll ), sub( s ), pub( p ), ibx( i ),
      len( ::strlen( s ) ), plen( ::strlen( p ) ),
      ilen( i ? ::strlen( i ) : 0 ),
      h( 0 ), ph( 0 ), ih( 0 ), per_sec( ps_rate ), ns_ival( 1e9 / ps_rate ),
      sum( 0 ), count( 0 ), print_count( 0 ), last_time( 0 ),
      active_ping( act ), round_trip( round ) {
  }
  /* start subcriptions for sub or inbox */
  void subscribe( void ) {
    uint32_t rcnt;
    this->h  = kv_crc_c( this->sub, this->len, 0 );
    this->ph = kv_crc_c( this->pub, this->plen, 0 );
    this->ih = kv_crc_c( this->ibx, this->ilen, 0 );
    if ( this->round_trip || ! this->active_ping ) {
      /* if using inbox for reply */
      if ( this->active_ping && this->ilen > 0 ) {
        rcnt = this->poll.sub_route.add_route( this->ih, this->fd );
        this->poll.pubsub->notify_sub( this->ih, this->ibx, this->ilen,
                                       this->fd, rcnt, 'K' );
      }
      else {
        rcnt = this->poll.sub_route.add_route( this->h, this->fd );
        this->poll.pubsub->notify_sub( this->h, this->sub, this->len,
                                       this->fd, rcnt, 'K' );
      }
    }
  }
  /* remove subcriptions for sub or inbox */
  void unsubscribe( void ) {
    uint32_t rcnt;
    if ( this->round_trip || ! this->active_ping ) {
      if ( this->active_ping && this->ilen > 0 ) {
        rcnt = this->poll.sub_route.del_route( this->ih, this->fd );
        this->poll.pubsub->notify_unsub( this->ih, this->ibx, this->ilen,
                                         this->fd, rcnt, 'K' );
      }
      else {
        rcnt = this->poll.sub_route.del_route( this->h, this->fd );
        this->poll.pubsub->notify_unsub( this->h, this->sub, this->len,
                                         this->fd, rcnt, 'K' );
      }
    }
  }
  /* recv an incoming message from a subscription above, sent from a peer or
   * myself if subscribing to the same subject as publishing */
  virtual bool on_msg( EvPublish &p ) {
    const char * out;
    size_t       out_len;
    uint32_t     out_hash;
    /* first case is the reflecter, just sending what was recved */
    if ( this->round_trip && ! this->active_ping ) {
      if ( p.reply_len > 0 ) {
        out      = (const char *) p.reply;
        out_len  = p.reply_len;
        out_hash = kv_crc_c( out, out_len, 0 );
      }
      else {
        out      = this->pub;
        out_len  = this->plen;
        out_hash = this->ph;
      }
      EvPublish rp( out, out_len, NULL, 0, p.msg,
                    p.msg_len, this->fd, out_hash,
                    p.msg_len_buf, p.msg_len_digits,
                    p.msg_enc, p.pub_type );
      this->poll.forward_msg( rp, NULL, 0, NULL );
    }
    /* the active pinger or one way prints */
    else {
      uint64_t s,
               /*t = kv_get_rdtsc();*/
               t = kv_current_monotonic_time_ns();
      if ( p.msg_len == sizeof( s ) && p.msg_enc == MD_UINT ) {
        ::memcpy( &s, p.msg, 8 );
        this->sum += ( t - s );
        if ( this->count++ == this->print_count ) {
          printf( "recv ping %lu nanos, cnt %lu\n", t - s, this->count );
          this->print_count += this->per_sec;
        }
      }
    }
    return true;
  }
  /* shutdown before close */
  virtual void process_shutdown( void ) {
    if ( this->h != 0 ) {
      this->unsubscribe();
      this->h = 0;
    }
  }
  /* start a timer at per_sec interval */
  void start_timer( void ) {
    if ( this->per_sec >= 1000 ) {
      this->poll.timer_queue->add_timer( this->fd, this->ns_ival, 1,
                                         IVAL_NANOS );
    }
    else {
      uint32_t us_ival = this->ns_ival / 1000;
      this->poll.timer_queue->add_timer( this->fd, us_ival, 1, IVAL_MICROS );
    }
  }
  /* a timer expires every ns_ival, send messages */
  virtual bool timer_expire( uint64_t ) {
    uint64_t now = this->poll.timer_queue->now;
    if ( this->last_time == 0 ) {
      this->last_time = now;
      return true;
    }
    for ( ; this->last_time < now;
          this->last_time += (uint64_t) this->ns_ival ) {
      uint64_t t = kv_current_monotonic_time_ns();
      /*uint64_t t = kv_get_rdtsc();*/
      EvPublish p( this->pub, this->plen, this->ibx, this->ilen, &t,
                   sizeof( t ), this->fd, this->ph,
                   NULL, 0, MD_UINT, 'u' );
      this->poll.forward_msg( p, NULL, 0, NULL );
      if ( this->per_sec < 100 )
        break;
    }
    return true;
  }
};

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvPoll        poll;

  const char * mn = get_arg( argc, argv, 1, "-m", "sysv2m:shm.test" ),
             * su = get_arg( argc, argv, 1, "-s", "PONG" ),
             * pu = get_arg( argc, argv, 1, "-p", "PING" ),
             * xx = get_arg( argc, argv, 1, "-x", "1" ),
             * ib = get_arg( argc, argv, 0, "-i", 0 ),
             * _1 = get_arg( argc, argv, 0, "-1", 0 ),
             * re = get_arg( argc, argv, 0, "-r", 0 ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 ),
             * inbox = NULL;
  char     inbox_buf[ 24 ];
  uint32_t per_sec = atoi( xx );
  bool     active_ping = ( re == NULL ),
           round_trip  = ( _1 == NULL );

  if ( he != NULL ) {
    printf( "%s"
      " [-m map] [-s sub] [-p pub] [-x rate] [-i] [-r] [-k] [-b]\n"
      "  map  = kv shm map name      (sysv2m:shm.test)\n"
      "  sub  = subject to subscribe (PONG)\n"
      "  pub  = subject to publish   (PING)\n"
      "  rate = publish rate per sec (1)\n"
      "  -i   = use inbox reply instead\n"
      "  -1   = time one way, not round trip\n"
      "  -r   = passively reflect/reverse pub/sub\n"
      "  -k   = don't use signal USR1 pub notification\n"
      "  -b   = busy poll\n", argv[ 0 ] );
    return 0;
  }
  if ( ! active_ping ) {
    const char *tmp = su;
    su = pu;
    pu = tmp;
  }
  if ( ib != NULL ) {
    snprintf( inbox_buf, sizeof( inbox_buf ), "_IBX.%d", getpid() );
    inbox = inbox_buf;
  }

  printf( "listening on subject %s publish %s\n", su, pu );
  poll.init( 5, false );
  PingTest shm( poll, su, pu, inbox, active_ping, round_trip, per_sec );
  if ( shm.open( mn ) != 0 )
    return 1;
  if ( poll.init_shm( shm ) != 0 || shm.init_poll() != 0 )
    return 1;
  shm.subscribe();
  sighndl.install();
  if ( active_ping ) {
    shm.start_timer();
  }
  if ( bu != NULL ) {
    poll.pubsub->idle_push( EV_BUSY_POLL );
  }
  if ( no != NULL ) {
    poll.pubsub->flags &= ~KV_DO_NOTIFY;
  }
  while ( poll.quit < 5 ) {
    bool idle = poll.dispatch(); /* true if idle, false if busy */
    poll.wait( idle ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  shm.close();
  if ( shm.count > 0 ) {
    printf( "count %lu avg %lu nanos\n", shm.count, shm.sum / shm.count );
  }

  return 0;
}

