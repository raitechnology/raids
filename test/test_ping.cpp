#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <raids/ev_client.h>
#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>
#include <raikv/timer_queue.h>
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

struct PingTest : public EvShmSvc, public RouteNotify {
  RoutePDB   & sub_route;
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
               pub_left,
               ns_ival;
  uint64_t     sum,
               count,
               print_count,
               last_time;
  bool         active_ping, /* initiates the round trip or one way */
               round_trip;

  PingTest( EvPoll &poll,  const char *s,  const char *p,  const char *i,
            bool act,  bool round,  uint32_t ps_rate,  uint32_t pub_cnt )
    : EvShmSvc( poll ), RouteNotify( poll.sub_route ),
      sub_route( poll.sub_route ), sub( s ), pub( p ),
      ibx( i ), len( ::strlen( s ) ), plen( ::strlen( p ) ),
      ilen( i ? ::strlen( i ) : 0 ),
      h( 0 ), ph( 0 ), ih( 0 ), per_sec( ps_rate ), pub_left( pub_cnt ),
      ns_ival( 1e9 / ps_rate ), sum( 0 ), count( 0 ), print_count( 0 ),
      last_time( 0 ), active_ping( act ), round_trip( round ) {
  }
  /* start subcriptions for sub or inbox */
  void subscribe( void ) {
    this->sub_route.add_route_notify( *this );
    this->h  = kv_crc_c( this->sub, this->len, 0 );
    this->ph = kv_crc_c( this->pub, this->plen, 0 );
    this->ih = kv_crc_c( this->ibx, this->ilen, 0 );
    if ( this->round_trip || ! this->active_ping ) {
      /* if using inbox for reply */
      if ( this->active_ping && this->ilen > 0 ) {
        NotifySub nsub( this->ibx, this->ilen, this->ih, this->fd, false, 'K' );
        this->sub_route.add_sub( nsub );
      }
      else {
        NotifySub nsub( this->sub, this->len, this->h, this->fd, false, 'K' );
        this->sub_route.add_sub( nsub );
      }
    }
  }
  /* remove subcriptions for sub or inbox */
  void unsubscribe( void ) {
    if ( this->round_trip || ! this->active_ping ) {
      if ( this->active_ping && this->ilen > 0 ) {
        NotifySub nsub( this->ibx, this->ilen, this->ih, this->fd, false, 'K' );
        this->sub_route.del_sub( nsub );
      }
      else {
        NotifySub nsub( this->sub, this->len, this->h, this->fd, false, 'K' );
        this->sub_route.del_sub( nsub );
      }
    }
  }
  /* recv an incoming message from a subscription above, sent from a peer or
   * myself if subscribing to the same subject as publishing */
  virtual bool on_msg( EvPublish &p ) noexcept {
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
                    p.msg_len, this->sub_route, this->fd,
                    out_hash, p.msg_enc, p.pub_type );
      this->sub_route.forward_msg( rp, NULL, 0, NULL );
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
  virtual void on_sub( const NotifySub &sub ) noexcept {
    printf( "on_sub src_fd=%u %.*s", sub.src_fd, (int) sub.subject_len,
            sub.subject );
    if ( sub.reply_len != 0 )
      printf( " reply %.*s", (int) sub.reply_len, sub.reply );
    printf( "\n" );
  }
  virtual void on_unsub( const NotifySub &sub ) noexcept {
    printf( "on_unsub src_fd=%u %.*s\n", sub.src_fd, (int) sub.subject_len,
            sub.subject );
  }
  /* shutdown before close */
  virtual void process_shutdown( void ) noexcept {
    if ( this->h != 0 ) {
      this->unsubscribe();
      this->h = 0;
    }
    this->sub_route.remove_route_notify( *this );
  }
  static const uint64_t PUB_TIMER = 1;
#ifdef EV_NET_DBG
  static const uint64_t DBG_TIMER = 2;
#endif
  /* start a timer at per_sec interval */
  void start_timer( void ) {
    if ( this->active_ping ) {
      if ( this->per_sec >= 1000 ) {
        this->poll.timer.add_timer_nanos( this->fd, this->ns_ival,
                                          PUB_TIMER, 0 );
      }
      else {
        uint32_t us_ival = this->ns_ival / 1000;
        this->poll.timer.add_timer_micros( this->fd, us_ival, PUB_TIMER, 0 );
      }
    }
#ifdef EV_NET_DBG
    this->poll.timer.add_timer_seconds( this->fd, 1, DBG_TIMER, 0 );
#endif
  }
  /* a timer expires every ns_ival, send messages */
  virtual bool timer_expire( uint64_t /*tid*/, uint64_t ) noexcept {
#ifdef EV_NET_DBG
    if ( tid == DBG_TIMER ) {
      for ( EvSocket *s = this->poll.active_list.hd; s != NULL;
            s = (EvSocket *) s->next ) {
        s->print_dbg();
      }
      return true;
    }
#endif
    uint64_t now = this->poll.timer.queue->epoch;
    if ( this->last_time == 0 ) {
      this->last_time = now;
      return true;
    }
    for ( ; this->last_time < now;
          this->last_time += (uint64_t) this->ns_ival ) {
      uint64_t t = kv_current_monotonic_time_ns();
      /*uint64_t t = kv_get_rdtsc();*/
      EvPublish p( this->pub, this->plen, this->ibx, this->ilen, &t,
                   sizeof( t ), this->sub_route, this->fd, this->ph,
                   MD_UINT, 'u' );
      if ( ! this->sub_route.forward_msg( p, NULL, 0, NULL ) ) {
        /* back pressure */
        break;
      }
      if ( this->per_sec < 100 )
        break;
    }
    if ( this->pub_left > 0 ) {
      if ( --this->pub_left == 0 )
        this->poll.quit++;
    }
    return true;
  }
};

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvPoll        poll;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * su = get_arg( argc, argv, 1, "-s", "PONG" ),
             * pu = get_arg( argc, argv, 1, "-p", "PING" ),
             * xx = get_arg( argc, argv, 1, "-x", "1" ),
             * cn = get_arg( argc, argv, 1, "-n", "0" ),
             * ib = get_arg( argc, argv, 0, "-i", 0 ),
             * _1 = get_arg( argc, argv, 0, "-1", 0 ),
             * re = get_arg( argc, argv, 0, "-r", 0 ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 ),
             * inbox = NULL;
  char     inbox_buf[ 24 ];
  uint32_t per_sec = atoi( xx ),
           pub_cnt = atoi( cn );
  bool     active_ping = ( re == NULL ),
           round_trip  = ( _1 == NULL );

  if ( he != NULL ) {
    printf( "%s"
      " [-m map] [-s sub] [-p pub] [-x rate] [-n cnt] [-i] [-r] [-k] [-b]\n"
      "  map  = kv shm map name      (" KV_DEFAULT_SHM ")\n"
      "  sub  = subject to subscribe (PONG)\n"
      "  pub  = subject to publish   (PING)\n"
      "  rate = publish rate per sec (1)\n"
      "  cnt  = count of publish     (inf)\n"
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

  poll.init( 5, false );
  PingTest shm( poll, su, pu, inbox, active_ping, round_trip, per_sec, pub_cnt);
  if ( shm.open( mn, 0 /* db */ ) != 0 )
    return 1;
  if ( poll.init_shm( shm ) != 0 || shm.init_poll() != 0 )
    return 1;
  shm.subscribe();
  printf( "listening on subject %s %x publish %s %x\n", shm.sub, shm.h,
          shm.pub, shm.ph );
  sighndl.install();
  shm.start_timer();
  if ( bu != NULL ) {
    poll.pubsub->idle_push( EV_BUSY_POLL );
  }
  if ( no != NULL ) {
    poll.pubsub->flags &= ~KV_DO_NOTIFY;
  }
  while ( poll.quit < 5 ) {
    int idle = poll.dispatch(); /* true if idle, false if busy */
    poll.wait( idle == EvPoll::DISPATCH_IDLE ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  shm.close();
  if ( shm.count > 0 ) {
    printf( "count %lu avg %lu nanos\n", shm.count, shm.sum / shm.count );
  }

  return 0;
}

