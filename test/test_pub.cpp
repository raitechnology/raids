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
#include <raimd/tib_msg.h>

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

struct PubTest : public EvShmSvc, public KvSubNotifyList {
  const char * sub;
  size_t       len;
  uint32_t     h,
               per_sec,
               ns_ival,
               count;
  uint64_t     last_time;
  MDMsgMem     mem;
  MDDict     * dict;

  PubTest( EvPoll &poll,  const char *s,  uint32_t ps_rate )
    : EvShmSvc( poll ), sub( s ), len( ::strlen( s ) ),
      h( 0 ), per_sec( ps_rate ), ns_ival( 1e9 / ps_rate ),
      count( 0 ), last_time( 0 ), dict( 0 ) {
  }
  /* shutdown before close */
  virtual void process_shutdown( void ) noexcept {
    if ( this->KvSubNotifyList::in_list ) {
      this->KvSubNotifyList::in_list = false;
      this->poll.pubsub->sub_notifyq.pop( this );
    }
  }
 /* start a timer at per_sec interval */
  void start_timer( void ) {
    this->h = kv_crc_c( this->sub, this->len, 0 );
    this->poll.pubsub->sub_notifyq.push_tl( this );
    this->KvSubNotifyList::in_list = true;
    if ( this->per_sec >= 1000 ) {
      this->poll.timer_queue->add_timer_units( this->fd, this->ns_ival,
                                               IVAL_NANOS, 1, 0 );
    }
    else {
      uint32_t us_ival = this->ns_ival / 1000;
      this->poll.timer_queue->add_timer_units( this->fd, us_ival,
                                               IVAL_MICROS, 1, 0 );
    }
  }
  virtual void on_sub( KvSubMsg &submsg ) noexcept {
    printf( "on_sub %s %.*s", submsg.msg_type_string(),
            (int) submsg.sublen, submsg.subject() );
    if ( submsg.replylen != 0 )
      printf( " reply %.*s", (int) submsg.replylen , submsg.reply() );
    printf( "\n" );
    if ( submsg.msg_type == KV_MSG_SUB && submsg.replylen > 0 &&
         submsg.sublen == this->len &&
         ::memcmp( submsg.subject(), this->sub, this->len ) == 0 ) {
      char buf[ 1600 ];
      TibMsgWriter tibmsg( buf, sizeof( buf ) );
      tibmsg.append_string( "hello", 6, "world", 6 );
      tibmsg.append_uint( "count", 6, this->count );
      tibmsg.append_uint( "time", 5, this->last_time );
      size_t sz = tibmsg.update_hdr();

      EvPublish p( submsg.reply(), submsg.replylen, NULL, 0, buf,
                   sz, this->fd, this->h,
                   NULL, 0, (uint8_t) RAIMSG_TYPE_ID, 'i' );
      this->poll.forward_msg( p, NULL, 0, NULL );
    }
  }
  /* a timer expires every ns_ival, send messages */
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept {
    uint64_t now = this->poll.timer_queue->epoch;
    if ( this->last_time == 0 ) {
      this->last_time = now;
      return true;
    }
    for ( ; this->last_time < now;
          this->last_time += (uint64_t) this->ns_ival ) {
      char buf[ 1600 ];
      TibMsgWriter tibmsg( buf, sizeof( buf ) );
      tibmsg.append_uint( "count", 6, this->count++ );
      tibmsg.append_uint( "time", 5, this->last_time + this->ns_ival );
      size_t sz = tibmsg.update_hdr();

      EvPublish p( this->sub, this->len, NULL, 0, buf,
                   sz, this->fd, this->h,
                   NULL, 0, (uint8_t) RAIMSG_TYPE_ID, 'u' );
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

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * su = get_arg( argc, argv, 1, "-s", "PING" ),
             * xx = get_arg( argc, argv, 1, "-x", "1" ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 );
  uint32_t per_sec = atoi( xx );

  if ( he != NULL ) {
    printf( "%s"
      " [-m map] [-s sub] [-x rate] [-k] [-b]\n"
      "  map  = kv shm map name      (" KV_DEFAULT_SHM ")\n"
      "  sub  = subject to subscribe (PING)\n"
      "  rate = publish rate per sec (1)\n"
      "  -k   = don't use signal USR1 pub notification\n"
      "  -b   = busy poll\n", argv[ 0 ] );
    return 0;
  }

  printf( "publish on subject %s\n", su );
  poll.init( 5, false );
  PubTest shm( poll, su, per_sec );
  if ( shm.open( mn, 0 /* db */ ) != 0 )
    return 1;
  if ( poll.init_shm( shm ) != 0 || shm.init_poll() != 0 )
    return 1;
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

  return 0;
}

