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
#include <raimd/md_msg.h>

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

struct SubTest : public EvShmSvc {
  RoutePDB   & sub_route;
  const char * sub,
             * ibx;
  size_t       len,
               ilen;
  uint32_t     h,
               ih;
  MDMsgMem     mem;
  MDDict     * dict;

  SubTest( EvPoll &poll,  const char *s,  const char *i )
    : EvShmSvc( poll ), sub_route( poll.sub_route ), sub( s ), ibx( i ),
      len( ::strlen( s ) ), ilen( i ? ::strlen( i ) : 0 ),
      h( 0 ), ih( 0 ), dict( 0 ) {
  }
  /* start subcriptions for sub or inbox */
  void subscribe( void ) {
    uint32_t rcnt;
    this->h  = kv_crc_c( this->sub, this->len, 0 );
    this->ih = kv_crc_c( this->ibx, this->ilen, 0 );
    /* if using inbox for reply */
    if ( this->ilen > 0 ) {
      rcnt = this->sub_route.add_sub_route( this->ih, this->fd );
      this->sub_route.notify_sub( this->ih, this->ibx, this->ilen,
                                  this->fd, rcnt, 'K' );
    }
    rcnt = this->sub_route.add_sub_route( this->h, this->fd );
    this->sub_route.notify_sub( this->h, this->sub, this->len,
                                this->fd, rcnt, 'K', this->ibx, this->ilen );
  }
  /* remove subcriptions for sub or inbox */
  void unsubscribe( void ) {
    uint32_t rcnt;
    if ( this->ilen > 0 ) {
      rcnt = this->sub_route.del_sub_route( this->ih, this->fd );
      this->sub_route.notify_unsub( this->ih, this->ibx, this->ilen,
                                    this->fd, rcnt, 'K' );
    }
    rcnt = this->sub_route.del_sub_route( this->h, this->fd );
    this->sub_route.notify_unsub( this->h, this->sub, this->len,
                                  this->fd, rcnt, 'K' );
  }
  /* recv an incoming message from a subscription above, sent from a peer or
   * myself if subscribing to the same subject as publishing */
  virtual bool on_msg( EvPublish &p ) noexcept {
    MDMsg * m = MDMsg::unpack( (void *) p.msg, 0, p.msg_len, p.msg_enc,
                               this->dict, &this->mem );
    if ( m != NULL ) {
      MDOutput mout;
      printf( "### %.*s (pub_type=%c)", (int) p.subject_len, p.subject,
              p.pub_type > ' ' && p.pub_type < 127 ? p.pub_type : '_' );
      if ( p.reply_len != 0 )
        printf( " reply %.*s", (int) p.reply_len, (char *) p.reply );
      printf( "\n" );
      m->print( &mout );
    }
    this->mem.reuse();
    return true;
  }
  /* shutdown before close */
  virtual void process_shutdown( void ) noexcept {
    if ( this->h != 0 ) {
      this->unsubscribe();
      this->h = 0;
    }
  }
};

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvPoll        poll;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * su = get_arg( argc, argv, 1, "-s", "PING" ),
             * ib = get_arg( argc, argv, 0, "-i", 0 ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * no = get_arg( argc, argv, 0, "-k", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 ),
             * inbox = NULL;
  char inbox_buf[ 24 ];

  if ( he != NULL ) {
    printf( "%s"
      " [-m map] [-s sub] [-i] [-k] [-b]\n"
      "  map  = kv shm map name      (" KV_DEFAULT_SHM ")\n"
      "  sub  = subject to subscribe (PING)\n"
      "  -i   = use inbox reply\n"
      "  -k   = don't use signal USR1 pub notification\n"
      "  -b   = busy poll\n", argv[ 0 ] );
    return 0;
  }
  if ( ib != NULL ) {
    snprintf( inbox_buf, sizeof( inbox_buf ), "_IBX.%d", getpid() );
    inbox = inbox_buf;
  }

  printf( "listening on subject %s\n", su );
  poll.init( 5, false );
  SubTest shm( poll, su, inbox );
  if ( shm.open( mn, 0 /* db */ ) != 0 )
    return 1;
  if ( poll.init_shm( shm ) != 0 || shm.init_poll() != 0 )
    return 1;
  shm.subscribe();
  sighndl.install();
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

