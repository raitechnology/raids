#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raids/ev_tcp.h>
#include <raids/ev_unix.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;
using namespace kv;

struct MyClient;
struct StdinCallback : public EvCallback {
  MyClient &me;
  StdinCallback( MyClient &m ) : me( m ) {}
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void on_close( void );
};

struct ClientCallback : public EvCallback {
  MyClient &me;
  ClientCallback( MyClient &m ) : me( m ) {}
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void on_close( void );
};

struct MyClient {
  EvPoll       & poll;
  ClientCallback clicb;
  StdinCallback  termcb;
  EvTcpClient    tclient;
  EvUnixClient   uclient;
  EvClient     * client;
  EvTerminal     term;

  MyClient( EvPoll &p ) : poll( p ), clicb( *this ),
     termcb( *this ), tclient( p, this->clicb ),
     uclient( p, this->clicb ), client( 0 ), term( p, this->termcb ) {}

  int connect( const char *h,  int p ) {
    int status;
    if ( (status = this->tclient.connect( h, p )) == 0 )
      this->client = &this->tclient;
    return status;
  }
  int connect( const char *path ) {
    int status;
    if ( (status = this->uclient.connect( path )) == 0 )
      this->client = &this->uclient;
    return status;
  }
  void print_err( const char *w,  RedisMsgStatus status ) {
    this->term.printf( "%s err: %d+%s;%s\n", w, status,
            redis_msg_status_string( status ),
            redis_msg_status_description( status ) );
  }
};

void
StdinCallback::on_msg( RedisMsg &msg )
{
  if ( msg.type == RedisMsg::BULK_ARRAY &&
       msg.len == 1 &&
       msg.match_arg( 0, "q", 1, NULL ) == 1 ) {
    this->on_close();
    return;
  }
  char buf[ 1024 ], *b = buf;
  size_t sz = msg.to_almost_json_size();

  if ( sz > sizeof( buf ) ) {
    b = (char *) ::malloc( sz );
    if ( b == NULL )
      this->me.term.printf( "msg too large\n" );
  }
  if ( b != NULL ) {
    msg.to_almost_json( b );
    this->me.term.printf( "executing: %.*s\n", (int) sz, b );
    if ( b != buf )
      ::free( b );
  }

  sz = 1024;
  void *tmp = this->me.client->tmp.alloc( msg.pack_size() );
  if ( tmp != NULL ) {
    this->me.client->append_iov( tmp, msg.pack( tmp ) );
    this->me.client->idle_push( EV_WRITE );
  }
  else
    this->me.term.printf( "pack allocation failed\n" );
}

void
StdinCallback::on_err( char *,  size_t,  RedisMsgStatus status )
{
  this->me.print_err( "terminal", status );
}

void
StdinCallback::on_close( void )
{
  this->me.term.printf( "bye\n" );
  this->me.poll.quit = 1;
}

void
ClientCallback::on_msg( RedisMsg &msg )
{
  char buf[ 64 * 1024 ], *b = buf;
  size_t sz = msg.to_almost_json_size();

  if ( sz > sizeof( buf ) ) {
    b = (char *) ::malloc( sz );
    if ( b == NULL )
      this->me.term.printf( "msg too large\n" );
  }
  if ( b != NULL ) {
    msg.to_almost_json( b );
    this->me.term.printf( "%.*s\n", (int) sz, b );
    if ( b != buf )
      ::free( b );
  }
}

void
ClientCallback::on_err( char *,  size_t,  RedisMsgStatus status )
{
  this->me.print_err( "client", status );
}

void
ClientCallback::on_close( void )
{
  this->me.term.printf( "client connection closed\n" );
  this->me.poll.quit = 1;
}

static const char *
get_arg( int argc, char *argv[], int n, int b, const char *f, const char *def )
{ 
  /* [1]=host, [2]=port, no flags */
  if ( n > 0 && argc > n && argv[ 1 ][ 0 ] != '-' )
    return argv[ n ];
  for ( int i = 1; i < argc - b; i++ ) 
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -p port */
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  int          status = 0;
  bool         is_connected = false;
  EvPoll       poll( NULL, 0 );
  MyClient     my( poll );

  const char * ho = get_arg( argc, argv, 1, 1, "-x", NULL ),
             * pt = get_arg( argc, argv, 2, 1, "-p", "8888" ),
             * pa = get_arg( argc, argv, 2, 1, "-a", NULL ),
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );
  if ( he != NULL ) {
    printf( "%s [-x host] [-p port] [-a /tmp/sock]\n", argv[ 0 ] );
    return 0;
  }

  poll.init( 5, false, false );
  if ( pa != NULL ) {
    if ( my.connect( pa ) != 0 ) {
      fprintf( stderr, "unable to connect unix socket to %s\n", pa );
      status = 1; /* bad path */
    }
    else {
      printf( "connected: %s\n", pa );
      is_connected = true;
    }
  }
  if ( ! is_connected ) {
    if ( my.connect( ho, atoi( pt ) ) != 0 ) {
      fprintf( stderr, "unable to connect tcp socket to %s\n", pt );
      status = 2; /* bad port or network error */
    }
    else {
      printf( "connected: %s\n", pt );
      is_connected = true;
    }
  }
  if ( is_connected ) {
//    printf( "? " ); fflush( stdout );
    my.term.start();
    sighndl.install();
    for (;;) {
      if ( poll.quit >= 5 )
        break;
      bool idle = poll.dispatch(); /* true if idle, false if busy */
      poll.wait( idle ? 100 : 0 );
      if ( sighndl.signaled )
        poll.quit++;
    }
    my.term.finish();
  }

  return status;
}

