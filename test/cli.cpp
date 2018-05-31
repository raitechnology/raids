#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/ev_client.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;
using namespace kv;

struct MyClient;
struct StdinCallback : public EvCallback {
  MyClient &me;
  EvClient &client;
  StdinCallback( MyClient &m,  EvClient &cl ) : me( m ), client( cl ) {}
  virtual void onMsg( RedisMsg &msg );
  virtual void onErr( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void onClose( void );
};

struct ClientCallback : public EvCallback {
  MyClient &me;
  ClientCallback( MyClient &m ) : me( m ) {}
  virtual void onMsg( RedisMsg &msg );
  virtual void onErr( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void onClose( void );
};

struct MyClient {
  EvPoll       & poll;
  ClientCallback clicb;
  StdinCallback  termcb;
  EvClient       client;
  EvTerminal     term;

  MyClient( EvPoll &p ) : poll( p ), clicb( *this ),
     termcb( *this, this->client ), client( p, this->clicb ),
     term( p, this->termcb ) {}
};

void
StdinCallback::onMsg( RedisMsg &msg )
{
  char buf[ 1024 ];
  size_t sz = sizeof( buf );
  if ( msg.to_json( buf, sz ) == REDIS_MSG_OK )
    printf( "executing: %s\n", buf );
  printf( "> " ); fflush( stdout );

  sz = 1024;
  void *tmp = this->client.out.alloc( sz );
  if ( msg.pack( tmp, sz ) == REDIS_MSG_OK ) {
    this->client.append_iov( tmp, sz );
    this->client.push( EV_WRITE );
  }
  else
    printf( "pack failed\n" );
}

static void
print_err( const char *w,  RedisMsgStatus status )
{
  printf( "%s err: %d+%s;%s\n", w, status, redis_msg_status_string( status ),
          redis_msg_status_description( status ) );
}

void
StdinCallback::onErr( char *buf,  size_t buflen,  RedisMsgStatus status )
{
  print_err( "terminal", status );
  printf( "? " ); fflush( stdout );
}

void
StdinCallback::onClose( void )
{
  printf( "bye\n" );
}

void
ClientCallback::onMsg( RedisMsg &msg )
{
  char buf[ 64 * 1024 ];
  size_t sz = sizeof( buf );
  if ( msg.to_json( buf, sz ) == REDIS_MSG_OK )
    printf( "%s\n", buf );
  else
    printf( "msg too large\n" );
  printf( "? " ); fflush( stdout );
}

void
ClientCallback::onErr( char *buf,  size_t buflen,  RedisMsgStatus status )
{
  print_err( "client", status );
  printf( "? " ); fflush( stdout );
}

void
ClientCallback::onClose( void )
{
  printf( "client connection closed\n" );
  this->me.poll.quit = 1;
}

static const char *
get_arg( int argc, char *argv[], int n, const char *f, const char *def )
{ 
  if ( argc > n && argv[ 1 ][ 0 ] != '-' ) /* [2]=port, no flags */
    return argv[ n ];
  for ( int i = 1; i < argc - 1; i++ ) 
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -p port */
      return argv[ i + 1 ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  int        status = 0;
  EvPoll     poll( NULL, 0 );
  MyClient   my( poll );
  poll.init( 5 );
  const char *pt = get_arg( argc, argv, 1, "-p", "8888" );

  if ( my.client.connect( NULL, atoi( pt ) ) != 0 )
    status = 1; /* bad port or network error */
  else {
    printf( "connected: %s\n", pt );

    printf( "? " ); fflush( stdout );
    my.term.start( 0 );
    sighndl.install();
    while ( poll.quit < 5 ) {
      poll.wait( 100 );
      if ( sighndl.signaled )
	poll.quit++;
      poll.dispatch();
    }
  }

  return status;
}

