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
  void *tmp = this->me.client->tmp.alloc( msg.pack_size() );
  if ( tmp != NULL ) {
    this->me.client->append_iov( tmp, msg.pack( tmp ) );
    this->me.client->push( EV_WRITE );
  }
  else
    printf( "pack allocation failed\n" );
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
  char buf[ 64 * 1024 ], *b = buf;
  size_t sz = sizeof( buf ), n;
  int nb;
  RedisMsgStatus status;
  for (;;) {
    if ( (status = msg.to_json( b, sz )) == REDIS_MSG_OK ) {
      fflush( stdout );
      for ( n = 0; n < sz; ) {
        nb = write( 1, &b[ n ], sz - n );
        if ( nb > 0 )
          n += nb;
      }
      break;
    }
    else {
      if ( status != REDIS_MSG_PARTIAL ) {
        printf( "msg error: %d\n", status );
        break;
      }
      if ( b == buf )
        b = NULL;
      char *tmp = (char *) ::realloc( b, sz * 2 );
      if ( tmp == NULL ) {
        printf( "msg too large" );
        break;
      }
      b = tmp;
      sz *= 2;
    }
  }
  if ( b != buf && b != NULL )
    ::free( b );
  printf( "\n? " ); fflush( stdout );
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

