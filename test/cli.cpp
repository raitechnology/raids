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
struct TermCallback : public EvCallback { /* from terminal */
  MyClient &me;
  TermCallback( MyClient &m ) : me( m ) {}
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void on_close( void );
};

struct ClientCallback : public EvCallback { /* from socket */
  MyClient &me;
  ClientCallback( MyClient &m ) : me( m ) {}
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void on_close( void );
};

struct MyClient {
  EvPoll       & poll;
  ClientCallback clicb;   /* cb when network socket reads msg */
  TermCallback   termcb;  /* cb when terminal reads msg */
  EvTcpClient    tclient; /* tcp sock connection */
  EvUnixClient   uclient; /* unix sock connection */
  EvShmClient    shm;     /* shm fake connection, executes directly, no wait */
  EvClient     * client;  /* one of the previous clients (tcp, unix, shm) */
  EvTerminal     term;    /* read from keyboard */
  uint64_t       msg_sent,
                 msg_recv;

  MyClient( EvPoll &p ) : poll( p ), clicb( *this ),
     termcb( *this ), tclient( p, this->clicb ),
     uclient( p, this->clicb ), shm( p, this->clicb ),
     client( 0 ), term( p, this->termcb ), msg_sent( 0 ), msg_recv( 0 ) {}

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
  int shm_open( const char *map ) {
    int status;
    if ( (status = this->shm.open( map )) == 0 ) {
      if ( this->poll.init_shm( this->shm ) != 0 ) {
        fprintf( stderr, "unable to init shm\n" );
        status = 3;
      }
      else if ( (status = this->shm.init_exec()) == 0 )
        this->client = &this->shm;
    }
    return status;
  }
  void print_err( const char *w,  RedisMsgStatus status ) {
    this->term.printf( "%s err: %d+%s;%s\n", w, status,
            redis_msg_status_string( status ),
            redis_msg_status_description( status ) );
  }
};

void
TermCallback::on_msg( RedisMsg &msg )
{
  if ( msg.type == RedisMsg::BULK_ARRAY &&
       msg.len == 1 &&
       msg.match_arg( 0, "q", 1, NULL ) == 1 ) {
    this->me.poll.quit = 1;
    /*this->on_close();*/
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

  this->me.msg_sent++;
  this->me.client->send_msg( msg );
}

void
TermCallback::on_err( char *,  size_t,  RedisMsgStatus status )
{
  this->me.print_err( "terminal", status );
}

void
TermCallback::on_close( void )
{
  this->me.term.printf( "bye\n" );
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
  this->me.msg_recv++;
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
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{ 
  for ( int i = 1; i < argc - b; i++ ) 
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -p port */
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  FILE       * input_fp = NULL;
  int          status = 0;
  bool         is_connected = false;
  EvPoll       poll;
  MyClient     my( poll );

  const char * ho = get_arg( argc, argv, 1, "-x", NULL ),
             * pt = get_arg( argc, argv, 1, "-p", "8888" ),
             * pa = get_arg( argc, argv, 1, "-a", NULL ),
             * ma = get_arg( argc, argv, 1, "-m", NULL ),
             * fi = get_arg( argc, argv, 1, "-f", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );
  if ( he != NULL ) {
    printf( "%s [-x host] [-p port] [-a /tmp/sock] [-m map]\n", argv[ 0 ] );
    return 0;
  }

  if ( fi != NULL ) {
    input_fp = ::fopen( fi, "r" );
    if ( input_fp == NULL ) {
      perror( fi );
      return 1;
    }
  }
  poll.init( 5, false/*, false*/ );
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
  else if ( ma != NULL ) {
    if ( my.shm_open( ma ) != 0 ) {
      fprintf( stderr, "unable to shm open map %s\n", ma );
      status = 3; /* bad map */
    }
    else {
      printf( "opened: %s\n", ma );
      is_connected = true;
    }
  }
  /* by default, connects to :8888 */
  if ( ! is_connected && status == 0 ) {
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
    my.term.start();
    sighndl.install();
    for (;;) {
      if ( poll.quit >= 5 )
        break;
      bool idle = poll.dispatch(); /* true if idle, false if busy */
      if ( idle && input_fp != NULL && my.term.line_len == 0 &&
           my.msg_sent == my.msg_recv ) {
        char tmp[ 1024 ];
      get_next_line:;
        if ( fgets( tmp, sizeof( tmp ), input_fp ) == NULL ) {
          ::fclose( input_fp );
          input_fp = NULL;
          poll.quit++;
        }
        else {
          if ( tmp[ 0 ] != '#' )
            my.term.process_line( tmp );
          else
            goto get_next_line;
        }
      }
      poll.wait( idle ? 100 : 0 );
      if ( sighndl.signaled )
        poll.quit++;
    }
    my.term.finish();
  }
  if ( input_fp != NULL )
    ::fclose( input_fp );

  return status;
}

