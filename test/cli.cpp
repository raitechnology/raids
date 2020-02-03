#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raids/ev_tcp.h>
#include <raids/ev_unix.h>
#include <raids/ev_memcached.h>
#include <raikv/util.h>

using namespace rai;
using namespace ds;
using namespace kv;

struct MyClient;
struct TermCallback : public EvCallback { /* from terminal */
  MyClient &me;
  TermCallback( MyClient &m ) : me( m ) {}
  void on_msg( RedisMsg &msg );
  void on_err( char *buf,  size_t buflen,  int status );
  virtual bool on_data( char *buf,  size_t &buflen );
  virtual void on_close( void );
};

struct ClientCallback : public EvCallback { /* from socket */
  MyClient &me;
  ClientCallback( MyClient &m ) : me( m ) {}
  void on_msg( RedisMsg &msg );
  void on_err( char *buf,  size_t buflen,  int status );
  virtual bool on_data( char *buf,  size_t &buflen );
  virtual void on_close( void );
};

struct MyClient {
  EvPoll       & poll;
  ClientCallback clicb;   /* cb when network socket reads msg */
  TermCallback   termcb;  /* cb when terminal reads msg */
  EvTcpClient    tclient; /* tcp sock connection */
  EvUnixClient   xclient; /* unix sock connection */
  EvShmClient    shm;     /* shm fake connection, executes directly, no wait */
  EvUdpClient    uclient; /* udp sock */
  EvTerminal     term;    /* read from keyboard */
  EvClient     * client;  /* one of the previous clients (tcp, unix, shm) */
  uint64_t       msg_sent,
                 msg_recv;
  RedisMsg       msg;
  MemcachedMsg   mc_msg;
  MemcachedRes   mc_res;
  kv::WorkAllocT< 64 * 1024 > wrk;
  bool is_mc;

  MyClient( EvPoll &p ) : poll( p ), clicb( *this ), termcb( *this ),
     tclient( p, this->clicb ),
     xclient( p, this->clicb ),
     shm( p, this->clicb ),
     uclient( p, this->clicb ),
     term( p, this->termcb ),
     client( 0 ), msg_sent( 0 ), msg_recv( 0 ), is_mc( false ) {}

  int tcp_connect( const char *h,  int p ) {
    int status;
    if ( (status = this->tclient.connect( h, p )) == 0 )
      this->client = &this->tclient;
    return status;
  }
  int udp_connect( const char *h,  int p ) {
    int status;
    if ( (status = this->uclient.connect( h, p )) == 0 )
      this->client = &this->uclient;
    return status;
  }
  int unix_connect( const char *path ) {
    int status;
    if ( (status = this->xclient.connect( path )) == 0 )
      this->client = &this->xclient;
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
  void print_err( const char *w,  int status ) {
    if ( this->is_mc ) {
      this->term.printf( "%s err: %d+%s;%s\n", w, status,
              memcached_status_string( (MemcachedStatus) status ),
              memcached_status_description( (MemcachedStatus) status ) );
    }
    else {
      this->term.printf( "%s err: %d+%s;%s\n", w, status,
              redis_msg_status_string( (RedisMsgStatus) status ),
              redis_msg_status_description( (RedisMsgStatus) status ) );
    }
  }
  int process_msg( char *buf,  size_t &buflen );
  int process_mc_msg( char *buf,  size_t &buflen );
  int process_mc_res( char *buf,  size_t &buflen );
};

int
MyClient::process_msg( char *buf,  size_t &buflen )
{
  this->wrk.reset();
  switch ( buf[ 0 ] ) {
    default:
    case RedisMsg::SIMPLE_STRING: /* + */
    case RedisMsg::ERROR_STRING:  /* - */
    case RedisMsg::INTEGER_VALUE: /* : */
    case RedisMsg::BULK_STRING:   /* $ */
    case RedisMsg::BULK_ARRAY:    /* * */
      return this->msg.unpack( buf, buflen, this->wrk );
    case ' ':
    case '\t':
    case '\r':
    case '\n':
      buflen = 1;
      return -1;
    case '"':
    case '\'':
    case '[':
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
      return this->msg.unpack_json( buf, buflen, this->wrk );
  }
}

int
MyClient::process_mc_msg( char *buf,  size_t &buflen )
{
  MemcachedStatus r;
  this->wrk.reset();
  size_t len = buflen;
  char * b = (char *) this->wrk.alloc( len );
  if ( b == NULL ) {
    this->term.printf( "msg too large\n" );
    return MEMCACHED_OK;
  }
  ::memcpy( b, buf, len );
  r = this->mc_msg.unpack( buf, len, this->wrk );
  if ( len > buflen )
    len = buflen;
  buflen = len;
  return r;
}

int
MyClient::process_mc_res( char *buf,  size_t &buflen )
{
  this->wrk.reset();
  return this->mc_res.unpack( buf, buflen, this->wrk );
}

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
  char * b;
  size_t sz  = msg.to_almost_json_size(),
         sz2 = msg.pack_size();

  b = (char *) this->me.wrk.alloc( sz > sz2 ? sz : sz2 );
  if ( b == NULL )
    this->me.term.printf( "msg too large\n" );
  else {
    msg.to_almost_json( b );
    /*this->me.term.printf( "executing: %.*s\n", (int) sz, b );*/
    msg.pack( b );
    this->me.msg_sent++;
    this->me.client->send_data( b, sz2 );
  }
}

bool
TermCallback::on_data( char *buf,  size_t &buflen )
{
  int status;
  if ( this->me.is_mc ) {
    if ( buflen == 3 && buf[ 0 ] == 'q' )
      this->me.poll.quit = 1;
    else {
      status = this->me.process_mc_msg( buf, buflen );
      if ( status != MEMCACHED_OK ) {
        if ( status == MEMCACHED_MSG_PARTIAL )
          return false;
        this->on_err( buf, buflen, status );
      }
      else {
        char * b = (char *) this->me.wrk.alloc( buflen );
        if ( b == NULL )
          this->me.term.printf( "msg too large\n" );
        else {
          ::memcpy( b, buf, buflen );
          this->me.msg_sent++;
          this->me.client->send_data( b, buflen );
        }
      }
    }
  }
  else {
    status = this->me.process_msg( buf, buflen );
    if ( status != REDIS_MSG_OK ) {
      if ( status < 0 )
        return true;
      if ( status == REDIS_MSG_PARTIAL )
        return false;
      this->on_err( buf, buflen, status );
    }
    else {
      this->on_msg( this->me.msg );
    }
  }
  return true;
}

void
TermCallback::on_err( char *,  size_t,  int status )
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
  /* if bulk string with newline termination, just print it */
  if ( msg.type == RedisMsg::BULK_STRING &&
       msg.len > 0 &&
       msg.strval[ msg.len - 1 ] == '\n' ) {
    this->me.term.printf( "%.*s\n", (int) msg.len, msg.strval );
  }
  else {
    char buf[ 1024 ], *b = buf;
    size_t sz;

    sz = msg.to_almost_json_size();
    if ( sz > sizeof( buf ) ) {
      b = (char *) this->me.wrk.alloc( sz );
      if ( b == NULL )
        this->me.term.printf( "msg too large\n" );
    }
    if ( b != NULL ) {
      msg.to_almost_json( b );
      this->me.term.printf( "%.*s\n", (int) sz, b );
    }
  }
  this->me.msg_recv++;
}

bool
ClientCallback::on_data( char *buf,  size_t &buflen )
{
  int status;
  if ( this->me.is_mc ) {
    status = this->me.process_mc_res( buf, buflen );
    if ( status != MEMCACHED_OK ) {
      if ( status == MEMCACHED_MSG_PARTIAL )
        return false;
      this->on_err( buf, buflen, status );
    }
    else {
      this->me.msg_recv++;
      this->me.term.printf( "%.*s", (int) buflen, buf );
    }
  }
  else {
    status = this->me.process_msg( buf, buflen );
    if ( status != REDIS_MSG_OK ) {
      if ( status < 0 )
        return true;
      if ( status == REDIS_MSG_PARTIAL )
        return false;
      this->on_err( buf, buflen, status );
    }
    else {
      this->on_msg( this->me.msg );
    }
  }
  return true;
}

void
ClientCallback::on_err( char *,  size_t,  int status )
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
             * pt = get_arg( argc, argv, 1, "-p", "7379" ),
             * pa = get_arg( argc, argv, 1, "-a", NULL ),
             * ma = get_arg( argc, argv, 1, "-m", NULL ),
             * fi = get_arg( argc, argv, 1, "-f", NULL ),
             * ud = get_arg( argc, argv, 0, "-u", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );
  if ( he != NULL ) {
    printf( "%s [-x host] [-p port] [-a /tmp/sock] [-m map] [-u]\n"
            "   -x host : hostname for TCP or UDP connect\n"
            "   -p port : port for TCP or UDP connect\n"
            "   -a path : path for unix connect\n"
            "   -m map  : map name to attach for shm client\n"
            "   -f file : file with commands to execute after connecting\n"
            "   -u      : use UDP instead of TCP (only memcached)\n"
            "client will use redis RESP proto unless UDP is true\n"
            "or port contains 211, in these cases use memcached ASCII proto\n",
            argv[ 0 ] );
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
    if ( my.unix_connect( pa ) != 0 ) {
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
  /* by default, connects to :7369 */
  if ( ! is_connected && status == 0 ) {
    if ( ud != NULL ) {
      if ( my.udp_connect( ho, atoi( pt ) ) != 0 ) {
        fprintf( stderr, "unable to connect udp socket to %s\n", pt );
        status = 2; /* bad port or network error */
      }
      else {
        my.is_mc = true;
        is_connected = true;
      }
    }
    else if ( my.tcp_connect( ho, atoi( pt ) ) != 0 ) {
      fprintf( stderr, "unable to connect tcp socket to %s\n", pt );
      status = 2; /* bad port or network error */
    }
    else {
      my.is_mc = ( ::strstr( pt, "211" ) != NULL );
      printf( "connected: %s (%s)\n", pt, ! my.is_mc ? "redis" : "memcached" );
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

