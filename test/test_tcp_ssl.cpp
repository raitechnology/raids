#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/ev_tcp_ssl.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;
using namespace ds;

struct TcpListen : public EvTcpListen {
  SSL_Context * ctx;

  TcpListen( EvPoll &p ) noexcept;

  virtual EvSocket *accept( void ) noexcept;
};

struct TcpConn : public SSL_Connection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  TcpConn( EvPoll &p,  uint8_t st ) : SSL_Connection( p, st ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct TcpPing : public SSL_Connection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  TcpPing( EvPoll &p ) : SSL_Connection( p, 0 ) {}
  void send_ping( void ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

TcpListen::TcpListen( EvPoll &p ) noexcept
         : EvTcpListen( p, "tcp_listen", "ssl_conn" ), ctx( NULL ) {}

EvSocket *
TcpListen::accept( void ) noexcept
{
  TcpConn *c = this->poll.get_free_list<TcpConn>( this->accept_sock_type );
  if ( c == NULL )
    return NULL;
  if ( this->accept2( *c, "ssl_accept" ) ) {
    printf( "accept %.*s\n", (int) c->get_peer_address_strlen(),
            c->peer_address.buf );
    c->init_ssl_accept( this->ctx );
    return c;
  }
  return NULL;
}

void
TcpConn::process( void ) noexcept
{
  if ( this->off < this->len ) {
    size_t buflen = this->len - this->off;
    printf( "%.*s", (int) buflen, &this->recv[ this->off ] );
    if ( ::memmem( &this->recv[ this->off ], buflen, "\r\n\r\n", 4 ) ) {
      static const char hello[] =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 40\r\n"
        "Content-Type: text/html\r\n"
        "\r\n" /* text must be 40 chars */
        "<html><body> hello world</body></html>\r\n";
      this->append( hello, sizeof( hello ) - 1 );
    }

    this->off = this->len;
    this->msgs_recv++;
  }
  this->pop( EV_PROCESS );
  this->push_write();
}

void
TcpConn::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->SSL_Connection::release_ssl();
  this->EvConnection::release_buffers();
}

void
TcpConn::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
TcpConn::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvSocket::process_close();
}

void
TcpPing::process( void ) noexcept
{
  size_t buflen = this->len - this->off;
  printf( "%.*s", (int) buflen, &this->recv[ this->off ] );
  this->off = off;
  this->msgs_recv++;
  this->pop( EV_PROCESS );
}

void
TcpPing::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
TcpPing::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->SSL_Connection::release_ssl();
  this->EvConnection::release_buffers();
}

void
TcpPing::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
  this->EvSocket::process_close();
}

void
TcpPing::send_ping( void ) noexcept
{
  static const char get[] = "GET / HTTP/1.1\r\n\r\n";
  this->append( get, sizeof( get ) - 1 );
  this->msgs_sent++;
  this->idle_push( EV_WRITE );
}

bool
TcpPing::timer_expire( uint64_t, uint64_t ) noexcept
{
  this->send_ping();
  return true;
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f,
         const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 ) {
      if ( b == 0 || argv[ i + b ][ 0 ] != '-' )
        return argv[ i + b ];
      return def;
    }
  }
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{ 
  SignalHandler sighndl;
  EvPoll        poll;
  SSL_Context * ctx = NULL;
  TcpListen     test( poll );
  TcpPing ping( poll );
  int idle_count = 0; 

  if ( get_arg( argc, argv, 0, "-h", NULL ) != NULL ) {
    printf( "%s [-c addr] [-f cert.pem] [-k key.pem] [-a ca.crt] [-d /etc/ssl]\n"
            "    -n           : no auth or encryption\n"
            "    -c address   : connect client to address\n"
            "    -f cert.pem  : certificate pem file to use\n"
            "    -k key.pem   : key pem file\n"
            "    -a ca.crt    : certificate authority pem file\n"
            "    -d /dir      : local cache of certificates\n", argv[ 0 ] );
    return 1;
  }

  bool is_client = ( get_arg( argc, argv, 0, "-c", NULL ) != NULL );
  if ( get_arg( argc, argv, 0, "-n", NULL ) == NULL ) {
    const char * crt_file = get_arg( argc, argv, 1, "-f", NULL ),
               * key_file = get_arg( argc, argv, 1, "-k", NULL ),
               * ca_file  = get_arg( argc, argv, 1, "-a", NULL ),
               * ca_dir   = get_arg( argc, argv, 1, "-d", NULL );
    ctx = new ( ::malloc( sizeof( SSL_Context ) ) ) SSL_Context();
    SSL_Config cfg( crt_file, key_file, ca_file, ca_dir, is_client, false );
    if ( ! ctx->init_config( cfg ) )
      return 1;
  }
  poll.init( 5, false );
  if ( is_client ) {
    const char * host = get_arg( argc, argv, 1, "-c", NULL );
    if ( EvTcpConnection::connect( ping, host, 9000,
                              DEFAULT_TCP_CONNECT_OPTS | OPT_CONNECT_NB ) != 0 )
      return 1;
    ping.init_ssl_connect( ctx );
    ping.send_ping();
    poll.timer.add_timer_seconds( ping.fd, 1, 1, 1 );
  }
  else {
    test.ctx = ctx;
    if ( test.listen( NULL, 9000, DEFAULT_TCP_LISTEN_OPTS ) != 0 )
      return 1;
  }
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */ 
    poll.wait( idle_count > 2 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  return 0;
}

