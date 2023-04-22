#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#else
#include <raikv/win.h>
#endif
#include <raids/ev_service.h>
#include <raids/ev_memcached.h>
#include <raids/ev_http.h>
#include <raikv/mainloop.h>

using namespace rai;
using namespace ds;
using namespace kv;

#define USE_REDIS
//#define REDIS_UNIX
#define USE_MEMCACHED
#define USE_MEMCACHED_UDP
#define USE_HTTP

struct Args : public MainLoopVars {
#ifdef USE_REDIS
  int redis_port;
#endif
#ifdef USE_MEMCACHED
  int memcached_port;
#endif
#ifdef USE_MEMCACHED_UDP
  int memcached_udp_port;
#endif
#ifdef USE_HTTP
  int http_port;
  SSL_Config ssl_cfg;
#endif

  Args() { ::memset( (void *) this, 0, sizeof( *this ) ); }
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  size_t num,  bool (*ini)( void * ) ) :
    MainLoop<Args>( m, args, num, ini ) {}

#ifdef USE_REDIS
  EvRedisListen   * redis_sv;
  bool redis_init( void ) {
    return Listen<EvRedisListen>( 0, this->r.redis_port, this->redis_sv,
                                  this->r.tcp_opts ); }
#endif
#ifdef USE_MEMCACHED
  EvMemcachedListen * memcached_sv;
  bool memcached_init( void ) {
    return Listen<EvMemcachedListen>( 0, this->r.memcached_port,
                                      this->memcached_sv, this->r.tcp_opts ); }
#endif
#ifdef USE_MEMCACHED_UDP
  EvMemcachedUdp  * memcached_udp_sv;
  bool memcached_udp_init( void ) {
    if ( ! Listen<EvMemcachedUdp>( 0, this->r.memcached_udp_port,
                                   this->memcached_udp_sv, this->r.udp_opts ) )
      return false;
    if ( this->memcached_udp_sv != NULL )
      this->memcached_udp_sv->init();
    return true;
  }
#endif
#ifdef USE_HTTP
  EvHttpListen    * http_sv;
  bool http_init( void ) {
   if ( ! Listen<EvHttpListen>( 0, this->r.http_port, this->http_sv,
                                this->r.tcp_opts ) )
     return false;
   if ( this->r.ssl_cfg.cert_file != NULL )
     return this->http_sv->ssl_ctx.init_config( this->r.ssl_cfg );
   return true;
 }
#endif
  bool init( void ) noexcept;

  static bool initialize( void *me ) noexcept {
    return ((Loop *) me)->init();
  }
};

bool
Loop::init( void ) noexcept
{
  int cnt = 0;
#ifdef USE_REDIS
  if ( this->thr_num == 0 )
    printf( "redis:                %d\n", this->r.redis_port );
  cnt += this->redis_init();
#endif
#ifdef USE_MEMCACHED
  if ( this->thr_num == 0 )
    printf( "memcached:            %d\n", this->r.memcached_port );
  cnt += this->memcached_init();
#endif
#ifdef USE_MEMCACHED_UDP
  if ( this->thr_num == 0 )
    printf( "memcached_udp:        %d\n", this->r.memcached_udp_port );
  cnt += this->memcached_udp_init();
#endif
#ifdef USE_HTTP
  if ( this->thr_num == 0 )
    printf( "http:                 %d\n", this->r.http_port );
  cnt += this->http_init();
#endif
  if ( this->thr_num == 0 )
    fflush( stdout );
  return cnt > 0;
}

int
main( int argc,  const char *argv[] )
{
  EvShm shm( "ds_server" );
  Args  r;
#ifdef USE_REDIS
  r.add_desc( "  -p redis = listen redis port     (6379)" );
#endif
#ifdef USE_MEMCACHED
  r.add_desc( "  -d memcd = listen memcached port (11211)" );
#endif
#ifdef USE_MEMCACHED_UDP
  r.add_desc( "  -u memcd_udp = memcached udp port (11211)" );
#endif
#ifdef USE_HTTP
  r.add_desc( "  -w web   = listen websocket      (48080)" );
  r.add_desc( "  -C cert  = certificate pem file" );
  r.add_desc( "  -K key   = key pem file" );
  r.add_desc( "  -A ca    = certificate authority" );
  r.add_desc( "  -L dir   = local dir of certificates" );
#endif
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  printf( "ds_version:           " kv_stringify( DS_VER ) "\n" );
  shm.print();

#ifdef USE_REDIS
  r.redis_port         = r.parse_port( argc, argv, "-p", "6379" );
#endif
#ifdef USE_MEMCACHED
  r.memcached_port     = r.parse_port( argc, argv, "-d", "11211" );
#endif
#ifdef USE_MEMCACHED_UDP
  r.memcached_udp_port = r.parse_port( argc, argv, "-u", "11211" );
#endif
#ifdef USE_HTTP
  r.http_port            = r.parse_port( argc, argv, "-w", "48080" );
  r.ssl_cfg.cert_file    = r.get_arg( argc, argv, 1, "-C", NULL );
  r.ssl_cfg.key_file     = r.get_arg( argc, argv, 1, "-K", NULL );
  r.ssl_cfg.ca_cert_file = r.get_arg( argc, argv, 1, "-A", NULL );
  r.ssl_cfg.ca_cert_dir  = r.get_arg( argc, argv, 1, "-L", NULL );
#endif
  Runner<Args, Loop> runner( r, shm, Loop::initialize );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
