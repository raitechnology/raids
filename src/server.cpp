#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
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
/*#define USE_NATS*/
/*#define USE_CAPR*/
/*#define USE_RV*/

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
#endif
#ifdef USE_NATS
  int nats_port;
#endif
#ifdef USE_CAPR
  int capr_port;
#endif
#ifdef USE_RV
  int rv_port;
#endif

  Args() { ::memset( (void *) this, 0, sizeof( *this ) ); }
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  int num,  bool (*ini)( void * ) ) :
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
    return Listen<EvHttpListen>( 0, this->r.http_port, this->http_sv,
                                 this->r.tcp_opts ); }
#endif
#ifdef USE_NATS
  EvNatsListen    * nats_sv;
  bool nats_init( void ) {
    return Listen<EvNatsListen>( 0, this->r.nats_port, this->nats_sv,
                                 this->r.tcp_opts ); }
#endif
#ifdef USE_CAPR
  EvCaprListen    * capr_sv;
  bool capr_init( void ) {
    return Listen<EvCaprListen>( 0, this->r.capr_port, this->capr_sv,
                                 this->r.tcp_opts ); }
#endif
#ifdef USE_RV
  EvRvListen      * rv_sv;
  bool rv_init( void ) {
    return Listen<EvRvListen>( 0, this->r.rv_port, this->rv_sv,
                               this->r.tcp_opts ); }
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
#ifdef USE_NATS
  if ( this->thr_num == 0 )
    printf( "nats:                 %d\n", this->r.nats_port );
  cnt += this->nats_init();
#endif
#ifdef USE_CAPR
  if ( this->thr_num == 0 )
    printf( "capr:                 %d\n", this->r.capr_port );
  cnt += this->capr_init();
#endif
#ifdef USE_RV
  if ( this->thr_num == 0 )
    printf( "rv:                   %d\n", this->r.rv_port );
  cnt += this->rv_init();
#endif
  if ( this->thr_num == 0 )
    fflush( stdout );
  return cnt > 0;
}

int
main( int argc,  const char *argv[] )
{
  EvShm shm;
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
#endif
#ifdef USE_NATS
  r.add_desc( "  -n nats  = listen nats port      (4222)" );
#endif
#ifdef USE_CAPR
  r.add_desc( "  -c capr  = listen capr port      (8866)" );
#endif
#ifdef USE_RV
  r.add_desc( "  -r rv    = listen rv port        (7500)" );
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
  r.http_port          = r.parse_port( argc, argv, "-w", "48080" );
#endif
#ifdef USE_NATS
  r.nats_port          = r.parse_port( argc, argv, "-n", "4222" );
#endif
#ifdef USE_CAPR
  r.capr_port          = r.parse_port( argc, argv, "-c", "8866" );
#endif
#ifdef USE_RV
  r.rv_port            = r.parse_port( argc, argv, "-r", "7500" );
#endif
  Runner<Args, Loop> runner( r, shm, Loop::initialize );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
