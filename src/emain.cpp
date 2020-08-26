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
#include <raids/ev_service.h>
#ifdef REDIS_UNIX
#include <raids/ev_unix.h>
#endif
#include <raids/ev_memcached.h>
#include <raids/ev_http.h>
#include <raids/ev_nats.h>
#include <raids/ev_capr.h>
#include <raids/ev_rv.h>
#include <raids/ev_client.h>
#include <raids/kv_pubsub.h>

using namespace rai;
using namespace ds;
using namespace kv;

#define USE_REDIS
//#define REDIS_UNIX
#define USE_MEMCACHED
#define USE_MEMCACHED_UDP
#define USE_HTTP
#define USE_NATS
#define USE_CAPR
#define USE_RV

struct MainLoop {
  EvShm   shm;
  EvPoll  poll;

  SignalHandler sighndl;
  int maxfd,
      timeout,
      thr_num,
      num_threads,
      tcp_opts,
      udp_opts,
      redis_port,
      memcached_port,
      memcached_udp_port,
      http_port,
      nats_port,
      capr_port,
      rv_port;
  const char * redis_sock;

  template <class Listen>
  bool Tcp( int pt,  Listen &sv ) {
    if ( pt != 0 && sv.listen( NULL, pt, this->tcp_opts ) != 0 ) {
      fprintf( stderr, "unable to open tcp listen socket on %d\n", pt );
      return false; /* bad port or network error */
    }
    return true;
  }
#ifdef USE_REDIS
  EvRedisListen     redis_sv;
  bool redis_init( void ) {
    return Tcp<EvRedisListen>( this->redis_port, this->redis_sv ); }
#endif
#ifdef REDIS_UNIX
  EvRedisUnixListen redis_un;
  bool redis_unix_init( void ) {
    if ( this->redis_sock != NULL &&
         this->redis_un.listen( this->redis_sock ) != 0 ) {
      fprintf( stderr, "unable to open unix listen socket on %s\n",
               this->redis_sock );
      return false;
    }
    return true;
  }
#endif
#ifdef USE_MEMCACHED
  EvMemcachedListen memcached_sv;
  bool memcached_init( void ) {
    return Tcp<EvMemcachedListen>( this->memcached_port, this->memcached_sv ); }
#endif
#ifdef USE_MEMCACHED_UDP
  EvMemcachedUdp    memcached_udp_sv;
  bool memcached_udp_init( void ) {
    if ( this->memcached_udp_port != 0 ) {
      if ( this->memcached_udp_sv.listen( NULL, this->memcached_udp_port,
                                          this->udp_opts ) != 0 ) {
        fprintf( stderr, "unable to open memcached udp listen socket %d\n",
                 this->memcached_udp_port );
        return false;
      }
      this->memcached_udp_sv.init();
    }
    return true;
  }
#endif
#ifdef USE_HTTP
  EvHttpListen      http_sv;
  bool http_init( void ) {
    return Tcp<EvHttpListen>( this->http_port, this->http_sv ); }
#endif
#ifdef USE_NATS
  EvNatsListen      nats_sv;
  bool nats_init( void ) {
    return Tcp<EvNatsListen>( this->nats_port, this->nats_sv ); }
#endif
#ifdef USE_CAPR
  EvCaprListen      capr_sv;
  bool capr_init( void ) {
    return Tcp<EvCaprListen>( this->capr_port, this->capr_sv ); }
#endif
#ifdef USE_RV
  EvRvListen        rv_sv;
  bool rv_init( void ) {
    return Tcp<EvRvListen>( this->rv_port, this->rv_sv ); }
#endif

  bool    use_reuseport,
          use_ipv4,
          use_sigusr,
          busy_poll,
          use_prefetch;
  uint8_t db_num;

  void * operator new( size_t, void *ptr ) { return ptr; }
  MainLoop( EvShm &m,  int nfd,  int secs,  int num,  int threads,
            bool reuseport, bool ipv4only,  bool usr,  bool busy,  bool pref )
    : shm( m ), maxfd( nfd ), timeout( secs ), thr_num( num ),
      num_threads( threads ), redis_port( 0 ), memcached_port( 0 ),
      memcached_udp_port( 0 ), http_port( 0 ), nats_port( 0 ), capr_port( 0 ),
      rv_port( 0 ), redis_sock( 0 )
#ifdef USE_REDIS
    , redis_sv( this->poll )
#endif
#ifdef REDIS_UNIX
    , redis_un( this->poll )
#endif
#ifdef USE_MEMCACHED
    , memcached_sv( this->poll )
#endif
#ifdef USE_MEMCACHED_UDP
    , memcached_udp_sv( this->poll )
#endif
#ifdef USE_HTTP
    , http_sv( this->poll )
#endif
#ifdef USE_NATS
    , nats_sv( this->poll )
#endif
#ifdef USE_CAPR
    , capr_sv( this->poll )
#endif
#ifdef USE_RV
    , rv_sv( this->poll )
#endif
    , use_ipv4( ipv4only ), use_sigusr( usr ),
      busy_poll( busy ), use_prefetch( pref ), db_num( 0 )
  {
    this->tcp_opts = DEFAULT_TCP_LISTEN_OPTS;
    this->udp_opts = DEFAULT_UDP_LISTEN_OPTS;
    /* do this if -P was used, solarflare onload does */
    if ( ! reuseport && threads <= 1 ) {
      this->use_reuseport = false;
      this->tcp_opts &= ~OPT_REUSEPORT; /* broken clustering when it is set */
      this->udp_opts &= ~OPT_REUSEPORT; /* broken clustering when it is set */
    }
    else {
      this->use_reuseport = true;
    }
    if ( ipv4only ) {
      this->tcp_opts &= ~OPT_AF_INET6;
      this->udp_opts &= ~OPT_AF_INET6;
    }
  }
  MainLoop( MainLoop &m,  int num )
    : shm( m.shm ), maxfd( m.maxfd ), timeout( m.timeout ),
      thr_num( num ), num_threads( m.num_threads ), tcp_opts( m.tcp_opts ),
      udp_opts( m.udp_opts ), redis_port( m.redis_port ),
      memcached_port( m.memcached_port ),
      memcached_udp_port( m.memcached_udp_port ),
      http_port( m.http_port ), nats_port( m.nats_port ),
      capr_port( m.capr_port ), rv_port( m.rv_port ),
      redis_sock( m.redis_sock )
#ifdef USE_REDIS
    , redis_sv( this->poll )
#endif
#ifdef REDIS_UNIX
    , redis_un( this->poll )
#endif
#ifdef USE_MEMCACHED
    , memcached_sv( this->poll )
#endif
#ifdef USE_MEMCACHED_UDP
    , memcached_udp_sv( this->poll )
#endif
#ifdef USE_HTTP
    , http_sv( this->poll )
#endif
#ifdef USE_NATS
    , nats_sv( this->poll )
#endif
#ifdef USE_CAPR
    , capr_sv( this->poll )
#endif
#ifdef USE_RV
    , rv_sv( this->poll )
#endif
    , use_ipv4( m.use_ipv4 ), use_sigusr( m.use_sigusr ),
      busy_poll( m.busy_poll ), use_prefetch( m.use_prefetch ),
      db_num( m.db_num ) {}

  int initialize( void ) noexcept;

  bool poll_init( bool reattach_shm ) noexcept;

  void run( void ) noexcept;

  void detach( void ) noexcept;
};

bool
MainLoop::poll_init( bool reattach_shm ) noexcept
{
  if ( reattach_shm ) {
    if ( this->shm.attach( this->db_num ) != 0 )
      return false;
  }
  /* set timeouts */
  this->poll.wr_timeout_ns   = (uint64_t) this->timeout * 1000000000;
  this->poll.so_keepalive_ns = (uint64_t) this->timeout * 1000000000;

  if ( this->poll.init( maxfd, this->use_prefetch ) != 0 ||
       this->poll.init_shm( this->shm ) != 0 ) {
    fprintf( stderr, "unable to init poll\n" );
    return false;
  }
  if ( this->busy_poll )
    this->poll.pubsub->idle_push( EV_BUSY_POLL );
  if ( ! this->use_sigusr )
    this->poll.pubsub->flags &= ~KV_DO_NOTIFY;
  return true;
}

void
MainLoop::run( void ) noexcept
{
  this->sighndl.install();
  for (;;) {
    if ( this->poll.quit >= 5 )
      break;
    int state = this->poll.dispatch(); /* 0 if idle, 1, 2, 3 if busy */
    this->poll.wait( state == EvPoll::DISPATCH_IDLE ? 100 : 0 );
    if ( this->sighndl.signaled && ! poll.quit )
      this->poll.quit++;
  }
#if 0
  if ( fe[ 0 ] == '1' ) {
    size_t j = 0;
    for ( size_t i = 0; i <= EvPoll::PREFETCH_SIZE; i++ ) {
      if ( poll.prefetch_cnt[ i ] != 0 ) {
        printf( "pre[%lu] = %lu (=%lu) (t:%lu)\n", i, poll.prefetch_cnt[ i ],
                                    i * poll.prefetch_cnt[ i ],
                                    j += i * poll.prefetch_cnt[ i ] );
      }
    }
  }
  printf( "bye\n" );
#endif
}

void
MainLoop::detach( void ) noexcept
{
  this->shm.detach();
}

static void *
thread_runner( void *loop )
{
  if ( ((MainLoop *) loop)->initialize() > 0 ) {
    ((MainLoop *) loop)->run();
  }
  return 0;
}

int
MainLoop::initialize( void ) noexcept
{
  int cnt = 0;
  if ( ! this->poll_init( this->thr_num > 0 ) )
    return -1;
#ifdef USE_REDIS
  if ( this->thr_num == 0 )
    printf( "redis:                %d\n", this->redis_port );
  cnt += this->redis_init();
#endif
#ifdef REDIS_UNIX
  if ( this->thr_num == 0 )
    printf( "redis_unix:           %s\n", this->redis_sock );
  cnt += this->redis_unix_init();
#endif
#ifdef USE_MEMCACHED
  if ( this->thr_num == 0 )
    printf( "memcached:            %d\n", this->memcached_port );
  cnt += this->memcached_init();
#endif
#ifdef USE_MEMCACHED_UDP
  if ( this->thr_num == 0 )
    printf( "memcached_udp:        %d\n", this->memcached_udp_port );
  cnt += this->memcached_udp_init();
#endif
#ifdef USE_HTTP
  if ( this->thr_num == 0 )
    printf( "http:                 %d\n", this->http_port );
  cnt += this->http_init();
#endif
#ifdef USE_NATS
  if ( this->thr_num == 0 )
    printf( "nats:                 %d\n", this->nats_port );
  cnt += this->nats_init();
#endif
#ifdef USE_CAPR
  if ( this->thr_num == 0 )
    printf( "capr:                 %d\n", this->capr_port );
  cnt += this->capr_init();
#endif
#ifdef USE_RV
  if ( this->thr_num == 0 )
    printf( "rv:                   %d\n", this->rv_port );
  cnt += this->rv_init();
#endif
  if ( this->thr_num == 0 )
    fflush( stdout );
  return cnt;
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  const char  * opts[ 8 ] = {"","","","","","","",""};
  const char  * desc[ 8 ] = {"","","","","","","",""};
  EvShm         shm;
  int           i, n = 0, status = 0;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM );
#ifdef USE_REDIS
  const char * pt = get_arg( argc, argv, 1, "-p", "6379" );  /* redis */
  opts[ n ]   = " [-p redis-port]";
  desc[ n++ ] = "  -p redis = listen redis port     (6379)\n";
#endif
#if defined( USE_MEMCACHED ) || defined( USE_MEMCACHED_UDP )
  const char * mc = get_arg( argc, argv, 1, "-d", "11211" ); /* memcached */
  opts[ n ]   = " [-d memcd-port]";
  desc[ n++ ] = "  -d memcd = listen memcached port (11211)\n";
#endif
/* unix doesn't have a SO_REUSEPORT option */
#ifdef REDIS_UNIX
  const char * sn = get_arg( argc, argv, 1, "-u", "/tmp/raids.sock" );/* unix */
  opts[ n ]   = " [-u unx-port]";
  desc[ n++ ] = "  -u unix  = listen unix name      (/tmp/raids.sock)\n";
#endif
#ifdef USE_HTTP
  const char * hp = get_arg( argc, argv, 1, "-w", "48080" ); /* http/websock */
  opts[ n ]   = " [-w www-port]";
  desc[ n++ ] = "  -w web   = listen websocket      (48080)\n";
#endif
#ifdef USE_NATS
  const char * np = get_arg( argc, argv, 1, "-n", "4222" ); /* nats */
  opts[ n ]   = " [-n nats-port]";
  desc[ n++ ] = "  -n nats  = listen nats port      (4222)\n";
#endif
#ifdef USE_CAPR
  const char * cp = get_arg( argc, argv, 1, "-c", "8866" );  /* capr */
  opts[ n ]   = " [-c capr-port]";
  desc[ n++ ] = "  -c capr  = listen capr port      (8866)\n";
#endif
#ifdef USE_RV
  const char * rv = get_arg( argc, argv, 1, "-r", "7500" );  /* rv */
  opts[ n ]   = " [-c rv-port]";
  desc[ n++ ] = "  -r rv    = listen rv port        (7500)\n";
#endif
  const char * fd = get_arg( argc, argv, 1, "-x", "10000" ), /* max num fds */
             * ti = get_arg( argc, argv, 1, "-k", "16" ),    /* secs timeout */
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * cl = get_arg( argc, argv, 0, "-P", 0 ),
             * th = get_arg( argc, argv, 0, "-t", "1" ),
             * i4 = get_arg( argc, argv, 0, "-4", 0 ),
             * no = get_arg( argc, argv, 0, "-s", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s [-m map]", argv[ 0 ] );
    for ( i = 0; i < n; i++ )
      printf( "%s", opts[ i ] );
    printf( " [-x maxfd] [-k secs] [-f prefe] [-P] [-t nthr] [-4] [-s] [-b]\n" );
    printf( "\n%s",
      "  -m map   = kv shm map name       (" KV_DEFAULT_SHM ")\n" );
    for ( i = 0; i < n; i++ )
      printf( "%s", desc[ i ] );
    printf( "%s",
      "  -x maxfd = max fds               (10000)\n"
      "  -k secs  = keep alive timeout    (16)\n"
      "  -f prefe = prefetch keys:        (1) 0 = no, 1 = yes\n"
      "  -P       = set SO_REUSEPORT for clustering multiple instances\n"
      "  -t nthr  = spawn N threads       (1) (implies -P)\n"
      "  -4       = use only ipv4 listeners\n"
      "  -s       = don't use signal USR1 publish notification\n"
      "  -b       = busy poll\n" );
    return 0;
  }
  const uint8_t db_num = 0; /* make parameter */

  if ( shm.open( mn, db_num ) != 0 )
    return 1;
  shm.print();

  MainLoop loop( shm, atoi( fd ), atoi( ti ), 0, atoi( th ),
              cl != NULL, i4 != NULL, no == NULL, bu != NULL, fe[ 0 ] == '1' );

#ifdef USE_REDIS
  loop.redis_port = atoi( pt );
#endif
#ifdef USE_MEMCACHED
  loop.memcached_port = atoi( mc );
#endif
#ifdef USE_MEMCACHED_UDP
  loop.memcached_udp_port = atoi( mc );
#endif
#ifdef REDIS_UNIX
  loop.redis_sock = sn;
#endif
#ifdef USE_HTTP
  loop.http_port = atoi( hp );
#endif
#ifdef USE_NATS
  loop.nats_port = atoi( np );
#endif
#ifdef USE_CAPR
  loop.capr_port = atoi( cp );
#endif
#ifdef USE_RV
  loop.rv_port = atoi( rv );
#endif
  printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
  printf( "max_fds:              %d\n", loop.maxfd );
  printf( "keepalive_timeout:    %d\n", loop.timeout );
  printf( "prefetch:             %s\n", loop.use_prefetch  ? "true" : "false" );
  printf( "SIGUSR1_notify:       %s\n", loop.use_sigusr    ? "true" : "false" );
  printf( "so_reuseport:         %s\n", loop.use_reuseport ? "true" : "false" );
  printf( "ipv4_only:            %s\n", loop.use_ipv4      ? "true" : "false" );
  printf( "busy_poll:            %s\n", loop.busy_poll     ? "true" : "false" );

  if ( loop.num_threads > 1 ) {
    pthread_t tid[ 256 ];
    MainLoop * children[ 256 ];

    pthread_create( &tid[ 0 ], NULL, thread_runner, &loop );
    for ( i = 1; i < loop.num_threads && i < 256;  i++ ) {
      children[ i ] = new ( malloc( sizeof( MainLoop ) ) ) MainLoop( loop, i );
      pthread_create( &tid[ i ], NULL, thread_runner, children[ i ] );
    }
    for ( i = 0; i < loop.num_threads && i < 256; i++ ) {
      pthread_join( tid[ i ], NULL );
    }
  }
  else {
    if ( loop.initialize() <= 0 )
      goto fail;
    loop.run();
  }
fail:;
  loop.detach();

  return status;
}
