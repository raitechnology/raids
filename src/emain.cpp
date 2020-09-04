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

struct MainArgs { /* argv[] parsed args */
  int     maxfd,
          timeout,
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
  bool    use_reuseport,
          use_ipv4,
          use_sigusr,
          busy_poll,
          use_prefetch;
  uint8_t db_num;
  const char * redis_sock;
};

static SignalHandler sighndl;

struct MainLoop {
  EvPoll     poll;
  EvShm      shm;
  MainArgs & r;
  int        thr_num;
  bool       running,
             done;

  template <class Sock>
  void Alloc( Sock * &s ) {
    void * p = aligned_malloc( sizeof( Sock ) );
    s = new ( p ) Sock( this->poll );
  }
  template <class Listen>
  bool Tcp( int pt,  Listen* &l ) {
    if ( pt != 0 ) {
      Alloc<Listen>( l );
      if ( l->listen( NULL, pt, this->r.tcp_opts ) != 0 ) {
        fprintf( stderr, "unable to open tcp listen socket on %d\n", pt );
        return false; /* bad port or network error */
      }
    }
    return true;
  }
#ifdef USE_REDIS
  EvRedisListen   * redis_sv;
  bool redis_init( void ) {
    return Tcp<EvRedisListen>( this->r.redis_port, this->redis_sv ); }
#endif
#ifdef REDIS_UNIX
  EvRedisUnixListen * redis_un;
  bool redis_unix_init( void ) {
    if ( this->r.redis_sock != NULL ) {
      Alloc<EvRedisUnixListen>( this->redis_un );
      if ( this->redis_un->listen( this->r.redis_sock ) != 0 ) {
        fprintf( stderr, "unable to open unix listen socket on %s\n",
                 this->r.redis_sock );
        return false;
      }
    }
    return true;
  }
#endif
#ifdef USE_MEMCACHED
  EvMemcachedListen * memcached_sv;
  bool memcached_init( void ) {
    return Tcp<EvMemcachedListen>( this->r.memcached_port, this->memcached_sv ); }
#endif
#ifdef USE_MEMCACHED_UDP
  EvMemcachedUdp  * memcached_udp_sv;
  bool memcached_udp_init( void ) {
    if ( this->r.memcached_udp_port != 0 ) {
      Alloc<EvMemcachedUdp>( this->memcached_udp_sv );
      if ( this->memcached_udp_sv->listen( NULL, this->r.memcached_udp_port,
                                           this->r.udp_opts ) != 0 ) {
        fprintf( stderr, "unable to open memcached udp listen socket %d\n",
                 this->r.memcached_udp_port );
        return false;
      }
      this->memcached_udp_sv->init();
    }
    return true;
  }
#endif
#ifdef USE_HTTP
  EvHttpListen    * http_sv;
  bool http_init( void ) {
    return Tcp<EvHttpListen>( this->r.http_port, this->http_sv ); }
#endif
#ifdef USE_NATS
  EvNatsListen    * nats_sv;
  bool nats_init( void ) {
    return Tcp<EvNatsListen>( this->r.nats_port, this->nats_sv ); }
#endif
#ifdef USE_CAPR
  EvCaprListen    * capr_sv;
  bool capr_init( void ) {
    return Tcp<EvCaprListen>( this->r.capr_port, this->capr_sv ); }
#endif
#ifdef USE_RV
  EvRvListen      * rv_sv;
  bool rv_init( void ) {
    return Tcp<EvRvListen>( this->r.rv_port, this->rv_sv ); }
#endif

  void * operator new( size_t, void *ptr ) { return ptr; }
  MainLoop( EvShm &m,  MainArgs &args,  int num )
    : shm( m ), r( args ) {
    uint8_t * b = (uint8_t *) (void *) &this->thr_num;
    ::memset( b, 0, (uint8_t *) (void *) &this[ 1 ] -  b );
    this->thr_num = num;
  }
  int initialize( void ) noexcept;

  bool poll_init( bool reattach_shm ) noexcept;

  void run( void ) noexcept;

  void detach( void ) noexcept;
};

bool
MainLoop::poll_init( bool reattach_shm ) noexcept
{
  if ( reattach_shm ) {
    if ( this->shm.attach( this->r.db_num ) != 0 )
      return false;
  }
  /* set timeouts */
  this->poll.wr_timeout_ns   = (uint64_t) this->r.timeout * 1000000000;
  this->poll.so_keepalive_ns = (uint64_t) this->r.timeout * 1000000000;

  if ( this->poll.init( this->r.maxfd, this->r.use_prefetch ) != 0 ||
       this->poll.init_shm( this->shm ) != 0 ) {
    fprintf( stderr, "unable to init poll\n" );
    return false;
  }
  if ( this->r.busy_poll )
    this->poll.pubsub->idle_push( EV_BUSY_POLL );
  if ( ! this->r.use_sigusr )
    this->poll.pubsub->flags &= ~KV_DO_NOTIFY;
  return true;
}

void
MainLoop::run( void ) noexcept
{
  static int idx;
  this->running = true;
  for (;;) {
    if ( this->poll.quit >= 5 ) {
      idx++;
      break;
    }
    int state = this->poll.dispatch(); /* 0 if idle, 1, 2, 3 if busy */
    this->poll.wait( state == EvPoll::DISPATCH_IDLE ? 100 : 0 );
    if ( sighndl.signaled && ! poll.quit ) {
      if ( idx == this->thr_num ) /* wait for my turn */
        this->poll.quit++;
    }
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
  ((MainLoop *) loop)->done = true;
  return 0;
}

int
MainLoop::initialize( void ) noexcept
{
  int cnt = 0;
  if ( ! this->poll_init( this->r.num_threads > 1 ) )
    return -1;
#ifdef USE_REDIS
  if ( this->thr_num == 0 )
    printf( "redis:                %d\n", this->r.redis_port );
  cnt += this->redis_init();
#endif
#ifdef REDIS_UNIX
  if ( this->thr_num == 0 )
    printf( "redis_unix:           %s\n", this->r.redis_sock );
  cnt += this->redis_unix_init();
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
  const char * xx = get_arg( argc, argv, 0, "-X", NULL ),
             * pt = NULL, * mc = NULL, * sn = NULL, * hp = NULL,
             * np = NULL, * cp = NULL, * rv = NULL;
#ifdef USE_REDIS
  const char * pt_def = ( xx ? NULL : "6379" );
               pt = get_arg( argc, argv, 1, "-p", pt_def );  /* redis */
  opts[ n ]   = " [-p redis-port]";
  desc[ n++ ] = "  -p redis = listen redis port     (6379)\n";
#endif
#if defined( USE_MEMCACHED ) || defined( USE_MEMCACHED_UDP )
  const char * mc_def = ( xx ? NULL : "11211" );
               mc = get_arg( argc, argv, 1, "-d", mc_def ); /* memcached */
  opts[ n ]   = " [-d memcd-port]";
  desc[ n++ ] = "  -d memcd = listen memcached port (11211)\n";
#endif
/* unix doesn't have a SO_REUSEPORT option */
#ifdef REDIS_UNIX
  const char * sn_def = ( xx ? NULL : "/tmp/raids.sock" );
               sn = get_arg( argc, argv, 1, "-u", sn_def );/* unix */
  opts[ n ]   = " [-u unx-port]";
  desc[ n++ ] = "  -u unix  = listen unix name      (/tmp/raids.sock)\n";
#endif
#ifdef USE_HTTP
  const char * hp_def = ( xx ? NULL : "48080" );
               hp = get_arg( argc, argv, 1, "-w", hp_def ); /* http/websock */
  opts[ n ]   = " [-w www-port]";
  desc[ n++ ] = "  -w web   = listen websocket      (48080)\n";
#endif
#ifdef USE_NATS
  const char * np_def = ( xx ? NULL : "4222" );
               np = get_arg( argc, argv, 1, "-n", np_def ); /* nats */
  opts[ n ]   = " [-n nats-port]";
  desc[ n++ ] = "  -n nats  = listen nats port      (4222)\n";
#endif
#ifdef USE_CAPR
  const char * cp_def = ( xx ? NULL : "8866" );
               cp = get_arg( argc, argv, 1, "-c", cp_def);  /* capr */
  opts[ n ]   = " [-c capr-port]";
  desc[ n++ ] = "  -c capr  = listen capr port      (8866)\n";
#endif
#ifdef USE_RV
  const char * rv_def = ( xx ? NULL : "7500" );
               rv = get_arg( argc, argv, 1, "-r", rv_def );  /* rv */
  opts[ n ]   = " [-c rv-port]";
  desc[ n++ ] = "  -r rv    = listen rv port        (7500)\n";
#endif
  const char * fd = get_arg( argc, argv, 1, "-x", "10000" ), /* max num fds */
             * ti = get_arg( argc, argv, 1, "-k", "16" ),    /* secs timeout */
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
             * cl = get_arg( argc, argv, 0, "-P", 0 ),
             * th = get_arg( argc, argv, 1, "-t", "1" ),
             * i4 = get_arg( argc, argv, 0, "-4", 0 ),
             * no = get_arg( argc, argv, 0, "-s", 0 ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
    printf( "%s [-m map]", argv[ 0 ] );
    for ( i = 0; i < n; i++ )
      printf( "%s", opts[ i ] );
    printf(
      " [-x maxfd] [-k secs] [-f prefe] [-P] [-t nthr] [-4] [-s] [-b] [-X]\n" );
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
      "  -s       = do not use signal USR1 publish notification\n"
      "  -b       = busy poll\n"
      "  -X       = do not listen to default ports, only using cmd line\n" );
    return 0;
  }
  const uint8_t db_num = 0; /* make parameter */

  if ( shm.open( mn, db_num ) != 0 )
    return 1;
  shm.print();

  MainArgs r;

  r.redis_port         = ( pt ? atoi( pt ) : 0 );
  r.memcached_port     = ( mc ? atoi( mc ) : 0 );
  r.memcached_udp_port = ( mc ? atoi( mc ) : 0 );
  r.redis_sock         = sn;
  r.http_port          = ( hp ? atoi( hp ) : 0 );
  r.nats_port          = ( np ? atoi( np ) : 0 );
  r.capr_port          = ( cp ? atoi( cp ) : 0 );
  r.rv_port            = ( rv ? atoi( rv ) : 0 );
  r.maxfd              = atoi( fd );
  r.timeout            = atoi( ti );
  r.use_prefetch       = ( fe != NULL && fe[ 0 ] == '1' );
  r.use_sigusr         = ( no == NULL );
  r.use_ipv4           = ( i4 != NULL );
  r.busy_poll          = ( bu != NULL );
  r.num_threads        = atoi( th );
  r.use_reuseport      = ( cl != NULL || r.num_threads > 1 );

  r.tcp_opts = DEFAULT_TCP_LISTEN_OPTS;
  r.udp_opts = DEFAULT_UDP_LISTEN_OPTS;
  if ( ! r.use_reuseport ) {
    r.tcp_opts &= ~OPT_REUSEPORT;
    r.udp_opts &= ~OPT_REUSEPORT;
  }
  if ( r.use_ipv4 ) {
    r.tcp_opts &= ~OPT_AF_INET6;
    r.udp_opts &= ~OPT_AF_INET6;
  }

  printf( "raids_version:        %s\n", kv_stringify( DS_VER ) );
  printf( "max_fds:              %d\n", r.maxfd );
  printf( "keepalive_timeout:    %d\n", r.timeout );
  printf( "prefetch:             %s\n", r.use_prefetch  ? "true" : "false" );
  printf( "SIGUSR1_notify:       %s\n", r.use_sigusr    ? "true" : "false" );
  printf( "so_reuseport:         %s\n", r.use_reuseport ? "true" : "false" );
  printf( "ipv4_only:            %s\n", r.use_ipv4      ? "true" : "false" );
  printf( "busy_poll:            %s\n", r.busy_poll     ? "true" : "false" );
  printf( "num_threads:          %d\n", r.num_threads );

  if ( r.num_threads > 1 ) {
    pthread_t tid[ 256 ];
    MainLoop * children[ 256 ];
    static const size_t size = kv::align<size_t>( sizeof( MainLoop ), 64 );
    char * buf = (char *) aligned_malloc( size * r.num_threads );
    size_t off = 0;

    shm.detach(); /* each child will attach */
    signal( SIGUSR2, SIG_IGN );
    sighndl.install(); /* catch sig int */

    for ( i = 0; i < r.num_threads && i < 256;  i++ ) {
      children[ i ] = new ( &buf[ off ] ) MainLoop( shm, r, i );
      off += size;
      pthread_create( &tid[ i ], NULL, thread_runner, children[ i ] );
    }
    for (;;) {
      int running = 0, done = 0;
      for ( i = 0; i < r.num_threads && i < 256; i++ ) {
        if ( children[ i ]->running )
          running++;
        if ( children[ i ]->done ) 
          done++;
      }
      if ( running + done == i ) {
        printf( "... %d ready ...\n", running );
        break;
      }
    }
    for ( i = 0; i < r.num_threads && i < 256; i++ ) {
      pthread_join( tid[ i ], NULL );
    }
  }
  else {
    MainLoop loop( shm, r, 0 );
    if ( loop.initialize() > 0 )
      loop.run();
  }
  shm.detach();

  return status;
}
