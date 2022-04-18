#ifndef _MSC_VER
#define _GNU_SOURCE /* for sched_setaffinity */
#endif
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <ctype.h>
#ifndef _MSC_VER
#include <sched.h>
#else
#define strncasecmp _strnicmp
#endif
#include <raids/redis_api.h>
#include <raids/int_str.h>
#include <raikv/shm_ht.h>
#include <raikv/util.h>

static kv_signal_handler_t hndlr;
static int err;

static char key[]  = "key:__rand_int__",     /* set,get */
            kcnt[] = "counter:__rand_int__", /* incr */
            hkey[] = "myset:__rand_int__",   /* hset */
            elem[] = "element:__rand_int__", /* hset field */
            lkey[] = "mylist",               /* xpush,xpop,lrange */
            skey[] = "myset";                /* sadd,spop */

static void mk_key( ds_msg_t *k )      { mk_str( k, key, (int32_t) sizeof( key )-1 ); }
static void mk_counter( ds_msg_t *k )  { mk_str( k, kcnt, (int32_t) sizeof( kcnt )-1 ); }
static void mk_hash_key( ds_msg_t *k ) { mk_str( k, hkey, (int32_t) sizeof( hkey )-1 ); }
static void mk_elem( ds_msg_t *k )     { mk_str( k, elem, (int32_t) sizeof( elem )-1 ); }
static void mk_list_key( ds_msg_t *k ) { mk_str( k, lkey, (int32_t) sizeof( lkey )-1 ); }
static void mk_set_key( ds_msg_t *k )  { mk_str( k, skey, (int32_t) sizeof( skey )-1 ); }

static uint64_t rand_state[ 2 ] = { 0x9e3779b9U, 0x7f4a7c13U };

static inline uint64_t
rotl( const uint64_t x,  int k )
{
  return (x << k) | (x >> (64 - k));
}

static uint64_t
next_rand( void ) /* deterministic random */
{
  uint64_t s0 = rand_state[ 0 ];
  uint64_t s1 = rand_state[ 1 ];
  uint64_t result = s0 + s1;
  s1 ^= s0;
  rand_state[ 0 ] = rotl(s0, 55) ^ s1 ^ (s1 << 14); /* a, b */
  rand_state[ 1 ] = rotl(s1, 36); /* c */
  return result;
}

static void
fill_12( char *str,  uint64_t i )
{
  size_t j = 12;
  while ( i > 0 ) {
    str[ --j ] = ( i % 10 ) + '0';
    if ( j == 0 )
      break;
    i /= 10;
  }
  while ( j > 0 )
    str[ --j ] = '0';
}

static void
rand_12( char *str, uint64_t randomcnt )
{
  /* get a random then fill 12 digits at str[ 0 .. 11 ] */
  fill_12( str, next_rand() % randomcnt );
}

static void rand_key(      uint64_t rcnt ) { rand_12( &key[ 4 ], rcnt ); }
static void rand_cnt_key(  uint64_t rcnt ) { rand_12( &kcnt[ 8 ], rcnt ); }
static void rand_hash_key( uint64_t rcnt ) { rand_12( &hkey[ 6 ], rcnt ); }
static void rand_elem(     uint64_t rcnt ) { rand_12( &elem[ 8 ], rcnt ); }

static void
remove_key( ds_t *h,  uint64_t randomcnt )
{
  ds_msg_t m, ar[ 2 ];
  mk_str( &ar[ 0 ], "del", 3 );
  mk_key( &ar[ 1 ] );
  mk_arr( &m, 2, ar );
  if ( randomcnt <= 1 )
    ds_run_cmd( h, NULL, &m ); /* del key:__rand_int__ */
  else {
    uint64_t i;
    for ( i = 0; i < randomcnt; i++ ) {
      fill_12( &key[ 4 ], i );
      ds_run_cmd( h, NULL, &m ); /* del key:000000000000 -> randomcnt */
    }
  }
}

static void
remove_counter_key( ds_t *h,  uint64_t randomcnt )
{
  ds_msg_t m, ar[ 2 ];
  mk_str( &ar[ 0 ], "del", 3 );
  mk_counter( &ar[ 1 ] );
  mk_arr( &m, 2, ar );
  if ( randomcnt <= 1 )
    ds_run_cmd( h, NULL, &m ); /* del counter:__rand_int__ */
  else {
    uint64_t i;
    for ( i = 0; i < randomcnt; i++ ) {
      fill_12( &kcnt[ 8 ], i );
      ds_run_cmd( h, NULL, &m ); /* del key:000000000000 -> randomcnt */
    }
  }
}

static void
remove_hash_key( ds_t *h,  uint64_t randomcnt )
{
  ds_msg_t m, ar[ 2 ];
  mk_str( &ar[ 0 ], "del", 3 );
  mk_hash_key( &ar[ 1 ] );
  mk_arr( &m, 2, ar );
  if ( randomcnt <= 1 )
    ds_run_cmd( h, NULL, &m ); /* del myset:__rand_int__ */
  else {
    uint64_t i;
    for ( i = 0; i < randomcnt; i++ ) {
      fill_12( &hkey[ 6 ], i );
      ds_run_cmd( h, NULL, &m ); /* del key:000000000000 -> randomcnt */
    }
  }
}

static void
remove_list_key( ds_t *h )
{
  ds_msg_t m, ar[ 2 ];
  mk_str( &ar[ 0 ], "del", 3 );
  mk_list_key( &ar[ 1 ] );
  mk_arr( &m, 2, ar );
  ds_run_cmd( h, NULL, &m ); /* del mylist */
}

static void
remove_set_key( ds_t *h )
{
  ds_msg_t m, ar[ 2 ];
  mk_str( &ar[ 0 ], "del", 3 );
  mk_set_key( &ar[ 1 ] );
  mk_arr( &m, 2, ar );
  ds_run_cmd( h, NULL, &m ); /* del myset */
}

static int
test_ping( ds_t *h,  size_t nrequests )
{
  ds_msg_t r;
  size_t i;
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_ping( h, &r, NULL );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_get( ds_t *h,  size_t nrequests,  size_t randomcnt )
{
  ds_msg_t r, k;
  size_t i;
  mk_key( &k ); /* key:__rand_int__ */
  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_get( h, &r, &k );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      rand_key( randomcnt ); /* change key:__rand_int__ */
      err += ds_get( h, &r, &k );
      ds_release_mem( h );
    }
  }
  return hndlr.signaled;
}

static int
test_set( ds_t *h,  size_t nrequests,  size_t randomcnt,  char *data,
        size_t size )
{
  ds_msg_t r, k, v;
  size_t i;
  mk_key( &k );             /* key:__rand_int__ */
  mk_str( &v, data, (int32_t) size ); /* xxx */
  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_set( h, &r, &k, &v, NULL );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      rand_key( randomcnt ); /* change key:__rand_int__ */
      err += ds_set( h, &r, &k, &v, NULL );
      ds_release_mem( h );
    }
  }
  return hndlr.signaled;
}

static int
test_incr( ds_t *h, size_t nrequests, size_t randomcnt )
{
  ds_msg_t r, k;
  size_t i;
  mk_counter( &k ); /* counter:__rand_int__ */
  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_incr( h, &r, &k );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      rand_cnt_key( randomcnt ); /* cnange counter:__rand_int__ */
      err += ds_incr( h, &r, &k );
      ds_release_mem( h );
    }
  }
  return hndlr.signaled;
}

static int
test_lpush( ds_t *h, size_t nrequests, char *data, size_t size )
{
  ds_msg_t r, k, v;
  size_t i;
  mk_list_key( &k );        /* mylist */
  mk_str( &v, data, (int32_t) size ); /* xxx */
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_lpush( h, &r, &k, &v, NULL );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_rpush( ds_t *h, size_t nrequests, char *data, size_t size )
{
  ds_msg_t r, k, v;
  size_t i;
  mk_list_key( &k );        /* mylist */
  mk_str( &v, data, (int32_t) size ); /* xxx */
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_rpush( h, &r, &k, &v, NULL );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_lpop( ds_t *h, size_t nrequests )
{
  ds_msg_t r, k;
  size_t i;
  mk_list_key( &k ); /* mylist */
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_lpop( h, &r, &k );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_rpop( ds_t *h, size_t nrequests )
{
  ds_msg_t r, k;
  size_t i;
  mk_list_key( &k ); /* mylist */
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_rpop( h, &r, &k );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_sadd( ds_t *h, size_t nrequests, size_t randomcnt )
{
  ds_msg_t r, k, v;
  size_t i;
  mk_set_key( &k ); /* myset */
  mk_elem( &v );    /* element:__rand_int__ */
  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_sadd( h, &r, &k, &v, NULL );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      rand_elem( randomcnt );     /* change element:__rand_int__ */
      err += ds_sadd( h, &r, &k, &v, NULL );
      ds_release_mem( h );
    }
  }
  return hndlr.signaled;
}

static int
test_hset( ds_t *h, size_t nrequests, size_t randomcnt, char *data, size_t size )
{
  ds_msg_t r, k, f, v;
  size_t i;
  mk_hash_key( &k );        /* myset:__rand_int__ */
  mk_elem( &f );            /* element:__rand_int__ */
  mk_str( &v, data, (int32_t) size ); /* xxx */
  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_hset( h, &r, &k, &f, &v, NULL );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      rand_hash_key( randomcnt ); /* change myset:__rand_int__ */
      rand_elem( randomcnt );     /* change element:__rand_int__ */
      err += ds_hset( h, &r, &k, &f, &v, NULL );
      ds_release_mem( h );
    }
  }
  return hndlr.signaled;
}

static int
test_spop( ds_t *h, size_t nrequests )
{
  ds_msg_t r, k;
  size_t i;
  mk_set_key( &k );
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_spop( h, &r, &k, NULL );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_lrange( ds_t *h, size_t nrequests, size_t cnt )
{
  ds_msg_t r, k, z, n;
  size_t i;
  mk_list_key( &k );
  mk_int( &z, 0 );
  mk_int( &n, cnt );
  for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
    err += ds_lrange( h, &r, &k, &z, &n );
    ds_release_mem( h );
  }
  return hndlr.signaled;
}

static int
test_mset( ds_t *h, size_t nrequests, size_t randomcnt, size_t nkeys,
           char *data, size_t size )
{
  ds_msg_t r, m, *ar;
  size_t i, k;
  char * kbuf, * kp;
  /* construct array: [mset, key:__rand_int__, xxx, x nkeys ] */
  ar   = (ds_msg_t *) malloc( sizeof( ds_msg_t ) * ( nkeys * 2 + 1 ) );
  kbuf = (char *) malloc( sizeof( key ) * nkeys );
  kp   = kbuf;
  mk_str( &ar[ 0 ], "mset", 4 );
  for ( k = 0; k < nkeys; k++ ) {
    memcpy( kp, key, sizeof( key ) );
    mk_str( &ar[ 1 + k * 2 ], kp, (int32_t) ( sizeof( key ) - 1 ) );
    mk_str( &ar[ 1 + k * 2 + 1 ], data, (int32_t) size );
    kp = &kp[ sizeof( key ) ];
  }
  mk_arr( &m, (int32_t) ( nkeys + 1 ), ar );

  if ( randomcnt <= 1 ) {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      err += ds_run_cmd( h, &r, &m );
      ds_release_mem( h );
    }
  }
  else {
    for ( i = 0; i < nrequests && ! hndlr.signaled; i++ ) {
      kp = kbuf;
      for ( k = 0; k < nkeys; k++ ) {
        rand_12( &kp[ 4 ], randomcnt ); /* randomize key:__rand_int__ */
        kp = &kp[ sizeof( key ) ];
      }
      err += ds_run_cmd( h, &r, &m );
      ds_release_mem( h );
    }
  }
  free( ar );
  free( kbuf );
  return hndlr.signaled;
}

static void
print_time( const char *cmd,  double secs,  size_t nrequests,  int csv )
{
  static const double NS = 1000.0 * 1000.0 * 1000.0;
  double rqs = (double) nrequests / secs;
  if ( csv ) {
    printf( "\"%s\",\"%.2f\"\n", cmd, rqs );
  }
  else {
    printf( "%s: ", cmd );
    printf( "%.2f req per second, ", rqs );
    printf( "%.2f ns per req", NS / rqs );
    if ( err != 0 ) {
      printf( ", errors %d", -err );
      err = 0;
    }
    printf( "\n" );
  }
}

static void
run_ping_busy_loop( ds_t *h,  double total )
{
  for (;;) {
    double t1 = kv_current_monotonic_time_s(), t2;
    int i;
    for ( i = 0; i < 10000; i++ ) {
      ds_ping( h, NULL, NULL );
      ds_release_mem( h );
    }
    t2 = kv_current_monotonic_time_s();
    total -= t2 - t1;
    if ( total < 0.0 )
      break;
  }
}

#ifndef _MSC_VER
static int
set_affinity( int cpu )
{
  cpu_set_t set;
  if ( cpu >= 0 ) {
    CPU_ZERO( &set );
    CPU_SET( cpu, &set );
    if ( sched_setaffinity( 0, sizeof( cpu_set_t ), &set ) == 0 )
      return cpu;
  }
  return -1;
}
#endif

static int
warm_up_cpu( ds_t *h,  const char *affinity )
{
#ifndef _MSC_VER
  int cpu = -1;
  /* warm up */
  if ( affinity != NULL ) {
    cpu = set_affinity( atoi( affinity ) );
    if ( cpu >= 0 )
      run_ping_busy_loop( h, 0.1 /* 100ms */ );
  }
  if ( cpu < 0 ) { /* let kernel choose the cpu */
    run_ping_busy_loop( h, 0.1 /* 100ms */ );
    cpu = sched_getcpu();
    if ( cpu >= 0 )
      cpu = set_affinity( cpu );
  }
  return cpu;
#else
  run_ping_busy_loop( h, 0.1 );
  return 0;
#endif
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  int i;
  for ( i = 1; i < argc - b; i++ )
    if ( strcmp( f, argv[ i ] ) == 0 ) /* -p port */
      return argv[ i + b ];
  return def; /* default value */
}

static void 
print_help( const char *argv0 )
{
  printf( "%s [-m map_name] [-x] [-n requests] [-d size]\n"
         "%*s [-r count] [-P num_req] [-l] [-t tests] [-a cpu] [-C 0|1][-c]\n"
  "   -m map_name : Name of shm to attach or create (default " KV_DEFAULT_SHM ")\n"
  "   -x          : Create the shm map_name\n"
  "   -n requests : Total number of requests (default 100000)\n"
  "   -d size     : Data size of set/get value in bytes (default 3)\n"
  "   -r count    : Use random keys for set/get/incr, random vals for sadd\n"
  "   -P num_req  : Pipeline requests (default 1)\n"
  "   -l          : Loop, run forever\n"
  "   -t tests    : Only run the comma list of tests\n"
  "   -a cpu      : Set affinity to cpu (default linux)\n"
  "   -C 0|1      : Delete keys if 1, don't clean if 0 (default 1)\n"
  "   -c          : Output CSV of test, msgs/sec\n",
  argv0, (int) strlen( argv0 ), "" );
}

/* tests to perform */
enum {
  DO_GET    = 1,   DO_HSET   = 2,   DO_INCR   = 4,    DO_LPOP   = 8,
  DO_LPUSH  = 16,  DO_LRANGE = 32,  DO_MSET   = 64,   DO_PING   = 128,
  DO_RPOP   = 256, DO_RPUSH  = 512, DO_SET    = 1024, DO_SPOP   = 2048,
  DO_SADD   = 4096
};

static uint32_t
parse_tests( const char *te ) /* comma separated list of tests */
{
  uint32_t fl;
  if ( te == NULL ) /* all tests */
    return 0xffffffff;

  for ( fl = 0;;) {
    while ( *te != '\0' && ! isalpha( *te ) )
      te++;
    if ( *te == '\0' )
      return fl;

    if      ( strncasecmp( te, "get", 3 ) == 0 )    fl |= DO_GET;
    else if ( strncasecmp( te, "hset", 4 ) == 0 )   fl |= DO_HSET;
    else if ( strncasecmp( te, "incr", 4 ) == 0 )   fl |= DO_INCR;
    else if ( strncasecmp( te, "lpop", 4 ) == 0 )   fl |= DO_LPOP;
    else if ( strncasecmp( te, "lpush", 5 ) == 0 )  fl |= DO_LPUSH;
    else if ( strncasecmp( te, "lrange", 6 ) == 0 ) fl |= DO_LRANGE;
    else if ( strncasecmp( te, "mset", 4 ) == 0 )   fl |= DO_MSET;
    else if ( strncasecmp( te, "ping", 4 ) == 0 )   fl |= DO_PING;
    else if ( strncasecmp( te, "rpop", 4 ) == 0 )   fl |= DO_RPOP;
    else if ( strncasecmp( te, "rpush", 5 ) == 0 )  fl |= DO_RPUSH;
    else if ( strncasecmp( te, "set", 3 ) == 0 )    fl |= DO_SET;
    else if ( strncasecmp( te, "spop", 4 ) == 0 )   fl |= DO_SPOP;
    else if ( strncasecmp( te, "sadd", 4 ) == 0 )   fl |= DO_SADD;
    else fprintf( stderr, "unknown test token: %s\n", te );

    while ( isalpha( *te ) )
      te++;
  }
}

int
main( int argc,  char *argv[] )
{
  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),/* mapname */
             * cr = get_arg( argc, argv, 0, "-x", NULL ), /* create */
             * nr = get_arg( argc, argv, 1, "-n", "100000" ), /* requests */
             * sz = get_arg( argc, argv, 1, "-d", "3" ),  /* size */
             * ra = get_arg( argc, argv, 1, "-r", NULL ), /* random */
             * pi = get_arg( argc, argv, 0, "-P", NULL ), /* pipeline */
             * lo = get_arg( argc, argv, 0, "-l", NULL ), /* loop */
             * te = get_arg( argc, argv, 1, "-t", NULL ), /* tests */
             * af = get_arg( argc, argv, 1, "-a", NULL ), /* cpu */
             * cl = get_arg( argc, argv, 1, "-C", "1" ),  /* clean keys */
             * bu = get_arg( argc, argv, 0, "-b", NULL ), /* busy */
             * cs = get_arg( argc, argv, 0, "-c", NULL ), /* csv */
             * he = get_arg( argc, argv, 0, "-h", NULL ); /* help */

  size_t    nrequests = strtol( nr, NULL, 0 ),
            size      = strtol( sz, NULL, 0 ),
            randomcnt = ( ra == NULL ? 0 : strtol( ra, NULL, 0 ) );
  char    * data = malloc( size ); /* alloc size bytes to set */
  ds_t    * h;      /* handle for shm kv */
  double    t1, t2; /* timers in seconds */
  int       clean_keys = atoi( cl ),
            status;
  uint32_t  fl = parse_tests( te ); /* bit mask of tests */
  int       cpu,
            csv  = ( cs != NULL ),
            busy = ( bu != NULL );

 if ( nrequests == 0 || size == 0 || he != NULL || pi != NULL ) {
    print_help( argv[ 0 ] );
    return 1;
  }
  if ( data == NULL ) {
    fprintf( stderr, "malloc %" PRId64 " bytes failed\n", size );
    return 2;
  }
  memset( data, 'x', size );
  if ( cr != NULL )
    status = ds_create( &h, mn, 0, busy, NULL, 0660 );
  else
    status = ds_open( &h, mn, 0, busy );

  if ( status != 0 ) {
    fprintf( stderr, "failed to %s map %s\n",
             cr ? "create" : "open", mn );
    return 3;
  }
  cpu = warm_up_cpu( h, af );
  if ( cpu >= 0 && ! csv )
    printf( "cpu affinity %d\n", cpu );

  kv_sighndl_install( &hndlr );
  /* if looping, goto here */
  for (;;) {
    if ( ( fl & DO_PING ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* PING */
      if ( test_ping( h, nrequests ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "PING_BULK", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_SET ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* SET */
      if ( test_set( h, nrequests, randomcnt, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "SET", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_GET ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* GET */
      if ( test_get( h, nrequests, randomcnt ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "GET", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_INCR ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* INCR */
      if ( test_incr( h, nrequests, randomcnt ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "INCR", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_LPUSH ) != 0 ) {
      remove_list_key( h );
      t1 = kv_current_monotonic_time_s(); /* LPUSH */
      if ( test_lpush( h, nrequests, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LPUSH", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_RPUSH ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* RPUSH */
      if ( test_rpush( h, nrequests, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "RPUSH", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_LPOP ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* LPOP */
      if ( test_lpop( h, nrequests ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LPOP", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_RPOP ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* RPOP */
      if ( test_rpop( h, nrequests ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "RPOP", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_SADD ) != 0 ) {
      remove_set_key( h );
      t1 = kv_current_monotonic_time_s(); /* SADD */
      if ( test_sadd( h, nrequests, randomcnt ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "SADD", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_HSET ) != 0 ) {
      remove_hash_key( h, randomcnt );
      t1 = kv_current_monotonic_time_s(); /* HSET */
      if ( test_hset( h, nrequests, randomcnt, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "HSET", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_SPOP ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* SPOP */
      if ( test_spop( h, nrequests ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "SPOP", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_LRANGE ) != 0 ) {
      remove_list_key( h );
      t1 = kv_current_monotonic_time_s();
      if ( test_lpush( h, 1000, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LPUSH (for LRANGE)", t2 - t1, 1000, csv );

      t1 = kv_current_monotonic_time_s(); /* LRANGE_100 */
      if ( test_lrange( h, nrequests, 100 ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LRANGE_100 (first 100 elements)", t2 - t1, nrequests, csv );

      t1 = kv_current_monotonic_time_s(); /* LRANGE_300 */
      if ( test_lrange( h, nrequests, 300 ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LRANGE_300 (first 300 elements)", t2 - t1, nrequests, csv );

      t1 = kv_current_monotonic_time_s(); /* LRANGE_500 (even tho it is 450) */
      if ( test_lrange( h, nrequests, 450 ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LRANGE_500 (first 450 elements)", t2 - t1, nrequests, csv );

      t1 = kv_current_monotonic_time_s(); /* LRANGE_600 */
      if ( test_lrange( h, nrequests, 600 ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "LRANGE_600 (first 600 elements)", t2 - t1, nrequests, csv );
    }
    if ( ( fl & DO_MSET ) != 0 ) {
      t1 = kv_current_monotonic_time_s(); /* MSET */
      if ( test_mset( h, nrequests, randomcnt, 10, data, size ) != 0 ) break;
      t2 = kv_current_monotonic_time_s();
      print_time( "MSET (10 keys)", t2 - t1, nrequests, csv );
    }
    if ( lo == NULL ) /* if not looping forever */
      break;
  }

  if ( hndlr.signaled && ! csv )
    printf( "Caught signal %d\n", hndlr.sig );
  if ( clean_keys && ! csv ) {
    printf( "Removing test keys\n" );
    if ( ( fl & ( DO_MSET | DO_SET ) ) != 0 )
      remove_key( h, randomcnt );
    if ( ( fl & DO_INCR ) != 0 )
      remove_counter_key( h, randomcnt );
    if ( ( fl & DO_SADD ) != 0 )
      remove_set_key( h );
    if ( ( fl & DO_HSET ) != 0 )
      remove_hash_key( h, randomcnt );
    if ( ( fl & ( DO_LPUSH | DO_RPUSH | DO_LRANGE ) ) != 0 )
      remove_list_key( h );
  }
  ds_close( h );
  return 0;
}

#if 0
    /* construct cmd:  [ get, "k" ] */
    /*
    mk_int( &ar[ 0 ], GET_CMD );
    mk_str( &ar[ 1 ], "j", 1 );
    mk_arr( &m, 2, ar );
    */
    if ( ds_parse_msg( h, &m, "set k xxx" ) == 0 &&
         ds_run_cmd( h, &r, &m ) == 0 &&
         ds_msg_to_json( h, &r, &j ) == 0 ) {
      printf( "set k -> %.*s\n", j.len, j.strval );
    }
    ds_release_mem( h );
    if ( ds_parse_msg( h, &m, "get k" ) == 0 &&
         ds_run_cmd( h, &r, &m ) == 0 &&
         ds_msg_to_json( h, &r, &j ) == 0 ) {
      printf( "get k -> %.*s\n", j.len, j.strval );
    }
    ds_release_mem( h );
    if ( ds_parse_msg( h, &m, "keys *" ) == 0 &&
         ds_run_cmd( h, &r, &m ) == 0 &&
         ds_msg_to_json( h, &r, &j ) == 0 ) {
      printf( "keys * -> %.*s\n", j.len, j.strval );
    }
    ds_release_mem( h );
#endif

