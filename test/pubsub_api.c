#define _GNU_SOURCE /* for sched_setaffinity */
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <ctype.h>
#include <signal.h>
#include <sched.h>
#include <raids/redis_api.h>
#include <raids/int_str.h>
#include <raikv/shm_ht.h>
#include <raikv/util.h>

static int was_signaled, err;
static void sighndlr( int sig ) { was_signaled = sig; }

static void
print_time( const char *cmd,  double secs,  size_t nrequests,  size_t size )
{
  static const double NS = 1000.0 * 1000.0 * 1000.0;
  double rqs = (double) nrequests / secs;
  printf( "%s: ", cmd );
  printf( "%.2f msgs/sec, ", rqs );
  printf( "%.2f ns/msg, ", NS / rqs );
  printf( "%.2f MB/sec", (double) size / 1024.0 / 1024.0 / secs );
  if ( err != 0 ) {
    printf( ", errors %d", -err );
    err = 0;
  }
  printf( "\n" );
}

struct subscribe_closure {
  ds_t * h;
  size_t i;
};

static void
subscribe_cb( const ds_event_t *event,  const ds_msg_t *msg,
              void *closure )
{
  double t1;
  struct subscribe_closure * cl = (struct subscribe_closure *) closure;
  ds_msg_t json;

  (void) event;
  cl->i += 1;
  t1 = kv_current_monotonic_time_s();
  if ( ds_msg_to_json( cl->h, msg, &json ) == 0 ) {
    printf( "%.6f %.*s <- %.*s\n", t1,
      (int) event->subject.len, event->subject.strval,
      (int) json.len, json.strval );
  }
  ds_release_mem( cl->h );
}

static void
do_subscribe( ds_t *h,  const char *subject,  size_t nrequests )
{
  ds_msg_t subscr;
  struct subscribe_closure cl;

  cl.h = h;
  cl.i = 0;
  mk_str( &subscr, subject, strlen( subject ) );
  ds_subscribe_with_cb( h, &subscr, subscribe_cb, &cl );
  ds_release_mem( h );
  while ( ! was_signaled ) {
    if ( ds_dispatch( h, 1 ) )
      if ( nrequests > 0 && cl.i >= nrequests )
        break;
  }
}

static void
do_psubscribe( ds_t *h,  const char *pattern,  size_t nrequests )
{
  ds_msg_t subscr;
  struct subscribe_closure cl;

  cl.h = h;
  cl.i = 0;
  mk_str( &subscr, pattern, strlen( pattern ) );
  ds_psubscribe_with_cb( h, &subscr, subscribe_cb, &cl );
  ds_release_mem( h );
  while ( ! was_signaled ) {
    if ( ds_dispatch( h, 1 ) )
      if ( nrequests > 0 && cl.i >= nrequests )
        break;
  }
}

static void
do_publish( ds_t *h,  const char *subject,  const char *data,
            size_t nrequests )
{
  ds_msg_t publish, msg, res, json;
  size_t   i;
  double   t1, t2;

  t1 = kv_current_monotonic_time_s();
  mk_str( &publish, subject, strlen( subject ) );
  mk_str( &msg, data, strlen( data ) );

  for ( i = 0; ! was_signaled; ) {
    ds_dispatch( h, 1 );
    t2 = kv_current_monotonic_time_s();
    if ( t2 - t1 >= 1.0 ) {
      t1 += 1.0;
      printf( "%.6f publish %s <- %s", t2, subject, data );
      ds_publish( h, &res, &publish, &msg );
      if ( ds_msg_to_json( h, &res, &json ) == 0 ) {
        printf( " : %.*s\n", (int) json.len, json.strval );
      }
      ds_release_mem( h );
      i++;
      if ( i > 0 && i == nrequests )
        break;
    }
  }
}

struct latency_closure {
  ds_t     * h;
  ds_msg_t * pub, * time, * reply;
  uint64_t   ns, reply_ns, sum;
  size_t     sent, recv;
  int        reflect, trigger;
};

static void
latency_cb( const ds_event_t *event,  const ds_msg_t *msg,
            void *closure )
{
  struct latency_closure * cl = (struct latency_closure *) closure;

  (void) event;
  if ( msg->len == 8 ) {
    memcpy( &cl->reply_ns, msg->strval, 8 );
    cl->sum += kv_current_monotonic_time_ns() - cl->reply_ns;
    cl->recv++;
    cl->trigger = 1;
    if ( cl->reflect ) {
      ds_publish( cl->h, NULL, cl->pub, cl->reply );
      ds_release_mem( cl->h );
      cl->sent++;
    }
  }
}

static void
do_latency( ds_t *h,  const char *ping,  const char *pong,  int reflect,
            size_t nrequests )
{
  struct latency_closure cl;
  ds_msg_t     subscr, publish, time, reply;
  const char * sub = ( reflect ? ping : pong ),
             * pub = ( reflect ? pong : ping );
  uint64_t     start = kv_current_monotonic_time_ns();
  size_t       j = 0;

  cl.h        = h;
  cl.pub      = &publish;
  cl.time     = &time;
  cl.reply    = &reply;
  cl.ns       = start;
  cl.reply_ns = 0;
  cl.sum      = 0;
  cl.sent     = 0;
  cl.recv     = 0;
  cl.reflect  = reflect;
  cl.trigger  = 1;

  mk_str( &subscr, sub, strlen( sub ) );
  ds_subscribe_with_cb( h, &subscr, latency_cb, &cl ); /* subscribe endpt */
  ds_release_mem( h );
  mk_str( &publish, pub, strlen( pub ) ); /* "ping" or "pong" */
  mk_str( &time, &cl.ns, 8 );             /* send ns stamp */
  mk_str( &reply, &cl.reply_ns, 8 );      /* reply with the same ns as sent */

  if ( ! reflect ) {
    while ( ! was_signaled ) {
      /* once a second, print the msg rate and latency */
      if ( cl.ns - start >= (uint64_t) 1000000000 ) {
        if ( cl.sum != 0 ) {
          printf( "roundtrip: %lu msgs, avg %lu ns\n",
                  cl.recv - j, cl.sum / ( cl.recv - j ) );
          j = cl.recv;
        }
        else { /* nothing recvd */
          cl.trigger = 1; /* resend */
        }
        start = cl.ns;
        cl.sum = 0;
      }
      /* if reply was recv, trigger will be 1 */
      if ( ! was_signaled && cl.trigger == 1 ) {
        cl.trigger = 0;
        ds_publish( h, NULL, &publish, &time ); /* send pings, wait for pongs */
        ds_release_mem( h );
        cl.sent++;
      }
      while ( ! was_signaled && cl.trigger == 0 ) { /* wait for reply */
        if ( ! ds_dispatch( h, 1 ) ) /* returns 0 when nothing is happening */
          break;
      }
      if ( nrequests > 0 && cl.recv >= nrequests )
        break;
      cl.ns = kv_current_monotonic_time_ns();
    }
  }
  else {
    while ( ! was_signaled ) {
      /* once a second, print the msg rate and latency */
      if ( cl.ns - start >= (uint64_t) 1000000000 ) {
        if ( cl.sum != 0 ) {
          printf( "oneway: %lu msgs, avg %lu ns\n",
                  cl.recv - j, cl.sum / ( cl.recv - j ) );
          j = cl.recv;
        }
        start = cl.ns;
        cl.sum = 0;
      }
      /* dispatch events until none left */
      while ( ! was_signaled && ds_dispatch( h, 1 ) )
        ;
      if ( nrequests > 0 && cl.recv >= nrequests )
        break;
      cl.ns = kv_current_monotonic_time_ns();
    }
  }
}

struct throughput_closure {
  size_t   i,
           total;
  uint32_t last;
};

static void
throughput_cb( const ds_event_t *event,  const ds_msg_t *msg,
               void *closure )
{
  struct throughput_closure * cl = (struct throughput_closure *) closure;
  uint32_t ctr;

  (void) event;
  if ( msg->len >= 4 ) {
    if ( msg->len >= 8 ) {
      uint32_t siz;
      memcpy( &siz, &msg->strval[ 4 ], 4 );
      if ( siz != (uint32_t) msg->len )
        fprintf( stderr, "siz %u != %u\n", siz, msg->len );
    }
    if ( memcpy( &ctr, msg->strval, 4 ) ) {
      if ( ctr != cl->last + 1 ) {
        if ( ctr < cl->last + 1 )
          fprintf( stderr, "-%u ", ( cl->last + 1 ) - ctr );
        else
          fprintf( stderr, "+%u ", ctr - ( cl->last + 1 ) );
        fprintf( stderr, "%u -> *%u* (sz=%d)\n", cl->last + 1, ctr, msg->len );
        fprintf( stderr, " oo \n" );
      }
      cl->last = ctr;
      cl->total += msg->len;
      cl->i++;
    }
  }
}

static void
do_throughput( ds_t *h,  const char *subject,  int sub,  size_t nrequests,
               size_t randsize )
{
  ds_msg_t pub;
  size_t   j = 0;
  double   t1, t2;/* timers in seconds */

  t1 = kv_current_monotonic_time_s();
  t2 = t1;
  mk_str( &pub, subject, strlen( subject ) );
  if ( sub ) {
    struct throughput_closure cl;
    size_t last = 0;
    cl.i     = 0;
    cl.last  = 0;
    cl.total = 0;
    ds_subscribe_with_cb( h, &pub, throughput_cb, &cl );
    ds_release_mem( h );
    while ( ! was_signaled ) {
      ds_dispatch( h, 1 );
      if ( cl.i >= j + 100000 ) {
        t2 = kv_current_monotonic_time_s();
        if ( t2 - t1 >= 1.0 ) {
          print_time( "subscribe", t2 - t1, cl.i - j, cl.total - last );
          t1 = t2;
          j = cl.i;
          last = cl.total;
        }
      }
      if ( nrequests != 0 && cl.i >= nrequests )
        break;
    }
    if ( cl.i > j ) {
      t2 = kv_current_monotonic_time_s();
      print_time( "subscribe", t2 - t1, cl.i - j, cl.total - last );
    }
    printf( "total = %lu, last = %u\n", cl.i, cl.last );
  }
  else if ( randsize == 0 ) {
    ds_msg_t msg;
    size_t   i, k = 0;
    uint32_t ctr = 1;

    mk_str( &msg, &ctr, 4 );
    for ( i = 0; ! was_signaled; ) {
      /* publish a message */
      ds_publish( h, NULL, &pub, &msg );
      ds_release_mem( h );
      ctr++;
      i++;
      if ( i >= k + 256 ) {
        ds_dispatch( h, 0 );
        k = i;
      }
      if ( i >= j + 100000 ) {
        t2 = kv_current_monotonic_time_s();
        if ( t2 - t1 >= 1.0 ) {
          print_time( "publish", t2 - t1, i - j, ( i - j ) * 4 );
          t1 = t2;
          j = i;
        }
      }
      if ( i > 0 && i == nrequests )
        break;
    }
    while ( ! was_signaled && ds_dispatch( h, 0 ) )
      ;
    if ( i > j ) {
      t2 = kv_current_monotonic_time_s();
      print_time( "publish", t2 - t1, i - j, ( i - j ) * 4 );
    }
    printf( "total = %lu, ctr = %u\n", i, ctr );
  }
  else {
    ds_msg_t  msg;
    size_t    i, k = 0, total = 0, last = 0;
    uint32_t * ctr;
    uint8_t  * randbuf;

    randbuf  = malloc( randsize + 4 );
    ctr      = (uint32_t *) randbuf;
    ctr[ 0 ] = 1;
    for ( i = 1; i < randsize / 4 + 1; i++ )
      ctr[ i ] = i | 0xffdd0000U;
    mk_str( &msg, randbuf, randsize );
    for ( i = 0; ! was_signaled; ) {
      /* publish a message */
      msg.len = rand() % randsize + 4;
      ctr[ 1 ] = msg.len;
      total += msg.len;
      ds_publish( h, NULL, &pub, &msg );
      ds_release_mem( h );
      ctr[ 0 ]++;
      i++;
      if ( i >= k + 256 ) {
        ds_dispatch( h, 0 );
        k = i;
      }
      if ( i >= j + 100000 ) {
        t2 = kv_current_monotonic_time_s();
        if ( t2 - t1 >= 1.0 ) {
          print_time( "publish", t2 - t1, i - j, total - last );
          last = total;
          t1 = t2;
          j = i;
        }
      }
      if ( i > 0 && i == nrequests )
        break;
    }
    while ( ! was_signaled && ds_dispatch( h, 0 ) )
      ;
    if ( i > j ) {
      t2 = kv_current_monotonic_time_s();
      print_time( "publish", t2 - t1, i - j, total - last );
    }
    printf( "total = %lu, avg size %lu, ctr = %u\n", i, total / i, ctr[ 0 ] );
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

static void
warm_up_cpu( ds_t *h,  const char *affinity )
{
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
  if ( cpu >= 0 )
    printf( "cpu affinity %d\n", cpu );
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( strcmp( f, argv[ i ] ) == 0 ) /* -p port */
      return argv[ i + b ];
  return def; /* default value */
}

static void 
print_help( const char *argv0 )
{
  printf(
  "%s [-m map_name] [-x] [-n num] [-r] [-l] [-s string] [-d data] [-a cpu]\n"
  "   -m map_name : Name of shm to attach or create (default " KV_DEFAULT_SHM ")\n"
  "   -x          : Create the shm map_name\n"
  "   -n num      : Total number of requests (default inf)\n"
  "   -r          : Reverse listen and publish, reflect\n"
  "   -z num      : Use random message sizes: rand() %% num + K\n"
  "   -l          : Latency test, ping/pong (default throughput ping)\n"
  "   -s string   : Subject to subscribe or publish\n"
  "   -p pattern  : Pattern to subscribe\n"
  "   -d data     : String data to publish to subject\n"
  "   -a cpu      : Set affinity to cpu (default linux)\n",
  argv0 );
}

int
main( int argc,  char *argv[] )
{
  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),/* mapname */
             * cr = get_arg( argc, argv, 0, "-x", NULL ),     /* create */
             * nr = get_arg( argc, argv, 1, "-n", NULL ),     /* requests */
             * re = get_arg( argc, argv, 0, "-r", NULL ),     /* reflect */
             * sz = get_arg( argc, argv, 1, "-z", NULL ),     /* reflect */
             * la = get_arg( argc, argv, 0, "-l", NULL ),     /* reflect */
             * da = get_arg( argc, argv, 1, "-d", NULL ),     /* send data */
             * su = get_arg( argc, argv, 1, "-s", NULL ),     /* subject */
             * pa = get_arg( argc, argv, 1, "-p", NULL ),     /* pattern */
             * af = get_arg( argc, argv, 1, "-a", NULL ),     /* cpu */
             * bu = get_arg( argc, argv, 0, "-b", NULL ),     /* busy */
             * he = get_arg( argc, argv, 0, "-h", NULL );     /* help */

  size_t nrequests = 0,
         randsize  = 0;
  ds_t * h;      /* handle for shm kv */
  int    status;

  if ( nr != NULL )
    nrequests = strtol( nr, NULL, 0 );
  if ( sz != NULL )
    randsize = strtol( sz, NULL, 0 );
  if ( he != NULL ) {
    print_help( argv[ 0 ] );
    return 1;
  }
  if ( cr != NULL )
    status = ds_create( &h, mn, 0, bu != NULL, 0, 0, 0 );
  else
    status = ds_open( &h, mn, 0, bu != NULL );

  if ( status != 0 ) {
    fprintf( stderr, "failed to %s map %s\n",
             cr ? "create" : "open", mn );
    return 3;
  }
  status = ds_get_ctx_id( h );
  printf( "ctx %d (0x%x)\n", status, status );
  warm_up_cpu( h, af );

  signal( SIGHUP, sighndlr );
  signal( SIGINT, sighndlr );
  signal( SIGTERM, sighndlr );

  if ( su != NULL || pa != NULL ) {
    if ( pa != NULL )
      do_psubscribe( h, pa, nrequests );
    else if ( da != NULL )
      do_publish( h, su, da, nrequests );
    else
      do_subscribe( h, su, nrequests );
  }
  else if ( la != NULL )
    do_latency( h, "ping", "pong", re != NULL, nrequests );
  else
    do_throughput( h, "ping", re != NULL, nrequests, randsize );

  if ( was_signaled )
    printf( "Caught signal %d\n", was_signaled );
  ds_close( h );
  return 0;
}

