#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>

#include <raids/redis_msg.h>

#define MAX_DS_ARGS 127

#define do_decl( cmd ) \
  ds_msg_t m, ar[ MAX_DS_ARGS ]; \
  m.type = DS_BULK_ARRAY; m.len = 1; m.array = ar; \
  ar[ 0 ].type = DS_INTEGER_VALUE; ar[ 0 ].len = 0; ar[ 0 ].ival = cmd

#define do_run \
  return ds_run_cmd( h, r, &m )

#define do_opt_run \
  if ( opt != NULL ) { \
    ds_msg_t * p; \
    va_list va; \
    va_start( va, opt ); \
    for (;;) { \
      p = va_arg( va, ds_msg_t * ); \
      if ( p == NULL ) \
        break; \
      if ( m.len == MAX_DS_ARGS ) \
        return -1; \
      ar[ m.len++ ] = *p; \
    } \
    va_end( va ); \
  } \
  do_run

#define do_arg( a ) \
  if ( m.len == MAX_DS_ARGS ) \
    return -1; \
  ar[ m.len++ ] = *a \

#define do_arg2( a, b ) \
  do_arg( a ); do_arg( b )

#define do_arg3( a, b, c ) \
  do_arg2( a, b ); do_arg( c )

#define do_arg4( a, b, c, d ) \
  do_arg3( a, b, c ); do_arg( d )

#define do_arg5( a, b, c, d, e ) \
  do_arg4( a, b, c, d ); do_arg( e )

#define _F_( cmd )        { do_decl( cmd );                       do_run; }
#define _F_A( cmd )       { do_decl( cmd ); do_arg( arg );        do_run; }
#define _F_AA( cmd )      { do_decl( cmd ); do_arg2( arg, arg2 );      do_run; }
#define _F_AKKO( cmd )    { do_decl( cmd ); do_arg3( arg, key, key2 ); do_opt_run; }
#define _F_AO( cmd )      { do_decl( cmd ); do_arg( arg );        do_opt_run; }
#define _F_K( cmd )       { do_decl( cmd ); do_arg( key );        do_run; }
#define _F_KA( cmd )      { do_decl( cmd ); do_arg2( key, arg );       do_run; }
#define _F_KAA( cmd )     { do_decl( cmd ); do_arg3( key, arg, arg2 ); do_run; }
#define _F_KAAA( cmd )    { do_decl( cmd ); do_arg4( key, arg, arg2, arg3 );       do_run; }
#define _F_KAAAAO( cmd )  { do_decl( cmd ); do_arg5( key, arg, arg2, arg3, arg4 ); do_opt_run; }
#define _F_KAAAO( cmd )   { do_decl( cmd ); do_arg4( key, arg, arg2, arg3 );       do_opt_run; }
#define _F_KAAO( cmd )    { do_decl( cmd ); do_arg3( key, arg, arg2 ); do_opt_run; }
#define _F_KAO( cmd )     { do_decl( cmd ); do_arg2( key, arg );       do_opt_run; }
#define _F_KK( cmd )      { do_decl( cmd ); do_arg2( key, key2 );      do_run; }
#define _F_KKA( cmd )     { do_decl( cmd ); do_arg3( key, key2, arg ); do_run; }
#define _F_KKO( cmd )     { do_decl( cmd ); do_arg2( key, key2 ); do_opt_run; }
#define _F_KO( cmd )      { do_decl( cmd ); do_arg( key );        do_opt_run; }
#define _F_O( cmd )       { do_decl( cmd );                       do_opt_run; }

extern "C" {
typedef struct ds_s ds_t;
typedef struct ds_msg_s ds_msg_t;

int ds_run_cmd( ds_t *h,  ds_msg_t *result,  ds_msg_t *cmd );
}

#define REDIS_XTRA
#include <raids/redis_cmd.h>

#if _F_CRC != 0x10e651ffU
#error ds generated function sigs do not match (_F_CRC)
#endif
