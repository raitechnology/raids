#ifndef __rai_raids__redis_exec_h__
#define __rai_raids__redis_exec_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/stream_buf.h>

namespace rai {
namespace ds {

enum ExecStatus {
  EXEC_OK         = 0,
  EXEC_SEND_OK    = 1,
  EXEC_KV_STATUS  = 2, /* kstatus != ok */
  EXEC_MSG_STATUS = 3, /* mstatus != ok */
  EXEC_BAD_ARGS   = 4,
  EXEC_BAD_CMD    = 5,
  EXEC_QUIT       = 6, /* quit/shutdown command */
  EXEC_ALLOC_FAIL = 7
};

struct RedisExec {
  kv::KeyBuf     kb;          /* current key */
  kv::KeyCtx     kctx;        /* current key context */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffers */
  StreamBuf    & out;
  RedisMsg       msg;         /* current command msg */
  RedisCmd       cmd;         /* current command (GET_CMD) */
  RedisMsgStatus mstatus;
  kv::KeyStatus  kstatus;
  uint16_t       flags;       /* command flags (CMD_READONLY_FLAG) */
  int            arity,       /* number of command args */
                 first,       /* first key in args */
                 last,        /* last key in args */
                 step;        /* incr between keys */
  size_t         argc;        /* count of args in cmd msg */
  const uint64_t seed, seed2; /* kv map hash seeds, set when map is created */

  RedisExec( kv::HashTab &map,  uint32_t ctx_id,  StreamBuf &o ) :
      kctx( map, ctx_id, &this->kb ), out( o ), seed( map.hdr.hash_key_seed ),
      seed2( map.hdr.hash_key_seed2 ) {}

  void release( void ) {
    this->wrk.reset();
    this->wrk.release_all();
  }
  ExecStatus exec( void ) {
    switch ( this->cmd ) {
      case COMMAND_CMD:  return this->exec_command();
      case ECHO_CMD:     
      case PING_CMD:     return this->exec_ping();
      case GET_CMD:      return this->exec_get();
      case SET_CMD:      return this->exec_set();
      case SHUTDOWN_CMD:
      case QUIT_CMD:     return EXEC_QUIT;
      default:           return EXEC_BAD_CMD;
    }
  }
  ExecStatus exec_get( void );
  ExecStatus exec_set( void );
  ExecStatus exec_command( void );
  ExecStatus exec_ping( void );
  ExecStatus exec_quit( void );
};

static inline void
str_to_upper( const char *in,  char *out,  size_t len )
{
  /* take away the 0x20 bits */
  for ( size_t i = 0; i < len; i += 4 )
    *(uint32_t *) &out[ i ] = *((uint32_t *) &in[ i ]) & 0xdfdfdfdfU;
}

static inline RedisCmd
get_upper_cmd( const char *name,  size_t len )
{
  if ( len < 32 ) {
    char tmp[ 32 ];
    str_to_upper( name, tmp, len ); 
    return get_redis_cmd( tmp, len );
  }   
  return NO_CMD;
}

}
}

#endif
