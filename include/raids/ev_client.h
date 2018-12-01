#ifndef __rai_raids__ev_client_h__
#define __rai_raids__ev_client_h__

#include <raids/ev_net.h>
#include <raids/redis_msg.h>
#include <raids/term.h>

namespace rai {
namespace ds {

struct EvCallback {
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void on_close( void );
};

struct EvClient {
  EvCallback &cb;

  EvClient( EvCallback &callback ) : cb( callback ) {}
  virtual void send_msg( RedisMsg &msg );
};

struct RedisExec;
struct EvShmClient : public EvClient, public StreamBuf {
  RedisExec   * exec;
  kv::HashTab * map;
  uint32_t      ctx_id;

  EvShmClient( EvCallback &callback ) : EvClient( callback ), exec( 0 ),
    map( 0 ), ctx_id( kv::MAX_CTX_ID ) {}
  ~EvShmClient();

  int open( const char *mn );
  int init_exec( EvPoll &p );
  void print( void );
  void close( void );
  virtual void send_msg( RedisMsg &msg );
};

struct EvNetClient : public EvClient, public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMsg msg;         /* current msg */

  EvNetClient( EvPoll &p, EvCallback &callback,  EvSockType t = EV_CLIENT_SOCK )
    : EvClient( callback ), EvConnection( p, t ) {}
  virtual void send_msg( RedisMsg &msg );
  RedisMsgStatus process_msg( char *buf,  size_t &buflen );
  void process( void );
  void process_close( void );
};

struct EvTerminal : public EvNetClient {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Term term;

  EvTerminal( EvPoll &p,  EvCallback &callback )
    : EvNetClient( p, callback, EV_TERMINAL ) {}
  int start( void );
  void flush_out( void );
  void process( void );
  void finish( void );
  void printf( const char *fmt,  ... )
#if defined( __GNUC__ )
      __attribute__((format(printf,2,3)));
#else
      ;
#endif
};

}
}

#endif
