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

struct EvClient : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvCallback &cb;
  RedisMsg msg;         /* current msg */

  EvClient( EvPoll &p, EvCallback &callback,  EvSockType t = EV_CLIENT_SOCK )
    : EvConnection( p, t ), cb( callback ) {}
  RedisMsgStatus process_msg( char *buf,  size_t &buflen );
  void process( void );
  void process_close( void );
};

struct EvTerminal : public EvClient {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Term term;
  EvTerminal( EvPoll &p,  EvCallback &callback )
    : EvClient( p, callback, EV_TERMINAL ) {}
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
