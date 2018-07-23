#ifndef __rai_raids__ev_client_h__
#define __rai_raids__ev_client_h__

#include <raids/ev_net.h>
#include <raids/redis_msg.h>

namespace rai {
namespace ds {

struct EvCallback {
  virtual void onMsg( RedisMsg &msg );
  virtual void onErr( char *buf,  size_t buflen,  RedisMsgStatus status );
  virtual void onClose( void );
};

struct EvClient : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvCallback &cb;
  RedisMsg msg;         /* current msg */

  EvClient( EvPoll &p, EvCallback &callback,  EvSockType t = EV_CLIENT_SOCK )
    : EvConnection( p, t ), cb( callback ) {}
  void process( void );
  void process_close( void );
};

struct EvTerminal : public EvClient {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvTerminal( EvPoll &p,  EvCallback &callback )
    : EvClient( p, callback, EV_TERMINAL ) {}
  int start( int sock );
};

}
}

#endif
