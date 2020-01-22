#ifndef __rai_raids__ev_unix_h__
#define __rai_raids__ev_unix_h__

#include <raids/ev_net.h>
#include <raids/ev_client.h>

namespace rai {
namespace ds {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p,  PeerOps &o ) : EvListen( p, o ) {}
  int listen( const char *sock,  const char *k );
  virtual bool accept( void ) { return false; }
};

struct EvRedisUnixListen : public EvUnixListen {
  EvListenOps ops;
  EvRedisUnixListen( EvPoll &p ) : EvUnixListen( p, this->ops ) {}
  virtual bool accept( void );
  int listen( const char *sock ) {
    return this->EvUnixListen::listen( sock, "unix_listen" );
  }
};

struct EvUnixClient : public EvNetClient {
  EvUnixClient( EvPoll &p, EvCallback &callback,
                EvSockType t = EV_CLIENT_SOCK )
    : EvNetClient( p, callback, t ) {}
  int connect( const char *sock );
};

}
}
#endif
