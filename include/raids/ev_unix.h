#ifndef __rai_raids__ev_unix_h__
#define __rai_raids__ev_unix_h__

#include <raids/ev_net.h>
#include <raids/ev_client.h>

namespace rai {
namespace ds {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p ) : EvListen( p ) {}
  int listen( const char *sock );
  virtual void accept( void ) {}
};

struct EvRedisUnixListen : public EvUnixListen {
  EvRedisUnixListen( EvPoll &p ) : EvUnixListen( p ) {}
  virtual void accept( void );
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
