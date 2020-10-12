#ifndef __rai_raids__ev_unix_h__
#define __rai_raids__ev_unix_h__

#include <raids/ev_net.h>
#include <raids/ev_client.h>

namespace rai {
namespace ds {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p,  uint8_t tp,  const char *name )
    : EvListen( p, tp, name ) {}
  int listen( const char *sock,  const char *k ) noexcept;
  virtual bool accept( void ) noexcept { return false; }
};

struct EvUnixClient : public EvNetClient {
  EvUnixClient( EvPoll &p, EvCallback &callback, uint8_t t = EV_CLIENT_SOCK )
    : EvNetClient( p, callback, t ) {}
  int connect( const char *sock ) noexcept;
};

}
}
#endif
