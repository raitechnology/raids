#ifndef __rai_raids__ev_tcp_h__
#define __rai_raids__ev_tcp_h__

#include <raids/ev_net.h>
#include <raids/ev_client.h>

namespace rai {
namespace ds {

struct EvTcpListen : public EvListen {
  EvTcpListen( EvPoll &p,  PeerOps &o ) : EvListen( p, o ) {}
  int listen( const char *ip,  int port,  const char *k );
  virtual void accept( void ) {}
};

struct EvTcpClient : public EvNetClient {
  EvTcpClient( EvPoll &p, EvCallback &callback,  EvSockType t = EV_CLIENT_SOCK )
    : EvNetClient( p, callback, t ) {}
  int connect( const char *ip,  int port );
};

}
}
#endif
