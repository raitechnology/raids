#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raids/ev_tcp.h>
#include <raids/redis_exec.h>

namespace rai {
namespace ds {

struct EvPrefetchQueue;

struct EvRedisListen : public EvTcpListen {
  uint64_t timer_id;
  EvListenOps ops;
  EvRedisListen( EvPoll &p );/* : EvTcpListen( p ) {}*/
  virtual bool accept( void );
  int listen( const char *ip,  int port ) {
    return this->EvTcpListen::listen( ip, port, "redis_listen" );
  }
};

struct EvRedisServiceOps : public EvConnectionOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen );
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka );
};

struct EvRedisService : public EvConnection, public RedisExec {
  EvRedisServiceOps ops;
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvRedisService( EvPoll &p ) : EvConnection( p, EV_REDIS_SOCK, this->ops ),
      RedisExec( *p.map, p.ctx_id, *this, p.sub_route, *this ) {}
  void process( void );
  bool on_msg( EvPublish &pub );
  bool timer_expire( uint64_t tid,  uint64_t event_id );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  void debug( void );
  void release( void );
  void push_free_list( void );
  void pop_free_list( void );
  void process_close() {}
};

}
}

#endif
