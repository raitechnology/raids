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
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRedisListen( EvPoll &p ) noexcept;/* : EvTcpListen( p ) {}*/
  virtual bool accept( void ) noexcept;
  int listen( const char *ip,  int port,  int opts ) {
    return this->EvTcpListen::listen( ip, port, opts, "redis_listen" );
  }
};

struct EvRedisServiceOps : public EvConnectionOps {
  virtual int client_list( PeerData &pd,  char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerData &pd,  PeerMatchArgs &ka ) noexcept;
};

struct EvRedisService : public EvConnection, public RedisExec {
  EvRedisServiceOps ops;
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvRedisService( EvPoll &p ) : EvConnection( p, EV_REDIS_SOCK, this->ops ),
      RedisExec( *p.map, p.ctx_id, p.dbx_id, *this, p.sub_route, *this ) {}
  void process( void ) noexcept;
  bool on_msg( EvPublish &pub ) noexcept;
  bool timer_expire( uint64_t tid,  uint64_t event_id ) noexcept;
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept;
  void debug( void ) noexcept;
  void release( void ) noexcept;
  void push_free_list( void ) noexcept;
  void pop_free_list( void ) noexcept;
  void process_close() {}
};

}
}

#endif
