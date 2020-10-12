#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raids/ev_tcp.h>
#include <raids/ev_unix.h>
#include <raids/redis_exec.h>

namespace rai {
namespace ds {

struct EvPrefetchQueue;

struct EvRedisService : public EvConnection, public RedisExec {
  static const uint8_t EV_REDIS_SOCK = 2,
                       EV_REDIS_UNIX_SOCK = 16; /* only for unique timers */
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvRedisService( EvPoll &p ) : EvConnection( p, EV_REDIS_SOCK ),
      RedisExec( *p.map, p.ctx_id, p.dbx_id, *this, p.sub_route, *this ) {}
  void debug( void ) noexcept;
  void push_free_list( void ) noexcept;
  void pop_free_list( void ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( EvPublish &pub ) noexcept final;
  virtual void key_prefetch( EvKeyCtx &ctx ) noexcept final;
  virtual int  key_continue( EvKeyCtx &ctx ) noexcept final;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept final;
  virtual bool match( PeerMatchArgs &ka ) noexcept final;
};

struct EvRedisListen : public EvTcpListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRedisListen( EvPoll &p ) noexcept;/* : EvTcpListen( p ) {}*/
  virtual bool accept( void ) noexcept;
  int listen( const char *ip,  int port,  int opts ) {
    return this->EvTcpListen::listen( ip, port, opts, "redis_listen" );
  }
};

struct EvRedisUnixListen : public EvUnixListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRedisUnixListen( EvPoll &p ) noexcept;
  virtual bool accept( void ) noexcept;
  int listen( const char *sock ) {
    return this->EvUnixListen::listen( sock, "unix_listen" );
  }
};

}
}

#endif
