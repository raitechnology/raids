#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raikv/ev_tcp.h>
#include <raikv/ev_unix.h>
#include <raids/redis_exec.h>

namespace rai {
namespace kv {
  struct EvPrefetchQueue;
}
namespace ds {

struct EvRedisService : public kv::EvConnection, public RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvRedisService( kv::EvPoll &p,  const uint8_t t ) : kv::EvConnection( p, t ),
      RedisExec( *p.map, p.ctx_id, p.dbx_id, *this, p.sub_route, *this ) {}
  void debug( void ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept final;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept final;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept final;
  virtual bool match( kv::PeerMatchArgs &ka ) noexcept final;
};

struct EvRedisListen : public kv::EvTcpListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRedisListen( kv::EvPoll &p ) noexcept;/* : EvTcpListen( p ) {}*/
  virtual bool accept( void ) noexcept;
  int listen( const char *ip,  int port,  int opts ) {
    return this->kv::EvTcpListen::listen( ip, port, opts, "redis_listen" );
  }
};

struct EvRedisUnixListen : public kv::EvUnixListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRedisUnixListen( kv::EvPoll &p ) noexcept;
  virtual bool accept( void ) noexcept;
  int listen( const char *sock,  int opts ) {
    return this->kv::EvUnixListen::listen( sock, opts, "unix_listen" );
  }
};

}
}

#endif
