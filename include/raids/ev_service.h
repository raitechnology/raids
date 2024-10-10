#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raikv/ev_tcp.h>
#include <raikv/ev_unix.h>
#include <raids/redis_exec.h>

extern "C" {
rai::kv::EvTcpListen *redis_create_listener( rai::kv::EvPoll *p,
                                             rai::kv::RoutePublish *sr,
                                      rai::kv::EvConnectionNotify *n ) noexcept;
}
namespace rai {
namespace kv {
  struct EvPrefetchQueue;
}
namespace ds {

struct EvRedisService : public kv::EvConnection, public RedisExec,
                        public PubDataLoss {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::EvListen & listen;

  EvRedisService( kv::EvPoll &p,  const uint8_t t,  kv::RoutePublish &sr,
                  kv::EvListen &l,  kv::EvConnectionNotify *n )
    : kv::EvConnection( p, t, n ),
      RedisExec( *sr.map, sr.ctx_id, sr.dbx_id, *this, sr, *this, p.timer ),
      listen( l ) {}
  void debug( void ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;

  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept;

  virtual void set_prefix( const char *pref,  size_t preflen ) noexcept;
  virtual void set_service( void *host,  uint16_t svc ) noexcept;
  virtual bool get_service( void *host,  uint16_t &svc ) const noexcept;
  virtual bool set_session( const char session[ MAX_SESSION_LEN ] ) noexcept;
  virtual size_t get_userid( char userid[ MAX_USERID_LEN ] ) const noexcept;
  virtual size_t get_session( uint16_t svc,
                              char session[ MAX_SESSION_LEN ] ) const noexcept;
  virtual size_t get_subscriptions( uint16_t svc,
                                    kv::SubRouteDB &subs ) noexcept;
  virtual size_t get_patterns( uint16_t svc,  int pat_fmt,
                               kv::SubRouteDB &pats ) noexcept;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( kv::PeerMatchArgs &ka ) noexcept;
  virtual void pub_data_loss( kv::EvPublish &pub ) noexcept;
};

struct EvRedisListen : public kv::EvTcpListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::RoutePublish & sub_route;
  void             * host;
  char               prefix[ MAX_PREFIX_LEN ];
  size_t             prefix_len;
  uint16_t           svc;

  EvRedisListen( kv::EvPoll &p, kv::RoutePublish &sr ) noexcept;
  EvRedisListen( kv::EvPoll &p ) noexcept;
  virtual EvSocket *accept( void ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept {
    return this->kv::EvTcpListen::listen2( ip, port, opts, "redis_listen",
                                           this->sub_route.route_id );
  }
  virtual void set_service( void *host,  uint16_t svc ) noexcept;
  virtual bool get_service( void *host,  uint16_t &svc ) const noexcept;
  virtual void set_prefix( const char *pref,  size_t preflen ) noexcept;
};

struct EvRedisUnixListen : public kv::EvUnixListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::RoutePublish & sub_route;
  void             * host;
  char               prefix[ MAX_PREFIX_LEN ];
  size_t             prefix_len;
  uint16_t           svc;

  EvRedisUnixListen( kv::EvPoll &p, kv::RoutePublish &sr ) noexcept;
  EvRedisUnixListen( kv::EvPoll &p ) noexcept;
  virtual EvSocket *accept( void ) noexcept;
  int listen( const char *sock,  int opts ) noexcept {
    return this->kv::EvUnixListen::listen2( sock, opts, "unix_listen",
                                            this->sub_route.route_id );
  }
  virtual void set_service( void *host,  uint16_t svc ) noexcept;
  virtual bool get_service( void *host,  uint16_t &svc ) const noexcept;
  virtual void set_prefix( const char *pref,  size_t preflen ) noexcept;
};

}
}

#endif
