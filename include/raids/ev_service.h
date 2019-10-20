#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raids/ev_tcp.h>
#include <raids/redis_exec.h>

namespace rai {
namespace ds {

struct EvPrefetchQueue;

struct EvRedisListen : public EvTcpListen {
  uint64_t timer_id;
  EvRedisListen( EvPoll &p );/* : EvTcpListen( p ) {}*/
  virtual void accept( void );
};

struct EvRedisService : public EvConnection, public RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvRedisService( EvPoll &p ) : EvConnection( p, EV_REDIS_SOCK ),
      RedisExec( *p.map, p.ctx_id, *this, p.sub_route ) {}
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
