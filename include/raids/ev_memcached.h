#ifndef __rai_raids__ev_memcached_h__
#define __rai_raids__ev_memcached_h__

#include <raids/ev_net.h>
#include <raids/ev_tcp.h>
#include <raids/memcached_exec.h>

namespace rai {
namespace ds {

struct EvMemcachedListen : public EvTcpListen {
  EvMemcachedListen( EvPoll &p ) : EvTcpListen( p ) {}
  virtual void accept( void );
};

struct EvPrefetchQueue;

struct EvMemcachedService : public EvConnection, public MemcachedExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvMemcachedService( EvPoll &p ) : EvConnection( p, EV_MEMCACHED_SOCK ),
      MemcachedExec( *p.map, p.ctx_id, *this ) {}
  void process( bool use_prefetch );
  void release( void );
  void push_free_list( void );
  void pop_free_list( void );
};

}
}

#endif
