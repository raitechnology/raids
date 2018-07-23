#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raids/ev_net.h>
#include <raids/redis_exec.h>

namespace rai {
namespace ds {

struct EvPrefetchQueue;

struct EvService : public EvConnection, public RedisExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvService( EvPoll &p ) : EvConnection( p, EV_SERVICE_SOCK ),
      RedisExec( *p.map, p.ctx_id, *this, p.single_thread ) {}
  void process( bool use_prefetch );
  void process_close( void ) {
    this->RedisExec::release();
  }
  void debug( void );
  virtual void release( void );
  void push_free_list( void );
  void pop_free_list( void );
};

}
}

#endif
