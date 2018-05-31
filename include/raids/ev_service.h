#ifndef __rai_raids__ev_service_h__
#define __rai_raids__ev_service_h__

#include <raids/ev_net.h>
#include <raids/redis_exec.h>

namespace rai {
namespace ds {

struct EvService : public EvConnection, public RedisExec {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvService( EvPoll &p ) : EvConnection( p, EV_SERVICE_SOCK ),
      RedisExec( *p.map, p.ctx_id, *this ) {}
  void process( void );
  void process_close( void ) {
    this->RedisExec::release();
  }
  void send_ok( void );
  void send_err_bad_args( void );
  void send_err_kv( int kstatus );
  void send_err_bad_cmd( void );
  void send_err_msg( int mstatus );
  void send_err_alloc_fail( void );
};

}
}

#endif
