#ifndef __rai_raids__ev_client_h__
#define __rai_raids__ev_client_h__

#include <raids/ev_net.h>
#include <raids/redis_msg.h>
#include <raids/term.h>

namespace rai {
namespace ds {

struct EvCallback {
  virtual bool on_data( char *buf,  size_t &buflen );
#if 0
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
#endif
  virtual void on_close( void );
};

struct EvClient {
  EvCallback &cb;

  EvClient( EvCallback &callback ) : cb( callback ) {}
  /*virtual void send_msg( RedisMsg &msg );*/
  virtual void send_data( char *buf,  size_t size );
  void exec_key_prefetch( EvKeyCtx & ) {}
  int exec_key_continue( EvKeyCtx & ) { return 0; }
  bool timer_expire( uint64_t, uint64_t ) { return false; }
  bool hash_to_sub( uint32_t, char *, size_t & ) { return false; }
  bool on_msg( EvPublish & ) { return true; }
};

struct EvShm {
  kv::HashTab * map;
  uint32_t      ctx_id;

  EvShm() : map( 0 ), ctx_id( kv::MAX_CTX_ID ) {}
  ~EvShm();
  int open( const char *mn );
  void print( void );
  void close( void );
};

struct RedisExec;
struct EvShmClient : public EvShm, public EvClient, public StreamBuf,
                     public EvSocket {
  RedisExec * exec;
  int         pfd[ 2 ];

  EvShmClient( EvPoll &p,  EvCallback &callback )
    : EvClient( callback ), EvSocket( p, EV_SHM_SOCK ), exec( 0 ) {
    this->pfd[ 0 ] = this->pfd[ 1 ] = -1;
  }
  ~EvShmClient();

  int init_exec( void );
  /*virtual void send_msg( RedisMsg &msg );*/
  void process( void ) {}
  void read( void ) {}
  void write( void ) {}
  virtual void send_data( char *buf,  size_t size );
  bool on_msg( EvPublish &pub );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  void stream_to_msg( void );
  void process_shutdown( void );
  void process_close( void ) {}
  void release( void ) {
    this->StreamBuf::reset();
  }
};

struct EvShmSvc : public EvShm, public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  EvShmSvc( EvPoll &p ) : EvSocket( p, EV_SHM_SVC ) {}
  virtual ~EvShmSvc();

  int init_poll( void );
  virtual bool timer_expire( uint64_t tid,  uint64_t eid );
  virtual void read( void );              /* return true if recv more data */
  virtual void write( void );             /* return amount sent */
  virtual bool on_msg( EvPublish &pub );  /* fwd pub message, true if fwded */
  virtual bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  void exec_key_prefetch( EvKeyCtx & ) {}
  int exec_key_continue( EvKeyCtx & ) { return 0; }
  virtual void process( void );           /* process protocol */
  virtual void process_shutdown( void );  /* start shutdown */
  virtual void process_close( void );     /* finish close */
  virtual void release( void );           /* release allocations */
};

struct EvNetClient : public EvClient, public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMsg msg;         /* current msg */

  EvNetClient( EvPoll &p, EvCallback &callback,  EvSockType t = EV_CLIENT_SOCK )
    : EvClient( callback ), EvConnection( p, t ) {}
  /*virtual void send_msg( RedisMsg &msg );*/
  virtual void send_data( char *buf,  size_t size );
  void process( void );
  void process_close( void );
  void release( void ) {
    this->EvConnection::release_buffers();
  }
};

struct EvTerminal : public EvNetClient {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Term term;
  char * line;
  size_t line_len;

  EvTerminal( EvPoll &p,  EvCallback &callback )
    : EvNetClient( p, callback, EV_TERMINAL ), line( 0 ), line_len( 0 ) {}
  int start( void );
  void flush_out( void );
  void process( void );
  void finish( void );
  void printf( const char *fmt,  ... )
#if defined( __GNUC__ )
      __attribute__((format(printf,2,3)));
#else
      ;
#endif
  void process_line( const char *line );
};

struct EvMemcachedMerge;
struct EvUdpClient : public EvClient, public EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvMemcachedMerge * sav;
  uint16_t req_id;

  EvUdpClient( EvPoll &p, EvCallback &callback,
               EvSockType t = EV_CLIENTUDP_SOCK )
    : EvClient( callback ), EvUdp( p, t ), sav( 0 ), req_id( 0 ) {}
  void process( void );
  void process_close( void );
  void release( void );
  virtual void send_data( char *buf,  size_t size );
  void write( void );
};

}
}

#endif
