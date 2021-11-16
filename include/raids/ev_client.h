#ifndef __rai_raids__ev_client_h__
#define __rai_raids__ev_client_h__

#include <raikv/ev_net.h>
#include <raids/redis_msg.h>
#include <raids/term.h>

namespace rai {
namespace ds {

struct EvCallback {
  virtual bool on_data( char *buf,  size_t &buflen ) noexcept;
#if 0
  virtual void on_msg( RedisMsg &msg );
  virtual void on_err( char *buf,  size_t buflen,  RedisMsgStatus status );
#endif
  virtual void on_close( void ) noexcept;
};

struct EvClient {
  EvCallback &cb;

  EvClient( EvCallback &callback ) : cb( callback ) {}
  /*virtual void send_msg( RedisMsg &msg );*/
  virtual void send_data( char *buf,  size_t size ) noexcept;
};

struct RedisExec;
struct EvShmClient : public kv::EvShm, public kv::StreamBuf,
                     public kv::EvSocket, public EvClient {
  RedisExec * exec;

  EvShmClient( kv::EvPoll &p,  EvCallback &callback )
    : kv::EvSocket( p, p.register_type( "shm_sock" ) ),
      EvClient( callback ), exec( 0 ) {
    this->sock_opts = kv::OPT_NO_POLL;
  }
  ~EvShmClient() noexcept;

  int init_exec( void ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool hash_to_sub( uint32_t h,  char *key,
                            size_t &keylen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
  void data_callback( void ) noexcept;
};

struct EvShmApi : public kv::EvShm, public kv::StreamBuf, public kv::EvSocket {
  RedisExec * exec;
  uint64_t    timer_id;

  EvShmApi( kv::EvPoll &p ) noexcept;
  int init_exec( void ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h,  char *key,
                            size_t &keylen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
};

struct EvShmSvc : public kv::EvShm, public kv::EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  EvShmSvc( kv::EvPoll &p ) : kv::EvSocket( p, p.register_type( "shm_svc" ) ) {
    this->sock_opts = kv::OPT_NO_POLL;
  }
  virtual ~EvShmSvc() noexcept;

  int init_poll( void ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct EvNetClient : public EvClient, public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMsg msg;         /* current msg */

  EvNetClient( kv::EvPoll &p, EvCallback &callback )
    : EvClient( callback ),
      kv::EvConnection( p, p.register_type( "net_client" ) ) {}
  /*virtual void send_msg( RedisMsg &msg );*/
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
};

struct EvTerminal : public EvClient, public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Term term;
  char * line;
  size_t line_len;
  int    stdin_fd, stdout_fd;

  EvTerminal( kv::EvPoll &p,  EvCallback &callback );
  int start( void ) noexcept;
  void flush_out( void ) noexcept;
  void finish( void ) noexcept;
  void output( const char *buf,  size_t buflen ) noexcept;
  int printf( const char *fmt,  ... ) noexcept
#if defined( __GNUC__ )
      __attribute__((format(printf,2,3)));
#else
      ;
#endif
  int vprintf( const char *fmt, va_list args ) noexcept;
  void process_line( const char *line ) noexcept;
  /* EvConnection */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct EvMemcachedMerge;
struct EvMemcachedUdpClient : public EvClient, public kv::EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvMemcachedMerge * sav;
  uint16_t           req_id;

  EvMemcachedUdpClient( kv::EvPoll &p, EvCallback &callback )
    : EvClient( callback ),
      kv::EvUdp( p, p.register_type( "memcached_udp_client" ) ),
      sav( 0 ), req_id( 0 ) {}
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
};

}
}

#endif
