#ifndef __rai_raids__ev_client_h__
#define __rai_raids__ev_client_h__

#include <raids/ev_net.h>
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
struct EvShmClient : public EvShm, public EvClient, public StreamBuf,
                     public EvSocket {
  static const uint8_t EV_SHM_SOCK = 10; /* local shm client (used with terminal) */
  RedisExec * exec;
  int         pfd[ 2 ];

  EvShmClient( EvPoll &p,  EvCallback &callback )
    : EvClient( callback ), EvSocket( p, EV_SHM_SOCK ), exec( 0 ) {
    this->pfd[ 0 ] = this->pfd[ 1 ] = -1;
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
  virtual bool on_msg( EvPublish &pub ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
  void data_callback( void ) noexcept;
};

struct EvShmApi : public EvShm, public StreamBuf, public EvSocket {
  static const uint8_t EV_SHM_API = 11; /* local shm api client */
  RedisExec * exec;
  int         pfd[ 2 ];
  uint64_t    timer_id;

  EvShmApi( EvPoll &p ) noexcept;
  int init_exec( void ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h,  char *key,
                            size_t &keylen ) noexcept final;
  virtual bool on_msg( EvPublish &pub ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
};

struct EvShmSvc : public EvShm, public EvSocket {
  static const uint8_t EV_SHM_SVC = 12;/* pubsub service */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  EvShmSvc( EvPoll &p ) : EvSocket( p, EV_SHM_SVC ) {
    this->sock_opts = OPT_NO_POLL | OPT_NO_CLOSE;
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
  virtual bool on_msg( EvPublish &pub ) noexcept;
  virtual void key_prefetch( EvKeyCtx &ctx ) noexcept;
  virtual int  key_continue( EvKeyCtx &ctx ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct EvNetClient : public EvClient, public EvConnection {
  static const uint8_t EV_CLIENT_SOCK = 4; /* redis client protocol */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisMsg        msg;         /* current msg */

  EvNetClient( EvPoll &p, EvCallback &callback,  uint8_t t = EV_CLIENT_SOCK )
    : EvClient( callback ), EvConnection( p, t ) {}
  /*virtual void send_msg( RedisMsg &msg );*/
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
};

struct EvTerminal : public EvNetClient {
  static const uint8_t EV_TERMINAL = 5; /* redis terminal (converts redis proto to json) */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Term term;
  char * line;
  size_t line_len;

  EvTerminal( EvPoll &p,  EvCallback &callback )
    : EvNetClient( p, callback, EV_TERMINAL ), line( 0 ), line_len( 0 ) {
    /* don't close stdin stdout */
    this->sock_opts = OPT_NO_CLOSE;
  }
  int start( void ) noexcept;
  void flush_out( void ) noexcept;
  void finish( void ) noexcept;
  void printf( const char *fmt,  ... ) noexcept
#if defined( __GNUC__ )
      __attribute__((format(printf,2,3)));
#else
      ;
#endif
  void process_line( const char *line ) noexcept;
  /* EvNetClient */
  virtual void process( void ) noexcept final;
};

struct EvMemcachedMerge;
struct EvUdpClient : public EvClient, public EvUdp {
  static const uint8_t EV_CLIENTUDP_SOCK = 15; /* udp client */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvMemcachedMerge * sav;
  uint16_t           req_id;

  EvUdpClient( EvPoll &p, EvCallback &callback, uint8_t t = EV_CLIENTUDP_SOCK )
    : EvClient( callback ), EvUdp( p, t ), sav( 0 ), req_id( 0 ) {}
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
  /* EvClient */
  virtual void send_data( char *buf,  size_t size ) noexcept final;
};

}
}

#endif
