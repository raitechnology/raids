#ifndef __rai_raids__ev_memcached_h__
#define __rai_raids__ev_memcached_h__

#include <raikv/ev_net.h>
#include <raikv/ev_tcp.h>
#include <raids/memcached_exec.h>

namespace rai {
namespace ds {

struct EvMemcached {
  char     * recv; /* current input data, from tcp or udp */
  uint32_t & off,  /* offset of consumed */
           & len;  /* length recv extent */
  EvMemcached( char *b,  uint32_t &o,  uint32_t &l ) : recv( b ), off( o ),
    len( l ) {}
  int process_loop( MemcachedExec &mex,  kv::EvPrefetchQueue *q,
                    kv::StreamBuf &strm,  kv::EvSocket *svc ) noexcept;
};

struct MemcachedHdr {
  uint16_t req_id, /* request id from client */
           seqno,  /* sequence number of request total [0->total-1] */
           total,  /* total number of udp frames */
           opaque; /* spec is zero, could useful to extend req_id to 32 bit */
};
static const uint16_t MC_HDR_SIZE = sizeof( MemcachedHdr );

/* XXX: This method of merging incoming frames could cause request ids to be
 * returned in a different order than they were sent.  If the app requires
 * in-order request_ids, then a timer would be required to time out incomplete
 * requests */
struct EvMemcachedMerge {
  void * operator new( size_t, void *ptr ) { return ptr; }
  struct iovec  * sav_mhdr; /* array of incomplete frames */
  uint32_t        sav_len;  /* resizes based on the last hdr.total recvd */

  EvMemcachedMerge() : sav_mhdr( 0 ), sav_len( 0 ) {}
  /* try to merge mhdr[ idx ] into a contiguous buffer, while saving frames
   * when they are an incomplete unit: req_id is missing a seqno out of total */
  bool merge_frames( kv::StreamBuf &strm,  struct mmsghdr *mhdr,
                     uint32_t nmsgs,  uint32_t req_id,  uint32_t idx,
                     uint32_t total,  uint32_t size ) noexcept;
  void release( void ) noexcept;
};

struct EvMemcachedUdp : public kv::EvUdp {
  uint8_t execbuf[ sizeof( MemcachedExec ) ];
  MemcachedExec  * exec;     /* execution context */
  uint32_t       * out_idx;  /* index into strm.iov[] for each result */
  EvMemcachedMerge sav;
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvMemcachedUdp( kv::EvPoll &p )
    : kv::EvUdp( p, p.register_type( "memcached_udp" ) ),
      exec( 0 ), out_idx( 0 ) {}
  int listen( const char *ip,  int port,  int opts ) noexcept;
  void init( void ) noexcept;
  bool merge_inmsgs( uint32_t req_id,  uint32_t i,  uint32_t total,
                     uint32_t size ) noexcept;

  virtual void read( void ) noexcept final;
  virtual void write( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept final;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept final;
};

struct MemcachedUdpFraming {
  uint32_t       * out_idx;
  struct mmsghdr * out_mhdr,
                 * in_mhdr;
  kv::StreamBuf  & strm;
  uint32_t         nmsgs,
                   iov_cnt, /* how many iov[] pointers */
                   out_nmsgs; /* how many memcached udp 1400 byte frames */
  const uint32_t   frame_size;

  MemcachedUdpFraming( uint32_t *oi,  struct mmsghdr *im,  kv::StreamBuf &st,
                       uint32_t nm,  uint32_t fs = 1400 )
    : out_idx( oi ), out_mhdr( 0 ), in_mhdr( im ), strm( st ),
      nmsgs( nm ), iov_cnt( 0 ), out_nmsgs( 0 ), frame_size( fs ) {}
  bool construct_frames( void ) noexcept;
};


struct EvMemcachedListen : public kv::EvTcpListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvMemcachedListen( kv::EvPoll &p ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept;
  virtual bool accept( void ) noexcept;
};

struct EvMemcachedService : public kv::EvConnection, public MemcachedExec {
  void * operator new( size_t, void *ptr ) { return ptr; }

  EvMemcachedService( kv::EvPoll &p,  const uint8_t t,  MemcachedStats &st )
    : kv::EvConnection( p, t ),
      MemcachedExec( *p.map, p.ctx_id, p.dbx_id, *this, st ) {}

  virtual void read( void ) noexcept final;
  virtual void write( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual void key_prefetch( kv::EvKeyCtx &ctx ) noexcept final;
  virtual int  key_continue( kv::EvKeyCtx &ctx ) noexcept final;
};

}
}

#endif
