#ifndef __rai_raids__kv_pubsub_h__
#define __rai_raids__kv_pubsub_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raids/stream_buf.h>
#include <raids/ev_net.h>
#include <raids/cube_route.h>
#include <raids/route_ht.h>

namespace rai {
namespace ds {

struct EvPublish;
struct RouteDB;

struct KvMsgQueue {
  uint64_t        hash1,          /* hash of inbox name */
                  hash2,
                  src_session_id, /* src session id */
                  ibx_seqno,      /* recv msg list seqno, out-lives session */
                  src_seqno;      /* src sender seqno */
  kv::KeyFragment kbuf;
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMsgQueue( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen )
    : src_session_id( 0 ), ibx_seqno( 0 ), src_seqno( 0 ) {
    this->kbuf.keylen = namelen;
    ::memcpy( this->kbuf.u.buf, name, namelen );
    kctx.ht.hdr.get_hash_seed( kctx.db_num, this->hash1, this->hash2 );
    this->kbuf.hash( this->hash1, this->hash2 ); 
  } 
};

struct KvMsg {
  uint64_t session_id, /* session at src */
           seqno;      /* seqno at src */
  uint32_t size;       /* size including header */
  uint8_t  src,        /* src = ctx_id */
           dest_start, /* the recipient */
           dest_end,   /* if > start, forward msg to others */
           msg_type;   /* what class of message (sub, unsub, pub) */
};

typedef union {
  uint8_t  b[ 8 ];
  uint64_t w;
} range_t;

struct KvMsgList {
  KvMsgList * next, * back;
  range_t     range;
  KvMsg       msg;
};

struct KvPrefHash {
  uint8_t pref;
  uint8_t hash[ 4 ];

  uint32_t get_hash( void ) const {
    uint32_t h;
    ::memcpy( &h, this->hash, sizeof( uint32_t ) );
    return h;
  }
  void set_hash( uint32_t h ) {
    ::memcpy( this->hash, &h, sizeof( uint32_t ) );
  }
};

struct KvSubMsg : public KvMsg {
  uint32_t hash,     /* hash of subject */
           msg_size; /* size of message data */
  uint16_t sublen,   /* length of subject, not including null char */
           replylen; /* length of reply, not including null char */
  uint8_t  code,
           msg_enc,
           pad1,
           pad2;
  char     buf[ 4 ];

  char * subject( void ) {
    return this->buf;
  }
  char * reply( void ) {
    return &this->buf[ this->sublen + 1 ];
  }
  uint8_t * trail( void ) {
    return (uint8_t *) &this->buf[ this->sublen + 1 + this->replylen + 1 ];
  }
  void set_subject( const char *s,  uint16_t len ) {
    this->sublen = len;
    ::memcpy( this->buf, s, len );
    this->buf[ len ] = '\0';
  }
  void set_reply( const char *s,  uint16_t len ) {
    this->replylen = len;
    ::memcpy( &this->buf[ this->sublen + 1 ], s, len );
    this->buf[ this->sublen + 1 + len ] = '\0';
  }
  char & src_type( void ) {
    char * ptr = (char *) this->trail();
    return ptr[ 0 ];
  }
  void * msg_data( void ) {
    uint32_t off = kv::align<uint32_t>( this->msg_size, sizeof( uint32_t ) );
    return &((char *) (void *) this)[ this->size - off ];
  }
  void set_msg_data( const void *p,  uint32_t len ) {
    uint32_t off = kv::align<uint32_t>( len, sizeof( uint32_t ) );
    this->msg_size = len;
    ::memcpy( &((char *) (void *) this)[ this->size - off ], p, len );
  }
  uint8_t & prefix_cnt( void ) {
    uint8_t * ptr = this->trail();
    return ptr[ 1 ];
  }
  KvPrefHash * prefix_array( void ) {
    uint8_t * ptr = this->trail();
    return (KvPrefHash *) (void *) &ptr[ 2 ];
  }
  KvPrefHash & prefix_hash( uint8_t i ) {
    return this->prefix_array()[ i ];
  }
  static size_t calc_size( size_t sublen,  size_t replylen,  size_t msg_size,
                           uint8_t pref_cnt ) {
    return kv::align<size_t>(
      sizeof( KvSubMsg ) - 4 + sublen + 1 + replylen + 1
      + 1 /* src */ + 1 /* pref_cnt */
      + (size_t) pref_cnt * 5 /* pref + hash */, sizeof( uint32_t ) )
      + kv::align<size_t>( msg_size, sizeof( uint32_t ) );
  }
};

enum KvMsgType {
  KV_MSG_HELLO = 0,
  KV_MSG_BYE,
  KV_MSG_STATUS,
  KV_MSG_SUB,
  KV_MSG_UNSUB,
  KV_MSG_PSUB,
  KV_MSG_PUNSUB,
  KV_MSG_PUBLISH
};

static inline bool is_kv_bcast( uint8_t msg_type ) {
  return msg_type < KV_MSG_PUBLISH; /* all others flood the network */
}

struct KvSubRoute {
  uint32_t hash;
  uint8_t  rt_bits[ sizeof( CubeRoute128 ) ];
  uint16_t len;
  char     value[ 2 ];
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

typedef RouteVec<KvSubRoute> KvSubTab;

struct KvLast {
  uint8_t start, end, cnt;

  void set( uint8_t start, uint8_t end, uint8_t cnt ) {
    this->start = start; this->end = end; this->cnt = cnt;
  }
  bool equals( uint8_t start,  uint8_t end,  KvSubMsg &submsg,
               KvMsgList *l ) const {
    if ( start == this->start && end == this->end ) {
      KvSubMsg &sub = (KvSubMsg &) l->msg;
      return sub.hash == submsg.hash &&
             sub.sublen == submsg.sublen &&
             ::memcmp( sub.subject(), submsg.subject(), sub.sublen ) == 0;
    }
    return false;
  }
};

struct KvPubSub : public EvSocket {
  uint16_t     ctx_id,                 /* my endpoint */
               pad[ 3 ];
  uint64_t     seed1, seed2,           /* seeds of the shm keys */
               session_id,             /* session id of the my endpoint */
               next_seqno;             /* next seqno of msg sent */

  KvSubTab     sub_tab;                /* subject route table to shm */
  kv::KeyCtx   kctx,                   /* a kv context for send/recv msgs */
               rt_kctx;                /* a kv context for route lookup */

  KvMsgQueue * inbox[ KV_MAX_CTX_ID ], /* _SYS.IBX.xx : inbox of each context */
             & mcast;                  /* _SYS.MC : ctx_ids to shm network */

  kv::WorkAllocT< 1024 >   wrk,        /* wrk for kctx */
                           rt_wrk,     /* wrk for rt_kctx kv */
                           wrkq;       /* for pending sends to shm */
  kv::DLinkList<KvMsgList> sendq;      /* sendq is to the network */

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSub( EvPoll &p,  int sock,  void *mcptr,  const char *mc,  size_t mclen )
    : EvSocket( p, EV_KV_PUBSUB ),
      ctx_id( p.ctx_id ), seed1( 0 ), seed2( 0 ), session_id( 0 ),
      next_seqno( 0 ),
      kctx( *p.map, p.map->ctx[ p.ctx_id ], p.map->ctx[ p.ctx_id ].stat2,
            p.map->ctx[ p.ctx_id ].db_num2, NULL ),
      rt_kctx( *p.map, p.map->ctx[ p.ctx_id ], p.map->ctx[ p.ctx_id ].stat2,
               p.map->ctx[ p.ctx_id ].db_num2, NULL ),
      mcast( *(new ( mcptr ) KvMsgQueue( this->kctx, mc, mclen )) ) {
    ::memset( this->inbox, 0, sizeof( this->inbox ) );
    this->EvSocket::fd = sock;
    this->session_id = p.map->ctx[ p.ctx_id ].rng.next();
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->seed1,
                                     this->seed2 );
  }

  static KvPubSub *create( EvPoll &p );
  bool register_mcast( bool activate );
  bool subscribe_mcast( const char *sub,  size_t len,  bool activate,
                        bool use_find );
  bool get_mcast_route( CubeRoute128 &cr );
  bool send_msg( KvMsg &msg );
  bool send_vec( size_t cnt,  void *vec,  uint64_t *siz,  size_t dest );
  KvMsg *create_kvmsg( KvMsgType mtype,  size_t sz );
  KvSubMsg *create_kvpublish( uint32_t h,  const char *sub,  size_t len,
                              const uint8_t *pref,  const uint32_t *hash,
                              uint8_t pref_cnt,  const char *reply, size_t rlen,
                              const void *msgdata,  size_t msgsz,
                              char src_type,  KvMsgType mtype,
                              uint8_t code,  uint8_t msg_enc );
  KvSubMsg *create_kvsubmsg( uint32_t h,  const char *sub,  size_t len,
                             char src_type,  KvMsgType mtype );
  KvSubMsg *create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t len,
                              const char *prefix,  uint8_t prefix_len,
                              char src_type,  KvMsgType mtype );
  void notify_sub( uint32_t h,  const char *sub,  size_t len,
                   uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void notify_unsub( uint32_t h,  const char *sub,  size_t len,
                     uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void notify_psub( uint32_t h,  const char *pattern,  size_t len,
                    const char *prefix,  uint8_t prefix_len,
                    uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                      const char *prefix,  uint8_t prefix_len,
                      uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void process( bool use_prefetch );
  void process_shutdown( void );
  void process_close( void );
  void write( void );
  bool get_sub_mcast( const char *sub,  size_t len,  CubeRoute128 &cr );
  void route_msg_from_shm( KvMsg &msg );
  void read( void );
  bool publish( EvPublish &pub );
  void publish_status( KvMsgType mtype );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
};

}
}

#endif
