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

static const uint32_t KV_CTX_INBOX_COUNT = 16;

struct KvBacklog {
  KvBacklog      * next, * back;
  size_t           cnt;
  void          ** vec;
  kv::msg_size_t * siz;
  uint64_t         vec_size;

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvBacklog( size_t n,  void **v,  kv::msg_size_t *sz,  uint64_t vsz )
    : next( 0 ), back( 0 ), cnt( n ), vec( v ), siz( sz ), vec_size( vsz ) {}
};

struct KvMcastKey {
  uint64_t        hash1, /* hash of mcast name: _SYS.MC */
                  hash2;
  kv::KeyFragment kbuf;  /* the key name (_SYS.MC) */

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMcastKey( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen ) {
    this->kbuf.keylen = namelen;
    ::memcpy( this->kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->kbuf, this->hash1, this->hash2 );
  }
};

struct KvInboxKey : public kv::KeyCtx {
  uint64_t        hash1,            /* hash of inbox name */
                  hash2,
                  ibx_pos,
                  ibx_seqno,
                  read_cnt,
                  read_msg,
                  read_size;
  kv::ValueCtr    ibx_value_ctr;    /* the value serial ctr, track change */
  kv::KeyFragment ibx_kbuf;         /* the inbox name (_SYS.XX) XX=ctx_id hex */

  KvInboxKey( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen )
    : kv::KeyCtx( kctx ), hash1( 0 ), hash2( 0 ),
      ibx_pos( 0 ), ibx_seqno( 0 ), read_cnt( 0 ), read_msg( 0 ),
      read_size( 0 ) {
    this->ibx_value_ctr.zero();
    this->ibx_kbuf.keylen = namelen;
    ::memcpy( this->ibx_kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->ibx_kbuf, this->hash1, this->hash2 );
  }
};

struct KvMsgQueue {
  KvMsgQueue * next, * back;     /* links when have backlog msgs */
  uint64_t     hash1,            /* hash of inbox name */
               hash2,
               high_water_size,  /* current high water send size */
               src_session_id,   /* src session id */
               src_seqno,        /* src sender seqno */
               pub_size,         /* total size sent */
               pub_cnt,          /* total msg vectors sent */
               pub_msg,          /* total msgs sent */
               backlog_size,     /* size of messages in backlog */
               backlog_cnt,      /* count of messages in backlog */
               backlog_progress, /* if backlog cleared some msgs */
               signal_cnt;       /* number of signals sent */
  uint32_t     ibx_num;
  bool         need_signal;      /* need to kill() recver to wake them */
  kv::DLinkList<KvBacklog> backlog; /* backlog for this queue */
  kv::WorkAllocT< 4096 > tmp;       /* msg queue mem when overflow */
  kv::KeyFragment kbuf;             /* the inbox name (_SYS.XX) XX=ctx_id hex */
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMsgQueue( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen,
              uint32_t n )
    : next( 0 ), back( 0 ), high_water_size( 128 * 1024 ),
      src_session_id( 0 ), src_seqno( 0 ), pub_size( 0 ),
      pub_cnt( 0 ), pub_msg( 0 ), backlog_size( 0 ), backlog_cnt( 0 ),
      signal_cnt( 0 ), ibx_num( n ), need_signal( true ) {
    this->kbuf.keylen = namelen;
    ::memcpy( this->kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->kbuf, this->hash1, this->hash2 );
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

struct KvMsg {
  uint32_t session_id_w[ 2 ], /* session at src (4 byte aligned) */
           seqno_w[ 2 ];      /* seqno at src */
  uint32_t size;              /* size including header */
  uint8_t  src,               /* src = ctx_id */
           dest_start,        /* the recipient */
           dest_end,          /* if > start, forward msg to others */
           msg_type;          /* what class of message (sub, unsub, pub) */

  static const char * msg_type_string( uint8_t msg_type ) noexcept;
  const char * msg_type_string( void ) const noexcept;
  void print( void ) noexcept;
  void print_sub( void ) noexcept;

  uint64_t session_id( void ) const {
    uint64_t sid;
    ::memcpy( &sid, this->session_id_w, sizeof( sid ) );
    return sid;
  }
  uint64_t seqno( void ) const {
    uint64_t seq;
    ::memcpy( &seq, this->seqno_w, sizeof( seq ) );
    return seq;
  }
  void set_session_id( uint64_t sid ) {
    ::memcpy( this->session_id_w, &sid, sizeof( sid ) );
  }
  void set_seqno( uint64_t seq ) {
    ::memcpy( this->seqno_w, &seq, sizeof( seq ) );
  }
  bool is_valid( uint32_t sz ) const {
    return sz == this->size &&
           this->msg_type <= KV_MSG_PUBLISH &&
           ( this->src | this->dest_start | this->dest_end ) < KV_MAX_CTX_ID;
  }
};

typedef union {
  uint8_t  b[ 8 ];
  uint64_t w;
} range_t;

struct KvRouteCache {
  CubeRoute128 cr;
  range_t range;
  size_t cnt;
};

struct KvMsgList {
  KvMsgList * next, * back;
  range_t     range;
  uint32_t    off, cnt;
  KvMsg       msg;

  void init_route( void ) {
    this->range.w = 0;
    this->off     = 0;
    this->cnt     = 0;
  }
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
  uint8_t  code,     /* 'K' */
           msg_enc;  /* MD msg encoding type */
  char     buf[ 2 ]; /* subject\0\reply\0 */

/* 00   03 9a eb 63  51 d7 c0 1c <- session
        02 00 00 00  00 00 00 00 <- seqno
   10   40 00 00 00              <- size
        02 01 01 07              <- src, dstart, dend, type (7 = publish)
        22 6d 9d ef              <- hash
        04 00 00 00              <- msg size
   20   04 00                    <- sublen
   22   00 00                    <- replylen
   24   70 02                    <- code, msg_enc
   26   70 69 6e 67  00          <- ping
   2B   00                       <- reply
   2C   4b                       <- src type (K)
   2D   01                       <- prefix cnt
   2E   40                       <- prefix 64 = full (0-63 = partial)
   2F   22 6d 9d ef              <- hash of prefix (dup of hash)
   33   00                       <- align
   34   01 00 00 00              <- msg_data (4 byte aligned) */
  char * subject( void ) {
    return this->buf;
  }
  char * reply( void ) {
    return &this->buf[ this->sublen + 1 ];
  }
  /* src type + prefix hashes + msg data */
  uint8_t * trail( void ) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    return (uint8_t *) &this->buf[ this->sublen + 1 + this->replylen + 1 ];
#pragma GCC diagnostic pop
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
  char src_type( void ) {
    char * ptr = (char *) this->trail();
    return ptr[ 0 ];
  }
  void set_src_type( char t ) {
    char * ptr = (char *) this->trail();
    ptr[ 0 ] = t;
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
  uint8_t prefix_cnt( void ) {
    uint8_t * ptr = this->trail();
    return ptr[ 1 ];
  }
  void set_prefix_cnt( uint8_t cnt ) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
    uint8_t * ptr = this->trail();
    ptr[ 1 ] = cnt;
#pragma GCC diagnostic pop
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
      sizeof( KvSubMsg ) - 2 + sublen + 1 + replylen + 1
      + 1 /* src */ + 1 /* pref_cnt */
      + (size_t) pref_cnt * 5 /* pref + hash */, sizeof( uint32_t ) )
      + kv::align<size_t>( msg_size, sizeof( uint32_t ) );
  }
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

struct KvSubNotifyList {
  KvSubNotifyList * next, * back;
  bool in_list;
  virtual void on_sub( KvSubMsg &submsg ) noexcept;
  KvSubNotifyList() : next( 0 ), back( 0 ), in_list( false ) {}
};

enum KvPubSubFlag {
  KV_DO_NOTIFY     = 1, /* whether to notify external processes of inbox msgs */
  KV_INITIAL_SCAN  = 2, /* set after initial scan is complete */
  KV_READ_INBOX    = 4  /* set when reading inbox */
};

struct KvPubSub : public EvSocket {
  static const uint16_t KV_NO_SIGUSR = 1;
  uint16_t     ctx_id,                 /* my endpoint */
               flags;                  /* KvPubSubFlags above */
  uint32_t     dbx_id;                 /* db xref */
  kv::HashSeed hs;                     /* seed for db */
  uint64_t     session_id,             /* session id of the my endpoint */
               next_seqno,             /* next seqno of msg sent */
               timer_id,               /* my timer id */
               timer_cnt,              /* count of timer expires */
               time_ns,                /* current coarse time */
               send_size,              /* amount sent to other inboxes */
               send_cnt,               /* count of msgs sent, includes sendq */
               route_size,             /* size of msgs routed from send queue */
               route_cnt,              /* count of msgs routed from send queue*/
               sigusr_recv_cnt,
               inbox_msg_cnt,
               mc_pos;                 /* hash position of the mcast route */
  kv::ValueCtr mc_value_ctr;           /* the value serial ctr, track change */
  CubeRoute128 mc_cr;                  /* the current mcast route */
  range_t      mc_range;               /* the range of current mcast */
  size_t       mc_cnt;                 /* count of bits in mc_range */
  KvRouteCache rte_cache[ 256 ];       /* cache of cube routes */

  KvSubTab     sub_tab;                /* subject route table to shm */
  kv::KeyCtx   kctx,                   /* a kv context for send/recv msgs */
               rt_kctx;                /* a kv context for route lookup */

  EvSocketOps  ops;
  CubeRoute128 dead_cr;                /* clean up old route inboxes */

  KvInboxKey * rcv_inbox[ KV_CTX_INBOX_COUNT ];
  KvMsgQueue * snd_inbox[ KV_MAX_CTX_ID ]; /* _SYS.IBX.xx : ctx send queues */
  KvMcastKey & mcast;                  /* _SYS.MC : ctx_ids to shm network */

  kv::DLinkList<KvMsgList>       sendq;       /* sendq is to the network */
  kv::DLinkList<KvSubNotifyList> sub_notifyq; /* notify for subscribes */
  kv::DLinkList<KvMsgQueue>      backlogq;    /* ibx which have pending sends */

  kv::WorkAllocT< 256 >  rt_wrk,     /* wrk for rt_kctx kv */
                         wrk;        /* wrk for kctx, send, mcast */
  kv::WorkAllocT< 8192 > ib_wrk,     /* wrk for inbox recv */ 
                         wrkq;       /* for pending sends to shm */

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSub( EvPoll &p,  int sock,  void *mcptr,  const char *mc,  size_t mclen,
            uint32_t xid )
      : EvSocket( p, EV_KV_PUBSUB, this->ops ),
        ctx_id( p.ctx_id ), flags( KV_DO_NOTIFY ), 
        dbx_id( xid ), session_id( 0 ), next_seqno( 0 ),
        timer_id( (uint64_t) EV_KV_PUBSUB << 56 ), timer_cnt( 0 ),
        time_ns( p.current_coarse_ns() ),
        send_size( 0 ), send_cnt( 0 ), route_size( 0 ), route_cnt( 0 ),
        sigusr_recv_cnt( 0 ), inbox_msg_cnt( 0 ), mc_pos( 0 ), mc_cnt( 0 ),
        kctx( *p.map, xid ),
        rt_kctx( *p.map, xid ),
        mcast( *(new ( mcptr ) KvMcastKey( this->kctx, mc, mclen )) ) {
    this->mc_value_ctr.zero();
    this->mc_cr.zero();
    this->mc_range.w = 0;
    ::memset( (void *) this->rte_cache, 0, sizeof( this->rte_cache ) );
    this->dead_cr.zero();
    ::memset( this->rcv_inbox, 0, sizeof( this->rcv_inbox ) );
    ::memset( this->snd_inbox, 0, sizeof( this->snd_inbox ) );
    this->PeerData::init_peer( sock, NULL, "kv" );
    this->session_id = p.map->ctx[ p.ctx_id ].rng.next();
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->hs );
  }

  static KvPubSub *create( EvPoll &p,  uint8_t db_num ) noexcept;
  void print_backlog( void ) noexcept;
  bool register_mcast( void ) noexcept;
  bool clear_mcast_dead_routes( void ) noexcept;
  bool unregister_mcast( void ) noexcept;
  enum UpdateEnum { DEACTIVATE = 0, ACTIVATE = 1, USE_FIND = 2 };
  bool update_mcast_sub( const char *sub,  size_t len,  int flags ) noexcept;
  bool update_mcast_route( void ) noexcept;
  bool push_backlog( KvMsgQueue &ibx,  size_t cnt,  void **vec,
                     kv::msg_size_t *siz,  uint64_t vec_size ) noexcept;
  bool clear_backlog( KvMsgQueue &ibx ) noexcept;
  bool send_msg( KvMsg &msg ) noexcept;
  bool send_vec( size_t cnt,  void **vec,  kv::msg_size_t *siz,
                 size_t dest,  uint64_t vec_size ) noexcept;
  KvMsg *create_kvmsg( KvMsgType mtype,  size_t sz ) noexcept;
  KvSubMsg *create_kvpublish( uint32_t h,  const char *sub,  size_t len,
                              const uint8_t *pref,  const uint32_t *hash,
                              uint8_t pref_cnt,  const char *reply, size_t rlen,
                              const void *msgdata,  size_t msgsz,
                              char src_type,  KvMsgType mtype,
                              uint8_t code,  uint8_t msg_enc ) noexcept;
  KvSubMsg *create_kvsubmsg( uint32_t h,  const char *sub,  size_t len,
                             char src_type,  KvMsgType mtype,  const char *rep,
                             size_t rlen ) noexcept;
  KvSubMsg *create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t len,
                              const char *prefix,  uint8_t prefix_len,
                              char src_type,  KvMsgType mtype ) noexcept;
  void do_sub( uint32_t h,  const char *sub,  size_t len,
               uint32_t sub_id,  uint32_t rcnt,  char src_type,
               const char *rep = NULL,  size_t rlen = 0 ) noexcept;
  void do_unsub( uint32_t h,  const char *sub,  size_t len,
                 uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void do_psub( uint32_t h,  const char *pattern,  size_t len,
                const char *prefix,  uint8_t prefix_len,
                uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void do_punsub( uint32_t h,  const char *pattern,  size_t len,
                  const char *prefix,  uint8_t prefix_len,
                  uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void forward_sub( KvSubMsg &submsg ) noexcept;
  void process( void ) noexcept;
  void scan_ht( void ) noexcept;
  void exec_key_prefetch( EvKeyCtx & ) {}
  int exec_key_continue( EvKeyCtx & ) { return 0; }
  bool timer_expire( uint64_t tid, uint64_t ) noexcept;
  void release( void ) {}
  void process_shutdown( void ) noexcept;
  void process_close( void ) noexcept;
  void write_send_queue( CubeRoute128 &used ) noexcept;
  void notify_peers( CubeRoute128 &used ) noexcept;
  void write( void ) noexcept;
  bool get_sub_mcast( const char *sub,  size_t len,
                      CubeRoute128 &cr ) noexcept;
  void route_msg_from_shm( KvMsg &msg ) noexcept;
  void read( void ) noexcept;
  size_t read_inbox( bool read_until_empty ) noexcept;
  size_t read_inbox2( KvInboxKey &ibx,  bool read_until_empty ) noexcept;
  bool on_msg( EvPublish &pub ) noexcept;
  void publish_status( KvMsgType mtype ) noexcept;
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept;
};

}
}

#endif
