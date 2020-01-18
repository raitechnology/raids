#ifndef __rai_raids__ev_capr_h__
#define __rai_raids__ev_capr_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raids/ev_tcp.h>
#include <raids/route_ht.h>

namespace rai {
namespace ds {

struct CaprSession;

struct EvCaprListen : public EvTcpListen {
  CaprSession * sess;
  uint64_t      timer_id;
  EvListenOps   ops;
  EvCaprListen( EvPoll &p );
  virtual void accept( void );
  int listen( const char *ip,  int port ) {
    return this->EvTcpListen::listen( ip, port, "capr-listen" );
  }
};

struct EvPrefetchQueue;
struct CaprMsgOut;
struct CaprMsgIn;

struct CaprSubRoute {
  uint32_t hash;
  uint32_t msg_cnt;
  uint16_t len;
  char     value[ 2 ];
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

enum CaprSubStatus {
  CAPR_SUB_OK        = 0,
  CAPR_SUB_EXISTS    = 1,
  CAPR_SUB_NOT_FOUND = 2
};

struct CaprSubRoutePos {
  CaprSubRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct CaprSubMap {
  RouteVec<CaprSubRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) {
    this->tab.release();
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  CaprSubStatus put( uint32_t h,  const char *sub,  size_t len ) {
    RouteLoc loc;
    CaprSubRoute * rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      return CAPR_SUB_OK;
    }
    return CAPR_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  CaprSubStatus updcnt( uint32_t h,  const char *sub,  size_t len ) const {
    CaprSubRoute * rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    rt->msg_cnt++;
    return CAPR_SUB_OK;
  }
  /* remove tab[ sub ] */
  CaprSubStatus rem( uint32_t h,  const char *sub,  size_t len ) {
    if ( ! this->tab.remove( h, sub, len ) )
      return CAPR_SUB_NOT_FOUND;
    return CAPR_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( CaprSubRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( CaprSubRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

struct CaprPatternRoute {
  uint32_t                  hash,
                            msg_cnt;
  pcre2_real_code_8       * re;
  pcre2_real_match_data_8 * md;
  uint16_t                  len;
  char                      value[ 2 ];

  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

struct CaprPatternRoutePos {
  CaprPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct CaprPatternMap {
  RouteVec<CaprPatternRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void );
  /* put in new sub
   * tab[ sub ] => {cnt} */
  CaprSubStatus put( uint32_t h,  const char *sub,  size_t len,
                      CaprPatternRoute *&rt ) {
    RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      rt->re = NULL;
      rt->md = NULL;
      return CAPR_SUB_OK;
    }
    return CAPR_SUB_EXISTS;
  }

  /* iterate first tab[ sub ] */
  bool first( CaprPatternRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( CaprPatternRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

struct EvCaprService : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }

  CaprSubMap     sub_tab;
  CaprPatternMap pat_tab;
  CaprSession  * sess;
  uint64_t       ms, bs,  /* msgs sent, bytes sent */
                 mr, br,  /* msgs recv, bytes recv */
                 timer_id;
  char           inbox[ 32 + 4 ]; /* _INBOX.127-000-000-001.6EB8C0CB.> */
  uint32_t       inboxlen;
  uint64_t       sid;
  EvConnectionOps ops;

  EvCaprService( EvPoll &p ) : EvConnection( p, EV_CAPR_SOCK, this->ops ) {}
  void initialize_state( uint64_t id ) {
    this->sess = NULL;
    this->ms = this->bs = 0;
    this->mr = this->br = 0;
    this->timer_id = id;
    this->inboxlen = 0;
    this->sid = 0;
  }
  void send( CaprMsgOut &rec,  size_t off,  const void *data, size_t data_len );
  void process( void );
  void exec_key_prefetch( EvKeyCtx & ) {}
  int exec_key_continue( EvKeyCtx & ) { return 0; }
  bool timer_expire( uint64_t tid,  uint64_t eid );
  void reassert_subs( CaprMsgIn &rec );
  void add_sub( CaprMsgIn &rec );
  void add_subscription( const char *sub,  uint32_t len,
                         const char *reply,  uint32_t replylen,  bool is_wild );
  void rem_sub( CaprMsgIn &rec );
  void rem_all_sub( void );
  bool fwd_pub( CaprMsgIn &rec );
  bool on_msg( EvPublish &pub );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  bool fwd_msg( EvPublish &pub,  const void *sid,  size_t sid_len );
  bool fwd_inbox( EvPublish &pub );
  void get_inbox_addr( EvPublish &pub,  const char *&subj,  uint8_t *addr );
  void release( void );
  void push_free_list( void );
  void pop_free_list( void );
  void pub_session( uint8_t code );
  void process_close( void ) {}
};

static inline bool is_rng( uint8_t c, uint8_t x, uint8_t y ) {
  return c >= x && c <= y;
}
static inline bool is_capr_pub( uint8_t c )  { return is_rng( c, 'a', 'z' ); }
static inline bool is_capr_sess( uint8_t c ) { return is_rng( c, ' ', '/' ); }
static inline bool is_capr_sub( uint8_t c )  { return is_rng( c, 'A', 'Z' ); }
static inline bool is_capr_meta( uint8_t c ) { return is_rng( c, '0', '@' ); }

static const uint8_t
  /* session ' ' -> '/' */
  CAPR_SESSION_INFO  = '!', /* session info are sent every 60 seconds w/stats */
  CAPR_SESSION_START = '+', /* sent at the start of a session */
  CAPR_SESSION_STOP  = '-', /* sent when the session ends */
  CAPR_SESSION_SUBS  = '/', /* sent to publishers by clients to notify */
                              /* subject interest when out of sync (reassert) */
  CAPR_SESSION_PUB   = '#', /* sent to subscribers to notify start/stop pub */

  /* meta '0' -> '@' */
  CAPR_REJECT   = '0',
  CAPR_ACK      = '1',
  CAPR_QUALITY  = '@',
  CAPR_PING     = '?',

  /* subs 'A' -> 'Z' */
  CAPR_CANCEL    = 'C', /* cancel a listen or a subscribe */
  CAPR_LISTEN    = 'L', /* listen does not expect an initial value */
  CAPR_SUBSCRIBE = 'S', /* subscribe client expects initial value or status */
  CAPR_SNAP_REQ  = 'X', /* one shot snapshot request */
  CAPR_DICT_REQ  = 'Z', /* one shot dictionary request */

  /* pubs 'a' -> 'z' */
  CAPR_DROP       = 'd', /* when publishers stop sending updates to a subject */
  CAPR_INITIAL    = 'i', /* initial subject message */
  CAPR_PUBLISH    = 'p', /* generic publish, any kind of msg, not categorized */
  CAPR_RECAP      = 'r', /* recap, generated by publisher for current state */
  CAPR_STATUS     = 's', /* a status message, not data */
  CAPR_UPDATE     = 'u', /* delta update */
  CAPR_VERIFY     = 'v', /* verify is delta that initializes if not cached */
  CAPR_SNAP_REPLY = 'x', /* one shot reply to a snapshot request */
  CAPR_DICT_REPLY = 'z'; /* one shot reply to a dictionary request */

static const uint32_t
  CAPR_SESSION_IVAL = 60;

struct CaprSession {
  uint64_t sid,           /* session id (unique endpoint) */
           stime;         /* start time */
  uint32_t start_count,   /* count of session start recvd */
           stop_count,    /* count of session stop recvd */
           info_count,    /* count of session info recvd */
           reassert_sent, /* count of reasserts sent */
           info_sent,     /* count of session info sent */
           id_len;        /* len msg in this->id */
  char   * addr,          /* addr of session */
         * user,          /* user of session */
         * host,          /* host of session */
         * app;           /* app  of session */
  uint8_t  addr_len,
           user_len,
           host_len,
           app_len;
  char     id[ 4 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  CaprSession() : sid( 0 ), stime( 0 ),
                  start_count( 0 ), stop_count( 0 ),
                  info_count( 0 ), reassert_sent( 0 ), info_sent( 0 ),
                  id_len( 0 ), addr( 0 ), user( 0 ), host( 0 ), app( 0 ),
                  addr_len( 0 ), user_len( 0 ), host_len( 0 ), app_len( 0 ) {}

  static CaprSession *create( const char *addr,
                              const char *user,
                              const char *host,
                              const char *app,
                              uint64_t sid );
  CaprSession *copy( void ) const;
};

static const uint8_t CAPR_MAGIC         = 0xca, /* hdr magic, first byte */
                     CAPR_IBX_PRESENT   = 0x80, /* hdr flags, fld present */
                     CAPR_SID_PRESENT   = 0x40, /* session id present */
                     CAPR_PTM_PRESENT   = 0x20, /* pub time present */
                     CAPR_RTM_PRESENT   = 0x10, /* route time present */
                     CAPR_CTR_PRESENT   = 0x08, /* update counter present */
                     CAPR_VERSION       = 0x01, /* hdr version number 1 */
                     CAPR_VERSION_MASK  = 0x03, /* 4 versions numbers */
                     CAPR_HDR_SIZE      = 12, /* capr frame header */
                     CAPR_SID_SIZE      = 8,  /* session id size */
                     CAPR_IBX_SIZE      = 12, /* inbox id size */
                     CAPR_PTM_SIZE      = 8,  /* publish time size */
                     CAPR_RTM_SIZE      = 8,  /* cache time size */
                     CAPR_CTR_SIZE      = 4;  /* update counter size */
static const size_t  CAPR_MAX_SUBJ_LEN  = 1032; /* 4 segs, 1024 encoded */

struct CaprMsgOut {
  uint8_t  capr_byte,       /* CAPR_MAGIC */
           code,            /* CAPR_LISTEN, CAPR_PING, etc */
           msg_enc,         /* (uint8_t) RAIMSG_TYPE_ID, or zero for no type */
           flags;           /* CAPR_xxx_PRESENT bits */
  uint32_t data_len,        /* length of data payload */
           subj_hash;       /* crc_c hash of subject */
  uint8_t  buf[ 2 * 1024 ]; /* space for subject, inbox, session */

  /* publish without publish time / create time */
  uint32_t encode_publish( CaprSession &sess,  const uint8_t *addr,
                           const char *subj,  uint8_t code,
                           uint32_t msg_len,  uint8_t msg_enc );
  /* publish with pub time from link and create time from source */
  uint32_t encode_publish_time( CaprSession &sess,  const uint8_t *addr,
                                const char *subj,  uint8_t code,
                                uint32_t msg_len,  uint8_t msg_enc,
                                uint64_t ptim,  uint64_t ctim,
                                uint32_t *counter );
  /* request with inbox reply */
  uint32_t encode_request( CaprSession &sess,  uint32_t inbox_id,
                           const char *subj,  uint8_t code );
  /* cancel sub or other non-inbox requests */
  uint32_t encode_cancel( CaprSession &sess,  const char *subj,  uint8_t code );
  /* request with inbox reply and message */
  uint32_t encode_request_msg( CaprSession &sess,  uint32_t inbox_id,
                               const char *subj,  uint8_t code,
                               uint32_t msg_len,  uint8_t msg_enc );
};

enum CaprDecodeStatus {
  DECODE_OK = 0,
  ERR_MISSING_SUBJECT   = -1,
  ERR_BAD_SUBJECT_SEG   = -2,
  ERR_TRUNCATED_SUBJECT = -3,
  ERR_TRUNCATED_MESSAGE = -4,
  ERR_BAD_PROTO_MAGIC   = -5,
  ERR_BAD_PROTO_VERSION = -6
};

struct CaprMsgIn {
  uint8_t   capr_byte, /* +--  CAPR_MAGIC */
            code,      /* |    CAPR_SNAP_REQ */
            msg_enc,   /* |    msg encoding */
            flags;     /* |    CAPR_xxx_PRESENT */
  uint32_t  data_len,  /* |    payload length */
            subj_hash, /* +-- 12 byte header always present + subject */
            msg_data_len,
            counter,
            subj_len;
  uint64_t  sid,
            ptime,
            rtime;
  uint8_t * subj,
          * msg_data,
          * addr;

  /* decode a message, return = 0 success, < 0 error, > 0 bytes needed */
  int32_t decode( uint8_t *capr_pkt,  size_t pkt_size );
  uint32_t get_subscription( char *s,  bool &is_wild );
  uint32_t get_inbox( char *buf );
  uint32_t get_subject( char *s );
};

}
}
#endif
