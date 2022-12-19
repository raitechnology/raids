#ifndef __rai_raids__redis_pubsub_h__
#define __rai_raids__redis_pubsub_h__

#ifdef __cplusplus
extern "C" {
#endif

struct pcre2_real_code_8;
struct pcre2_real_match_data_8;

#ifdef __cplusplus
}

#include <raids/redis_api.h>
#include <raikv/route_ht.h>
#include <raikv/ev_publish.h>
#include <raimd/md_msg.h>

namespace rai {
namespace ds {

struct RedisRouteData {
  ds_on_msg_t callback;
  void      * closure;
};

struct RedisContinueMsg;
struct RedisContinueData {
  RedisContinueMsg * continue_msg; /* the continuation that has this key */
  uint32_t           keynum,       /* which key this is 0 -> keycnt -1 */
                     keycnt;       /* total keys */
};

union RedisSubData {
  RedisRouteData    rte;
  RedisContinueData cont;
};

struct RedisContinuePtr {
  uint32_t hash;     /* hash of this subject */
  uint16_t len,      /* length of subject */
           save_len; /* length of save data */
  char   * value;    /* the subject which notifies that a key is changed */
};

enum {
  CM_WAIT_LIST = 1, /* in RedisExec::wait_list */
  CM_CONT_LIST = 2, /* in RedisExec::cont_list */
  CM_CONT_TAB  = 4, /* in RedisExec::continue_tab */
  CM_TIMER     = 8, /* has timer */
  CM_TIMEOUT   = 16,/* timer expired */
  CM_RELEASE   = 32,/* mark release */
  CM_PUB_HIT   = 64 /* publish hit */
};

struct RedisContinueMsg {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  RedisContinueMsg * next, /* list links if multiple continuations pending */
                   * back;
  RedisContinuePtr * ptr;     /* subject keys in this msg */
  uint16_t           keycnt,  /* count of ptr[] */
                     state;   /* CM_WAIT_LIST, CM_CONT_LIST, CM_EXPIRED */
  uint32_t           msgid;   /* event_id for timer */
  char             * msg;     /* the redis msg ascii buffer */
  size_t             msglen;  /* length of msg[] */

  RedisContinueMsg( size_t mlen,  uint16_t kcnt ) noexcept;
};

enum RedisSubStatus {
  REDIS_SUB_OK        = 0,
  REDIS_SUB_EXISTS    = 1,
  REDIS_SUB_NOT_FOUND = 2
};

enum RedisSubState {
  SUB_STATE_ROUTE_DATA    = 1,
  SUB_STATE_CONTINUE_DATA = 2,
  SUB_STATE_REMOVED       = 4
};

struct RedisSubRoute {
  RedisSubData data;
  uint32_t     msg_cnt,
               ibx_id,
               hash;
  uint16_t     state,
               len;
  char         value[ 2 ];

  RedisRouteData & rte( void ) { return this->data.rte; }
  RedisContinueData & cont( void  ) { return this->data.cont; }
  bool is_route( void ) const {
    return ( this->state & SUB_STATE_ROUTE_DATA ) != 0;
  }
  bool is_continue( void ) const {
    return ( this->state & SUB_STATE_CONTINUE_DATA ) != 0;
  }
  void zero( void ) {
    ::memset( &this->data, 0, sizeof( this->data ) );
    this->msg_cnt = 0;
    this->ibx_id  = 0;
    this->state   = 0;
  }
};

struct RedisSubRoutePos {
  RedisSubRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct RedisSubMap {
  kv::RouteVec<RedisSubRoute> tab;
  uint32_t sub_count,
           cont_count,
           next_inbox_id;

  RedisSubMap() : sub_count( 0 ), cont_count( 0 ), next_inbox_id( 0 ) {}

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len,
                      RedisSubRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->zero();
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       RedisSubRoute *&rt ) const {
    rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    return REDIS_SUB_OK;
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       bool &collision ) {
    kv::RouteLoc loc;
    RedisSubRoute *rt;
    uint32_t hcnt;
    rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return REDIS_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return REDIS_SUB_OK;
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       RedisSubRoute *&rt,  kv::RouteLoc &loc ) {
    rt = this->tab.find( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    return REDIS_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RedisSubRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RedisSubRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* remove tab[ sub ] */
  RedisSubStatus rem( uint32_t h,  const char *sub,  size_t len,
                      uint16_t kind,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    RedisSubRoute * rt = this->tab.find2( h, sub, len, loc, hcnt );
    collision = ( hcnt > 1 );
    if ( rt == NULL || ( rt->state & kind ) == 0 )
      return REDIS_SUB_NOT_FOUND;
    this->tab.remove( loc );
    return REDIS_SUB_OK;
  }

  bool rem_collision( RedisSubRoute *rt ) {
    kv::RouteLoc loc;
    RedisSubRoute * rt2;
    rt->state |= SUB_STATE_REMOVED;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        if ( ( rt2->state & SUB_STATE_REMOVED ) == 0 )
          return true;
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
  void release( void ) noexcept;
};

struct RedisWildMatch {
  RedisWildMatch          * next,
                          * back;
  ds_on_msg_t               callback;
  void                    * closure;
  pcre2_real_code_8       * re;         /* pcre match the subject */
  pcre2_real_match_data_8 * md;
  uint32_t                  msg_cnt;    /* count of msgs matched */
  uint16_t                  len;        /* length of the pattern subject */
  char                      value[ 2 ]; /* the pattern subject */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisWildMatch( size_t patlen,  const char *pat,  pcre2_real_code_8 *r,
                  pcre2_real_match_data_8 *m )
    : next( 0 ), back( 0 ), callback( 0 ), closure( 0 ), re( r ), md( m ),
      msg_cnt( 0 ), len( (uint16_t) patlen ) {
    ::memcpy( this->value, pat, patlen );
    this->value[ patlen ] = '\0';
  }
  static RedisWildMatch *create( size_t patlen,  const char *pat,
                           pcre2_real_code_8 *r, pcre2_real_match_data_8 *m ) {
    size_t sz = sizeof( RedisWildMatch ) + patlen - 2;
    void * p  = ::malloc( sz );
    if ( p == NULL ) return NULL;
    return new ( p ) RedisWildMatch( patlen, pat, r, m );
  }
};

struct RedisPatternRoute {
  uint32_t                      hash,
                                count;
  kv::DLinkList<RedisWildMatch> list;
  uint16_t                      len;
  char                          value[ 2 ];

  void zero( void ) {
    this->count = 0;
    this->list.init();
  }
  void release( void ) noexcept;
};

struct RedisPatternRoutePos {
  RedisPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};
/*typedef RedisDataRoutePos< RedisPatternRoute > RedisPatternRoutePos;*/

struct RedisPatternMap {
  kv::RouteVec<RedisPatternRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) noexcept;
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len,
                      RedisPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->zero();
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       RedisPatternRoute *&rt ) {
    rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    return REDIS_SUB_OK;
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       RedisPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    return this->find( h, sub, len, loc, rt, collision );
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       kv::RouteLoc &loc,  RedisPatternRoute *&rt,
                       bool &collision ) {
    uint32_t hcnt;
    rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return REDIS_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return REDIS_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RedisPatternRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RedisPatternRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  bool rem_collision( RedisPatternRoute *rt,  RedisWildMatch *m ) {
    kv::RouteLoc        loc;
    RedisPatternRoute * rt2;
    RedisWildMatch    * m2;
    m->msg_cnt = ~(uint32_t) 0;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        for ( m2 = rt2->list.tl; m2 != NULL; m2 = m2->back ) {
          if ( m2->msg_cnt != ~(uint32_t) 0 )
            return true;
        }
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};

struct PubDataLoss;
struct RedisMsgTransform {
  md::MDMsgMem  spc;
  const void  * msg;
  PubDataLoss * notify;
  uint32_t      msg_len,
                msg_enc;
  bool          is_ready,
                is_redis;
  RedisMsgTransform( kv::EvPublish &pub,  PubDataLoss *n )
    : msg( pub.msg ), notify( n ), msg_len( pub.msg_len ),
      msg_enc( pub.msg_enc ), is_ready( false ), is_redis( false ) {}
  void check_transform( void ) {
    if ( this->msg_len == 0 || this->msg_enc == md::MD_STRING )
      return;
    if ( this->msg_enc == md::MD_MESSAGE ) {
      if ( RedisMsg::valid_type_char( ((const char *) this->msg)[ 0 ] ) ) {
        this->is_redis = true;
        return;
      }
    }
    this->transform();
  }
  void transform( void ) noexcept;
};

#if 0
typedef RedisDataRoutePos< RedisContinue > RedisContinuePos;

struct RedisContinueMap : public RedisDataMap< RedisContinue > {
  void release( void ) noexcept;

  bool rem_collision( RedisContinue *rt ) {
    kv::RouteLoc loc;
    RedisContinue * rt2;
    rt->mark = 1;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        if ( rt2->mark != 1 )
          return true;
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};
#endif
}
}
#endif
#endif
