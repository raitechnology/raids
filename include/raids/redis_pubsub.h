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

namespace rai {
namespace ds {

struct RedisSubRoute {
  ds_on_msg_t callback;
  void      * closure;
  uint64_t    msg_cnt;
  uint32_t    hash;
  uint16_t    len;
  char        value[ 2 ];

  void zero( void ) {
    this->callback = NULL;
    this->closure  = NULL;
    this->msg_cnt  = 0;
  }
  void incr( void ) {
    this->msg_cnt++;
  }
};

template <class RedisData>
struct RedisDataRoutePos {
  RedisData * rt;
  uint32_t v;
  uint16_t off;
};

enum RedisSubStatus {
  REDIS_SUB_OK        = 0,
  REDIS_SUB_EXISTS    = 1,
  REDIS_SUB_NOT_FOUND = 2
};

template <class RedisData>
struct RedisDataMap {
  kv::RouteVec<RedisData> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) noexcept;

  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len ) {
    RedisData *rt;
    return this->put( h, sub, len, rt );
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len,
                      RedisData *&rt ) {
    kv::RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->zero();
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  RedisSubStatus updcnt( uint32_t h,  const char *sub,  size_t len,
                         RedisData *&rt ) const {
    rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    rt->incr();
    return REDIS_SUB_OK;
  }
  RedisSubStatus find( uint32_t h,  const char *sub,  size_t len,
                       RedisData *&rt,  kv::RouteLoc &loc ) {
    rt = this->tab.find( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    return REDIS_SUB_OK;
  }
  /* remove tab[ sub ] */
  RedisSubStatus rem( uint32_t h,  const char *sub,  size_t len ) {
    if ( ! this->tab.remove( h, sub, len ) )
      return REDIS_SUB_NOT_FOUND;
    return REDIS_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RedisDataRoutePos< RedisData > &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RedisDataRoutePos< RedisData > &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

typedef RedisDataRoutePos< RedisSubRoute > RedisSubRoutePos;
typedef RedisDataMap< RedisSubRoute >      RedisSubMap;

struct RedisPatternRoute {
  ds_on_msg_t               callback;
  void                    * closure;
  pcre2_real_code_8       * re;
  pcre2_real_match_data_8 * md;
  uint64_t                  msg_cnt;
  uint32_t                  hash;
  uint16_t                  len;
  char                      value[ 2 ];

  void zero( void ) {
    this->callback = NULL;
    this->closure  = NULL;
    this->re       = NULL;
    this->md       = NULL;
    this->msg_cnt  = 0;
  }
  void incr( void ) {
    this->msg_cnt++;
  }
  void release( void );
};

template <class RedisPatData>
struct RedisPatDataMap {
  kv::RouteVec<RedisPatData> tab;

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
                      RedisPatData *&rt ) {
    kv::RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->zero();
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
  }
  /* iterate first tab[ sub ] */
  bool first( RedisDataRoutePos< RedisPatternRoute > &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RedisDataRoutePos< RedisPatternRoute > &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

typedef RedisDataRoutePos< RedisPatternRoute > RedisPatternRoutePos;
typedef RedisPatDataMap< RedisPatternRoute >   RedisPatternMap;

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

struct RedisContinue {
  RedisContinueMsg * continue_msg; /* the continuation that has this key */
  uint32_t           hash,         /* the hash of value */
                     keynum,       /* which key this is 0 -> keycnt -1 */
                     keycnt;       /* total keys */
  uint16_t           len;          /* length of key subject */
  char               value[ 2 ];   /* subject */

  void zero( void ) {
    this->continue_msg = NULL;
    this->keynum = 0;
    this->keycnt = 1;
  }
  void incr( void ) {}
};

typedef RedisDataRoutePos< RedisContinue > RedisContinuePos;
typedef RedisDataMap< RedisContinue >      RedisContinueMap;

}
}
#endif
#endif
