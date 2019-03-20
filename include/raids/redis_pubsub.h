#ifndef __rai_raids__redis_pubsub_h__
#define __rai_raids__redis_pubsub_h__


extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raids/route_ht.h>

namespace rai {
namespace ds {

struct RedisSubRoute {
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

struct RedisSubRoutePos {
  RedisSubRoute * rt;
  uint32_t v;
  uint16_t off;
};

enum RedisSubStatus {
  REDIS_SUB_OK        = 0,
  REDIS_SUB_EXISTS    = 1,
  REDIS_SUB_NOT_FOUND = 2
};

struct RedisSubMap {
  RouteVec<RedisSubRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop();
  }
  void release( void ) {
    this->tab.release();
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len ) {
    RouteLoc loc;
    RedisSubRoute * rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  RedisSubStatus updcnt( uint32_t h,  const char *sub,  size_t len ) const {
    RedisSubRoute * rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    rt->msg_cnt++;
    return REDIS_SUB_OK;
  }
  /* remove tab[ sub ] */
  RedisSubStatus rem( uint32_t h,  const char *sub,  size_t len ) {
    if ( ! this->tab.remove( h, sub, len ) )
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
};

struct RedisPatternRoute {
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

struct RedisPatternRoutePos {
  RedisPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct RedisPatternMap {
  RouteVec<RedisPatternRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop();
  }
  void release( void );
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RedisSubStatus put( uint32_t h,  const char *sub,  size_t len,
                      RedisPatternRoute *&rt ) {
    RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return REDIS_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      rt->re = NULL;
      rt->md = NULL;
      return REDIS_SUB_OK;
    }
    return REDIS_SUB_EXISTS;
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
};

}
}

#endif
