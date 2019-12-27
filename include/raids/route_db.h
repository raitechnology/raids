#ifndef __rai_raids__route_db_h__
#define __rai_raids__route_db_h__

#include <raids/uint_ht.h>
#include <raids/delta_coder.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/prio_queue.h>

namespace rai {
namespace ds {

#define SYS_WILD_PREFIX "_SYS.W"

struct SysWildSub { /* wildcard prefix: _SYS.W3.XYZ */
  size_t len;
  char   sub[ sizeof( SYS_WILD_PREFIX ) - 1 + 4 + 64 ];

  SysWildSub( const char *s,  uint8_t prefix_len ) {
    size_t  i = sizeof( SYS_WILD_PREFIX ) - 1;
    uint8_t j = prefix_len;
    ::memcpy( this->sub, SYS_WILD_PREFIX, i );
    if ( prefix_len > 9 ) { /* max 63 */
      this->sub[ i++ ] = ( j / 10 ) + '0';
      j %= 10;
    }
    this->sub[ i++ ] = j + '0';
    this->sub[ i++ ] = '.'; 
    if ( s != NULL ) {
      ::memcpy( &this->sub[ i ], s, prefix_len );
      i += prefix_len;
    }
    this->sub[ i ] = '\0';
    this->len = i;
  }
};

struct CodeRef { /* refs to a route space, which is a list of fds */
  uint32_t hash, /* hash of the route */
           ref,  /* how many refs to this route (multiple subs) */
           ecnt, /* encoded number of ints needed to represent route (zipped) */
           rcnt, /* count of elems in a route (not encoded) */
           code; /* the routes code */

  CodeRef( uint32_t *co,  uint32_t ec,  uint32_t rc,  uint32_t h ) :
    hash( h ), ref( 1 ), ecnt( ec ), rcnt( rc ) {
    ::memcpy( &this->code, co, sizeof( co[ 0 ] ) * ec );
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  bool equals( const uint32_t *c,  uint32_t ec ) const {
    const uint32_t *c2 = &this->code;
    if ( ec != this->ecnt ) return false;
    for (;;) {
      if ( *c++ != *c2++ )
        return false;
      if ( --ec == 0 )
        return true;
    }
  }
  static uint32_t hash_code( const uint32_t *code,  uint32_t ecnt,
                             uint32_t seed ) {
    return kv_crc_c( code, ecnt * sizeof( code[ 0 ] ), seed ) & 0x7fffffffU;
  }
  static size_t alloc_words( uint32_t ecnt ) {
    return sizeof( CodeRef ) / sizeof( uint32_t ) + ( ecnt - 1 );
  }
  size_t word_size( void ) const {
    return alloc_words( this->ecnt );
  }
};

struct EvPublish;
struct RoutePublishData {   /* structure for publish queue heap */
  unsigned int prefix : 7,  /* prefix size */
               rcount : 25; /* count of routes */
  uint32_t     hash;        /* hash of prefix */
  uint32_t   * routes;      /* routes for this hash */

  static inline uint32_t precmp( uint32_t p ) {
    return ( ( ( p & 63 ) << 1 ) + 1 ) | ( p >> 6 );
  }
  static bool is_greater( RoutePublishData *r1,  RoutePublishData *r2 ) {
    if ( r1->routes[ 0 ] > r2->routes[ 0 ] )
      return true;
    if ( r1->routes[ 0 ] < r2->routes[ 0 ] )
      return false;
    return precmp( r1->prefix ) > precmp( r2->prefix );
  }
};

/* queue for publishes, to merge multiple sub matches into one pub, for example:
 *   SUB* -> 1, 7
 *   SUBJECT* -> 1, 3, 7
 *   SUBJECT_MATCH -> 1, 3, 8
 * The publish to SUBJECT_MATCH has 3 matches for 1 and 2 matches for 7 and 3
 * The heap sorts by route id (fd) to merge the publishes to 1, 3, 7, 8 */
typedef struct kv::PrioQueue<RoutePublishData *, RoutePublishData::is_greater>
 RoutePublishQueue;

struct KvPrefHash;
struct RoutePublish {
  int32_t keyspace_cnt, /* count of __keyspace@N__ subscribes active */
          keyevent_cnt, /* count of __keyevent@N__ subscribes active */
          listblkd_cnt, /* count of __listblkd@N__ subscribes active */
          zsetblkd_cnt, /* count of __zsetblkd@N__ subscribes active */
          strmblkd_cnt; /* count of __strmblkd@N__ subscribes active */
  uint16_t key_flags;    /* bits set for key subs above (EKF_KEYSPACE_FWD|..) */
  bool forward_msg( EvPublish &pub,  uint32_t *rcount_total,  uint8_t pref_cnt,
                    KvPrefHash *ph );
  bool hash_to_sub( uint32_t r,  uint32_t h,  char *key,  size_t &keylen );
  void update_keyspace_count( const char *sub,  size_t len,  int add );
  void notify_sub( uint32_t h,  const char *sub,  size_t len,
                   uint32_t sub_id,  uint32_t rcnt,  char src_type,
                   const char *rep = NULL,  size_t rlen = 0 );
  void notify_unsub( uint32_t h,  const char *sub,  size_t len,
                     uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void notify_psub( uint32_t h,  const char *pattern,  size_t len,
                    const char *prefix,  uint8_t prefix_len,
                    uint32_t sub_id,  uint32_t rcnt,  char src_type );
  void notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                      const char *prefix,  uint8_t prefix_len,
                      uint32_t sub_id,  uint32_t rcnt,  char src_type );
  bool add_timer_seconds( int id,  uint32_t ival,  uint64_t timer_id,
                          uint64_t event_id );
  bool add_timer_millis( int id,  uint32_t ival,  uint64_t timer_id,
                         uint64_t event_id );
  bool remove_timer( int id,  uint64_t timer_id,  uint64_t event_id );

  RoutePublish() : keyspace_cnt( 0 ), keyevent_cnt( 0 ), listblkd_cnt( 0 ),
                   zsetblkd_cnt( 0 ), strmblkd_cnt( 0 ), key_flags( 0 ) {}
};

struct RouteDB {
  static const uint32_t INI_SPC = 16;
  struct PushRouteSpc {
    uint32_t   size;
    uint32_t * ptr;
    uint32_t   spc[ INI_SPC ];
    PushRouteSpc() : size( INI_SPC ) {
      this->ptr = this->spc;
    }
  };

  RoutePublish & rte;
  DeltaCoder     dc;             /* code          -> route list */
  UIntHashTab  * xht,            /* route hash    -> code | code ref hash */
               * zht;            /* code ref hash -> code buf offset */
  uint64_t       pat_mask;       /* mask of subject prefixes, up to 64 */
  uint32_t     * code_buf,       /* list of code ref, which is array of code */
               * code_spc_ptr,   /* temporary code space */
               * route_spc_ptr,  /* temporary route space */
                 code_end,       /* end of code_buf[] list */
                 code_size,      /* size of code_buf[] */
                 code_free,      /* amount free between 0 -> code_end */
                 code_spc_size,  /* size of code_spc_ptr */
                 route_spc_size, /* size of route_spc_ptr */
                 code_buf_spc[ INI_SPC * 4 ], /* initial code_buf[] */
                 code_spc[ INI_SPC ],         /* initial code_spc_ptr[] */
                 route_spc[ INI_SPC ],        /* initial code_route_ptr[] */
                 pre_seed[ 64 ],  /* hash seed of the prefix: _SYS.W<N>. */
                 pre_count[ 64 ]; /* count of subjects sharing a prefix N */
  PushRouteSpc   push_route_spc[ 64 ];

  RouteDB( RoutePublish &rp ) : rte( rp ), xht( 0 ), zht( 0 ), code_buf( 0 ),
              code_end( 0 ), code_size( INI_SPC * 4 ), code_free( 0 ),
              code_spc_size( INI_SPC ), route_spc_size( INI_SPC ) {
    this->code_buf      = this->code_buf_spc;
    this->code_spc_ptr  = this->code_spc;
    this->route_spc_ptr = this->route_spc;
    this->init_prefix_seed();
  }

  void init_prefix_seed( void );

  bool first_hash( uint32_t &pos,  uint32_t &h,  uint32_t &v ) {
    if ( this->xht != NULL && this->xht->first( pos ) ) {
      this->xht->get( pos, h, v );
      return true;
    }
    return false;
  }
  bool next_hash( uint32_t &pos,  uint32_t &h,  uint32_t &v ) {
    if ( this->xht->next( pos ) ) {
      this->xht->get( pos, h, v );
      return true;
    }
    return false;
  }
  uint32_t *make_route_space( uint32_t i );

  uint32_t *make_push_route_space( uint8_t n,  uint32_t i );

  uint32_t *make_code_space( uint32_t i );

  uint32_t *make_code_ref_space( uint32_t i,  uint32_t &off );

  void gc_code_ref_space( void );

  uint32_t compress_routes( uint32_t *routes,  uint32_t rcnt );

  uint32_t decompress_routes( uint32_t r,  uint32_t *&routes,  bool deref );

  uint32_t push_decompress_routes( uint8_t n,  uint32_t r,  uint32_t *&routes );

  uint32_t decompress_one( uint32_t r );

  uint32_t add_route( uint32_t hash,  uint32_t r );

  uint32_t del_route( uint32_t hash,  uint32_t r );

  uint32_t add_pattern_route( uint32_t hash,  uint32_t r,  uint16_t pre_len );

  uint32_t del_pattern_route( uint32_t hash,  uint32_t r,  uint16_t pre_len );

  uint32_t prefix_seed( size_t prefix_len ) {
    if ( prefix_len > 63 )
      return this->pre_seed[ 63 ];
    return this->pre_seed[ prefix_len ];
  }
  bool is_member( uint32_t hash,  uint32_t x );

  uint32_t get_route( uint32_t hash,  uint32_t *&routes ) {
    uint32_t pos, val;
    if ( this->xht != NULL && this->xht->find( hash, pos, val ) )
      return this->decompress_routes( val, routes, false );
    return 0;
  }
  uint32_t push_get_route( uint8_t n,  uint32_t hash,  uint32_t *&routes ) {
    uint32_t pos, val;
    if ( this->xht != NULL && this->xht->find( hash, pos, val ) )
      return this->push_decompress_routes( n, val, routes );
    return 0;
  }

  uint32_t get_route_count( uint32_t hash );
};

}
}
#endif
