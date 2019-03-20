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

struct SysWildSub {
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


struct CodeRef {
  uint32_t hash, ref, ecnt, rcnt, code;

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
struct RoutePublishData {
  unsigned int prefix : 7,
               rcount : 25;
  uint32_t     hash;
  uint32_t   * routes;

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

typedef struct kv::PrioQueue<RoutePublishData *, RoutePublishData::is_greater>
 RoutePublishQueue;

struct KvPrefHash;
struct RoutePublish {
  bool publish( EvPublish &pub,  uint32_t *rcount_total,  uint8_t pref_cnt,
                KvPrefHash *ph );
  bool hash_to_sub( uint32_t r,  uint32_t h,  char *key,  size_t &keylen );
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
