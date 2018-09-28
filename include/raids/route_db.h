#ifndef __rai_raids__route_db_h__
#define __rai_raids__route_db_h__

#include <raids/uint_ht.h>
#include <raids/delta_coder.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>

namespace rai {
namespace ds {

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

struct RouteDB {
  static const uint32_t INI_SPC = 16;
  DeltaCoder    dc;             /* code          -> route list */
  UIntHashTab * xht,            /* route hash    -> code | code ref hash */
              * zht;            /* code ref hash -> code buf offset */
  uint32_t    * code_buf,       /* list of code ref, which is array of code */
              * code_spc_ptr,   /* temporary code space */
              * route_spc_ptr,  /* temporary route space */
                code_end,       /* end of code_buf[] list */
                code_size,      /* size of code_buf[] */
                code_free,      /* amount free between 0 -> code_end */
                code_spc_size,  /* size of code_spc_ptr */
                route_spc_size, /* size of route_spc_ptr */
                code_buf_spc[ INI_SPC * 4 ], /* initial code_buf[] */
                code_spc[ INI_SPC ],         /* initial code_spc_ptr[] */
                route_spc[ INI_SPC ];        /* initial code_route_ptr[] */

  RouteDB() : xht( 0 ), zht( 0 ), code_buf( 0 ), code_end( 0 ),
              code_size( INI_SPC * 4 ), code_free( 0 ),
              code_spc_size( INI_SPC ), route_spc_size( INI_SPC ) {
    this->code_buf      = this->code_buf_spc;
    this->code_spc_ptr  = this->code_spc;
    this->route_spc_ptr = this->route_spc;
  }

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

  uint32_t *make_code_space( uint32_t i );

  uint32_t *make_code_ref_space( uint32_t i,  uint32_t &off );

  void gc_code_ref_space( void );

  uint32_t compress_routes( uint32_t *routes,  uint32_t rcnt );

  uint32_t decompress_routes( uint32_t r,  uint32_t *&routes,  bool deref );

  uint32_t decompress_one( uint32_t r );

  void add_route( uint32_t hash,  uint32_t r );

  void del_route( uint32_t hash,  uint32_t r );

  uint32_t get_route( uint32_t hash,  uint32_t *&routes ) {
    uint32_t pos, val;
    if ( this->xht != NULL && this->xht->find( hash, pos, val ) )
      return this->decompress_routes( val, routes, false );
    return 0;
  }
  uint32_t get_route_count( uint32_t hash );
};

}
}
#endif
