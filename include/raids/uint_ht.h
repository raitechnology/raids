#ifndef __rai_raids__uint_ht_h__
#define __rai_raids__uint_ht_h__

namespace rai {
namespace ds {

struct UIntHashTab {
  static const uint32_t SLOT_USED = 1U << 31;

  struct Elem {
    uint32_t hash;
    uint32_t val;
    bool is_used( void )      const { return ( this->hash & SLOT_USED ) != 0; }
    bool equals( uint32_t h ) const { return this->hash == ( h | SLOT_USED ); }
    void set( uint32_t h,  uint32_t v ) {
      this->hash = h | SLOT_USED;
      this->val  = v;
    }
  };

  uint32_t elem_count,  /* num elems used */
           tab_mask;    /* tab_size - 1 */
  Elem     tab[ 1 ];

  UIntHashTab( uint32_t sz ) : elem_count( 0 ), tab_mask( sz - 1 ) {
    ::memset( this->tab, 0, sizeof( this->tab[ 0 ] ) * sz );
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  bool     is_empty( void ) const { return this->elem_count == 0; }
  uint32_t tab_size( void ) const { return this->tab_mask + 1; }

  static size_t alloc_size( uint32_t sz ) {
    return sizeof( UIntHashTab ) + ( ( sz - 1 ) * sizeof( Elem ) );
  }
  /* preferred is between load of 33% and 66% */
  size_t preferred_size( void ) const {
    uint32_t cnt = this->elem_count + this->elem_count / 2;
    return cnt ? ( (size_t) 1 << ( 32 - __builtin_clz( cnt ) ) ) : 1;
  }
  /* alloc, copy, delete, null argument is ok */
  static UIntHashTab *resize( UIntHashTab *xht ) {
    size_t sz      = ( xht == NULL ? 1 : xht->preferred_size() );
    void * p       = ::malloc( UIntHashTab::alloc_size( sz ) );
    UIntHashTab * yht = NULL;
    if ( p != NULL ) {
      yht = new ( p ) UIntHashTab( sz );
      if ( xht != NULL ) {
        yht->copy( *xht );
        delete xht;
      }
    }
    return yht;
  }
  /* copy from cpy into this, does not check for duplicate entries or resize */
  void copy( const UIntHashTab &cpy ) {
    uint32_t sz = cpy.tab_size(), cnt = cpy.elem_count;
    this->elem_count += cnt; /* they should be unique */
    if ( cnt == 0 )
      return;
    for ( uint32_t i = 0; i < sz; i++ ) {
      if ( cpy.tab[ i ].is_used() ) {
        uint32_t h   = cpy.tab[ i ].hash,
                 v   = cpy.tab[ i ].val,
                 pos = h & this->tab_mask;
        for ( ; ; pos = ( pos + 1 ) & this->tab_mask ) {
          if ( ! this->tab[ pos ].is_used() ) {
            this->tab[ pos ].set( h, v );
            if ( --cnt == 0 )
              return;
            break;
          }
        }
      }
    }
  }
  /* find hash, return it's position and the value if found */
  bool find( uint32_t h,  uint32_t &pos,  uint32_t &val ) const {
    for ( pos = h & this->tab_mask; ; pos = ( pos + 1 ) & this->tab_mask ) {
      if ( ! this->tab[ pos ].is_used() )
        return false;
      if ( this->tab[ pos ].equals( h ) ) {
        val = this->tab[ pos ].val;
        return true;
      }
    }
  }
  /* set the hash at pos, which should be located using find() */
  void set( uint32_t h,  uint32_t pos,  uint32_t val ) {
    if ( ! this->tab[ pos ].is_used() )
      this->elem_count++;
    this->tab[ pos ].set( h, val );
  }
  /* udpate or insert hash */
  void upsert( uint32_t h,  uint32_t val ) {
    uint32_t pos, val2;
    this->find( h, pos, val2 );
    this->set( h, pos, val );
  }
  /* check if current size is preferred */
  bool need_resize( void ) const {
    return this->tab_size() != this->preferred_size();
  }
  /* remove by reorganizing table, check the natural position of elems directly
   * following the removed elem, leaving no find() gaps in the table */
  void remove( uint32_t pos ) {
    this->tab[ pos ].hash = 0;
    for (;;) {
      pos = ( pos + 1 ) & this->tab_mask;
      if ( ! this->tab[ pos ].is_used() )
        break;
      uint32_t h = this->tab[ pos ].hash,
               j = h & this->tab_mask;
      if ( pos != j ) {
        this->tab[ pos ].hash = 0;
        while ( this->tab[ j ].is_used() )
          j = ( j + 1 ) & this->tab_mask;
        this->tab[ j ].val  = this->tab[ pos ].val;
        this->tab[ j ].hash = h;
      }
    }
    this->elem_count--;
  }
};

}
}
#endif
