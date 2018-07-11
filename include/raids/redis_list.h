#ifndef __rai_raids__redis_list_h__
#define __rai_raids__redis_list_h__

namespace rai {
namespace ds {

enum ListStatus {
  LIST_OK        = 0,
  LIST_NOT_FOUND = 1,
  LIST_SPLIT     = 2,
  LIST_FULL      = 3
};

static const char *
list_status_string[] = { "ok", "not found", "split", "full" };

template <class UIntSig, class UIntType>
struct ListStorage {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  const UIntSig  list_sig;
  const UIntType list_index_mask,
                 list_data_mask;

  size_t index_mask( void ) const { return (size_t) this->list_index_mask; }
  size_t data_mask( void )  const { return (size_t) this->list_data_mask; }
  size_t index_size( void ) const { return (size_t) this->list_index_mask+1; }
  size_t data_size( void )  const { return (size_t) this->list_data_mask+1; }
  size_t max_count( void )  const { return (size_t) this->list_index_mask; }
  /* wrap len around */
  size_t offset( size_t start,  ssize_t len ) const {
    return ( start + len ) & this->data_mask();
  }

  UIntType first,
           count,
           data_start,
           data_len,
           idx[ 0 ];

  ListStorage() {}
  ListStorage( const UIntSig sig,  const size_t idx_size,
               const size_t dat_size ) :
    list_sig( sig ), list_index_mask( (UIntType) ( idx_size - 1 ) ),
    list_data_mask( (UIntType) ( dat_size - 1 ) ) {}

  void init( void ) {
    this->first      = 0;
    this->count      = 0;
    this->data_start = 0;
    this->data_len   = 0;
    this->idx[ 0 ]   = 0;
  }
  void *blob( size_t off ) const {
    return &((uint8_t *) (void *) &this->idx[ this->index_size() ])[ off ];
  }
  bool empty( void ) const {
    return this->count == 0;
  }
  bool data_full( size_t size ) const {
    return size + (size_t) this->data_len >
           this->data_mask();/* can't store data_mask + 1 */
  }
  bool full( size_t size ) const {
    /* idx[] needs at least one empty slot for start and end */
    return (size_t) this->count >= this->max_count() ||
           this->data_full( size );
  }
  /* copy into circular buffer */
  void copy_into( const void *data,  size_t size,  size_t start ) {
    if ( start + size <= this->data_size() )
      ::memcpy( this->blob( start ), data, size );
    else {
      size_t len = this->data_size() - start;
      ::memcpy( this->blob( start ), data, len );
      ::memcpy( this->blob( 0 ), &((const uint8_t *) data)[ len ], size - len );
    }
  }
  /* return a reference to the nth element starting from off */
  UIntType &index_ref( size_t n ) {
    return this->idx[ ( (size_t) this->first + n ) & this->index_mask() ];
  }
  /* push data/size at tail */
  ListStatus rpush( const void *data,  size_t size ) {
    if ( this->full( size ) )
      return LIST_FULL;
    size_t start = this->get_offset( this->count ),
           end   = this->offset( start, size );
    this->index_ref( ++this->count ) = (UIntType) end;
    this->copy_into( data, size, start );
    this->data_len += size;
    return LIST_OK;
  }
  /* push data/size at head */
  ListStatus lpush( const void *data,  size_t size ) {
    if ( this->full( size ) )
      return LIST_FULL;
    size_t end   = this->get_offset( 0 ),
           start = this->offset( end, -size );
    this->first = ( this->first - 1 ) & this->index_mask();
    this->count++;
    this->data_start = start;
    this->index_ref( 0 ) = (UIntType) start;
    this->copy_into( data, size, start );
    this->data_len += size;
    return LIST_OK;
  }
  /* return data offset at idx, if end is zero then return data_size */
  size_t get_offset( size_t i,  bool end = false ) const {
    i = ( this->first + i ) & this->index_mask();
    size_t j = this->idx[ i ];
    return ( ! end || j != 0 || i == this->first ||
               this->idx[ ( i - 1 ) & this->index_mask() ] == 0 )
             ? j : this->data_size();
  }
  /* fetch nth item, if wrapped around buffer, p2/sz2 will be set */
  ListStatus lindex( size_t n,  const void *&p,  size_t &sz,
                     const void *&p2,  size_t &sz2 ) const {
    sz = sz2 = 0;
    if ( n >= (size_t) this->count )
      return LIST_NOT_FOUND;
    size_t start = this->get_offset( n ),
           end   = this->get_offset( n+1, true );
    p = this->blob( start );
    if ( end >= start ) { /* not wrapped */
      sz = end - start;
      return LIST_OK;
    }
    /* wrapped, head is at data end, tail is at data start */
    size_t len = this->data_size() - start;
    sz = len;
    p2 = this->blob( 0 );
    sz2 = end;
    return LIST_SPLIT;
  }
  /* scan tail to head to find data and return pos */
  ListStatus scan_rev( const void *data,  size_t size,  size_t &pos ) {
    size_t start, end, len;
    start = this->get_offset( this->count );
    for ( size_t n = pos; n > 0; ) {
      end   = start;
      start = this->get_offset( --n );
      if ( end == 0 && start != 0 )
        end = this->data_size();
      if ( start <= end ) {
        len = end - start;
        if ( len == size &&
             ::memcmp( this->blob( start ), data, size ) == 0 ) {
          pos = n;
          return LIST_OK;
        }
      }
      else {
        len = this->data_size() - start;
        if ( len + end == size &&
             ::memcmp( this->blob( start ), data, len ) == 0 &&
             ::memcmp( this->blob( 0 ), &((char *) data)[ len ], end ) == 0 ) {
          pos = n;
          return LIST_OK;
        }
      }
    }
    return LIST_NOT_FOUND;
  }
  /* scan head to tail to find data and return pos+1 */
  ListStatus scan_fwd( const void *data,  size_t size,  size_t &pos ) {
    size_t start, end, len;
    size_t n = pos;
    end = this->get_offset( n, true );
    for ( ; n < this->count; n++ ) {
      start = ( end == this->data_size() ) ? 0 : end;
      end   = this->get_offset( n + 1, true );
      if ( start <= end ) {
        len = end - start;
        if ( len == size &&
             ::memcmp( this->blob( start ), data, size ) == 0 ) {
          pos = n;
          return LIST_OK;
        }
      }
      else {
        len = this->data_size() - start;
        if ( len + end == size &&
             ::memcmp( this->blob( start ), data, len ) == 0 &&
             ::memcmp( this->blob( 0 ), &((char *) data)[ len ], end ) == 0 ) {
          pos = n;
          return LIST_OK;
        }
      }
    }
    return LIST_NOT_FOUND;
  }
  /* find an piv in the list and insert data before/after it */
  ListStatus linsert( const void *piv,  size_t pivlen,  const void *data,
                      size_t size,  bool after ) {
    size_t n = 0;
    if ( this->scan_fwd( piv, pivlen, n ) == LIST_NOT_FOUND )
      return LIST_NOT_FOUND;
    if ( after ) {
      if ( n == (size_t) this->count )
        return this->rpush( data, size );
    }
    else {
      if ( n == 1 )
        return this->lpush( data, size );
      n -= 1;
    }
    if ( this->full( size ) )
      return LIST_FULL;
    this->move_tail( n, size );
    this->adjust_tail( n, size );
    for ( size_t end = ++this->count; end > n+1; end-- )
      this->index_ref( end ) = this->index_ref( end-1 );

    UIntType &j = this->index_ref( n+1 );
    j = this->offset( this->index_ref( n+2 ), -size );
    this->copy_into( data, size, j );
    this->data_len += size;
    return LIST_OK;
  }
  /* calculate new length after trimming */
  void trim_size( void ) {
    size_t start = this->get_offset( 0 ),
           end   = this->get_offset( this->count, true );
    this->data_len = ( end >= start ) ? end - start :
                     this->data_size() - start + end;
  }
  /* trim left */
  void ltrim( size_t n ) {
    if ( n > this->count )
      n = this->count;
    this->count -= n;
    this->first  = ( this->first + n ) & this->index_mask();
    this->trim_size();
  }
  /* trim right */
  void rtrim( size_t n ) {
    if ( n > this->count )
      n = this->count;
    this->count -= n;
    this->trim_size();
  }
  /* pop data item at head */
  ListStatus lpop( const void *&p,  size_t &sz,  const void *&p2,
                   size_t &sz2 ) {
    ListStatus n;
    n = this->lindex( 0, p, sz, p2, sz2 );
    if ( n != LIST_NOT_FOUND ) {
      this->first = ( this->first + 1 ) & this->index_mask();
      this->count -= 1;
      this->data_len -= ( sz + sz2 );
    }
    return n;
  }
  /* pop data item at tail */
  ListStatus rpop( const void *&p,  size_t &sz,  const void *&p2,
                   size_t &sz2 ) {
    ListStatus n;
    n = this->lindex( this->count - 1, p, sz, p2, sz2 );
    if ( n != LIST_NOT_FOUND ) {
      this->count -= 1;
      this->data_len -= ( sz + sz2 );
    }
    return n;
  }
  /* get the size of nth item */
  size_t get_size( size_t n,  size_t &start,  size_t &end ) {
    start = this->get_offset( n );
    end   = this->get_offset( n+1, true );
    return ( end >= start ? end - start : this->data_size() - start + end );
  }
  /* remove the nth item */
  ListStatus lrem( size_t n ) {
    size_t start, end, size = this->get_size( n, start, end );
    if ( n < this->count ) {
      if ( n == 0 || n + 1 == this->count ) {
        if ( n == 0 )
          this->first = ( this->first + 1 ) & this->index_mask();
        this->count -= 1;
        this->data_len -= size;
        return LIST_OK;
      }
      if ( size > 0 )
        this->move_tail( n, -size );
      for ( n = n + 1; n < this->count; n++ ) {
        UIntType &j = this->index_ref( n );
        j = (UIntType) this->offset( this->index_ref( n + 1 ), -size );
      }
      this->count -= 1;
      this->data_len -= size;
      return LIST_OK;
    }
    return LIST_NOT_FOUND;
  }
  /* replace nth data item, which could expand or contract data buffer */
  ListStatus lset( size_t n,  const void *data,  size_t size ) {
    if ( n >= (size_t) this->count ) /* if n doesn't exist */
      return LIST_NOT_FOUND;
    size_t start, end, cur_size = this->get_size( n, start, end );
    ssize_t amt = (ssize_t) size - (ssize_t) cur_size;
    if ( amt > 0 ) { /* expand nth data item */
      if ( this->data_full( amt ) )
        return LIST_FULL;
    }
    if ( amt != 0 ) {
      if ( n < this->count / 2 ) {
        this->move_head( n, amt );
        this->adjust_head( n, amt );
      }
      else {
        this->move_tail( n, amt );
        this->adjust_tail( n, amt );
      }
      this->data_len += amt;
    }
    /* replace nth item */
    this->copy_into( data, size, this->get_offset( n ) );
    return LIST_OK;
  }
  /* expand or contract upto nth item, doesn't check for space available */
  void move_head( size_t n,  ssize_t amt ) {
    if ( n == 0 ) return; /* alter after head item */
    /* in the middle, moves the head backward amt bytes */
    size_t start     = this->get_offset( 0 ),
           end       = this->get_offset( n, true ),
           new_start = this->offset( start, -amt );
    if ( start <= end ) /* not wrapped around buffer */
      this->copy_move( start, end - start, new_start );
    else if ( amt < 0 ) {
      size_t len = this->data_size() - start;
      this->copy_move( 0, end, -amt );
      this->copy_move( start, len, new_start );
    }
    else {
      size_t len = this->data_size() - start;
      this->copy_move( start, len, new_start );
      this->copy_move( 0, end, new_start + len );
    }
  }
  void adjust_head( size_t n,  ssize_t amt ) {
    /* move index pointers */
    for ( ; ; n-- ) {
      UIntType &j = this->index_ref( n );
      j = (UIntType) this->offset( (size_t) j, -amt );
      if ( n == 0 )
        break;
    }
  }
  /* expand or contract after nth item, doesn't check for space available */
  void move_tail( size_t n,  ssize_t amt ) {
    if ( n == (size_t) this->count - 1 ) return; /* alter before tail item */
    /* in the middle, moves the tail forward amt bytes */
    size_t start     = this->get_offset( n+1 ),
           end       = this->get_offset( this->count, true ),
           new_start = this->offset( start, amt );
    if ( start <= end ) /* not wrapped around buffer */
      this->copy_move( start, end - start, new_start );
    else if ( amt > 0 ) {
      size_t len = this->data_size() - start;
      this->copy_move( 0, end, amt );
      this->copy_move( start, len, new_start );
    }
    else {
      size_t len = this->data_size() - start;
      this->copy_move( start, len, new_start );
      this->copy_move( 0, end, new_start + len );
    }
  }
  void adjust_tail( size_t n,  ssize_t amt ) {
    /* move index pointers */
    for ( n = n + 1; n <= this->count; n++ ) {
      UIntType &j = this->index_ref( n );
      j = (UIntType) this->offset( (size_t) j, amt );
    }
  }
  /* move bytes, careful not to overwrite the buffer being moved */
  void copy_move( size_t off,  size_t size,  size_t start ) {
    if ( size == 0 )
      return;
    if ( start + size <= this->data_size() )
      ::memmove( this->blob( start ), this->blob( off ), size );
    else {
      char        tmp[ 256 ];
      size_t      len   = this->data_size() - start;
      void       * p    = NULL;
      const void * data = this->blob( off );
      void       * p0   = this->blob( 0 ),
                 * ps   = this->blob( start );
      const void * dend = &((char *) data)[ len ];
      /* if splitting the buf cuts into the source data */
      if ( start >= off || size - len > off + size ) {
        if ( size <= sizeof( tmp ) ) {
          ::memcpy( tmp, data, size );
          data = tmp;
          dend = &tmp[ len ];
        }
        else {
          p = ::malloc( size );
          ::memcpy( p, data, size );
          data = p;
          dend = &((char *) data)[ len ];
        }
      }
      size -= len;
      ::memmove( ps, data, len );
      ::memmove( p0, dend, size );
      if ( p != NULL )
        ::free( p );
    }
  }
  /* copy buffer, when start == end && count > 0, data part is full */
  size_t copy_data( void *data ) const {
    size_t size = 0;
    if ( this->count > 0 ) {
      size_t start = this->get_offset( 0 ),
             end   = this->get_offset( this->count, true );
      if ( start <= end ) {
        size = end - start;
        ::memcpy( data, this->blob( start ), size );
      }
      else {
        size = this->data_size() - start;
        ::memcpy( data, this->blob( start ), size );
        ::memcpy( &((char *) data)[ size ], this->blob( 0 ), end );
        size += end;
      }
    }
    return size;
  }
  /* copy data from a uint8_t index buffer */
  void copy( ListStorage<uint16_t, uint8_t> &cp ) const {
    cp.init();
    cp.count = this->count;
    cp.data_len = this->data_len;
    if ( this->count > 0 ) {
      size_t start, end, size;
      this->copy_data( cp.blob( 0 ) );
      end = this->get_offset( 0, true );
      for ( size_t n = 0; n < this->count; n++ ) {
        start = ( end == this->data_size() ) ? 0 : end;
        end   = this->get_offset( n + 1, true );
        size  = ( start <= end ? end - start : this->data_size() - start + end);
        cp.idx[ n + 1 ] = cp.idx[ n ] + (uint8_t) size;
      }
    }
  }
  /* copy data from a uint16_t index buffer */
  void copy( ListStorage<uint32_t, uint16_t> &cp ) const {
    cp.init();
    cp.count = this->count;
    cp.data_len = this->data_len;
    if ( this->count > 0 ) {
      size_t start, end, size;
      this->copy_data( cp.blob( 0 ) );
      end = this->get_offset( 0, true );
      for ( size_t n = 0; n < this->count; n++ ) {
        start = ( end == this->data_size() ) ? 0 : end;
        end   = this->get_offset( n + 1, true );
        size  = ( start <= end ? end - start : this->data_size() - start + end);
        cp.idx[ n + 1 ] = cp.idx[ n ] + (uint16_t) size;
      }
    }
  }
  /* copy data from a uint32_t index buffer */
  void copy( ListStorage<uint64_t, uint32_t> &cp ) const {
    cp.init();
    cp.count = this->count;
    cp.data_len = this->data_len;
    if ( this->count > 0 ) {
      size_t start, end, size;
      this->copy_data( cp.blob( 0 ) );
      end = this->get_offset( 0, true );
      for ( size_t n = 0; n < this->count; n++ ) {
        start = ( end == this->data_size() ) ? 0 : end;
        end   = this->get_offset( n + 1, true );
        size  = ( start <= end ? end - start : this->data_size() - start + end);
        cp.idx[ n + 1 ] = cp.idx[ n ] + (uint32_t) size;
      }
    }
  }
};

typedef ListStorage<uint16_t, uint8_t>  ListStorage8;
typedef ListStorage<uint32_t, uint16_t> ListStorage16;
typedef ListStorage<uint64_t, uint32_t> ListStorage32;

struct ListData {
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  union {
    void          * listp;
    ListStorage8  * list8;
    ListStorage16 * list16;
    ListStorage32 * list32;
  };
  const size_t size;
  ListData( void *l,  size_t sz ) : size( sz ) {
    this->listp = l;
  }

  static bool is_uint8( size_t alloc_size ) {
    return alloc_size < ( 0x100 << 1 );
  }
  static bool is_uint16( size_t alloc_size ) {
    return alloc_size < ( 0x10000 << 1 );
  }
  static size_t pow2size( size_t sz ) {
    if ( ( sz & ( sz - 1 ) ) != 0 )
      sz = (size_t) 1 << ( 64 - __builtin_clzl( sz ) );
    return sz;
  }
  static size_t alloc_size( size_t &idx_size,  size_t &dat_size ) {
    size_t sz, tz;
    idx_size = pow2size( idx_size );
    dat_size = pow2size( dat_size );
    if ( ( idx_size | ( dat_size - 1 ) ) <= 0xff ) {
      sz = sizeof( ListStorage8 );
      tz = sizeof( uint8_t );
    }
    else if ( ( idx_size | ( dat_size - 1 ) ) <= 0xffff ) {
      sz = sizeof( ListStorage16 );
      tz = sizeof( uint16_t );
    }
    else {
      sz = sizeof( ListStorage32 );
      tz = sizeof( uint32_t );
    }
    return sz + tz * idx_size + tz * dat_size;
  }
  static const uint16_t lst8_sig  = 0xf7e4U;
  static const uint32_t lst16_sig = 0xddbe7a69UL;
  static const uint64_t lst32_sig = 0xf5ff85c9f6c343ULL;
#define LIST_CALL( GOTO ) \
  ( is_uint8( this->size ) ? this->list8->GOTO : \
    is_uint16( this->size ) ? this->list16->GOTO : this->list32->GOTO )
  void init( size_t count,  size_t data_len ) {
    if ( is_uint8( this->size ) )
      (new ( this->listp ) ListStorage8( lst8_sig, count, data_len ))->init();
    else if ( is_uint16( this->size ) )
      (new ( this->listp ) ListStorage16( lst16_sig, count, data_len ))->init();
    else
      (new ( this->listp ) ListStorage32( lst32_sig, count, data_len ))->init();
  }
  size_t max_count( void ) const { return LIST_CALL( max_count() ); }
  size_t data_size( void ) const { return LIST_CALL( data_size() ); }
  size_t count( void )     const { return LIST_CALL( count ); }
  size_t data_len( void )  const { return LIST_CALL( data_len ); }

  void copy( ListData &list ) const {
    if ( is_uint8( list.size ) )
      LIST_CALL( copy( *list.list8 ) );
    else if ( is_uint16( list.size ) )
      LIST_CALL( copy( *list.list16 ) );
    else
      LIST_CALL( copy( *list.list32 ) );
  }
  size_t offset( size_t n ) {
    return LIST_CALL( get_offset( n ) );
  }

  ListStatus rpush( const void *data,  size_t size ) {
    return LIST_CALL( rpush( data, size ) );
  }
  ListStatus lpush( const void *data,  size_t size ) {
    return LIST_CALL( lpush( data, size ) );
  }
  ListStatus lindex( size_t n,  const void *&p,  size_t &sz,
                     const void *&p2,  size_t &sz2 ) const {
    return LIST_CALL( lindex( n, p, sz, p2, sz2 ) );
  }
  void ltrim( size_t n ) {
    LIST_CALL( ltrim( n ) );
  }
  void rtrim( size_t n ) {
    LIST_CALL( rtrim( n ) );
  }
  ListStatus lpop( const void *&p,  size_t &sz,  const void *&p2,
                   size_t &sz2 ) {
    return LIST_CALL( lpop( p, sz, p2, sz2 ) );
  }
  ListStatus rpop( const void *&p,  size_t &sz,  const void *&p2,
                   size_t &sz2 ) {
    return LIST_CALL( rpop( p, sz, p2, sz2 ) );
  }
  ListStatus lset( size_t n,  const void *data,  size_t size ) {
    return LIST_CALL( lset( n, data, size ) );
  }
  ListStatus linsert( const void *piv,  size_t pivlen,  const void *data,
                      size_t size,  bool after ) {
    return LIST_CALL( linsert( piv, pivlen, data, size, after ) );
  }
  ListStatus scan_fwd( const void *data,  size_t size,  size_t &pos ) {
    return LIST_CALL( scan_fwd( data, size, pos ) );
  }
  ListStatus scan_rev( const void *data,  size_t size,  size_t &pos ) {
    return LIST_CALL( scan_rev( data, size, pos ) );
  }
  ListStatus lrem( size_t n ) {
    return LIST_CALL( lrem( n ) );
  }
};

}
}
#endif
