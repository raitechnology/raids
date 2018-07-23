#ifndef __rai_raids__redis_hash_h__
#define __rai_raids__redis_hash_h__

#include "redis_list.h"

namespace rai {
namespace ds {

enum HashStatus {
  HASH_OK        = LIST_OK,        /* success and/or new key/value */
  HASH_NOT_FOUND = LIST_NOT_FOUND, /* key not found */
  HASH_FULL      = LIST_FULL,      /* no room, needs resize */
  HASH_UPDATED   = 3,              /* success, replaced existing key/value */
  HASH_EXISTS    = 4               /* found existing item, not updated */
};

struct HashVal : public ListVal {
  char key[ 256 ];
  size_t keylen, pos;
  void zero( void ) {
    this->sz = this->sz2 = this->keylen = 0;
  }
};

struct FindPos {
  size_t   i;
  uint32_t h;
  void init( const void *key,  size_t keylen ) {
    this->i = 0;
    this->h = kv_crc_c( key, keylen, 0 );
  }
};

template <class UIntSig, class UIntType>
struct HashStorage : public ListStorage<UIntSig, UIntType> {

  bool init_hash( const ListHeader &hdr ) {
    size_t start;
    if ( this->rpush_size( hdr, 0, start ) != LIST_OK )
      return false;
    return true;
  }

  bool resize_hash( const ListHeader &hdr ) {
    size_t start, end, cur_size, new_size, need, new_start;
    cur_size  = this->get_size( hdr, 0, start, end );
    new_size  = ( max<size_t>( cur_size, this->count + 1 ) | 7 ) + 1;
    need      = new_size - cur_size;
    new_start = hdr.data_offset( start, -need );
    if ( this->data_full( hdr, need ) )
      return false;
    this->data_start = new_start;
    this->index_ref( hdr, 0 ) = (UIntType) new_start;
    this->data_len += need;
    if ( start + cur_size <= hdr.data_size() )
      this->copy_move( hdr, start, cur_size, new_start );
    else {
      need = hdr.data_size() - start;
      this->copy_move( hdr, start, need, new_start );
      this->copy_move( hdr, 0, cur_size - need,
                       hdr.data_offset( new_start, need ) );
    }
    return true;
  }

  bool hash_find( const ListHeader &hdr,  FindPos &pos ) const {
    if ( this->count == 0 )
      return false;
    size_t          start,
                    end,
                    len,
                    sz;
    const uint8_t * map,
                  * el;
    uint8_t         k = (uint8_t) pos.h;
    sz    = this->get_size( hdr, 0, start, end );
    len   = min<size_t>( this->count, sz );
    end   = hdr.data_offset( start, len );
    start = hdr.data_offset( start, pos.i );
    len  -= pos.i;
    map   = (const uint8_t *) hdr.blob( start );
    if ( end >= start ) {
      if ( (el = (const uint8_t *) ::memchr( map, k, len )) != NULL ) {
        pos.i += (size_t) ( el - map );
        return true;
      }
      goto not_found;
    }
    if ( (el = (const uint8_t *) ::memchr( map, k, len - end )) != NULL ) {
      pos.i += (size_t) ( el - map );
      return true;
    }
    map = (const uint8_t *) hdr.blob( 0 );
    if ( (el = (const uint8_t *) ::memchr( map, k, end )) != NULL ) {
      pos.i += (size_t) ( el - map ) + ( len - end );
      return true;
    }
  not_found:;
    pos.i = this->count;
    return false;
  }

  HashStatus hash_append( const ListHeader &hdr,  const FindPos &pos ) {
    if ( this->count == 0 )
      this->init_hash( hdr );
    size_t start,
           end,
           sz = this->get_size( hdr, 0, start, end );
    if ( this->count >= sz ) {
      if ( ! this->resize_hash( hdr ) )
        return HASH_FULL;
      start = this->get_offset( hdr, 0 );
    }
    ((uint8_t *) hdr.blob(
      hdr.data_offset( start, this->count ) ))[ 0 ] = (uint8_t) pos.h;
    return HASH_OK;
  }

  void hash_del( const ListHeader &hdr,  const FindPos &pos ) {
    size_t    start,
              end,
              len,
              sz;
    uint8_t * map,
            * hmap;
    sz    = this->get_size( hdr, 0, start, end );
    len   = min<size_t>( this->count + 1, sz ); /* already deleted list item */
    end   = hdr.data_offset( start, len );
    start = hdr.data_offset( start, pos.i );
    len  -= pos.i;
    map   = (uint8_t *) hdr.blob( start );
    if ( end >= start ) {
      ::memmove( map, &map[ 1 ], len );
    }
    else {
      hmap = (uint8_t *) hdr.blob( 0 );
      if ( start + 1 < hdr.data_size() )
        ::memmove( map, &map[ 1 ], hdr.data_size() - ( start + 1 ) );
      hmap[ hdr.data_size() - 1 ] = hmap[ 0 ];
      if ( end > 0 )
        ::memmove( hmap, &hmap[ 1 ], end - 1 );
    }
    /*printf( "[" );
    this->print( hdr );
    printf( "]\n" );*/
  }

  void print( const ListHeader &hdr ) const {
    size_t          start, end, len, sz, off;
    const uint8_t * map;
    sz  = this->get_size( hdr, 0, start, end );
    len = min<size_t>( this->count, sz );
    off = hdr.data_offset( start, len );
    map = (const uint8_t *) hdr.blob( 0 );
    printf( "sz:%ld,len:%ld -- ", sz, len );
    if ( len > 0 ) {
      size_t i;
      printf( "%d <", map[ start ] );
      i = ( start + 1 ) & hdr.data_mask;
      for ( ; i != off; i = ( i + 1 ) & hdr.data_mask ) {
        printf( "%d ", map[ i ] );
      }
      printf( "> " );
      for ( ; i != ( end & hdr.data_mask ); i = ( i + 1 ) & hdr.data_mask ) {
        printf( "%d ", map[ i ] );
      }
    }
  }

  bool match( const ListHeader &hdr,  const void *key,  size_t keylen,
             size_t pos,  size_t *data_off = 0,  size_t *data_size = 0 ) const {
    size_t start,
           len,
           end = this->get_offset( hdr, pos, true );
    if ( pos < this->count ) {
      start = ( end == hdr.data_size() ) ? 0 : end;
      end   = this->get_offset( hdr, pos + 1, true );
      len   = ( end >= start ? end - start : hdr.data_size() - start + end );
      if ( len >= keylen + 1 ) {
        uint8_t * kp = (uint8_t *) hdr.blob( start );
        if ( kp[ 0 ] == (uint8_t) keylen ) {
          if ( start + keylen + 1 <= hdr.data_size() ) {
            if ( ::memcmp( &kp[ 1 ], key, keylen ) == 0 ) {
              if ( data_off != NULL )
                goto found;
              return true;
            }
          }
          else {
            size_t part = hdr.data_size() - ( start + 1 );
            if ( ( part == 0 || ::memcmp( &kp[ 1 ], key, part ) == 0 ) &&
                 ::memcmp( hdr.blob( 0 ), &((uint8_t *) key)[ part ],
                           keylen - part ) == 0 ) {
              if ( data_off != NULL )
                goto found;
              return true;
            }
          }
        }
      }
    }
    return false;
  found:;
    *data_off  = hdr.data_offset( start, keylen + 1 );
    *data_size = len - ( keylen + 1 );
    return true;
  }

  HashStatus hexists( const ListHeader &hdr,  const void *key,
                      size_t keylen ) const {
    FindPos pos;
    for ( pos.init( key, keylen ); ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match( hdr, key, keylen, pos.i ) )
        return HASH_OK;
    }
  }

  HashStatus hindex( const ListHeader &hdr,  size_t n,  HashVal &kv ) const {
    HashStatus hstat;
    kv.zero();
    hstat = (HashStatus) this->lindex( hdr, n, kv );
    if ( hstat != HASH_NOT_FOUND ) {
      kv.pos    = n;
      kv.keylen = *(uint8_t *) kv.data;
      if ( kv.sz >= kv.keylen + 1 ) {
        ::memcpy( kv.key, &((uint8_t *) kv.data)[ 1 ], kv.keylen );
        kv.data = &((uint8_t *) kv.data)[ kv.keylen + 1 ];
        kv.sz  -= kv.keylen + 1;
      }
      else { /* key is split between data & data2 */
        ::memcpy( kv.key, &((uint8_t *) kv.data)[ 1 ], kv.sz - 1 );
        size_t overlap = kv.keylen + 1 - kv.sz;
        ::memcpy( &kv.key[ kv.sz - 1 ], kv.data2, overlap );
        kv.data = &((uint8_t *) kv.data2)[ overlap ];
        kv.sz   = kv.sz2 - overlap;
        kv.sz2  = 0;
      }
    }
    return hstat;
  }

  HashStatus hgetpos( const ListHeader &hdr,  const void *key,  size_t keylen,
                      ListVal &kv,  FindPos &pos ) const {
    size_t  data_off,
            data_len;
    kv.zero();
    for ( pos.init( key, keylen ); ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match( hdr, key, keylen, pos.i, &data_off, &data_len ) ) {
        kv.data = hdr.blob( data_off );
        kv.sz   = data_len;
        if ( data_off + data_len > hdr.data_size() ) {
          kv.sz    = hdr.data_size() - data_off;
          kv.sz2   = data_len - kv.sz;
          kv.data2 = hdr.blob( 0 );
        }
        return HASH_OK;
      }
    }
  }

  HashStatus hget( const ListHeader &hdr,  const void *key,  size_t keylen,
                   ListVal &kv ) const {
    FindPos pos;
    return this->hgetpos( hdr, key, keylen, kv, pos );
  }

  void copy_item( const ListHeader &hdr,  const void *key,  size_t keylen,
                  const void *val,  size_t vallen,  size_t start ) {
    size_t          size = keylen + vallen + 1;
    uint8_t       * ptr  = (uint8_t *) hdr.blob( start );
    const uint8_t * k    = (const uint8_t *) key,
                  * v    = (const uint8_t *) val;

    *ptr++ = (uint8_t) keylen;
    if ( start + size <= hdr.data_size() ) {
      ::memcpy( ptr, k, keylen );
      ptr = &ptr[ keylen ];
    }
    else {
      size_t off  = start + 1,
             left = hdr.data_size() - off;
      if ( left > 0 ) {
        size_t n = min<size_t>( left, keylen );
        ::memcpy( ptr, k, n );
        left   -= n;
        keylen -= n;
        k       = &k[ n ];
        ptr     = &ptr[ n ];

        if ( left > 0 ) {
          ::memcpy( ptr, v, left );
          vallen -= left;
          v       = &v[ left ];
        }
      }
      ptr = (uint8_t *) hdr.blob( 0 );
      if ( keylen > 0 ) {
        ::memcpy( ptr, k, keylen );
        ptr = &ptr[ keylen ];
      }
    }
    ::memcpy( ptr, v, vallen );
  }

  void copy_value( const ListHeader &hdr,  size_t keylen,
                   const void *val,  size_t vallen,  size_t start ) {
    size_t          size = keylen + vallen + 1;
    uint8_t       * ptr  = (uint8_t *) hdr.blob( start );
    const uint8_t * v    = (const uint8_t *) val;

    if ( start + size <= hdr.data_size() ) {
      ptr = &ptr[ 1 + keylen ];
    }
    else {
      size_t off  = start + 1,
             left = hdr.data_size() - off;
      if ( left > 0 ) {
        size_t n = min<size_t>( left, keylen );
        left   -= n;
        keylen -= n;
        ptr     = &ptr[ 1 + n ];

        if ( left > 0 ) {
          ::memcpy( ptr, v, left );
          vallen -= left;
          v       = &v[ left ];
        }
      }
      ptr = (uint8_t *) hdr.blob( 0 );
      if ( keylen > 0 )
        ptr = &ptr[ keylen ];
    }
    ::memcpy( ptr, v, vallen );
  }

  HashStatus hupdate( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  const FindPos &pos ) {
    size_t start, end,
           old_size = this->get_size( hdr, pos.i, start, end ),
           new_size = keylen + vallen + 1;
    if ( old_size != new_size ) {
      ssize_t amt = (ssize_t) new_size - (ssize_t) old_size;
      if ( amt > 0 ) { /* expand nth data item */
        if ( this->data_full( hdr, amt ) )
          return HASH_FULL;
      }
      if ( amt != 0 ) {
        if ( pos.i < this->count / 2 ) {
          this->move_head( hdr, pos.i, amt );
          this->adjust_head( hdr, pos.i, amt );
        }
        else {
          this->move_tail( hdr, pos.i, amt );
          this->adjust_tail( hdr, pos.i, amt );
        }
        this->data_len += amt;
      }
      /* replace nth item */
      this->copy_item( hdr, key, keylen, val, vallen,
                       this->get_offset( hdr, pos.i ) );
    }
    else {
      this->copy_value( hdr, keylen, val, vallen,
                        this->get_offset( hdr, pos.i ) );
    }
    return HASH_UPDATED;
  }

  HashStatus happend( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  const FindPos &pos ) {
    size_t     start;
    HashStatus hstat = this->hash_append( hdr, pos );
    if ( hstat == HASH_OK )
      hstat = (HashStatus) this->rpush_size( hdr, keylen + vallen + 1, start );
    if ( hstat == HASH_OK )
      this->copy_item( hdr, key, keylen, val, vallen, start );
    return hstat;
  }

  HashStatus hset( const ListHeader &hdr,  const void *key,  size_t keylen,
                   const void *val,  size_t vallen ) {
    FindPos pos;
    for ( pos.init( key, keylen ); ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return this->happend( hdr, key, keylen, val, vallen, pos );
      if ( this->match( hdr, key, keylen, pos.i ) )
        return this->hupdate( hdr, key, keylen, val, vallen, pos );
    }
  }

  HashStatus hsetnx( const ListHeader &hdr,  const void *key,  size_t keylen,
                     const void *val,  size_t vallen ) {
    FindPos pos;
    for ( pos.init( key, keylen ); ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return this->happend( hdr, key, keylen, val, vallen, pos );
      if ( this->match( hdr, key, keylen, pos.i ) )
        return HASH_EXISTS;
    }
  }

  HashStatus hsetpos( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  const FindPos &pos ) {
    if ( pos.i != this->count )
      return this->hupdate( hdr, key, keylen, val, vallen, pos );
    return this->happend( hdr, key, keylen, val, vallen, pos );
  }

  HashStatus hdel( const ListHeader &hdr,  const void *key,  size_t keylen ) {
    FindPos pos;
    for ( pos.init( key, keylen ); ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match( hdr, key, keylen, pos.i ) ) {
        HashStatus hstat = (HashStatus) this->lrem( hdr, pos.i );
        if ( hstat == HASH_OK )
          this->hash_del( hdr, pos );
        return hstat;
      }
    }
  }

  int hverify( const ListHeader &hdr ) const {
    if ( this->count == 0 )
      return 0;
    size_t          start,
                    end,
                    len,
                    sz;
    const uint8_t * el;
    HashVal         kv;
    sz = this->get_size( hdr, 0, start, end );
    if ( this->count > sz )
      return -20;
    len = min<size_t>( this->count, sz );
    for ( size_t i = 1; i < len; i++ ) {
      HashStatus hstat = (HashStatus) this->lindex( hdr, i, kv );
      if ( hstat == HASH_NOT_FOUND )
        return -21;
      if ( kv.sz + kv.sz2 == 0 )
        return -22;
      if ( kv.sz + kv.sz2 >= hdr.data_size() )
        return -23;
      kv.keylen = *(uint8_t *) kv.data;
      if ( kv.keylen + 1 > kv.sz + kv.sz2 )
        return -24;
      if ( kv.sz >= kv.keylen + 1 ) {
        ::memcpy( kv.key, &((uint8_t *) kv.data)[ 1 ], kv.keylen );
      }
      else { /* key is split between data & data2 */
        ::memcpy( kv.key, &((uint8_t *) kv.data)[ 1 ], kv.sz - 1 );
        size_t overlap = kv.keylen + 1 - kv.sz;
        ::memcpy( &kv.key[ kv.sz - 1 ], kv.data2, overlap );
      }
      uint32_t h = kv_crc_c( kv.key, kv.keylen, 0 );
      el = (const uint8_t *) hdr.blob( hdr.data_offset( start, i ) );
      if ( el[ 0 ] != (uint8_t) h )
        return -25;
    }
    return 0;
  }
};

typedef HashStorage<uint16_t, uint8_t>  HashStorage8;
typedef HashStorage<uint32_t, uint16_t> HashStorage16;
typedef HashStorage<uint64_t, uint32_t> HashStorage32;

struct HashData : public ListData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  HashData() : ListData() {}
  HashData( void *l,  size_t sz ) : ListData( l, sz ) {}

  size_t resize_size( size_t &idx_size,  size_t &dat_size ) {
    dat_size += this->data_len();
    dat_size += dat_size / 2 + 2;
    idx_size += this->count();
    idx_size += idx_size / 2 + 2;
    idx_size  = ( idx_size | 7 ) + 1;
    dat_size += idx_size;
    return alloc_size( idx_size, dat_size );
  }
#define HASH_CALL( GOTO ) \
  ( is_uint8( this->size ) ? ((HashStorage8 *) this->listp)->GOTO : \
    is_uint16( this->size ) ? ((HashStorage16 *) this->listp)->GOTO : \
                              ((HashStorage32 *) this->listp)->GOTO )

  int hverify( void ) const {
    int x = this->lverify();
    if ( x == 0 )
      x = HASH_CALL( hverify( *this ) );
    return x;
  }
  HashStatus hexists( const void *key,  size_t keylen ) {
    return HASH_CALL( hexists( *this, key, keylen ) );
  }
  HashStatus hget( const void *key,  size_t keylen,  ListVal &kv ) const {
    return HASH_CALL( hget( *this, key, keylen, kv ) );
  }
  HashStatus hgetpos( const void *key,  size_t keylen,  ListVal &kv,
                      FindPos &pos ) const {
    return HASH_CALL( hgetpos( *this, key, keylen, kv, pos ) );
  }
  HashStatus hset( const void *key,  size_t keylen,
                   const void *val,  size_t vallen ) {
    return HASH_CALL( hset( *this, key, keylen, val, vallen ) );
  }
  HashStatus hsetnx( const void *key,  size_t keylen,
                     const void *val,  size_t vallen ) {
    return HASH_CALL( hsetnx( *this, key, keylen, val, vallen ) );
  }
  HashStatus hsetpos( const void *key,  size_t keylen,  const void *val,
                      size_t vallen,  const FindPos &pos ) {
    return HASH_CALL( hsetpos( *this, key, keylen, val, vallen, pos ) );
  }
  HashStatus hindex( size_t n,  HashVal &kv ) const {
    return HASH_CALL( hindex( *this, n, kv ) );
  }
  HashStatus hdel( const void *key,  size_t keylen ) {
    return HASH_CALL( hdel( *this, key, keylen ) );
  }
  void print( void ) {
    return HASH_CALL( print( *this ) );
  }
};

}
}

#endif
