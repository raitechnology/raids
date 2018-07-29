#ifndef __rai_raids__redis_set_h__
#define __rai_raids__redis_set_h__

#include "redis_hash.h"

namespace rai {
namespace ds {

enum SetStatus {
  SET_OK        = HASH_OK,        /* success and/or new key/value */
  SET_NOT_FOUND = HASH_NOT_FOUND, /* key not found */
  SET_FULL      = HASH_FULL,      /* no room, needs resize */
  SET_UPDATED   = HASH_UPDATED,   /* success, replaced existing key/value */
  SET_EXISTS    = HASH_EXISTS     /* found existing item, not updated */
};

template <class UIntSig, class UIntType>
struct SetStorage : public HashStorage<UIntSig, UIntType> {

  bool match_member( const ListHeader &hdr,  const void *key,  size_t keylen,
                     size_t pos ) const {
    size_t start, end, len;
    if ( pos < hdr.index( this->count ) ) {
      len = this->get_size( hdr, pos, start, end );
      if ( len == keylen ) {
        const uint8_t * kp = (const uint8_t *) hdr.blob( start );
        if ( start + keylen <= hdr.data_size() ) {
          if ( ::memcmp( kp, key, keylen ) == 0 )
            return true;
        }
        else {
          size_t part = hdr.data_size() - start;
          if ( ::memcmp( kp, key, part ) == 0 &&
               ::memcmp( hdr.blob( 0 ), &((uint8_t *) key)[ part ],
                         keylen - part ) == 0 )
            return true;
        }
      }
    }
    return false;
  }

  bool match_member( const ListHeader &hdr,  const ListVal &lv,
                     size_t pos ) const {
    if ( lv.sz2 == 0 )
      return this->match_member( hdr, lv.data, lv.sz, pos );
    if ( lv.sz == 0 )
      return this->match_member( hdr, lv.data2, lv.sz2, pos );
    size_t start, end, len;
    size_t keylen = lv.sz + lv.sz2;
    if ( pos < hdr.index( this->count ) ) {
      len = this->get_size( hdr, pos, start, end );
      if ( len == keylen ) {
        const uint8_t * kp = (const uint8_t *) hdr.blob( start );
        if ( start + keylen <= hdr.data_size() ) {
          if ( ::memcmp( kp, lv.data, lv.sz ) == 0 &&
               ::memcmp( &kp[ lv.sz ], lv.data2, lv.sz2 ) == 0 )
            return true;
        }
        else {
          size_t part         = hdr.data_size() - start;
          size_t hd           = min<size_t>( part, lv.sz );
          const uint8_t * kp2 = (const uint8_t *) hdr.blob( 0 ),
                        * p   = (const uint8_t *) lv.data,
                        * p2  = (const uint8_t *) lv.data2;
          if ( ::memcmp( kp, p, hd ) == 0 ) {
            if ( hd == part ) {
              if ( ::memcmp( kp2, &p[ hd ], lv.sz - hd ) == 0 &&
                   ::memcmp( &kp2[ lv.sz - hd ], p2, lv.sz2 ) == 0 )
                return true;
            }
            else { /* hd == lv.sz */
              part -= lv.sz;
              if ( ::memcmp( &kp[ hd ], p2, part ) == 0 &&
                   ::memcmp( kp2, &p2[ part ], lv.sz2 - part ) == 0 )
                return true;
            }
          }
        }
      }
    }
    return false;
  }

  SetStatus sismember( const ListHeader &hdr,  const void *key,  size_t keylen,
                       HashPos &pos ) const {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return SET_NOT_FOUND;
      if ( this->match_member( hdr, key, keylen, pos.i ) )
        return SET_OK;
    }
  }

  SetStatus sismember( const ListHeader &hdr,  const ListVal &lv,
                       HashPos &pos ) const {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return SET_NOT_FOUND;
      if ( this->match_member( hdr, lv, pos.i ) )
        return SET_OK;
    }
  }

  SetStatus sappend( const ListHeader &hdr,  const void *key,  size_t keylen,
                     const HashPos &pos ) {
    SetStatus sstat = (SetStatus) this->hash_append( hdr, pos );
    if ( sstat == SET_OK ) {
      sstat = (SetStatus) this->rpush( hdr, key, keylen );
      if ( sstat == SET_OK )
        return SET_UPDATED;
    }
    return sstat;
  }

  SetStatus sappend( const ListHeader &hdr,  const ListVal &lv,
                     const HashPos &pos ) {
    SetStatus sstat = (SetStatus) this->hash_append( hdr, pos );
    if ( sstat == SET_OK ) {
      sstat = (SetStatus) this->rpush( hdr, lv );
      if ( sstat == SET_OK )
        return SET_UPDATED;
    }
    return sstat;
  }

  SetStatus sadd( const ListHeader &hdr,  const void *key,  size_t keylen,
                  HashPos &pos ) {
    SetStatus sstat = this->sismember( hdr, key, keylen, pos );
    if ( sstat == SET_OK )
      return SET_OK;
    return this->sappend( hdr, key, keylen, pos );
  }

  SetStatus sadd( const ListHeader &hdr,  const ListVal &lv,  HashPos &pos ) {
    SetStatus sstat = this->sismember( hdr, lv, pos );
    if ( sstat == SET_OK )
      return SET_OK;
    return this->sappend( hdr, lv, pos );
  }

  SetStatus srem( const ListHeader &hdr,  const void *key,  size_t keylen,
                  HashPos &pos ) {
    SetStatus sstat = this->sismember( hdr, key, keylen, pos );
    if ( sstat == SET_NOT_FOUND )
      return SET_NOT_FOUND;
    sstat = (SetStatus) this->lrem( hdr, pos.i );
    if ( sstat == SET_OK )
      this->hash_del( hdr, pos.i );
    return sstat;
  }

  SetStatus srem( const ListHeader &hdr,  const ListVal &lv,  HashPos &pos ) {
    SetStatus sstat = this->sismember( hdr, lv, pos );
    if ( sstat == SET_NOT_FOUND )
      return SET_NOT_FOUND;
    sstat = (SetStatus) this->lrem( hdr, pos.i );
    if ( sstat == SET_OK )
      this->hash_del( hdr, pos.i );
    return sstat;
  }

  SetStatus spopn( const ListHeader &hdr,  size_t n ) {
    SetStatus sstat = (SetStatus) this->lrem( hdr, n );
    if ( sstat == SET_OK )
      this->hash_del( hdr, n );
    return sstat;
  }

  SetStatus spopall( const ListHeader & ) {
    if ( this->count > 1 )
      this->count = 1;
    return SET_OK;
  }

  int sverify( const ListHeader &hdr ) const {
    if ( this->count == 0 )
      return 0;
    size_t          start,
                    end,
                    len,
                    sz;
    const uint8_t * el;
    ListVal         lv;
    uint32_t         h;
    sz = this->get_size( hdr, 0, start, end );
    if ( this->count > sz )
      return -30;
    len = min<size_t>( this->count, sz );
    for ( size_t i = 1; i < len; i++ ) {
      SetStatus sstat = (SetStatus) this->lindex( hdr, i, lv );
      if ( sstat == SET_NOT_FOUND )
        return -31;
      if ( lv.sz + lv.sz2 >= hdr.data_size() )
        return -32;
      h = kv_crc_c( lv.data, lv.sz, 0 );
      if ( lv.sz2 > 0 )
        h = kv_crc_c( lv.data2, lv.sz2, h );
      el = (const uint8_t *) hdr.blob( hdr.data_offset( start, i ) );
      if ( el[ 0 ] != (uint8_t) h )
        return -33;
    }
    return 0;
  }

  void get_hash_bits( const ListHeader &hdr,  uint64_t *bits ) const {
    size_t          start, end, len, sz, i, j;
    const uint8_t * map;
    /* set hash bits for each element */
    sz  = this->get_size( hdr, 0, start, end );
    len = min<size_t>( this->count, sz );
    for ( j = 0; j < 4; j++ )
      bits[ j ] = 0;
    map = (const uint8_t *) hdr.blob( hdr.data_offset( start, 0 ) );
    if ( end >= start )
      j = len;
    else
      j = min<size_t>( hdr.data_size() - start, len );
    for ( i = 1; i < j; i++ )
      bits[ map[ i ] >> 6 ] |= (uint64_t) 1U << ( map[ i ] & 63 );
    if ( j != len ) {
      map = (const uint8_t *) hdr.blob( 0 );
      for ( j = 0; i < len; i++, j++ )
        bits[ map[ j ] >> 6 ] |= (uint64_t) 1U << ( map[ j ] & 63 );
    }
  }
  /* me={a,b,c,d} | x={a,c,e} = me={a,b,c,d,e} */
  template<class T, class U>
  SetStatus sunion( const ListHeader &myhdr,  const ListHeader &xhdr,
                    SetStorage<T, U> &x ) {
    HashPos         pos;
    size_t          start,
                    end,
                    len,
                    sz,
                    i, j, k;
    ListVal         lv;
    uint64_t        bits[ 4 ];
    const uint8_t * map;
    SetStatus       sstat;

    if ( x.count <= 1 )
      return SET_OK;
    this->get_hash_bits( myhdr, bits );
    sz  = x.get_size( xhdr, 0, start, end );
    len = min<size_t>( x.count, sz );
    map = (const uint8_t *) xhdr.blob( xhdr.data_offset( start, 0 ) );
    j   = min<size_t>( xhdr.data_size() - start, len );
    for ( i = 1, k = 1; ; i++ ) {
      if ( i == j ) {
        if ( k == len )
          return SET_OK;
        j = len - i;
        i = 0;
        map = (const uint8_t *) xhdr.blob( 0 );
      }
      /* get element */
      sstat = (SetStatus) x.lindex( xhdr, k++, lv );
      if ( sstat != SET_OK )
        return sstat;
      pos.i = 0;
      pos.h = map[ i ];
      /* if not present */
      if ( ( bits[ pos.h >> 6 ] & ( (uint64_t) 1U << ( pos.h & 63 ) ) ) == 0 )
        sstat = this->sappend( myhdr, lv, pos );
      else /* search for it */
        sstat = this->sadd( myhdr, lv, pos );
      if ( sstat == SET_FULL )
        return sstat;
    }
  }
  /* intersect:  me={a,b,c,d} & x={a,c,e} = me={a,c} */
  /* difference: me={a,b,c,d} - x={a,c,e} = me={b,d} */
  template<class T, class U>
  SetStatus sinter( const ListHeader &myhdr,  const ListHeader &xhdr,
                    SetStorage<T, U> &x,  bool is_intersect /* or diff */) {
    HashPos         pos;
    size_t          start,
                    end,
                    len,
                    sz,
                    i, j;
    ListVal         lv;
    uint64_t        bits[ 4 ];
    const uint8_t * map,
                  * map2;
    SetStatus       sstat;
    bool            diff;

    if ( x.count <= 1 || this->count <= 1 ) {
      if ( is_intersect )
        this->count = 0;
      return SET_OK;
    }
    x.get_hash_bits( xhdr, bits );
    sz   = this->get_size( myhdr, 0, start, end );
    len  = min<size_t>( this->count, sz );
    map  = (const uint8_t *) myhdr.blob( myhdr.data_offset( start, 0 ) );
    j    = min<size_t>( myhdr.data_size() - start, len );
    map2 = (const uint8_t *) myhdr.blob( 0 );

    for ( i = len; ; ) {
      if ( (i -= 1) == 0 )
        break;
      pos.h = ( i >= j ? map2[ i - j ] : map[ i ] );
      diff  = false;
      if ( ( bits[ pos.h >> 6 ] & ( (uint64_t) 1U << ( pos.h & 63 ) ) ) == 0 )
        diff = true;
      else {
        sstat = (SetStatus) this->lindex( myhdr, i, lv );
        if ( sstat != SET_OK )
          return sstat;
        pos.i = 0;
        if ( x.sismember( xhdr, lv, pos ) == SET_NOT_FOUND )
          diff = true;
      }
      if ( is_intersect ) {
        if ( diff )
          this->spopn( myhdr, i );
      }
      else {
        if ( ! diff )
          this->spopn( myhdr, i );
      }
    }
    return SET_OK;
  }
};

typedef SetStorage<uint16_t, uint8_t>  SetStorage8;
typedef SetStorage<uint32_t, uint16_t> SetStorage16;
typedef SetStorage<uint64_t, uint32_t> SetStorage32;

struct SetData : public HashData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  SetData() : HashData() {}
  SetData( void *l,  size_t sz ) : HashData( l, sz ) {}

#define SET_CALL( GOTO ) \
  ( is_uint8( this->size ) ? ((SetStorage8 *) this->listp)->GOTO : \
    is_uint16( this->size ) ? ((SetStorage16 *) this->listp)->GOTO : \
                              ((SetStorage32 *) this->listp)->GOTO )
  int sverify( void ) const {
    int x = this->lverify();
    if ( x == 0 )
      x = SET_CALL( sverify( *this ) );
    return x;
  }
  SetStatus sismember( const void *key,  size_t keylen,  HashPos &pos ) {
    return SET_CALL( sismember( *this, key, keylen, pos ) );
  }
  SetStatus sadd( const void *key,  size_t keylen,  HashPos &pos ) {
    return SET_CALL( sadd( *this, key, keylen, pos ) );
  }
  SetStatus srem( const void *key,  size_t keylen,  HashPos &pos ) {
    return SET_CALL( srem( *this, key, keylen, pos ) );
  }
  SetStatus spopn( size_t n ) {
    return SET_CALL( spopn( *this, n ) );
  }
  SetStatus spopall( void ) {
    return SET_CALL( spopall( *this ) );
  }
  template<class T, class U>
  SetStatus sunion( SetData &set ) {
    if ( is_uint8( this->size ) )
      return ((SetStorage8 *) this->listp)->sunion<T, U>( *this, set,
                                              *(SetStorage<T, U> *) set.listp );
    else if ( is_uint16( this->size ) )
      return ((SetStorage16 *) this->listp)->sunion<T, U>( *this, set,
                                              *(SetStorage<T, U> *) set.listp );
    return ((SetStorage32 *) this->listp)->sunion<T, U>( *this, set,
                                              *(SetStorage<T, U> *) set.listp );
  }
  template<class T, class U>
  SetStatus sinter( SetData &set,  bool is_intersect ) {
    if ( is_uint8( this->size ) )
      return ((SetStorage8 *) this->listp)->sinter<T, U>( *this, set,
                                *(SetStorage<T, U> *) set.listp, is_intersect );
    else if ( is_uint16( this->size ) )
      return ((SetStorage16 *) this->listp)->sinter<T, U>( *this, set,
                                *(SetStorage<T, U> *) set.listp, is_intersect );
    return ((SetStorage32 *) this->listp)->sinter<T, U>( *this, set,
                                *(SetStorage<T, U> *) set.listp, is_intersect );
  }
  SetStatus sunion( SetData &set ) {
    if ( is_uint8( set.size ) )
      return this->sunion<uint16_t, uint8_t>( set );
    else if ( is_uint16( set.size ) )
      return this->sunion<uint32_t, uint16_t>( set );
    return this->sunion<uint64_t, uint32_t>( set );
  }
  SetStatus sinter( SetData &set ) {
    if ( is_uint8( set.size ) )
      return this->sinter<uint16_t, uint8_t>( set, true );
    else if ( is_uint16( set.size ) )
      return this->sinter<uint32_t, uint16_t>( set, true );
    return this->sinter<uint64_t, uint32_t>( set, true );
  }
  SetStatus sdiff( SetData &set ) {
    if ( is_uint8( set.size ) )
      return this->sinter<uint16_t, uint8_t>( set, false );
    else if ( is_uint16( set.size ) )
      return this->sinter<uint32_t, uint16_t>( set, false );
    return this->sinter<uint64_t, uint32_t>( set, false );
  }
};

}
}

#endif
