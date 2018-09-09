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
  HASH_EXISTS    = 4,              /* found existing item, not updated */
  HASH_BAD       = 5               /* hash corrupt */
};

struct HashVal : public ListVal {
  char key[ 256 ];
  size_t keylen;
  void zero( void ) {
    this->sz = this->sz2 = this->keylen = 0;
  }
};

struct HashPos {
  size_t   i;
  uint32_t h;
  HashPos( uint32_t hash = 0 ) : i( 0 ), h( hash ) {}
  void init( const void *key,  size_t keylen ) {
    this->i = 0;
    this->h = kv_crc_c( key, keylen, 0 );
  }
};

struct MergeCtx {
  size_t          len,    /* number of members */
                  i,      /* counter of members */
                  j,      /* index into map[] */
                  left;   /* map[ left ], map2[ len - left ] */
  const uint8_t * map,    /* current map */
                * map2;   /* next map, may be split across buffer */
  uint64_t        bits[ 4 ]; /* hash filter, 1 bit set for each member */
  bool            is_started, /* if state started */
                  is_reverse; /* end to start */

  void init( void ) {
    this->is_started = false;
  }
  void start( size_t start,  size_t sz,  size_t count,  size_t data_size,
              const void *m,  const void *m2,  bool rev ) {
    this->i          = 1; /* start index, 0 is the map element */
    this->len        = kv::min<size_t>( sz, count ); /* count of hash elems */
    this->left       = kv::min<size_t>( data_size - start, this->len );
    this->map        = (const uint8_t *) m;
    this->map2       = (const uint8_t *) m2;
    this->is_started = true;
    this->is_reverse = rev;
  }
  bool is_complete( void ) {
    this->j = ( ! this->is_reverse ? this->i : this->len - this->i );
    return ( this->i == this->len );
  }
  void incr( void ) {
    this->i += 1;
  }
  bool get_pos( HashPos &pos ) {
    pos.i = 0;
    pos.h = ( this->j < this->left ? this->map[ this->j ] :
                                     this->map2[ this->j - this->left ] );
    /* if not present, no need to search for it */
    if ( ( this->bits[ pos.h >> 6 ] &
           ( (uint64_t) 1U << ( pos.h & 63 ) ) ) == 0 )
      return false;
    return true;
  }
};

template <class UIntSig, class UIntType>
struct HashStorage : public ListStorage<UIntSig, UIntType> {
  /* add a zero length hash map at position talbe[ 0 ] */
  bool init_hash( const ListHeader &hdr ) {
    size_t start;
    if ( this->rpush_size( hdr, 0, start ) != LIST_OK )
      return false;
    return true;
  }
  /* resize the hash map */
  bool resize_hash( const ListHeader &hdr ) {
    size_t start, end, cur_size, new_size, need, new_start, pad;
    cur_size  = this->get_size( hdr, 0, start, end );
    if ( (pad = cur_size / 4) < 2 )
      pad = 2;
    new_size  = kv::max<size_t>( cur_size, this->count + pad );
    new_size  = ( new_size + 7 ) & ~(size_t) 7;
    need      = new_size - cur_size;
    new_start = hdr.data_offset( start, -need );
    if ( this->data_full( hdr, need ) )
      return false;
    this->data_start = new_start;
    this->index_ref( hdr, 0 ) = (UIntType) new_start;
    this->data_len += need;
    if ( cur_size == 0 )
      ((uint8_t *) hdr.blob( new_start ))[ 0 ] = 0;
    else if ( start + cur_size <= hdr.data_size() )
      this->copy_move( hdr, start, cur_size, new_start );
    else {
      need = hdr.data_size() - start;
      this->copy_move( hdr, start, need, new_start );
      this->copy_move( hdr, 0, cur_size - need,
                       hdr.data_offset( new_start, need ) );
    }
    return true;
  }
  /* find a hash element, this is called repeatedly incr the hash pos */
  bool hash_find( const ListHeader &hdr,  HashPos &pos ) const {
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
    len   = kv::min<size_t>( this->count, sz );
    end   = hdr.data_offset( start, len );
    start = hdr.data_offset( start, pos.i );
    if ( pos.i >= len )
      return false;
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
    pos.i = hdr.index( this->count );
    return false;
  }
  /* append the hash element at the end of the table */
  HashStatus hash_append( const ListHeader &hdr,  const HashPos &pos ) {
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
  /* delete the hash element table[ pos ] */
  void hash_delete( const ListHeader &hdr,  size_t pos ) {
    size_t    start,
              end,
              len,
              sz;
    uint8_t * map,
            * hmap;
    /* count should already be decremented */
    if ( pos == this->count ) /* delete the last item */
      return;
    sz    = this->get_size( hdr, 0, start, end );
    len   = kv::min<size_t>( this->count + 1, sz ); /* already deleted list item */
    end   = hdr.data_offset( start, len );
    start = hdr.data_offset( start, pos );
    len  -= pos;
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
  /* insert a new hash element pos.h at pos.i, resizing if necessary */
  HashStatus hash_insert( const ListHeader &hdr,  const HashPos &pos ) {
    size_t    start,
              end,
              len,
              sz;
    uint8_t * map,
            * hmap;
    sz = this->get_size( hdr, 0, start, end );
    if ( this->count >= sz ) {
      if ( ! this->resize_hash( hdr ) )
        return HASH_FULL;
      sz = this->get_size( hdr, 0, start, end );
    }
    len   = kv::min<size_t>( this->count, sz );
    end   = hdr.data_offset( start, len+1 );
    start = hdr.data_offset( start, pos.i );
    len  -= pos.i;
    map   = (uint8_t *) hdr.blob( start );
    if ( end >= start ) {
      ::memmove( &map[ 1 ], map, len );
    }
    else {
      hmap = (uint8_t *) hdr.blob( 0 );
      if ( end > 1 )
        ::memmove( &hmap[ 1 ], hmap, end - 1 );
      hmap[ 0 ] = hmap[ hdr.data_size() - 1 ];
      if ( start + 1 < hdr.data_size() )
        ::memmove( &map[ 1 ], map, hdr.data_size() - ( start + 1 ) );
    }
    map[ 0 ] = (uint8_t) pos.h;
    return HASH_OK;
  }
  /* print the hash map */
  void print_hashes( const ListHeader &hdr ) const {
    size_t          start, end, len, sz, off;
    const uint8_t * map;
    sz  = this->get_size( hdr, 0, start, end );
    len = kv::min<size_t>( this->count, sz );
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
  /* set a bit in a 256 bit array for each hash item in the table */
  void get_hash_bits( const ListHeader &hdr,  uint64_t *bits ) const {
    size_t          start, end, len, sz, i, j;
    const uint8_t * map;
    /* set hash bits for each element */
    sz  = this->get_size( hdr, 0, start, end );
    len = kv::min<size_t>( this->count, sz );
    for ( j = 0; j < 4; j++ )
      bits[ j ] = 0;
    map = (const uint8_t *) hdr.blob( start );
    if ( end >= start )
      j = len;
    else
      j = kv::min<size_t>( hdr.data_size() - start, len );
    for ( i = 1; i < j; i++ )
      bits[ map[ i ] >> 6 ] |= (uint64_t) 1U << ( map[ i ] & 63 );
    if ( j != len ) {
      map = (const uint8_t *) hdr.blob( 0 );
      for ( j = 0; i < len; i++, j++ )
        bits[ map[ j ] >> 6 ] |= (uint64_t) 1U << ( map[ j ] & 63 );
    }
  }
  /* match a key at table[ pos ] */
  bool match_key( const ListHeader &hdr,  const void *key,  size_t keylen,
                  size_t pos,  size_t *data_off = 0,
                  size_t *data_size = 0 ) const {
    size_t start, end, len;
    if ( pos < hdr.index( this->count ) ) {
      len = this->get_size( hdr, pos, start, end );
      if ( len >= keylen + 1 ) {
        if ( *(uint8_t *) hdr.blob( start ) == keylen &&
             hdr.equals( hdr.data_offset( start, 1 ), key, keylen ) ) {
          if ( data_off != NULL ) {
            *data_off  = hdr.data_offset( start, keylen + 1 );
            *data_size = len - ( keylen + 1 );
          }
          return true;
        }
      }
    }
    return false;
  }
  /* test if the key exists, hash pos iterator updated */
  HashStatus hexists( const ListHeader &hdr,  const void *key,
                      size_t keylen,  HashPos &pos ) const {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match_key( hdr, key, keylen, pos.i ) )
        return HASH_OK;
    }
  }
  /* get the key + value at table[ n ] */
  HashStatus hindex( const ListHeader &hdr,  size_t n,  HashVal &kv ) const {
    HashStatus hstat;
    kv.zero();
    hstat = (HashStatus) this->lindex( hdr, n, kv );
    if ( hstat != HASH_NOT_FOUND ) {
      if ( kv.sz == 0 ||
           (kv.keylen = *(uint8_t *) kv.data) + 1 > kv.sz + kv.sz2 )
        return HASH_BAD;
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
  /* get the value, returned in a list val, using the hash pos iterator  */
  HashStatus hget( const ListHeader &hdr,  const void *key,  size_t keylen,
                   ListVal &kv,  HashPos &pos ) const {
    size_t  data_off,
            data_len;
    kv.zero();
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match_key( hdr, key, keylen, pos.i, &data_off, &data_len ) ) {
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
  /* update the key + value */
  void copy_item( const ListHeader &hdr,  const void *key,  size_t keylen,
                  const void *val,  size_t vallen,  size_t start ) {
    *(uint8_t *) hdr.blob( start ) = (uint8_t) keylen;
    start  = hdr.data_offset( start, 1 );
    hdr.copy2( start, key, keylen );
    start  = hdr.data_offset( start, keylen );
    hdr.copy2( start, val, vallen );
  }
  /* update the key + value, list val style */
  void copy_item( const ListHeader &hdr,  const void *key,  size_t keylen,
                  const ListVal &lv,  size_t start ) {
    *(uint8_t *) hdr.blob( start ) = (uint8_t) keylen;
    start  = hdr.data_offset( start, 1 );
    hdr.copy2( start, key, keylen );
    start  = hdr.data_offset( start, keylen );
    if ( lv.sz > 0 ) {
      hdr.copy2( start, lv.data, lv.sz );
      start = hdr.data_offset( start, lv.sz );
    }
    if ( lv.sz2 > 0 )
      hdr.copy2( start, lv.data2, lv.sz2 );
  }
  /* update the value */
  void copy_value( const ListHeader &hdr,  size_t keylen,
                   const void *val,  size_t vallen,  size_t start ) {
    hdr.copy2( hdr.data_offset( start, keylen + 1 ), val, vallen );
  }
  /* append a new kv, list val style */
  HashStatus happend( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const ListVal &lv,  const HashPos &pos ) {
    size_t     start;
    HashStatus hstat = this->hash_append( hdr, pos );
    if ( hstat == HASH_OK ) {
      hstat = (HashStatus) this->rpush_size( hdr, keylen + lv.sz + lv.sz2 + 1,
                                             start );
      if ( hstat == HASH_OK )
        this->copy_item( hdr, key, keylen, lv, start );
    }
    return hstat;
  }
  /* append a new kv */
  HashStatus happend( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  const HashPos &pos ) {
    size_t     start;
    HashStatus hstat = this->hash_append( hdr, pos );
    if ( hstat == HASH_OK ) {
      hstat = (HashStatus) this->rpush_size( hdr, keylen + vallen + 1, start );
      if ( hstat == HASH_OK )
        this->copy_item( hdr, key, keylen, val, vallen, start );
    }
    return hstat;
  }
  /* update or append a kv after it is located */
  HashStatus hupdate( const ListHeader &hdr,  const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  const HashPos &pos ) {
    if ( pos.i >= this->count )
      return this->happend( hdr, key, keylen, val, vallen, pos );

    size_t old_size = this->get_size( hdr, pos.i ),
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
  /* set a kv, update or append */
  HashStatus hset( const ListHeader &hdr,  const void *key,  size_t keylen,
                   const void *val,  size_t vallen,  HashPos &pos ) {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) ||
           this->match_key( hdr, key, keylen, pos.i ) )
        break;
    }
    return this->hupdate( hdr, key, keylen, val, vallen, pos );
  }
  /* set a kv if it doesn't exist */
  HashStatus hsetnx( const ListHeader &hdr,  const void *key,  size_t keylen,
                     const void *val,  size_t vallen,  HashPos &pos ) {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return this->happend( hdr, key, keylen, val, vallen, pos );
      if ( this->match_key( hdr, key, keylen, pos.i ) )
        return HASH_EXISTS;
    }
  }
  /* set a kv if it doesn't exist, list val style */
  HashStatus hsetnx( const ListHeader &hdr,  const void *key,  size_t keylen,
                     const ListVal &lv,  HashPos &pos ) {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return this->happend( hdr, key, keylen, lv, pos );
      if ( this->match_key( hdr, key, keylen, pos.i ) )
        return HASH_EXISTS;
    }
  }
  /* remove key at table[ pos ] */
  HashStatus hrem( const ListHeader &hdr,  size_t pos ) {
    HashStatus hstat = (HashStatus) this->lrem( hdr, pos );
    if ( hstat == HASH_OK )
      this->hash_delete( hdr, pos );
    return hstat;
  }
  HashStatus hremall( const ListHeader & ) {
    if ( this->count > 1 )
      this->count = 1;
    return HASH_OK;
  }
  /* find key and delete it */
  HashStatus hdel( const ListHeader &hdr,  const void *key,  size_t keylen,
                   HashPos &pos ) {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return HASH_NOT_FOUND;
      if ( this->match_key( hdr, key, keylen, pos.i ) )
        return this->hrem( hdr, pos.i );
    }
  }
  /* check that the hashes are correct */
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
    len = kv::min<size_t>( this->count, sz );
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
  /* me={a,b,c,d} | x={a,c,e} = me={a,b,c,d,e} */
  template<class T, class U>
  HashStatus hunion( const ListHeader &myhdr,  const ListHeader &xhdr,
                     HashStorage<T, U> &x,  MergeCtx &ctx ) {
    HashPos    pos;
    HashVal    kv;
    HashStatus hstat;
    if ( x.count <= 1 )
      return HASH_OK;
    if ( ! ctx.is_started ) {
      size_t start, end, sz;
      this->get_hash_bits( myhdr, ctx.bits );
      sz = x.get_size( xhdr, 0, start, end );
      ctx.start( start, sz, x.count, xhdr.data_size(), xhdr.blob( start ),
                 xhdr.blob( 0 ), false );
    }
    while ( ! ctx.is_complete() ) {
      if ( (hstat = x.hindex( xhdr, ctx.j, kv )) != HASH_OK )
        return hstat;
      if ( ! ctx.get_pos( pos ) ) {
        hstat = this->hsetnx( myhdr, kv.key, kv.keylen, kv, pos );
        if ( hstat == HASH_FULL || hstat == HASH_BAD )
          return hstat;
      }
      ctx.incr();
    }
    return HASH_OK;
  }
  /* intersect:  me={a,b,c,d} & x={a,c,e} = me={a,c} */
  /* difference: me={a,b,c,d} - x={a,c,e} = me={b,d} */
  template<class T, class U>
  HashStatus hinter( const ListHeader &myhdr,  const ListHeader &xhdr,
                    HashStorage<T, U> &x,  MergeCtx &ctx,
                    bool is_intersect /* or diff */) {
    HashPos    pos;
    HashVal    kv;
    HashStatus hstat;
    if ( x.count <= 1 || this->count <= 1 ) {
      if ( is_intersect )
        this->count = 0;
      return HASH_OK;
    }
    if ( ! ctx.is_started ) {
      size_t start, end, sz;
      x.get_hash_bits( xhdr, ctx.bits );
      sz = this->get_size( myhdr, 0, start, end );
      ctx.start( start, sz, this->count, myhdr.data_size(), myhdr.blob( start ),
                 myhdr.blob( 0 ), true );
    }
    while ( ! ctx.is_complete() ) {
      bool diff = ! ctx.get_pos( pos ); /* diff true when not present */
      if ( ! diff ) {
        hstat = (HashStatus) this->hindex( myhdr, ctx.j, kv );
        if ( hstat != HASH_OK )
          return hstat;
        if ( x.hexists( xhdr, kv.key, kv.keylen, pos ) == HASH_NOT_FOUND )
          diff = true;
      }
      if ( is_intersect ) {
        if ( diff )
          this->hrem( myhdr, ctx.j );
      }
      else {
        if ( ! diff )
          this->hrem( myhdr, ctx.j );
      }
      ctx.incr();
    }
    return HASH_OK;
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
  /* first list slot is the hash index */
  size_t hcount( void ) {
    size_t cnt = this->count();
    if ( cnt >= 1 ) return cnt - 1;
    return 0;
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
  HashStatus hexists( const void *key,  size_t keylen,  HashPos &pos ) {
    return HASH_CALL( hexists( *this, key, keylen, pos ) );
  }
  HashStatus hget( const void *key,  size_t keylen,  ListVal &kv,
                   HashPos &pos ) const {
    return HASH_CALL( hget( *this, key, keylen, kv, pos ) );
  }
  HashStatus hset( const void *key,  size_t keylen,
                   const void *val,  size_t vallen,  HashPos &pos ) {
    return HASH_CALL( hset( *this, key, keylen, val, vallen, pos ) );
  }
  HashStatus hupdate( const void *key,  size_t keylen,
                      const void *val,  size_t vallen,  HashPos &pos ) {
    return HASH_CALL( hupdate( *this, key, keylen, val, vallen, pos ) );
  }
  HashStatus hsetnx( const void *key,  size_t keylen,
                     const void *val,  size_t vallen,  HashPos &pos ) {
    return HASH_CALL( hsetnx( *this, key, keylen, val, vallen, pos ) );
  }
  HashStatus hindex( size_t n,  HashVal &kv ) const {
    return HASH_CALL( hindex( *this, n, kv ) );
  }
  HashStatus hdel( const void *key,  size_t keylen,  HashPos &pos ) {
    return HASH_CALL( hdel( *this, key, keylen, pos ) );
  }
  HashStatus hrem( size_t pos ) {
    return HASH_CALL( hrem( *this, pos ) );
  }
  HashStatus hremall( void ) {
    return HASH_CALL( hremall( *this ) );
  }
  void print_hashes( void ) {
    return HASH_CALL( print_hashes( *this ) );
  }
  template<class T, class U>
  HashStatus hunion( HashData &hash,  MergeCtx &ctx ) {
    if ( is_uint8( this->size ) )
      return ((HashStorage8 *) this->listp)->hunion<T, U>( *this, hash,
                                       *(HashStorage<T, U> *) hash.listp, ctx );
    else if ( is_uint16( this->size ) )
      return ((HashStorage16 *) this->listp)->hunion<T, U>( *this, hash,
                                       *(HashStorage<T, U> *) hash.listp, ctx );
    return ((HashStorage32 *) this->listp)->hunion<T, U>( *this, hash,
                                       *(HashStorage<T, U> *) hash.listp, ctx );
  }
  template<class T, class U>
  HashStatus hinter( HashData &hash,  MergeCtx &ctx,  bool is_intersect ) {
    if ( is_uint8( this->size ) )
      return ((HashStorage8 *) this->listp)->hinter<T, U>( *this, hash,
                         *(HashStorage<T, U> *) hash.listp, ctx, is_intersect );
    else if ( is_uint16( this->size ) )
      return ((HashStorage16 *) this->listp)->hinter<T, U>( *this, hash,
                         *(HashStorage<T, U> *) hash.listp, ctx, is_intersect );
    return ((HashStorage32 *) this->listp)->hinter<T, U>( *this, hash,
                         *(HashStorage<T, U> *) hash.listp, ctx, is_intersect );
  }
  HashStatus hunion( HashData &hash,  MergeCtx &ctx ) {
    if ( is_uint8( hash.size ) )
      return this->hunion<uint16_t, uint8_t>( hash, ctx );
    else if ( is_uint16( hash.size ) )
      return this->hunion<uint32_t, uint16_t>( hash, ctx );
    return this->hunion<uint64_t, uint32_t>( hash, ctx );
  }
  HashStatus hinter( HashData &hash,  MergeCtx &ctx ) {
    if ( is_uint8( hash.size ) )
      return this->hinter<uint16_t, uint8_t>( hash, ctx, true );
    else if ( is_uint16( hash.size ) )
      return this->hinter<uint32_t, uint16_t>( hash, ctx, true );
    return this->hinter<uint64_t, uint32_t>( hash, ctx, true );
  }
  HashStatus hdiff( HashData &hash,  MergeCtx &ctx ) {
    if ( is_uint8( hash.size ) )
      return this->hinter<uint16_t, uint8_t>( hash, ctx, false );
    else if ( is_uint16( hash.size ) )
      return this->hinter<uint32_t, uint16_t>( hash, ctx, false );
    return this->hinter<uint64_t, uint32_t>( hash, ctx, false );
  }
};

}
}

#endif
