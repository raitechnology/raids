#ifndef __rai_raids__redis_zset_h__
#define __rai_raids__redis_zset_h__

#include "redis_hash.h"

namespace rai {
namespace ds {

enum ZSetStatus {
  ZSET_OK        = HASH_OK,        /* success and/or new key/value */
  ZSET_NOT_FOUND = HASH_NOT_FOUND, /* key not found */
  ZSET_FULL      = HASH_FULL,      /* no room, needs resize */
  ZSET_UPDATED   = HASH_UPDATED,   /* success, replaced existing key/value */
  ZSET_EXISTS    = HASH_EXISTS,    /* found existing item, not updated */
  ZSET_BAD       = HASH_BAD        /* data corrupt */
};

enum ZAddFlags {
  ZADD_MUST_NOT_EXIST = 1, /* NX -- only add if doesn't exist */
  ZADD_MUST_EXIST     = 2, /* XX -- only update if exists */
  ZADD_RET_CHANGED    = 4, /* CH -- count which were changed and/or added */
  ZADD_INCR           = 8  /* INCR -- same as ZINCR */
};

enum ZAggregateType {
  ZAGGREGATE_NONE = 0,
  ZAGGREGATE_SUM  = 1,
  ZAGGREGATE_MIN  = 2,
  ZAGGREGATE_MAX  = 3
};

typedef _Decimal64 ZScore;

struct ZSetVal : public ListVal {
  ZScore score;
  void zero( void ) {
    this->sz = this->sz2 = 0;
    this->score = 0;
  }
  ZSetStatus split_score( void ) {
    if ( sizeof( ZScore ) > this->sz + this->sz2 )
      return ZSET_BAD;
    if ( this->sz >= sizeof ( ZScore ) ) {
      ::memcpy( &this->score, this->data, sizeof( ZScore ) );
      this->data = &((uint8_t *) this->data)[ sizeof( ZScore ) ];
      this->sz  -= sizeof( ZScore );
    }
    else { /* score is split between data & data2 */
      ::memcpy( &this->score, this->data, this->sz );
      size_t overlap = sizeof( ZScore ) - this->sz;
      ::memcpy( &((uint8_t *) &this->score)[ this->sz ], this->data2, overlap );
      this->data = &((uint8_t *) this->data2)[ overlap ];
      this->sz   = this->sz2 - overlap;
      this->sz2  = 0;
    }
    return ZSET_OK;
  }
};

struct ZMergeCtx : public MergeCtx {
  ZScore          weight; /* weighted merge, multiply before aggregation */
  ZAggregateType  type;   /* aggregate by sum, min, max */
  bool            weighted;   /* if weight > 1 */

  void init( ZScore w,  ZAggregateType t ) {
    this->MergeCtx::init();
    this->weight     = w;
    this->type       = t;
    this->weighted   = ( w != 1 );
  }
};

template <class UIntSig, class UIntType>
struct ZSetStorage : public HashStorage<UIntSig, UIntType> {

  bool match_member( const ListHeader &hdr,  const void *key,  size_t keylen,
                     size_t pos,  ZScore *score=0,  size_t *key_off=0 ) const {
    size_t key_start, score_off, end, len;
    if ( pos < hdr.index( this->count ) ) {
      len = this->get_size( hdr, pos, score_off, end );
      if ( len == sizeof( ZScore ) + keylen ) {
        key_start = hdr.data_offset( score_off, sizeof( ZScore ) );
        if ( hdr.equals( key_start, key, keylen ) ) {
          if ( score != NULL ) {
            hdr.copy( score, score_off, sizeof( ZScore ) );
            if ( key_off != NULL )
              *key_off = key_start;
          }
          return true;
        }
      }
    }
    return false;
  }

  bool match_member( const ListHeader &hdr,  const ListVal &lv,
                     size_t pos ) const {
    size_t key_start, score_off, end, len;
    if ( pos < hdr.index( this->count ) ) {
      len = this->get_size( hdr, pos, score_off, end );
      if ( len == sizeof( ZScore ) + lv.sz + lv.sz2 ) {
        key_start = hdr.data_offset( score_off, sizeof( ZScore ) );
        if ( hdr.equals( key_start, lv.data, lv.sz, lv.data2, lv.sz2 ) )
          return true;
      }
    }
    return false;
  }

  ZSetStatus zexists( const ListHeader &hdr,  const void *key,
                      size_t keylen,  HashPos &pos,  ZScore &score ) const {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return ZSET_NOT_FOUND;
      if ( this->match_member( hdr, key, keylen, pos.i, &score ) )
        return ZSET_OK;
    }
  }

  ZSetStatus zexists( const ListHeader &hdr,  const ListVal &lv,
                      HashPos &pos ) const {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return ZSET_NOT_FOUND;
      if ( this->match_member( hdr, lv, pos.i ) )
        return ZSET_OK;
    }
  }

  ZSetStatus zindex( const ListHeader &hdr,  size_t n,  ZSetVal &zv ) const {
    ZSetStatus zstat;
    zv.zero();
    zstat = (ZSetStatus) this->lindex( hdr, n, zv );
    if ( zstat != ZSET_NOT_FOUND )
      zstat = zv.split_score();
    return zstat;
  }

  ZSetStatus zget( const ListHeader &hdr,  const void *key,  size_t keylen,
                   ZSetVal &zv,  HashPos &pos ) const {
    size_t key_off;
    zv.zero();
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return ZSET_NOT_FOUND;
      if ( this->match_member( hdr, key, keylen, pos.i, &zv.score, &key_off ) ) {
        zv.data = hdr.blob( key_off );
        zv.sz   = keylen;
        if ( key_off + keylen > hdr.data_size() ) {
          zv.sz    = hdr.data_size() - key_off;
          zv.sz2   = ( keylen + sizeof( ZScore ) ) - zv.sz;
          zv.data2 = hdr.blob( 0 );
        }
        return ZSET_OK;
      }
    }
  }
  /* copy score + key into set */
  void copy_item( const ListHeader &hdr,  const void *key,  size_t keylen,
                  ZScore score,  size_t start ) {
    hdr.copy2( start, &score, sizeof( ZScore ) );
    hdr.copy2( hdr.data_offset( start, sizeof( ZScore ) ), key, keylen );
  }
  /* copy score into set */
  void copy_score( const ListHeader &hdr,  ZScore score,  size_t start ) {
    hdr.copy2( start, &score, sizeof( ZScore ) );
  }
  /* append a new score + key */
  ZSetStatus zappend( const ListHeader &hdr,  const void *key,  size_t keylen,
                      ZScore score,  const HashPos &pos ) {
    size_t     start;
    ZSetStatus zstat = (ZSetStatus) this->hash_append( hdr, pos );
    if ( zstat == ZSET_OK ) {
      zstat = (ZSetStatus)
              this->rpush_size( hdr, keylen + sizeof( ZScore ), start );
      if ( zstat == ZSET_OK )
        this->copy_item( hdr, key, keylen, score, start );
    }
    return zstat;
  }
  /* add or insert, depending on bool ins */
  ZSetStatus zaddins( const ListHeader &hdr,  const ZSetVal &zv,
                      HashPos &pos,  bool ins,  ZAggregateType t,
                      int add_flags ) {
    char       buf[ 256 ];
    void     * key;
    size_t     keylen;
    ZSetStatus zstat;
    bool       is_alloced = false;

    keylen = zv.unitary( key, buf, sizeof( buf ), is_alloced );
    if ( ins )
      zstat = this->zinsert( hdr, key, keylen, zv.score, pos );
    else
      zstat = this->zadd( hdr, key, keylen, zv.score, pos, t, add_flags, NULL );
    if ( is_alloced )
      ::free( key );
    return zstat;
  }
  /* insert in score + lexical sorted order */
  ZSetStatus zinsert( const ListHeader &hdr,  const void *key,  size_t keylen,
                      ZScore score,  HashPos &pos ) {
    size_t start;  /* find the insert position */
    ZSetStatus zstat = this->zbsearch_key( hdr, key, keylen, score, true,
                                           pos.i );
    /* if no items in the list or inserts at the end of the list */
    if ( zstat == ZSET_NOT_FOUND ||
         ( zstat == ZSET_OK && pos.i == this->count ) ) {
      zstat = this->zappend( hdr, key, keylen, score, pos );
    }
    else if ( zstat == ZSET_OK ) {
      /* make sure there is enough hash space before modifying the set */
      if ( (size_t) ( this->count + 1 ) >= this->get_size( hdr, 0 ) )
        if ( ! this->resize_hash( hdr ) )
          return ZSET_FULL;
      /* insert a space in the set */
      zstat = (ZSetStatus) this->insert_space( hdr, pos.i - 1, keylen +
                                               sizeof( ZScore ), start );
      /* update the hash and copy into the set */
      if ( zstat == ZSET_OK ) {
        zstat = (ZSetStatus) this->hash_insert( hdr, pos );
        if ( zstat == ZSET_OK )
          this->copy_item( hdr, key, keylen, score, start );
      }
    }
    return zstat;
  }
  /* do insert or append, modified by add_flags and aggregate type */
  ZSetStatus zadd( const ListHeader &hdr,  const void *key,  size_t keylen,
                   ZScore score,  HashPos &pos,  ZAggregateType t,
                   int add_flags,  ZScore *result ) {
    ZSetVal    zv;
    ZScore     r;
    ZSetStatus zstat;
    bool       in_order,
               is_new = false;

    if ( this->zexists( hdr, key, keylen, pos, r ) == ZSET_NOT_FOUND ) {
      if ( ( add_flags & ZADD_MUST_EXIST ) != 0 )
        return ZSET_NOT_FOUND;
      is_new = true;
    do_insert:;
      if ( result != NULL )
        *result = score;
      zstat = this->zinsert( hdr, key, keylen, score, pos );
      if ( zstat == ZSET_OK ) {
        if ( is_new || ( add_flags & ZADD_RET_CHANGED ) != 0 )
          return ZSET_UPDATED;
        return ZSET_OK;
      }
      return zstat;
    }
    else {
      if ( result != NULL )
        *result = score;
      if ( ( add_flags & ZADD_MUST_NOT_EXIST ) != 0 )
        return ZSET_EXISTS;
    }
    if ( ( add_flags & ZADD_INCR ) != 0 )
      score += r;
    else {
      switch ( t ) {
        case ZAGGREGATE_NONE: break;
        case ZAGGREGATE_SUM:  score += r; break;
        case ZAGGREGATE_MIN:  if ( r < score ) score = r; break;
        case ZAGGREGATE_MAX:  if ( r > score ) score = r; break;
      }
    }
    if ( result != NULL )
      *result = score;
    if ( score == r ) /* if no change */
      return ZSET_OK;
    in_order = true;
    if ( pos.i > 1 ) {
      if ( (zstat = this->zindex( hdr, pos.i - 1, zv )) != ZSET_OK )
        return zstat;
      if ( score <= zv.score ) {
        if ( zv.score != score || zv.cmp_key( key, keylen ) < 0 )
          in_order = false;
      }
    }
    if ( in_order && pos.i + 1 < this->count ) {
      if ( (zstat = this->zindex( hdr, pos.i + 1, zv )) != ZSET_OK )
        return zstat;
      if ( score >= zv.score ) {
        if ( zv.score != score || zv.cmp_key( key, keylen ) > 0 )
          in_order = false;
      }
    }
    if ( in_order ) {
      this->copy_score( hdr, score, this->get_offset( hdr, pos.i ) );
      if ( ( add_flags & ZADD_RET_CHANGED ) != 0 )
        return ZSET_UPDATED;
      return ZSET_OK;
    }
    zstat = (ZSetStatus) this->lrem( hdr, pos.i );
    if ( zstat == ZSET_OK ) {
      this->hash_delete( hdr, pos.i );
      goto do_insert;
    }
    return zstat;
  }
  /* find key in hash and remove */
  ZSetStatus zrem( const ListHeader &hdr,  const void *key,  size_t keylen,
                   HashPos &pos ) {
    for ( ; ; pos.i++ ) {
      if ( ! this->hash_find( hdr, pos ) )
        return ZSET_NOT_FOUND;
      if ( this->match_member( hdr, key, keylen, pos.i ) ) {
        ZSetStatus zstat = (ZSetStatus) this->lrem( hdr, pos.i );
        if ( zstat == ZSET_OK )
          this->hash_delete( hdr, pos.i );
        return zstat;
      }
    }
  }
  /* remove the score + key at table[ pos ] */
  ZSetStatus zrem_index( const ListHeader &hdr,  size_t pos ) {
    ZSetStatus zstat = (ZSetStatus) this->lrem( hdr, pos );
    if ( zstat == ZSET_OK )
      this->hash_delete( hdr, pos );
    return zstat;
  }

  ZSetStatus zremall( const ListHeader & ) {
    if ( this->count > 1 )
      this->count = 1;
    return ZSET_OK;
  }
  /* get the score at table[ n ] */
  ZSetStatus zscore( const ListHeader &hdr,  size_t n,  ZScore &score ) const {
    ListVal lv;
    ZSetStatus zstat = (ZSetStatus) this->lindex( hdr, n, lv );
    if ( zstat != ZSET_NOT_FOUND ) {
      if ( lv.concat( &score, sizeof( ZScore ) ) != sizeof( ZScore ) )
        zstat = ZSET_BAD;
    }
    return zstat;
  }
  /* search by score within a range of the table */
  ZSetStatus zbsearch_range( const ListHeader &hdr,  ZScore score,
                             size_t &pos,  bool gt,  size_t size,
                             ZScore &r ) {
    size_t     piv;
    ZSetStatus zstat;
    /* if gt, find position where score > elem, otherwise where score >= elem
     * ex:  1, 2, 2, 2, 3  (pos=1, size=5)
     * search for 2, gt = false, pos = 2 (el=2)
     * search for 2, gt = true,  pos = 5 (el=3) */
    for (;;) {
      piv = size / 2;
      /* return ZSET_OK unless ZSET_BAD (if score doesn't fit in elem) */
      if ( (zstat = this->zscore( hdr, pos + piv, r )) != ZSET_OK )
        return ( zstat == ZSET_NOT_FOUND ) ? ZSET_OK : zstat;
      if ( size == 0 )
        break;
      if ( gt && score < r )
        size = piv;
      else if ( ! gt && score <= r )
        size = piv;
      else {
        size -= piv + 1;
        pos  += piv + 1;
      }
    }
    if ( gt && score == r )
      pos += 1;
    return ZSET_OK;
  }
  /* search by score, table is sorted by score then by lexical order */
  ZSetStatus zbsearch( const ListHeader &hdr,  ZScore score,  size_t &pos,
                       bool gt,  ZScore &r ) {
    size_t cnt  = this->count,
           size = cnt - 1;
    pos = 1;
    r   = score;
    if ( cnt <= 1 )
      return ZSET_NOT_FOUND;
    /* search pos=1 -> count - pos */
    return this->zbsearch_range( hdr, score, pos, gt, size, r );
  }
  /* search by key within a range of the table */
  ZSetStatus zbsearch_key_range( const ListHeader &hdr,  const void *key,
                                 size_t keylen,  bool gt,  size_t &pos,
                                 size_t size ) {
    size_t     piv;
    ZSetVal    zv;
    int        cmp;
    ZSetStatus zstat;
    for (;;) {
      if ( size == 0 )
        return ZSET_OK;
      piv = size / 2;
      /* return ZSET_OK unless ZSET_BAD (if score doesn't fit in elem) */
      if ( (zstat = this->zindex( hdr, pos + piv, zv )) != ZSET_OK )
        return ( zstat == ZSET_NOT_FOUND ) ? ZSET_OK : zstat;
      cmp = zv.cmp_key( key, keylen );
      if ( gt && cmp < 0 )
        size = piv;
      else if ( ! gt && cmp <= 0 )
        size = piv;
      else {
        size -= piv + 1;
        pos  += piv + 1;
      }
    }
  }
  /* find a key by score then by lexical order */
  ZSetStatus zbsearch_key( const ListHeader &hdr,  const void *key,
                           size_t keylen,  ZScore score,  bool gt,
                           size_t &pos )
  {
    ZScore r;
    size_t end;
    ZSetStatus zstat = this->zbsearch( hdr, score, pos, false, r );
    if ( zstat != ZSET_OK )
      return zstat;
    if ( pos >= this->count || score != r )
      return ZSET_OK;
    end = pos;
    zstat = this->zbsearch_range( hdr, score, end, true, this->count - pos, r );
    if ( zstat == ZSET_OK )
      zstat = this->zbsearch_key_range( hdr, key, keylen, gt, pos, end - pos );
    return zstat;
  }
  /* does not check that elements have the same score, which will screw
   * with the search results */
  ZSetStatus zbsearch_all( const ListHeader &hdr,  const void *key,
                           size_t keylen,  bool gt,  size_t &pos )
  {
    size_t end;
    pos = 1;
    if ( (end = this->count) <= 1 )
      return ZSET_NOT_FOUND;
    return this->zbsearch_key_range( hdr, key, keylen, gt, pos, end - pos );
  }

  int zverify( const ListHeader &hdr ) const {
    if ( this->count == 0 )
      return 0;
    size_t          start,
                    end,
                    len,
                    sz;
    const uint8_t * el;
    ZSetVal         zv;
    uint32_t         h;
    sz = this->get_size( hdr, 0, start, end );
    if ( this->count > sz )
      return -30;
    len = min<size_t>( this->count, sz );
    for ( size_t i = 1; i < len; i++ ) {
      ZSetStatus zstat = this->zindex( hdr, i, zv );
      if ( zstat != ZSET_OK )
        return -31;
      if ( zv.sz + zv.sz2 >= hdr.data_size() )
        return -32;
      h = kv_crc_c( zv.data, zv.sz, 0 );
      if ( zv.sz2 > 0 )
        h = kv_crc_c( zv.data2, zv.sz2, h );
      el = (const uint8_t *) hdr.blob( hdr.data_offset( start, i ) );
      if ( el[ 0 ] != (uint8_t) h )
        return -33;
    }
    return 0;
  }

  ZSetStatus zscale( const ListHeader &hdr,  ZScore w ) {
    if ( this->count == 0 )
      return ZSET_OK;
    size_t     len,
               sz;
    ZScore     score;
    ZSetStatus zstat;

    sz  = this->get_size( hdr, 0 );
    len = min<size_t>( this->count, sz );
    for ( size_t i = 1; i < len; i++ ) {
      if ( (zstat = this->zscore( hdr, i, score )) != ZSET_OK )
        break;
      this->copy_score( hdr, w * score, this->get_offset( hdr, i ) );
    }
    return ZSET_OK;
  }

  /* me={a,b,c,d} | x={a,c,e} = me={a,b,c,d,e} */
  template<class T, class U>
  ZSetStatus zunion( const ListHeader &myhdr,  const ListHeader &xhdr,
                     ZSetStorage<T, U> &x,  ZMergeCtx &ctx ) {
    HashPos    pos;
    ZSetVal    zv;
    ZSetStatus zstat;
    if ( x.count <= 1 )
      return ZSET_OK;
    if ( ! ctx.is_started ) {
      size_t start, end, sz;
      this->get_hash_bits( myhdr, ctx.bits );
      sz = x.get_size( xhdr, 0, start, end );
      ctx.start( start, sz, x.count, xhdr.data_size(), xhdr.blob( start ),
                 xhdr.blob( 0 ), false );
    }
    while ( ! ctx.is_complete() ) {
      if ( (zstat = (ZSetStatus) x.lindex( xhdr, ctx.j, zv )) == ZSET_OK )
        zstat = zv.split_score();
      if ( zstat != ZSET_OK )
        return zstat;
      bool ins = ! ctx.get_pos( pos );
      if ( ctx.weighted )
        zv.score *= ctx.weight;
      zstat = this->zaddins( myhdr, zv, pos, ins, ctx.type, 0 );
      if ( zstat == ZSET_FULL || zstat == ZSET_BAD )
        return zstat;
      ctx.incr();
    }
    return ZSET_OK;
  }
  /* intersect:  me={a,b,c,d} & x={a,c,e} = me={a,c} */
  /* difference: me={a,b,c,d} - x={a,c,e} = me={b,d} */
  template<class T, class U>
  ZSetStatus zinter( const ListHeader &myhdr,  const ListHeader &xhdr,
                    ZSetStorage<T, U> &x,  ZMergeCtx &ctx,
                    bool is_intersect /* or diff */) {
    HashPos    pos;
    ZSetVal    zv;
    ZSetStatus zstat;
    size_t     start, end, sz;
    if ( x.count <= 1 || this->count <= 1 ) {
      if ( is_intersect )
        this->count = 0;
      return ZSET_OK;
    }
    x.get_hash_bits( xhdr, ctx.bits );
    sz = this->get_size( myhdr, 0, start, end );
    ctx.start( start, sz, this->count, myhdr.data_size(), myhdr.blob( start ),
               myhdr.blob( 0 ), true );
    /* iterate through my keys, remove those that are not found in x */
    while ( ! ctx.is_complete() ) {
      bool diff = ! ctx.get_pos( pos ); /* diff true when not present */
      if ( ! diff ) {
        zstat = (ZSetStatus) this->zindex( myhdr, ctx.j, zv );
        if ( zstat != ZSET_OK )
          return zstat;
        if ( x.zexists( xhdr, zv, pos ) == ZSET_NOT_FOUND )
          diff = true;
      }
      if ( is_intersect ) {
        if ( diff )
          this->zrem_index( myhdr, ctx.j );
      }
      else {
        if ( ! diff )
          this->zrem_index( myhdr, ctx.j );
      }
      ctx.incr();
    }
    /* iterate through x, update my scores sum+min+max and weights */
    if ( ctx.type != ZAGGREGATE_NONE ) {
      this->get_hash_bits( myhdr, ctx.bits );
      sz = x.get_size( xhdr, 0, start, end );
      ctx.start( start, sz, x.count, xhdr.data_size(), xhdr.blob( start ),
                 xhdr.blob( 0 ), false );
      while ( ! ctx.is_complete() ) {
        if ( (zstat = (ZSetStatus) x.lindex( xhdr, ctx.j, zv )) == ZSET_OK )
          zstat = zv.split_score();
        if ( zstat != ZSET_OK )
          return zstat;
        if ( ctx.get_pos( pos ) ) { /* if key present */
          if ( ctx.weighted )
            zv.score *= ctx.weight;
          zstat = this->zaddins( myhdr, zv, pos, false, ctx.type,
                                 ZADD_MUST_EXIST ); /* no insert, just update */
          if ( zstat == ZSET_FULL || zstat == ZSET_BAD )
            return zstat;
        }
        ctx.incr();
      }
    }
    return ZSET_OK;
  }
};

typedef ZSetStorage<uint16_t, uint8_t>  ZSetStorage8;
typedef ZSetStorage<uint32_t, uint16_t> ZSetStorage16;
typedef ZSetStorage<uint64_t, uint32_t> ZSetStorage32;

struct ZSetData : public HashData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  ZSetData() : HashData() {}
  ZSetData( void *l,  size_t sz ) : HashData( l, sz ) {}

#define ZSET_CALL( GOTO ) \
  ( is_uint8( this->size ) ? ((ZSetStorage8 *) this->listp)->GOTO : \
    is_uint16( this->size ) ? ((ZSetStorage16 *) this->listp)->GOTO : \
                              ((ZSetStorage32 *) this->listp)->GOTO )
  int zverify( void ) const {
    int x = this->lverify();
    if ( x == 0 )
      x = ZSET_CALL( zverify( *this ) );
    return x;
  }
  ZSetStatus zexists( const char *key,  size_t keylen,  HashPos &pos,
                      ZScore &score ) {
    return ZSET_CALL( zexists( *this, key, keylen, pos, score ) );
  }
  ZSetStatus zget( const char *key,  size_t keylen,  ZSetVal &zv,
                   HashPos &pos ) {
    return ZSET_CALL( zget( *this, key, keylen, zv, pos ) );
  }
  ZSetStatus zadd( const char *key,  size_t keylen,  ZScore score,
                   HashPos &pos,  int add_flags,  ZScore *result ) {
    return ZSET_CALL( zadd( *this, key, keylen, score, pos, ZAGGREGATE_NONE,
                            add_flags, result ) );
  }
  ZSetStatus zindex( size_t n,  ZSetVal &zv ) {
    return ZSET_CALL( zindex( *this, n, zv ) );
  }
  ZSetStatus zbsearch( ZScore score,  size_t &pos,  bool gt,  ZScore &r ) {
    return ZSET_CALL( zbsearch( *this, score, pos, gt, r ) );
  }
  ZSetStatus zbsearch_key( const char *key,  size_t keylen,  ZScore score,
                           bool gt,  size_t &pos ) {
    return ZSET_CALL( zbsearch_key( *this, key, keylen, score, gt, pos ) );
  }
  ZSetStatus zbsearch_all( const char *key,  size_t keylen,  bool gt,
                           size_t &pos ) {
    return ZSET_CALL( zbsearch_all( *this, key, keylen, gt, pos ) );
  }
  ZSetStatus zrem( const void *key,  size_t keylen,  HashPos &pos ) {
    return ZSET_CALL( zrem( *this, key, keylen, pos ) );
  }
  ZSetStatus zrem_index( size_t pos ) {
    return ZSET_CALL( zrem_index( *this, pos ) );
  }
  ZSetStatus zremall( void ) {
    return ZSET_CALL( zremall( *this ) );
  }
  ZSetStatus zscale( ZScore w ) {
    return ZSET_CALL( zscale( *this, w ) );
  }
  template<class T, class U>
  ZSetStatus zunion( ZSetData &set,  ZMergeCtx &ctx ) {
    if ( is_uint8( this->size ) )
      return ((ZSetStorage8 *) this->listp)->zunion<T, U>( *this, set,
                                       *(ZSetStorage<T, U> *) set.listp, ctx );
    else if ( is_uint16( this->size ) )
      return ((ZSetStorage16 *) this->listp)->zunion<T, U>( *this, set,
                                       *(ZSetStorage<T, U> *) set.listp, ctx );
    return ((ZSetStorage32 *) this->listp)->zunion<T, U>( *this, set,
                                       *(ZSetStorage<T, U> *) set.listp, ctx );
  }
  template<class T, class U>
  ZSetStatus zinter( ZSetData &set,  ZMergeCtx &ctx,  bool is_intersect ) {
    if ( is_uint8( this->size ) )
      return ((ZSetStorage8 *) this->listp)->zinter<T, U>( *this, set,
                          *(ZSetStorage<T, U> *) set.listp, ctx, is_intersect );
    else if ( is_uint16( this->size ) )
      return ((ZSetStorage16 *) this->listp)->zinter<T, U>( *this, set,
                          *(ZSetStorage<T, U> *) set.listp, ctx, is_intersect );
    return ((ZSetStorage32 *) this->listp)->zinter<T, U>( *this, set,
                          *(ZSetStorage<T, U> *) set.listp, ctx, is_intersect );
  }
  ZSetStatus zunion( ZSetData &set,  ZMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->zunion<uint16_t, uint8_t>( set, ctx );
    else if ( is_uint16( set.size ) )
      return this->zunion<uint32_t, uint16_t>( set, ctx );
    return this->zunion<uint64_t, uint32_t>( set, ctx );
  }
  ZSetStatus zinter( ZSetData &set,  ZMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->zinter<uint16_t, uint8_t>( set, ctx, true );
    else if ( is_uint16( set.size ) )
      return this->zinter<uint32_t, uint16_t>( set, ctx, true );
    return this->zinter<uint64_t, uint32_t>( set, ctx, true );
  }
  ZSetStatus zdiff( ZSetData &set,  ZMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->zinter<uint16_t, uint8_t>( set, ctx, false );
    else if ( is_uint16( set.size ) )
      return this->zinter<uint32_t, uint16_t>( set, ctx, false );
    return this->zinter<uint64_t, uint32_t>( set, ctx, false );
  }
};

}
}

#endif
