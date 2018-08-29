#ifndef __rai_raids__redis_geo_h__
#define __rai_raids__redis_geo_h__

#include <raids/redis_zset.h>
#include <h3/h3api.h>

namespace rai {
namespace ds {

enum GeoStatus {
  GEO_OK        = ZSET_OK,
  GEO_NOT_FOUND = ZSET_NOT_FOUND,
  GEO_FULL      = ZSET_FULL,
  GEO_UPDATED   = ZSET_UPDATED,
  GEO_EXISTS    = ZSET_EXISTS,
  GEO_BAD       = ZSET_BAD
};

typedef ZSetStorage<uint16_t, uint8_t, H3Index>  GeoStorage8;
typedef ZSetStorage<uint32_t, uint16_t, H3Index> GeoStorage16;
typedef ZSetStorage<uint64_t, uint32_t, H3Index> GeoStorage32;
typedef ZSetValT<H3Index>   GeoVal;
typedef ZMergeCtxT<H3Index> GeoMergeCtx;

struct GeoData : public HashData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  GeoData() : HashData() {}
  GeoData( void *l,  size_t sz ) : HashData( l, sz ) {}

#define GEO_CALL( GOTO ) \
  ( is_uint8( this->size ) ? ((GeoStorage8 *) this->listp)->GOTO : \
    is_uint16( this->size ) ? ((GeoStorage16 *) this->listp)->GOTO : \
                              ((GeoStorage32 *) this->listp)->GOTO )
  int gverify( void ) const {
    int x = this->lverify();
    if ( x == 0 )
      x = GEO_CALL( zverify( *this ) );
    return x;
  }

  GeoStatus geoexists( const char *key,  size_t keylen,  HashPos &pos,
                       H3Index &h3i ) {
    return (GeoStatus) GEO_CALL( zexists( *this, key, keylen, pos, h3i ) );
  }
  GeoStatus geoget( const char *key,  size_t keylen,  GeoVal &gv,
                    HashPos &pos ) {
    return (GeoStatus) GEO_CALL( zget( *this, key, keylen, gv, pos ) );
  }
  GeoStatus geoadd( const char *key,  size_t keylen,  H3Index h3i,
                    HashPos &pos ) {
    return (GeoStatus)
      GEO_CALL( zadd( *this, key, keylen, h3i, pos, ZAGGREGATE_NONE, 0, NULL ));
  }
  GeoStatus georem( const char *key,  size_t keylen,  HashPos &pos ) {
    return (GeoStatus)
      GEO_CALL( zrem( *this, key, keylen, pos ) );
  }
  GeoStatus geoappend( const char *key,  size_t keylen,  H3Index h3i,
                       HashPos &pos ) {
    return (GeoStatus)
      GEO_CALL( zadd( *this, key, keylen, h3i, pos, ZAGGREGATE_NONE,
                      ZADD_DUP_KEY_OK, NULL ) );
  }
  GeoStatus geoindex( size_t n,  GeoVal &gv ) {
    return (GeoStatus) GEO_CALL( zindex( *this, n, gv ) );
  }
  GeoStatus geobsearch( H3Index h3i,  size_t &pos,  bool gt,  H3Index &h3j ) {
    return (GeoStatus) GEO_CALL( zbsearch( *this, h3i, pos, gt, h3j ) );
  }
  GeoStatus georem_index( size_t pos ) {
    return (GeoStatus) GEO_CALL( zrem_index( *this, pos ) );
  }
  GeoStatus georemall( void ) {
    return (GeoStatus) GEO_CALL( zremall( *this ) );
  }
  template<class T, class U, class Z>
  GeoStatus geounion( GeoData &set,  GeoMergeCtx &ctx ) {
    if ( is_uint8( this->size ) )
      return (GeoStatus) ((GeoStorage8 *) this->listp)->zunion<T, U, Z>(
        *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx );
    else if ( is_uint16( this->size ) )
      return (GeoStatus) ((GeoStorage16 *) this->listp)->zunion<T, U, Z>(
        *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx );
    return (GeoStatus) ((GeoStorage32 *) this->listp)->zunion<T, U, Z>(
      *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx );
  }
  template<class T, class U, class Z>
  GeoStatus geointer( GeoData &set,  GeoMergeCtx &ctx,  bool is_intersect ) {
    if ( is_uint8( this->size ) )
      return (GeoStatus) ((GeoStorage8 *) this->listp)->zinter<T, U, Z>(
        *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx, is_intersect );
    else if ( is_uint16( this->size ) )
      return (GeoStatus) ((GeoStorage16 *) this->listp)->zinter<T, U, Z>(
        *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx, is_intersect );
    return (GeoStatus) ((GeoStorage32 *) this->listp)->zinter<T, U, Z>(
      *this, set, *(ZSetStorage<T, U, Z> *) set.listp, ctx, is_intersect );
  }
  GeoStatus geounion( GeoData &set,  GeoMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->geounion<uint16_t, uint8_t, H3Index>( set, ctx );
    else if ( is_uint16( set.size ) )
      return this->geounion<uint32_t, uint16_t, H3Index>( set, ctx );
    return this->geounion<uint64_t, uint32_t, H3Index>( set, ctx );
  }
  GeoStatus geointer( GeoData &set,  GeoMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->geointer<uint16_t, uint8_t, H3Index>( set, ctx, true );
    else if ( is_uint16( set.size ) )
      return this->geointer<uint32_t, uint16_t, H3Index>( set, ctx, true );
    return this->geointer<uint64_t, uint32_t, H3Index>( set, ctx, true );
  }
  GeoStatus geodiff( GeoData &set,  GeoMergeCtx &ctx ) {
    if ( is_uint8( set.size ) )
      return this->geointer<uint16_t, uint8_t, H3Index>( set, ctx, false );
    else if ( is_uint16( set.size ) )
      return this->geointer<uint32_t, uint16_t, H3Index>( set, ctx, false );
    return this->geointer<uint64_t, uint32_t, H3Index>( set, ctx, false );
  }
};

}
}

#endif
