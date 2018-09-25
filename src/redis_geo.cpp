#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#include <raids/redis_geo.h>
#include <raids/exec_list_ctx.h>

using namespace rai;
using namespace ds;
using namespace kv;

enum {
  DO_GEOHASH           = 1<<0,
  DO_GEOPOS            = 1<<1,
  DO_GEODIST           = 1<<2,
  DO_GEORADIUS         = 1<<3,
  DO_GEORADIUSBYMEMBER = 1<<4
};

ExecStatus
RedisExec::exec_geoadd( RedisKeyCtx &ctx )
{
  ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
  GeoCoord     coord  = {0,0};
  const char * arg    = NULL;
  size_t       arglen = 0,
               argi   = 2;
  H3Index      h3i    = 0;
  HashPos      pos;
  size_t       count,
               ndata;
  GeoStatus    gstatus;

  /* GEOADD key long lat mem [long lat mem ...] */
  if ( ! this->msg.get_arg( argi, coord.lon ) ||
       ! this->msg.get_arg( argi+1, coord.lat ) )
    return ERR_BAD_ARGS;
  /* hash the coords */
  coord.lon = degsToRads( coord.lon );
  coord.lat = degsToRads( coord.lat );
  h3i = geoToH3( &coord, 15 );
  argi += 2;
  /* get the place name */
  if ( ! this->msg.get_arg( argi, arg, arglen ) )
    return ERR_BAD_ARGS;
  argi++;
  pos.init( arg, arglen );

  switch ( geo.get_key_write() ) {
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
    case KEY_IS_NEW:
      count = this->argc / 3; /* set by alloc_size() */
      ndata = 2 + arglen + sizeof( H3Index ); /* calc length of update */
      for ( size_t j = argi + 2; j < this->argc; j += 3 ) {
        const char * tmparg;
        size_t       tmplen;
        if ( ! this->msg.get_arg( j, tmparg, tmplen ) )
          return ERR_BAD_ARGS;
        ndata += 2 + tmplen + sizeof( H3Index );
      }
      /* create the geo structure */
      if ( ! geo.create( count, ndata ) )
        return ERR_KV_STATUS;
      break;
    case KEY_OK:
      /* open the geo structure */
      if ( ! geo.open() )
        return ERR_KV_STATUS;
      break;
  }
  /* loop through the arguments */
  for (;;) {
    gstatus = geo.x->geoadd( arg, arglen, h3i, pos );
    if ( gstatus == GEO_FULL ) {
      /* structure full, resize */
      if ( ! geo.realloc( arglen + 1 + sizeof( H3Index ) ) )
        return ERR_KV_STATUS;
      continue;
    }
    if ( gstatus == GEO_UPDATED ) /* incr when geo updated */
      ctx.ival++;
    if ( this->argc == argi ) /* no more elements to add */
      return EXEC_SEND_INT;

    /* get the next coords and hash */
    if ( ! this->msg.get_arg( argi, coord.lon ) ||
         ! this->msg.get_arg( argi+1, coord.lat ) )
      return ERR_BAD_ARGS;
    coord.lon = degsToRads( coord.lon );
    coord.lat = degsToRads( coord.lat );
    h3i = geoToH3( &coord, 15 );
    argi += 2;
    /* get the next place name */
    if ( ! this->msg.get_arg( argi++, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
  }
}

static uint64_t
interleave_minmax( double minv,  double maxv,  double code )
{
  uint64_t b = 0;
  double mid;

  for ( int i = 0; i < 28; i++ ) {
    mid = ( minv + maxv ) / 2.0;
    if ( code > mid ) {
      b |= 1;
      minv = mid;
    }
    else {
      maxv = mid;
    }
    b <<= 2;
  }
  return b;
}

static const double PI = 3.14159265358979323846;
static size_t
geohash( double latrads,  double longrads,  char *hashbuf )
{                                     /* 0123456789012345678901 */
  static const char alpha[] = "0123456789bcdefghjkmnpqrstuvwxyz";
  uint64_t lat, lon, bits;
  size_t k = 0;
  /* wikipedia test: lat=57.64911, lon=10.40744 == u4pruydqqvj (Jutland, Den) */
  lat  = interleave_minmax( -PI/2.0, PI/2.0, latrads );
  lon  = interleave_minmax( -PI, PI, longrads );
  bits = ( lon << 1 ) | lat;
  for ( int i = 53; ; i -= 5 ) {
    hashbuf[ k++ ] = alpha[ ( bits >> i ) & 0x1f ];
    if ( i < 5 )
      break;
  }
  return k;
}
#if 0
/* returns the h3 hash instead of the redis geo hash */
static uint64_t
georedishash( double latrads,  double longrads )
{
  uint64_t lat, lon;
  lat  = interleave_minmax( -PI/2.0, PI/2.0, latrads );
  lon  = interleave_minmax( -PI, PI, longrads );
  return ( ( lon << 1 ) | lat ) >> 6; /* redis hash interleave */
}
#endif
static double
haversine_dist( GeoCoord &coordi,  GeoCoord &coordj )
{
  /* wiki: 6353 to 6384 km, 6372.8 = the average great-elliptic or great-circle
   * radius), where the boundaries are the meridian (6367.45 km) and the
   * equator (6378.14 km);  6371 = value is recommended by the International
   * Union of Geodesy and Geophysics and it minimizes the RMS relative error
   * between the great circle and geodesic distance 
   * -- https://rosettacode.org/wiki/Haversine_formula */
  static const double R = 6371.0088 * 1000.0;
  double dx, dy, dz,
         ph1 = coordi.lon - coordj.lon,
         th1 = coordi.lat,
         th2 = coordj.lat;

  dz = sin( th1 ) - sin( th2 );
  dx = cos( ph1 ) * cos( th1 ) - cos( th2 );
  dy = sin( ph1 ) * cos( th1 );
  return asin( sqrt( dx * dx + dy * dy + dz * dz ) / 2 ) * 2 * R;
}

static double
parse_units( RedisMsg &msg,  size_t i,  double base,  double &u,  bool inv )
{
  static const double meters_in_km    = 1000.0,
                      meters_in_miles = 1609.34,
                      meters_in_feet  = 0.3048;
  double x;
  switch ( msg.match_arg( i, "m", 1, "meter",     5, "meters",     6,
                            "km", 2, "kilometer", 9, "kilometers", 10,
                            "ft", 2, "feet",      4, "foot",       4,
                            "mi", 2, "mile",      4, "miles",      5, NULL ) ) {
    case 1: case 2: case 3:    u = base; return 1;
    case 4: case 5: case 6:    x = meters_in_km; break;
    case 7: case 8: case 9:    x = meters_in_feet; break;
    case 10: case 11: case 12: x = meters_in_miles; break;
    default:                   u = base; return 0;
  }
  if ( inv )
    u = base / x;
  else
    u = base * x;
  return x;
}

ExecStatus
RedisExec::exec_geohash( RedisKeyCtx &ctx )
{
  /* GEOHASH key mem [mem ...] */
  return this->do_gread( ctx, DO_GEOHASH );
}

ExecStatus
RedisExec::exec_geopos( RedisKeyCtx &ctx )
{
  /* GEOPOS key mem [mem ...] */
  return this->do_gread( ctx, DO_GEOPOS );
}

ExecStatus
RedisExec::exec_geodist( RedisKeyCtx &ctx )
{
  /* GEODIST key mem1 mem2 [units] */
  return this->do_gread( ctx, DO_GEODIST );
}

ExecStatus
RedisExec::do_gread( RedisKeyCtx &ctx,  int flags )
{
  ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
  StreamBuf::BufQueue q( this->strm );
  const char * arg     = NULL;
  size_t       arglen  = 0;
  const char * dest    = NULL;
  size_t       destlen = 0;
  char         buf[ 128 ];
  HashPos      pos,
               pos2;
  GeoCoord     coordi, coordj;
  H3Index      h3i, h3j;
  size_t       sz     = 0,
               nitems = 0,
               argi   = 3;
  ExecStatus   status = EXEC_OK;

  /* GEOHASH key mem [mem ...] */
  /* GEOPOS key mem [mem ...] */
  if ( ! this->msg.get_arg( 2, arg, arglen ) )
    return ERR_BAD_ARGS;
  pos.init( arg, arglen );
  /* GEODIST key mem1 mem2 [units] */
  if ( ( flags & DO_GEODIST ) != 0 ) {
    if ( ! this->msg.get_arg( 3, dest, destlen ) )
      return ERR_BAD_ARGS;
    pos2.init( dest, destlen );
  }
  switch ( geo.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_OK:        break;
  }
  if ( ! geo.open_readonly() )
    return ERR_KV_STATUS;
  for (;;) {
    if ( geo.x->geoexists( arg, arglen, pos, h3i ) == GEO_OK ) {
      h3ToGeo( h3i, &coordi );
      switch ( flags & ( DO_GEOHASH | DO_GEOPOS | DO_GEODIST ) ) {
        case DO_GEOHASH:
        case DO_GEOPOS:
          if ( ( flags & DO_GEOHASH ) != 0 ) {
            sz = geohash( coordi.lat, coordi.lon, buf ); /* array of hash */
            q.append_string( buf, sz );
          }
          else {
            q.append_bytes( "*2\r\n", 4 ); /* array of [lon,lat] */
            sz = ::snprintf( buf, sizeof( buf ), "%.6f",
                             radsToDegs( coordi.lon ) );
            q.append_string( buf, sz );
            sz = ::snprintf( buf, sizeof( buf ), "%.6f",
                             radsToDegs( coordi.lat ) );
            q.append_string( buf, sz );
          }
          nitems++;
          break;
        case DO_GEODIST:
          if ( geo.x->geoexists( dest, destlen, pos2, h3j ) == GEO_OK ) {
            double val;
            h3ToGeo( h3j, &coordj );
            if ( parse_units( this->msg, 4,
                              haversine_dist( coordi, coordj ),
                              val /* output */, true /* invert */ ) == 0 ) {
              if ( this->argc > 4 ) /* if no arg, skip the error */
                return ERR_BAD_ARGS;
            }
            sz = ::snprintf( buf, sizeof( buf ), "%.6f", val );
            sz = this->send_string( buf, sz );
          }
          else {
            status = EXEC_SEND_NIL;
          }
          break;
      }
    }
    else {
      if ( ( flags & DO_GEODIST ) != 0 ) /* GEODIST */
        status = EXEC_SEND_NIL;
      else { /* GEOHASH is a nil, GEOPOS is a null */
        q.append_nil( ( flags & DO_GEOPOS ) != 0 );
        nitems++;
      }
    }
    if ( ( flags & ( DO_GEOHASH | DO_GEOPOS ) ) != 0 ) {
      /* if more elements */
      if ( argi < this->argc ) {
        if ( ! this->msg.get_arg( argi++, arg, arglen ) )
          return ERR_BAD_ARGS;
        pos.init( arg, arglen );
        continue;
      }
    }
    if ( ! geo.validate_value() ) /* check if not mutated */
      return ERR_KV_STATUS;
    if ( status == EXEC_OK ) {
      if ( ( flags & DO_GEODIST ) != 0 )
        this->strm.sz += sz;
      else {
        q.finish_tail();
        q.prepend_array( nitems );
        this->strm.append_iov( q );
      }
      return EXEC_OK;
    }
    return status;
  }
}

ExecStatus
RedisExec::exec_georadius( RedisKeyCtx &ctx )
{
  return this->do_gradius( ctx, DO_GEORADIUS );
}

ExecStatus
RedisExec::exec_georadiusbymember( RedisKeyCtx &ctx )
{
  return this->do_gradius( ctx, DO_GEORADIUSBYMEMBER );
}

namespace rai {
namespace ds {
/* a list of places for sorting and storing */
struct Place {
  Place  * next;
  double   dist;
  GeoCoord coord;
  H3Index  h3i;
  char     name[ 8 ];
  void * operator new( size_t, void *ptr ) { return ptr; }

  Place( const GeoVal &gv,  const GeoCoord &coordj,  double d,
         Place *nxt ) : next( nxt ), dist( d ), h3i( gv.score ) {
    this->coord = coordj;
    ::memcpy( this->name, gv.data, gv.sz );
    if ( gv.sz2 > 0 )
      ::memcpy( &this->name[ gv.sz ], gv.data2, gv.sz2 );
    this->name[ gv.sz + gv.sz2 ] = '\0';
  }
  static size_t alloc_size( GeoVal &gv ) {
    return align<size_t>( gv.sz + gv.sz2 + 1 + sizeof( Place ), 8 ) - 8;
  }
};
/* save place list for storing */
struct StorePlaceList {
  size_t   listcnt,
           listsz;
  Place ** par;
  int      withflags;
  double   units;

  StorePlaceList( size_t cnt,  size_t sz,  Place **ar,  int fl,  double u )
    : listcnt( cnt ), listsz( sz ), par( ar ), withflags( fl ), units( u ) {}
};
}
}
/* qsort comparison functions */
static int
cmp_ascend_dist( const void *p1,  const void *p2 )
{
  const Place *x = *(const Place **) p1;
  const Place *y = *(const Place **) p2;
  double d = x->dist - y->dist;
  return d < 0 ? -1 : ( d > 0 ? 1 : 0 );
}

static int
cmp_ascend_hash( const void *p1,  const void *p2 )
{
  const Place *x = *(const Place **) p1;
  const Place *y = *(const Place **) p2;
  return ( x->h3i < y->h3i ) ? -1 :
         ( ( x->h3i > y->h3i ) ? 1 : 0 );
}

static int
cmp_descend_dist( const void *p1,  const void *p2 )
{
  const Place *x = *(const Place **) p1;
  const Place *y = *(const Place **) p2;
  double d = x->dist - y->dist;
  return d < 0 ? 1 : ( d > 0 ? -1 : 0 );
}
/* flags used by GEORADIUS command */
enum {
  WITHCOORD_F = 1,    WITHDIST_F = 2,    WITHHASH_F = 4,    COUNT_F     = 8,
  ASC_F       = 0x10, DESC_F     = 0x20, STORE_F    = 0x40, STOREDIST_F = 0x80
};
/* place appended to return value */
static void
append_place( StreamBuf::BufQueue &q,  GeoVal &gv,  int withflags,
              char withbuf[ 4 ],  GeoCoord &coordj,  double dist,
              double units )
{
  char buf[ 16 ];
  size_t sz;
  if ( ( withflags & ( WITHCOORD_F | WITHDIST_F | WITHHASH_F ) ) != 0 )
    q.append_bytes( withbuf, 4 ); /* array of with */
  /* add the place name */
  q.append_string( gv.data, gv.sz, gv.data2, gv.sz2 );
  /* extra with parms */
  if ( ( withflags & WITHDIST_F ) != 0 ) {
    sz = ::snprintf( buf, sizeof( buf ), "%.4f", dist / units );
    q.append_string( buf, sz );
  }
  if ( ( withflags & WITHHASH_F ) != 0 ) {
    /*q.append_uint( georedishash( coordj.lat, coordj.lon ) );*/
    q.append_uint( gv.score );
  }
  if ( ( withflags & WITHCOORD_F ) != 0 ) {
    q.append_bytes( "*2\r\n", 4 );
    sz = ::snprintf( buf, sizeof( buf ), "%.6f", radsToDegs( coordj.lon ) );
    q.append_string( buf, sz );
    sz = ::snprintf( buf, sizeof( buf ), "%.6f", radsToDegs( coordj.lat ) );
    q.append_string( buf, sz );
  }
}

ExecStatus
RedisExec::do_gradius( RedisKeyCtx &ctx,  int flags )
{
  /* if store key */
  if ( ctx.argn != 1 ) {
    if ( this->key_cnt != this->key_done + 1 )
      return EXEC_DEPENDS;
    return this->do_gradius_store( ctx );
  }

  ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );
  StreamBuf::BufQueue q( this->strm );
  const char * arg     = NULL;
  size_t       arglen  = 0;
  HashPos      pos,
               pos2;
  GeoCoord     coordi = {0,0},
               coordj,
               coord;
  GeoBoundary  bound;
  double       radius,
               dist,
               units;
  static const size_t MAX_H3K = 8;
  H3Index      h3i,
               h3k[ MAX_H3K ], start[ MAX_H3K ];
  size_t       length[ 8 ],
               nitems = 0,
               count  = 0,
               argi,
               cell,
               i, j, k,
               hcnt,
               listcnt = 0,
               listsz  = 0;
  GeoVal       gv;
  Place     ** par  = NULL,
             * list = NULL;
  int          withflags = 0,
               shift;
  char         withbuf[ 4 ];
  GeoStatus    gstatus;

  /* GEORADIUS key long lat radius [unit] [WITHCOORD|WITHDIST|WITHHASH]
   *          [COUNT cnt] [ASC|DESC] [STORE key] [STOREDIST key]  */
  if ( ( flags & DO_GEORADIUS ) != 0 ) {
    if ( ! this->msg.get_arg( 2, coord.lon ) ||
         ! this->msg.get_arg( 3, coord.lat ) )
      return ERR_BAD_ARGS;
    coordi.lon = degsToRads( coord.lon );
    coordi.lat = degsToRads( coord.lat );
    argi = 4;
  }
  /* GEORADIUSBYMEMBER key member ... */
  else {
    if ( ! this->msg.get_arg( 2, arg, arglen ) )
      return ERR_BAD_ARGS;
    pos.init( arg, arglen );
    argi = 3;
  }
  if ( ! this->msg.get_arg( argi++, radius ) )
    return ERR_BAD_ARGS;
  if ( (units = parse_units( this->msg, argi++, radius, radius, false )) == 0 )
    return ERR_BAD_ARGS;
  withflags = 0;
  ::memset( withbuf, 0, sizeof( withbuf ) );
  if ( argi < this->argc ) {
    int withct = 0;
    do {
      switch ( this->msg.match_arg( argi++, "withcoord", 9,
                                            "withdist",  8,
                                            "withhash",  8,
                                            "count",     5,
                                            "asc",       3,
                                            "desc",      4,
                                            "store",     5,
                                            "storedist", 9, NULL ) ) {
        case 1: withflags |= WITHCOORD_F; withct++; break;
        case 2: withflags |= WITHDIST_F;  withct++; break;
        case 3: withflags |= WITHHASH_F;  withct++; break;
        case 4: withflags |= COUNT_F; {
                int64_t cnt;
                if ( ! this->msg.get_arg( argi++, cnt ) || cnt < 0 )
                  return ERR_BAD_ARGS;
                count = (uint64_t) cnt;
                break;
              }
        case 5: withflags |= ASC_F;       break;
        case 6: withflags |= DESC_F;      break;
                /* only one of these, the last one */
        case 7: withflags = ( withflags & ~STOREDIST_F ) | STORE_F;
                argi++; break;
        case 8: withflags = ( withflags & ~STORE_F ) | STOREDIST_F;
                argi++; break;
        default:
          return ERR_BAD_ARGS;
      }
    } while ( argi < this->argc );
    if ( withct > 0 ) {
      withbuf[ 0 ] = '*';
      withbuf[ 1 ] = '0' + withct + 1;
      crlf( withbuf, 2 );
    }
  }
  switch ( geo.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: goto finished; /* empty list */
    case KEY_OK:        break;
  }
  if ( ! geo.open_readonly() )
    return ERR_KV_STATUS;

  if ( ( flags & DO_GEORADIUSBYMEMBER ) != 0 ) {
    if ( geo.x->geoexists( arg, arglen, pos, h3i ) != GEO_OK ) /* empty */
      goto finished;
    h3ToGeo( h3i, &coordi );
  }
  for ( i = 0; i < 15; i++ ) { /* find hexagon where radius fits inside */
    cell = 15 - i;
    if ( edgeLengthM( cell ) >= radius )
      break;
  }
  h3i = geoToH3( &coordi, cell ); /* get parent cell */
  ::memset( h3k, 0, sizeof( h3k ) );
  hexRange( h3i, 1, h3k );
  /* order h3 indexes low to high, as that is the search order */
  for ( i = 0; i < MAX_H3K && h3k[ i ] != 0; i++ ) {
    size_t m = i;
    for ( k = i + 1; k < MAX_H3K && h3k[ k ] != 0; k++ ) {
      if ( h3k[ k ] < h3k[ m ] )
        m = k;
    }
    if ( m != i ) {
      H3Index x = h3k[ i ];
      h3k[ i ] = h3k[ m ];
      h3k[ m ] = x;
    }
  }
  /* toss h3 indexes which are too far away by comparing hexigon
   * vert distances with the origin */
  hcnt = 0;
  for ( i = 0; i < MAX_H3K && h3k[ i ] != 0; i++ ) {
    if ( h3k[ i ] != h3i ) { /* include the hexigon surrounding origin */
      h3ToGeoBoundary( h3k[ i ], &bound ); /* check if overlap */
      for ( int n = 0; n < bound.numVerts; n++ ) {
        dist = haversine_dist( coordi, bound.verts[ n ] );
        if ( dist <= radius )
          goto add_hexigon;
      }
    }
    else {
    add_hexigon:;
      if ( hcnt < i )
        h3k[ hcnt ] = h3k[ i ];
      hcnt++;
    }
  }
  /* combine hexigons which are h3 index neighbors */
  shift = ( 15 - cell ) * 3;
  k = 0;
  for ( i = 0; i < hcnt; i++ ) {
    h3ToGeoBoundary( h3k[ i ], &bound );
    H3Index next = h3k[ i ] >> shift;
    if ( k > 0 && start[ k-1 ] + (H3Index) length[ k-1 ] == next ) {
      length[ k-1 ]++;
    }
    else {
      start[ k ]  = next;
      length[ k ] = 1;
      k++;
    }
  }
  /* search each h3 index region, which may be multiple hexigons */
  for ( i = 0; i < k; i++ ) {
    /* set resolution=15 of the h3 hash, that resolution is indexed */
    H3Index base = ( (H3Index) 0xf << 52 ) | ( start[ i ] << shift ),
            end  = ( (H3Index) 0xf << 52 ) |
                   ( ( start[ i ] + length[ i ] ) << shift ),
            x, y;
    size_t  n, m;
    gstatus = geo.x->geobsearch( base, n, false, x );
    if ( gstatus == GEO_NOT_FOUND )
      continue;
    gstatus = geo.x->geobsearch( end, m, false, y );
    if ( gstatus == GEO_NOT_FOUND )
      continue;
    for ( j = n; j < m; j++ ) {
      gstatus = geo.x->geoindex( j, gv );
      if ( gstatus == GEO_OK ) {
        h3ToGeo( gv.score, &coordj );
        dist = haversine_dist( coordi, coordj );
        /* test if within radius */
        if ( dist <= radius ) {
          /* save for later processing */
          if ( ( withflags & ( ASC_F | DESC_F |
                               STORE_F | STOREDIST_F ) ) != 0 ) {
            void *p = this->strm.alloc_temp( Place::alloc_size( gv ) );
            if ( p == NULL )
              return ERR_ALLOC_FAIL;
            list = new ( p ) Place( gv, coordj, dist, list );
            listcnt++;
            listsz += gv.sz + gv.sz2;
          }
          /* append to output stream */
          else {
            append_place( q, gv, withflags, withbuf, coordj, dist, units );
            if ( ++nitems == count )
              goto finished;
          }
        }
      }
    }
  }
  /* if not returning in any order, may need to sort or store */
  if ( listcnt > 0 ) {
    par = (Place **) this->strm.alloc_temp( sizeof( Place * ) * listcnt );
    if ( par == NULL )
      return ERR_ALLOC_FAIL;
    for ( listcnt = 0; list != NULL; list = list->next )
      par[ listcnt++ ] = list;
    int (*cmpf)( const void *, const void * );

    if ( ( withflags & STOREDIST_F ) != 0 ) /* these override ASC/DESC */
      cmpf = cmp_ascend_dist;
    else if ( ( withflags & STORE_F ) != 0 )
      cmpf = cmp_ascend_hash;
    else if ( ( withflags & DESC_F ) != 0 )
      cmpf = cmp_descend_dist;
    else
      cmpf = cmp_ascend_dist;

    ::qsort( par, listcnt, sizeof( Place * ), cmpf );
  }
finished:;
  /* save the list for the store key */
  if ( ( withflags & ( STORE_F | STOREDIST_F ) ) != 0 ) {
    StorePlaceList stp( listcnt, listsz, par, withflags, units );
    if ( ! this->save_data( ctx, &stp, sizeof( stp ), 0 ) )
      return ERR_ALLOC_FAIL;
  }
  /* append to output stream */
  else if ( listcnt > 0 ) {
    gv.sz2   = 0;
    gv.data2 = NULL;
    do {
      gv.data  = par[ nitems ]->name;
      gv.sz    = ::strlen( (const char *) gv.data );
      gv.score = par[ nitems ]->h3i;
      append_place( q, gv, withflags, withbuf, par[ nitems ]->coord,
                    par[ nitems ]->dist, units );
      nitems += 1;
    } while ( nitems != count && nitems != listcnt );
  }
  if ( ! geo.validate_value() )
    return ERR_KV_STATUS;
  if ( ( withflags & ( STORE_F | STOREDIST_F ) ) != 0 )
    return EXEC_OK;
  q.finish_tail();
  q.prepend_array( nitems );
  this->strm.append_iov( q );
  return EXEC_OK;
}

ExecStatus
RedisExec::do_gradius_store( RedisKeyCtx &ctx )
{
  if ( this->keys[ 0 ]->part == NULL )
    return EXEC_SEND_ZERO;

  StorePlaceList &stp = *(StorePlaceList *) this->keys[ 0 ]->part->data( 0 );
  size_t  count,
          ndata;
  HashPos pos;

  ctx.ival = stp.listcnt;
  /* store key, a geo type */
  if ( (stp.withflags & STORE_F) != 0 ) {
    ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );

    switch ( geo.get_key_write() ) {
      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
      case KEY_IS_NEW:
        count = stp.listcnt;
        ndata = stp.listsz + stp.listcnt * sizeof( H3Index );
        if ( ! geo.create( count, ndata ) )
          return ERR_KV_STATUS;
        break;
      case KEY_OK:
        if ( ! geo.open() )
          return ERR_KV_STATUS;
        break;
    }
    for ( size_t i = 0; i < stp.listcnt; i++ ) {
      H3Index      h3i     = stp.par[ i ]->h3i;
      const char * name    = stp.par[ i ]->name;
      size_t       namelen = ::strlen( name );

      pos.init( name, namelen );
      while ( geo.x->geoadd( name, namelen, h3i, pos ) == GEO_FULL )
        if ( ! geo.realloc( namelen + 1 + sizeof( H3Index ) ) )
          return ERR_KV_STATUS;
    }
  }
  /* storedist key, a sortedset type */
  else if ( (stp.withflags & STOREDIST_F) != 0 ) {
    ExecListCtx<ZSetData, MD_SORTEDSET> zset( *this, ctx );
    double units = stp.units;
    size_t i;

    switch ( zset.get_key_write() ) {
      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
      case KEY_IS_NEW:
        count = stp.listcnt;
        ndata = stp.listsz + stp.listcnt * sizeof( ZScore );
        if ( ! zset.create( count, ndata ) )
          return ERR_KV_STATUS;
        break;
      case KEY_OK:
        if ( ! zset.open() )
          return ERR_KV_STATUS;
        break;
    }
    if ( units != 1 ) {
      for ( i = 0; i < stp.listcnt; i++ )
        stp.par[ i ]->dist /= units; /* m -> km */
    }
    for ( i = 0; i < stp.listcnt; i++ ) {
      ZScore       dist;
      const char * name    = stp.par[ i ]->name;
      size_t       namelen = ::strlen( name );

      dist = stp.par[ i ]->dist;
      pos.init( name, namelen );
      while ( zset.x->zadd( name, namelen, dist, pos, 0, NULL ) == ZSET_FULL )
        if ( ! zset.realloc( namelen + 1 + sizeof( ZScore ) ) )
          return ERR_KV_STATUS;
    }
  }
  return EXEC_SEND_INT;
}
