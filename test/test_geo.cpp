#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <math.h>
#include <raids/redis_cmd.h>
#include <raids/redis_msg.h>
#include <raids/redis_exec.h>
#include <raikv/work.h>
#include <raikv/key_hash.h>
#include <raimd/md_geo.h>
#include <h3api.h>

static const char *
geo_status_string[] = { "ok", "not found", "full", "updated", "exists" };

using namespace rai;
using namespace ds;
using namespace md;

static GeoData *
resize_geo( GeoData *curr,  size_t add_len,  bool is_copy = false )
{
  size_t count;
  size_t data_len;
  count = ( add_len >> 3 ) | 1;
  data_len = add_len + 1;
  if ( curr != NULL ) {
    data_len  = add_len + curr->data_len();
    data_len += data_len / 2 + 2;
    count     = curr->count();
    count    += count / 2 + 2;
  }
  size_t asize = GeoData::alloc_size( count, data_len );
  printf( "asize %" PRId64 ", count %" PRId64 ", data_len %" PRId64 "\n", asize, count, data_len );
  void *m = malloc( sizeof( GeoData ) + asize );
  void *p = &((char *) m)[ sizeof( GeoData ) ];
  GeoData *newbe = new ( m ) GeoData( p, asize );
  newbe->init( count, data_len );
  if ( curr != NULL ) {
    int x, y;
    printf( "verify curr: %d\n", x = curr->gverify() );
    curr->copy( *newbe );
    printf( "verify newbe: %d\n", y = newbe->gverify() );
    if ( x != 0 || y != 0 ) {
      printf( "curr: " );
      curr->lprint();
      printf( "newbe: " );
      newbe->lprint();
    }
    if ( ! is_copy )
      delete curr;
  }
  printf( "%.2f%% data %.2f%% hash\n",
          ( newbe->data_len() + add_len ) * 100.0 / newbe->data_size(),
          ( newbe->count() + 1 ) * 100.0 / newbe->max_count() );
  return newbe;
}

struct GeoKey {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  GeoKey  * next;
  GeoData * geo;
  uint32_t  hash;
  GeoKey() : next( 0 ), geo( 0 ), hash( 0 ) {}
  ~GeoKey() {
    if ( this->geo != NULL )
      delete this->geo;
  }

  GeoKey *copy( void ) {
    void *p = ::malloc( sizeof( GeoKey ) );
    if ( p == NULL ) return NULL;
    GeoKey *sk = new ( p ) GeoKey();
    sk->hash = this->hash;
    sk->geo  = resize_geo( this->geo, 0, true );
    sk->next = NULL;
    return sk;
  }
};

struct GeoDB {
  GeoKey *list;

  GeoKey *fetch( const char *k,  size_t klen ) {
    GeoKey *sk = this->list;
    uint32_t h = kv_crc_c( k, klen, 0 );
    for ( ; sk != NULL; sk = sk->next ) {
      if ( sk->hash == h )
        return sk;
    }
    void *p = ::malloc( sizeof( GeoKey ) );
    if ( p == NULL ) return NULL;
    sk = new ( p ) GeoKey();
    sk->hash = h;
    sk->geo  = resize_geo( NULL, 16 );
    sk->next = this->list;
    this->list = sk;
    return sk;
  }
  void save( const char *k,  size_t klen,  GeoKey *sk ) {
    sk->hash = kv_crc_c( k, klen, 0 );
    sk->next = this->list;
    this->list = sk;
    for ( GeoKey *p = sk; p->next != NULL; p = p->next ) {
      if ( p->next->hash == sk->hash ) {
        GeoKey *q = p->next;
        p->next = q->next;
        delete q;
        break;
      }
    }
  }
} geodb;

double
h3_distance( GeoCoord &coordi, GeoCoord &coordj )
{
  static const double R = 6371.0088 * 1000.0;
  double dx, dy, dz;
  double ph1 = coordi.lon - coordj.lon,
         th1 = coordi.lat,
         th2 = coordj.lat;

  dz = sin(th1) - sin(th2);
  dx = cos(ph1) * cos(th1) - cos(th2);
  dy = sin(ph1) * cos(th1);
  return asin(sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2 * R;
}

struct Place {
  Place * next;
  H3Index h3i;
  char    name[ 8 ];
  void * operator new( size_t, void *ptr ) { return ptr; }

  Place( H3Index i,  const char *n,  size_t len ) : h3i( i ) {
    ::strncpy( this->name, n, len );
    this->name[ len ] = '\0';
  }
};

struct BufList {
  BufList * next;
  size_t    used;
  char      buf[ 64 * 1023 ];
  void * operator new( size_t, void *ptr ) { return ptr; }

  BufList( BufList *&hd ) : next( hd ), used( 0 ) { hd = this; }

  void * alloc( size_t n ) {
    n = ( n + 7 ) & ~7;
    if ( n + this->used <= sizeof( this->buf ) ) {
      this->used += n;
      return &this->buf[ this->used - n ];
    }
    return NULL;
  }
  static void release( BufList *hd ) {
    while ( hd != NULL ) {
      BufList *p = hd->next;
      ::free( hd );
      hd = p;
    }
  }
};

Place *
new_place( const char *name,  size_t namelen,  GeoCoord &c,  BufList *&buffers )
{
  void *p;
  if ( buffers == NULL ||
       (p = buffers->alloc( sizeof( Place ) + namelen + 1 - 8 )) == NULL ) {
    p = ::malloc( sizeof( BufList ) );
    new ( p ) BufList( buffers );
    p = buffers->alloc( sizeof( Place ) + namelen + 1 - 8 );
  }
  return new ( p ) Place( geoToH3( &c, 15 ), name, namelen );
}

int
compare_place( const void *p1,  const void *p2 )
{
  const Place *x = *(const Place **) p1;
  const Place *y = *(const Place **) p2;
  int64_t d = (int64_t) x->h3i - (int64_t) y->h3i;
  return d < 0 ? -1 : ( d > 0 ? 1 : 0 );
}

Place *
load_places( const char *fn, size_t fnlen, BufList *&buffers )
{
  char fname[ 256 ];
  ::strncpy( fname, fn, fnlen );
  fname[ fnlen ] = '\0';

  FILE *fp = ::fopen( fname, "r" );
  if ( fp == NULL )
    return NULL;
  
  char buf[ 2048 ], *p, *s, *col[ 32 ], *name;
  Place **par = NULL;
  size_t plsize = 0, plcnt = 0;
  GeoCoord c;
  size_t namelen, i;

  while ( ::fgets( buf, sizeof( buf ), fp ) != NULL ) {
    s = buf;
    i = 0;
    do {
      if ( (p = strchr( s, '\t' )) == NULL )
        if ( (p = strchr( s, '\n' )) == NULL )
          break;
      col[ i++ ] = &p[ 1 ];
      if ( *p == '\n' )
        break;
      s = &p[ 1 ];
    } while ( i < 32 );
    if ( i > 6 ) {
      c.lat   = degsToRads( strtod( col[ 3 ], NULL ) );
      c.lon   = degsToRads( strtod( col[ 4 ], NULL ) );
      name    = col[ 1 ];
      namelen = (size_t) ( col[ 2 ] - col[ 1 ] ) - 1;
      if ( plcnt == plsize ) {
        par = (Place **)
              ::realloc( par, sizeof( par[ 0 ] ) * ( plsize + 64 * 1024 ) );
        plsize += 64 * 1024;
      }
      par[ plcnt++ ] = new_place( name, namelen, c, buffers );
    }
  }
  ::fclose( fp );
  if ( plcnt == 0 )
    return NULL;
  ::qsort( par, plcnt, sizeof( Place * ), compare_place );
  for ( i = 0; i < plcnt - 1; i++ )
    par[ i ]->next = par[ i + 1 ];
  par[ i ]->next = NULL;
  Place *plist = par[ 0 ];
  ::free( par );
  return plist;
}

int
main( int, char ** )
{
  GeoKey *sk;
  char buf[ 1024 ], buf2[ 1024 ];
  RedisMsg msg;
  kv::WorkAllocT< 4096 > tmp;
  char key[ 256 ];
  size_t i, j, k, cnt, sz, count, namelen, keylen, shift;
  size_t cmdlen, argcount, arglen, destlen, unitlen;
  const char *cmdbuf, *name, *arg, *dest, *unit;
  int64_t ival, jval;
  double radius, dist, min_dist;
  HashPos pos;
  GeoCoord coord, coordi, coordj;
  GeoBoundary bound;
  GeoVal gv;
  H3Index h3i, h3j, h3k[ 8 ]; /* maxkring(1) = 7 */
  H3Index start[ 8 ];
  size_t length[ 8 ];
  RedisMsgStatus mstatus;
  GeoStatus gstat;
  RedisCmd cmd;
  bool withscores;

  printf( "> " ); fflush( stdout );
  for (;;) {
    if ( fgets( buf, sizeof( buf ), stdin ) == NULL )
      break;
    if ( buf[ 0 ] == '#' || buf[ 0 ] == '\n' )
      continue;
    tmp.reset();
    cmdlen = ::strlen( buf );
    if ( buf[ 0 ] == '[' )
      mstatus = msg.unpack_json( buf, cmdlen, tmp );
    else
      mstatus = msg.unpack( buf, cmdlen, tmp );
    if ( mstatus != DS_MSG_STATUS_OK ) {
      printf( "error %d/%s\n", mstatus, ds_msg_status_string( mstatus ) );
      continue;
    }
    cmdbuf = msg.command( cmdlen, argcount );
    if ( cmdlen >= 32 ) {
      printf( "cmd to large\n" );
      continue;
    }
    if ( cmdlen == 0 )
      continue;
    cmd = get_redis_cmd( get_redis_cmd_hash( cmdbuf, cmdlen ) );
    if ( cmd == NO_CMD ) {
      printf( "no cmd\n" );
      continue;
    }
    sz = msg.to_almost_json( buf2 );
    printf( "\nexec %.*s\n", (int) sz, buf2 );
    if ( ! msg.get_arg( 1, name, namelen ) )
      goto bad_args;
    sk = geodb.fetch( name, namelen );
    if ( sk == NULL ) {
      printf( "out of memory\n" );
      return 1;
    }
    switch ( cmd ) {
      case GEOADD_CMD:  /* GEOADD key long lat mem [long lat mem] */
        count = 0;
        if ( msg.match_arg( 2, "file", 4, NULL ) == 1 ) {
          if ( msg.get_arg( 3, arg, arglen ) ) {
            BufList *buffers = NULL;
            Place *plist = load_places( arg, arglen, buffers );
            count = 0;
            for ( ; plist != NULL; plist = plist->next ) {
              size_t len = ::strlen( plist->name );
              pos.init( plist->name, len );
              for (;;) {
                gstat = sk->geo->geoappend( plist->name, len, plist->h3i, pos );
                //printf( "%s\n", geo_status_string[ gstat ] );
                if ( gstat != GEO_FULL ) {
                  count++;
                  break;
                }
                sk->geo = resize_geo( sk->geo, len );
              }
            }
            printf( "Loaded %" PRId64 "\n", count );
            BufList::release( buffers );
          }
          break;
        }
        for ( i = 2; i < argcount; i += 3 ) {
          if ( ! msg.get_arg( i, coord.lon ) ||
               ! msg.get_arg( i+1, coord.lat ) ||
               ! msg.get_arg( i+2, arg, arglen ) )
            goto bad_args;
          coord.lon = degsToRads( coord.lon );
          coord.lat = degsToRads( coord.lat );
          h3i = geoToH3( &coord, 15 );
          pos.init( arg, arglen );
          for (;;) {
            gstat = sk->geo->geoadd( arg, arglen, h3i, pos );
            printf( "%s\n", geo_status_string[ gstat ] );
            if ( gstat != GEO_FULL ) break;
            sk->geo = resize_geo( sk->geo, arglen );
          }
          if ( gstat == GEO_UPDATED )
            count++;
        }
        printf( "Updated %" PRId64 "\n", count );
        break;

      case GEODIST_CMD: /* GEODIST key from to [units] */
        if ( ! msg.get_arg( 2, arg, arglen ) ||
             ! msg.get_arg( 3, dest, destlen ) )
          goto bad_args;
        if ( ! msg.get_arg( 4, unit, unitlen ) )
          unit = "m";
        pos.init( arg, arglen );
        gstat = sk->geo->geoexists( arg, arglen, pos, h3i );
        if ( gstat != GEO_OK ) {
          printf( "%.*s: %s\n", (int) arglen, arg, geo_status_string[ gstat ] );
          break;
        }
        pos.init( dest, destlen );
        gstat = sk->geo->geoexists( dest, destlen, pos, h3j );
        if ( gstat != GEO_OK ) {
          printf( "%.*s: %s\n", (int) destlen, dest, geo_status_string[ gstat ] );
          break;
        }
        printf( "%" PRIx64 " - %" PRIx64 " = %d\n", h3i, h3j, h3Distance( h3i, h3j ) );
        h3ToGeo( h3i, &coord );
        coord.lat = radsToDegs( coord.lat );
        coord.lon = radsToDegs( coord.lon );
        printf( "%.*s: %.6f %.6f\n", (int) arglen, arg, coord.lon, coord.lat );
        h3ToGeo( h3j, &coord );
        coord.lat = radsToDegs( coord.lat );
        coord.lon = radsToDegs( coord.lon );
        printf( "%.*s: %.6f %.6f\n", (int) destlen, dest, coord.lon, coord.lat);
        h3ToGeo( h3i, &coordi );
        h3ToGeo( h3j, &coordj );
        printf( "%.6f%c\n", h3_distance( coordi, coordj ), unit[ 0 ] );
        break;

      case GEOPOS_CMD: /* GEOPOS key mem [mem ...] */
        for ( i = 2; i < argcount; i++ ) {
          if ( ! msg.get_arg( i, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          gstat = sk->geo->geoexists( arg, arglen, pos, h3i );
          if ( gstat != GEO_OK )
            printf( "%.*s: %s\n", (int) arglen, arg, geo_status_string[ gstat ] );
          else {
            h3ToGeo( h3i, &coord );
            coord.lat = radsToDegs( coord.lat );
            coord.lon = radsToDegs( coord.lon );
            printf( "%.*s: %.6f %.6f\n", (int) arglen, arg, coord.lon, coord.lat );
          }
        }
        break;

      case GEORADIUS_CMD: /* GEORADIUS key long lat radius [unit]
                             [WITHCOORD|WITHDIST|WITHHASH] [COUNT cnt] 
                             [ASC|DESC] [STORE key] [STOREDIST key] */
        if ( ! msg.get_arg( 2, coord.lon ) ||
             ! msg.get_arg( 3, coord.lat ) )
          goto bad_args;
        coordi.lon = degsToRads( coord.lon );
        coordi.lat = degsToRads( coord.lat );
        i = 4;

        if ( 0 ) {
      case GEORADIUSBYMEMBER_CMD: /* GEORADIUSBYMEMBER key member ... */
          if ( ! msg.get_arg( 2, arg, arglen ) )
            goto bad_args;
          pos.init( arg, arglen );
          gstat = sk->geo->geoexists( arg, arglen, pos, h3i );
          if ( gstat != GEO_OK ) {
            printf( "%.*s: %s\n", (int) arglen, arg, geo_status_string[ gstat ] );
            break;
          }
          h3ToGeo( h3i, &coordi );
          i = 3;
        }
        if ( ! msg.get_arg( i, radius ) )
          goto bad_args;
        for ( i = 0; i < 15; i++ ) {
          j = 15 - i;
          if ( edgeLengthM( (int) j ) >= radius ) {
            printf( "edge %.1f\n", edgeLengthM( (int) j ) );
            break;
          }
        }
        h3i = geoToH3( &coordi, (int) j );
        ::memset( h3k, 0, sizeof( h3k ) );
        hexRange( h3i, 1, h3k );
        /* order h3 indexes low to high, as that is the search order */
        for ( i = 0; h3k[ i ] != 0; i++ ) {
          size_t m = i;
          for ( k = i + 1; h3k[ k ] != 0; k++ ) {
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
        cnt = 0;
        for ( i = 0; h3k[ i ] != 0; i++ ) {
          int n = 0;
          h3ToGeoBoundary( h3k[ i ], &bound ); /* check if overlap */
          dist = 0;
          min_dist = radius * radius;
          if ( h3k[ i ] != h3i ) { /* include the hexigon surrounding origin */
            for ( ; n < bound.numVerts; n++ ) {
              dist = h3_distance( coordi, bound.verts[ n ] );
              if ( dist < min_dist )
                min_dist = dist;
              if ( dist <= radius )
                break;
            }
          }
          if ( n == bound.numVerts ) {
            printf( "exclude %" PRIx64 " (dist %.6f)\n", h3k[ i ], min_dist );
          }
          else {
            printf( "include %" PRIx64 " (dist %.6f)\n", h3k[ i ], min_dist );
            if ( cnt < i )
              h3k[ cnt ] = h3k[ i ];
            cnt++;
          }
        }
        /* combine hexigons which are h3 index neighbors */
        shift = ( 15 - j ) * 3;
        k = 0;
        for ( i = 0; i < cnt; i++ ) {
          h3ToGeoBoundary( h3k[ i ], &bound );
          for ( int n = 0; n < bound.numVerts; n++ ) {
            printf( "%" PRIu64 ". %.6f %.6f (dist %.6f)\n", i,
                    radsToDegs( bound.verts[ n ].lon ),
                    radsToDegs( bound.verts[ n ].lat ),
                    h3_distance( coordi, bound.verts[ n ] ) );
          }
          H3Index next = h3k[ i ] >> shift;
          if ( k > 0 && start[ k-1 ] + (H3Index) length[ k-1 ] == next ) {
            length[ k-1 ]++;
            printf( " + length[%" PRIu64 "] = %" PRIu64 "\n", k-1, length[ k-1 ]  );
          }
          else {
            start[ k ]  = next;
            length[ k ] = 1;
            k++;
          }
        }
        /* search each h3 index region, which may be multiple hexigons */
        cnt = 0;
        for ( i = 0; i < k; i++ ) {
          H3Index base = ( (H3Index) 0xf << 52 ) | ( start[ i ] << shift ),
                  end  = ( (H3Index) 0xf << 52 ) |
                         ( ( start[ i ] + length[ i ] ) << shift ),
                  x, y;
          size_t  n, m;
          GeoVal  gv;
          double  dist;
          printf( "base %" PRIx64 " end %" PRIx64 "\n", base, end );
          gstat = sk->geo->geobsearch( base, n, false, x );
          if ( gstat == GEO_NOT_FOUND ) {
            printf( "not found\n" );
            continue;
          }
          gstat = sk->geo->geobsearch( end, m, false, y );
          if ( gstat == GEO_NOT_FOUND ) {
            printf( "not found\n" );
            continue;
          }
          printf( "pos %" PRId64 " -> %" PRId64 "\n", n, m );
          for ( j = n; j < m; j++ ) {
            gstat = sk->geo->geoindex( j, gv );
            if ( gstat == GEO_OK ) {
              h3ToGeo( gv.score, &coordj );
              coord.lat = radsToDegs( coordj.lat );
              coord.lon = radsToDegs( coordj.lon );
              dist = h3_distance( coordi, coordj );
              if ( dist <= radius ) {
                printf( "%" PRId64 ". %.*s%.*s (%.6f %.6f d=%.6fm)\n", cnt++,
                        (int) gv.sz, (const char *) gv.data,
                        (int) gv.sz2, (const char *) gv.data2,
                        coord.lon, coord.lat, dist );
              }
            }
            else {
              printf( "%" PRId64 ". status=%d\n", cnt++, (int) gstat );
            }
          }
        }
        break;

      case ZRANGE_CMD:          /* ZRANGE key start stop [WITHSCORES] */
        withscores = ( msg.match_arg( 4, "withscores", 10, NULL ) == 1 );
        if ( ! msg.get_arg( 2, ival ) || ! msg.get_arg( 3, jval ) )
          goto bad_args;
        count = sk->geo->hcount();
        if ( ival < 0 )
          ival = count + ival;
        if ( jval < 0 )
          jval = count + jval;
        ival = kv::min_int<int64_t>( count, kv::max_int<int64_t>( 0, ival ) );
        jval = kv::min_int<int64_t>( count, kv::max_int<int64_t>( 0, jval + 1 ) );
        if ( ival >= jval ) {
          printf( "null range\n" );
          break;
        }
        i = ival + 1;
        j = jval;
        for (;;) {
          gstat = sk->geo->geoindex( i, gv );
          if ( gstat == GEO_OK ) {
            printf( "%" PRId64 ". off(%" PRId64 ") %sscore(", i, sk->geo->offset( i ),
                    withscores ? "*" : "" );
            printf( "%" PRIx64 "", (uint64_t) gv.score );
            printf( ") %.*s", (int) gv.sz, (const char *) gv.data );
            if ( gv.sz2 > 0 )
              printf( "%.*s", (int) gv.sz2, (const char *) gv.data2 );
            printf( "\n" );
          }
          if ( i == j )
            break;
          i++;
        }
        withscores = false;
        printf( "count %" PRIu64 " of %" PRIu64 "\n", count, sk->geo->max_count() - 1 );
        printf( "bytes %" PRIu64 " of %" PRIu64 "\n", (size_t) sk->geo->data_len(),
                sk->geo->data_size() );
        if ( sk->geo->hcount() < 256 ) {
          printf( "[" ); sk->geo->print_hashes(); printf( "]\n" );
        }
        i = ival + 1;
        j = jval;
        for (;;) {
          gstat =  sk->geo->geoindex( i, gv );
          if ( gstat == GEO_OK ) {
            keylen = kv::min_int<size_t>( gv.sz, sizeof( key ) );
            ::memcpy( key, gv.data, keylen );
            sz = kv::min_int<size_t>( sizeof( key ) - keylen, gv.sz2 );
            ::memcpy( &key[ keylen ], gv.data2, sz );
            keylen += sz;
            pos.init( key, keylen );
            gstat = sk->geo->geoexists( key, keylen, pos, h3j );
            if ( gstat == GEO_OK )
              printf( "%" PRId64 ". idx(%" PRId64 ") h(%u) %.*s\n", i, pos.i, (uint8_t) pos.h,
                      (int) keylen, key );
            else
              printf( "%" PRId64 ". idx(****) h(%u) %.*s\n", i, (uint8_t) pos.h,
                      (int) keylen, key );
          }
          else {
            printf( "%" PRId64 ". status=%d\n", i, (int) gstat );
          }
          if ( i == j )
            break;
          i++;
        }
        break;
      default:
        printf( "bad cmd\n" );
        if ( 0 ) {
      bad_args:;
          printf( "bad args\n" );
        }
        break;
    }
    printf( "> " ); fflush( stdout );
  }
  return 0;
}

