#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/route_db.h>

using namespace rai;
using namespace ds;

static uint32_t *
make_space( uint32_t i,  uint32_t &size,  uint32_t *&ptr,  uint32_t *stat_spc )
{
  if ( i > size ) {
    uint32_t * old_ptr = ptr;
    size_t     old_siz = size;
    if ( ptr == stat_spc )
      ptr = NULL;
    i    = kv::align<uint32_t>( i, 256 );
    ptr  = (uint32_t *) ::realloc( ptr, i * sizeof( uint32_t ) );
    size = i;
    if ( old_ptr == stat_spc )
      ::memcpy( ptr, stat_spc, old_siz * sizeof( uint32_t ) );
  }
  return ptr;
}

uint32_t *
RouteDB::make_route_space( uint32_t i )
{
  return make_space( i, this->route_spc_size, this->route_spc_ptr,
                     this->route_spc );
}

uint32_t *
RouteDB::make_code_space( uint32_t i )
{
  return make_space( i, this->code_spc_size, this->code_spc_ptr,
                     this->code_spc );
}

uint32_t *
RouteDB::make_code_ref_space( uint32_t i,  uint32_t &off )
{
  make_space( this->code_end + i, this->code_size, this->code_buf,
              this->code_buf_spc );
  off = this->code_end;
  this->code_end += i;
  return &this->code_buf[ off ];
}

void
RouteDB::gc_code_ref_space( void )
{
  uint32_t i, j = 0, k, e, pos;
  CodeRef * p;
  /* remove from zht */
  for ( i = 0; i < this->code_end; i += e ) {
    p = (CodeRef *) (void *) &this->code_buf[ i ];
    e = p->word_size();
    if ( this->zht->find( p->hash, pos, k ) ) {
      if ( p->ref == 0 )
        this->zht->remove( pos );
      else {
        if ( j != i ) {
          this->zht->set( p->hash, pos, j );
          ::memmove( &this->code_buf[ j ], p, e * sizeof( uint32_t ) );
        }
        j += e;
      }
    }
  }
  if ( this->zht != NULL && this->zht->need_resize() )
    this->zht = UIntHashTab::resize( this->zht );
  this->code_end  = j;
  this->code_free = 0;
}

static uint32_t
insert_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt )
{
  uint32_t i = 0;
  while ( i < rcnt && r > routes[ i ] )
    i++;
  if ( i < rcnt && r == routes[ i ] )
    return rcnt;
  for ( uint32_t j = rcnt++; j > i; j-- )
    routes[ j ] = routes[ j - 1 ];
  routes[ i ] = r;
  return rcnt;
}

static uint32_t
delete_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt )
{
  uint32_t i = 0;
  while ( i < rcnt && r > routes[ i ] )
    i++;
  if ( i == rcnt || r != routes[ i ] )
    return rcnt;
  for ( ; i < rcnt - 1; i++ )
    routes[ i ] = routes[ i + 1 ];
  return rcnt - 1;
}

uint32_t
RouteDB::compress_routes( uint32_t *routes,  uint32_t rcnt )
{
  CodeRef  * p;
  uint32_t * code, ecnt;
  uint32_t   h, pos, val, seed;

  if ( rcnt < 5 ) { /* most likely single value code, skips bsearch */
    h = this->dc.encode( rcnt, routes, 0 );
    if ( h != 0 )
      return h;
  }
  code = this->make_code_space( rcnt );
  ecnt = this->dc.encode_stream( rcnt, routes, 0, code );
  if ( ecnt == 1 ) /* single value code */
    return code[ 0 ];

  seed = 0; /* multi value code, use zht */
find_next_hash:;
  h = CodeRef::hash_code( code, ecnt, seed );
  if ( this->zht == NULL )
    this->zht = UIntHashTab::resize( NULL );
  if ( this->zht->find( h, pos, val ) ) {
    p = (CodeRef *) (void *) &this->code_buf[ val ];
    if ( ! p->equals( code, ecnt ) ) { /* hash needs to be unique */
      seed++;
      goto find_next_hash;
    }
    if ( p->ref++ == 0 )
      this->code_free -= p->word_size();
  }
  else { /* new code ref */
    uint32_t * spc =
      this->make_code_ref_space( CodeRef::alloc_words( ecnt ), val );
    new ( spc ) CodeRef( code, ecnt, rcnt, h /* , seed */ );
    this->zht->set( h, pos, val ); /* val is offset of the code ref */
    if ( this->zht->need_resize() )
      this->zht = UIntHashTab::resize( this->zht );
  }
  return h; /* unique route code */
}

uint32_t
RouteDB::decompress_routes( uint32_t r,  uint32_t *&routes,  bool deref )
{
  uint32_t rcnt;
  if ( this->dc.is_not_encoded( r ) ) { /* if is a multi value code */
    CodeRef * p;
    uint32_t  pos,
              val;
    if ( this->zht != NULL && this->zht->find( r, pos, val ) ) {
      p = (CodeRef *) (void *) &this->code_buf[ val ];
      routes = this->make_route_space( p->rcnt + 1 ); /* in case insert */
      rcnt = this->dc.decode_stream( p->ecnt, &p->code, 0, routes );
      if ( deref ) {
        if ( --p->ref == 0 )
          this->code_free += p->word_size();
      }
    }
    else { /* no route exists */
      routes = NULL;
      rcnt = 0;
    }
  }
  else { /* single value code */
    routes = this->make_route_space( MAX_DELTA_CODE_LENGTH + 1 );
    rcnt = this->dc.decode( r, routes, 0 );
  }
  return rcnt;
}

void
RouteDB::add_route( uint32_t hash,  uint32_t r )
{
  uint32_t pos, val, rcnt = 0;
  uint32_t *routes, tmp_route;

  if ( this->xht == NULL )
    this->xht = UIntHashTab::resize( NULL );

  if ( this->xht->find( hash, pos, val ) ) {
    rcnt = this->decompress_routes( val, routes, true );
    if ( rcnt > 0 )
      rcnt = insert_route( r, routes, rcnt );
  }
  if ( rcnt == 0 ) { /* new route */
    rcnt      = 1;
    tmp_route = r;
    routes    = &tmp_route;
  }
  val = this->compress_routes( routes, rcnt );
  this->xht->set( hash, pos, val );
  if ( this->xht->need_resize() )
    this->xht = UIntHashTab::resize( this->xht );
  if ( this->code_free * 2 > this->code_end )
    this->gc_code_ref_space();
}

void
RouteDB::del_route( uint32_t hash,  uint32_t r )
{
  uint32_t pos, val, rcnt;
  uint32_t *routes;

  if ( this->xht != NULL && this->xht->find( hash, pos, val ) ) {
    rcnt = this->decompress_routes( val, routes, true );
    rcnt = delete_route( r, routes, rcnt );
    if ( rcnt > 0 ) {
      val = this->compress_routes( routes, rcnt );
      this->xht->set( hash, pos, val );
    }
    else {
      this->xht->remove( pos );
      if ( this->xht->need_resize() )
        this->xht = UIntHashTab::resize( this->xht );
    }
    if ( this->code_free * 2 > this->code_end )
      this->gc_code_ref_space();
  }
}

