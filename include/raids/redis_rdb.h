#ifndef __rai_raids__redis_rdb_h__
#define __rai_raids__redis_rdb_h__

#include <rdbparser/rdb_encode.h>
#include <raimd/md_list.h>
#include <raimd/md_hash.h>
#include <raimd/md_set.h>
#include <raimd/md_zset.h>
#include <raimd/md_geo.h>
#include <raimd/md_hll.h>
#include <raimd/md_stream.h>

namespace rai {
namespace ds {

struct ExecRestore : public rdbparser::RdbOutput {
  RedisExec  & exec;
  EvKeyCtx   & ctx;     /* the key where data is restored */
  size_t       msg_len; /* size of the restore data */
  ExecStatus   status;  /* status to return to cmd execution */
  bool         is_geo;  /* when zset code, if zset or geo type */

  union {
    md::ListData   * list; /* the data to store */
    md::SetData    * set;
    md::ZSetData   * zset;
    md::GeoData    * geo;
    md::HashData   * hash;
    md::StreamData * strm;
  };
  uint64_t last_ns; /* the last used stream ms */
  size_t   pcount;  /* the pending record count */
  struct {
    uint64_t id_ms, id_ser, last_delivery, delivery_cnt;
  } * pend; /* if last_delivery != id_ms or delivery_cnt != 1 */

  md::MDMsgMem tmp;

  ExecRestore( rdbparser::RdbDecode &d,  RedisExec &e,  EvKeyCtx &c,
               size_t len )
    : rdbparser::RdbOutput( d ), exec( e ), ctx( c ), msg_len( len ),
      status( EXEC_SEND_OK ) {}

  void resize_list( void );
  void resize_set( void );
  void resize_zset( void );
  void resize_hash( void );
  void resize_stream( void );

  void set_value( uint8_t type,  uint16_t fl,  const void *value,  size_t len );

  virtual void d_start_key( void ) noexcept;
  virtual void d_end_key( void ) noexcept;
  /* rdb types */
  virtual void d_string( const rdbparser::RdbString &str ) noexcept;
  virtual void d_list( const rdbparser::RdbListElem &l ) noexcept;
  virtual void d_set( const rdbparser::RdbSetMember &s ) noexcept;
  virtual void d_zset( const rdbparser::RdbZSetMember &z ) noexcept;
  virtual void d_hash( const rdbparser::RdbHashEntry &h ) noexcept;
  virtual void d_stream_entry( const rdbparser::RdbStreamEntry &entry ) noexcept;
  virtual void d_stream_info( const rdbparser::RdbStreamInfo &info ) noexcept;
  virtual void d_stream_group( const rdbparser::RdbGroupInfo &group ) noexcept;
  virtual void d_stream_pend( const rdbparser::RdbPendInfo &pend ) noexcept;
  virtual void d_stream_cons( const rdbparser::RdbConsumerInfo &cons ) noexcept;
  virtual void d_stream_cons_pend(
                              const rdbparser::RdbConsPendInfo &pend ) noexcept;
#if 0
  virtual void d_stream_start( rdbparser::StreamPart c ) noexcept;
  virtual void d_stream_end( rdbparser::StreamPart c ) noexcept;
  virtual void d_module( const RdbString &str ) noexcept;
#endif
};

struct ExecReStrBuf {
  char         buf[ 32 ];
  const char * s;
  size_t       len;

  ExecReStrBuf( const rdbparser::RdbString &str )
      : s( str.s ), len( str.s_len ) {
    if ( str.coding != rdbparser::RDB_STR_VAL ) {
      if ( str.coding == rdbparser::RDB_INT_VAL ) {
        this->len = int_to_str( str.ival, this->buf );
        this->s   = this->buf;
      }
      else {
        this->len = md::float_str( str.fval, this->buf );
      }
    }
  }
};

template <class LIST_CLASS>
struct ExecRestoreCtx {
  ExecRestore & restore;
  LIST_CLASS  * x;
  size_t        iter;
  ExecRestoreCtx( ExecRestore &r,  LIST_CLASS * p )
    : restore( r ), x( p ), iter( 0 ) {}

  LIST_CLASS * realloc( size_t elem_sz,  size_t elem_cnt ) {
    size_t       data_len, count, asize;
    LIST_CLASS * newbe;
    void       * m, * p;

    if ( this->x == NULL ) {
      count    = elem_cnt + 2;  /* may be ok up to 64k (zip list, zllen) */
      data_len = this->restore.msg_len / 2 + 2;
      if ( data_len < elem_sz )
        data_len = elem_sz;
      asize    = LIST_CLASS::alloc_size( count, data_len );
    }
    else {
      count    = elem_cnt + ++this->iter;
      data_len = elem_sz + this->iter;
      asize    = this->x->resize_size( count, data_len );
    }

    this->restore.tmp.alloc( sizeof( LIST_CLASS ) + asize, &m );
    p = &((char *) m)[ sizeof( LIST_CLASS ) ];
    newbe = new ( m ) LIST_CLASS( p, asize );
    newbe->init( count, data_len );
    if ( this->x != NULL )
      this->x->copy( *newbe );
    this->x = newbe;
    return this->x;
  }
};

struct ExecRestoreStream {
  ExecRestore    & restore;
  md::StreamData * x;
  size_t           iter;
  ExecRestoreStream( ExecRestore &r,  md::StreamData * p )
    : restore( r ), x( p ), iter( 0 ) {}

  enum StreamPart { STREAM_PART, GROUP_PART, PENDING_PART };

  md::StreamData * realloc( size_t elem_sz,  size_t elem_cnt,
                            StreamPart part = STREAM_PART ) {
    size_t           data_len, count, asize, l, c;
    md::StreamGeom   geom;
    md::StreamData * newbe;
    void           * m, * p;

    if ( this->x == NULL ) {
      count    = elem_cnt + 2;
      data_len = this->restore.msg_len / 2 + 2;
      if ( data_len < elem_sz )
        data_len = elem_sz;
      l = 8, c = 1;
    }
    else {
      count    = elem_cnt + ++this->iter;
      data_len = elem_sz + this->iter;
      l = 0, c = 0;
    }
    if ( part == STREAM_PART )
      geom.add( this->x, data_len, count, l, c, l, c );
    else if ( part == GROUP_PART )
      geom.add( this->x, l, c, data_len, count, l, c );
    else
      geom.add( this->x, l, c, l, c, data_len, count );

    asize = geom.asize();
    this->restore.tmp.alloc( sizeof( md::StreamData ) + asize, &m );
    p = &((char *) m)[ sizeof( md::StreamData ) ];
    newbe = geom.make_new( m, p );
    if ( this->x != NULL )
      this->x->copy( *newbe );
    this->x = newbe;
    return this->x;
  }

  md::StreamData * realloc_group( size_t elem_sz,  size_t elem_cnt ) {
    return this->realloc( elem_sz, elem_cnt, GROUP_PART );
  }

  md::StreamData * realloc_pending( size_t elem_sz,  size_t elem_cnt ) {
    return this->realloc( elem_sz, elem_cnt, PENDING_PART );
  }
};

}
}

#endif
