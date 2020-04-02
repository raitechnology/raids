#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <raimd/md_types.h>
#include <raids/redis_exec.h>
#include <rdbparser/rdb_encode.h>
#include <raimd/md_list.h>
#include <raimd/md_hash.h>
#include <raimd/md_set.h>
#include <raimd/md_zset.h>
#include <raimd/md_geo.h>
#include <raids/exec_list_ctx.h>
#include <raids/exec_stream_ctx.h>

using namespace rai;
using namespace ds;
using namespace md;
using namespace kv;
using namespace rdbparser;


ExecStatus
RedisExec::exec_dump( EvKeyCtx &ctx ) noexcept
{
  /* DUMP key (serialize key) */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK: {
      switch ( this->kctx.get_type() ) {
        case MD_STRING: return this->dump_string( ctx );
        case MD_LIST:   return this->dump_list( ctx );
        case MD_HASH:   return this->dump_hash( ctx );
        case MD_SET:    return this->dump_set( ctx );
        case MD_ZSET:   return this->dump_zset( ctx );
        case MD_GEO:    return this->dump_geo( ctx );
        case MD_HYPERLOGLOG:
                        return this->dump_hll( ctx );
        case MD_STREAM: return this->dump_stream( ctx );
        default:        return ERR_BAD_TYPE;
      }
    }
    /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
  }
}

static int
mk_aux( char *out,  const char *name,  int64_t val )
{
  RdbLenEncode rdb;   /* if save, this is the encoded key length */
  out[ 0 ] = RDB_AUX;
  int i = rdb.str_size( ::strlen( name ) );
  rdb.str_encode( &out[ 1 ], name );
  int j = rdb.int_size( val );
  rdb.int_encode( &out[ i + 1 ] );
  return 1 + i + j;
}

ExecStatus
RedisExec::exec_save( void ) noexcept
{
  static const char ver[] = "REDIS0009";
  uint64_t     ht_size  = this->kctx.ht.hdr.ht_size,
               pos;
  size_t       strm_idx = 0,
               nbytes;
  int          fd = ::open( "dump.rdb", O_CREAT | O_TRUNC | O_WRONLY, 0666 );
  ExecStatus   status;
  char         ctime_aux[ 32 ],
               aof_aux[ 32 ];
  uint64_t     cur_time = this->kctx.ht.hdr.current_stamp;
  int          ctime_sz = mk_aux( ctime_aux, "ctime", cur_time / 1000000000 ),
               aof_sz   = mk_aux( aof_aux, "aof-preamble", 0 ),
               db_sz;
  uint8_t      db_select[ 8 ];
  RdbLenEncode rdb;
  /* construct hdr */
  db_select[ 0 ] = RDB_DBSELECT;
  db_sz = 1 + rdb.len_encode( &db_select[ 1 ], this->kctx.db_num );
  if ( fd < 0 ||
       ::write( fd, ver, 9 ) != 9 ||
       ::write( fd, ctime_aux, ctime_sz ) != ctime_sz ||
       ::write( fd, aof_aux, aof_sz ) != aof_sz ||
       ::write( fd, db_select, db_sz ) != db_sz ) {
    perror( "dump.rdb" );
    return ERR_SAVE;
  }
  this->strm.reset();
  this->cmd_state |= CMD_STATE_SAVE;
  /* update crc */
  EvKeyCtx key( this->kctx.ht );
  key.ival = rdbparser::jones_crc64( 0, ver, 9 );
  key.ival = rdbparser::jones_crc64( key.ival, ctime_aux, ctime_sz );
  key.ival = rdbparser::jones_crc64( key.ival, aof_aux, aof_sz );
  key.ival = rdbparser::jones_crc64( key.ival, db_select, db_sz );
  /* for each position in ht, convert key to rdb format */
  for ( pos = 0; pos < ht_size; pos++ ) {
    for (;;) {
      status = ERR_KV_STATUS;
      key.kstatus = this->kctx.fetch( &this->wrk, pos, true );
      if ( key.kstatus == KEY_OK ) {
        this->save_key = NULL;
        key.kstatus = this->kctx.get_key( this->save_key );
        if ( key.kstatus == KEY_OK ) {
          this->kctx.get_stamps( key.hash1 /*exp*/, key.hash2 /*upd*/);
          if ( key.hash1 != 0 )
            key.hash1 /= 1000000; /* in millisecs */
          if ( key.hash2 != 0 )
            key.hash2 = ( cur_time - key.hash2 ) / 1000000000; /* in seconds */
          status = this->exec_dump( key );
        }
      }
      if ( key.kstatus != KEY_MUTATED || status != ERR_KV_STATUS )
        break;
    }
    nbytes = this->strm.pending() - this->strm_start;
    if ( nbytes >= 16 * 1024 ) {
      this->strm.flush();
      if ( ::writev( fd, &this->strm.iov[ strm_idx ],
                     this->strm.idx - strm_idx ) != (ssize_t) nbytes ) {
        status = ERR_SAVE;
        goto break_loop;
      }
      this->strm.reset();
    }
  }
  status = EXEC_SEND_OK;
  nbytes = this->strm.pending() - this->strm_start;
  if ( nbytes > 0 ) {
    static const uint8_t eof = 0xff;
    key.ival = rdbparser::jones_crc64( key.ival, &eof, 1 );
    this->strm.flush();
    if ( ::writev( fd, &this->strm.iov[ strm_idx ],
                   this->strm.idx - strm_idx ) != (ssize_t) nbytes ||
         ::write( fd, &eof, 1 ) != 1 ||     /* terminate dump */
         ::write( fd, &key.ival, 8 ) != 8 ) /* crc */
      status = ERR_SAVE;
  }
break_loop:;
  this->strm.reset();
  this->cmd_state &= ~CMD_STATE_SAVE;
  this->save_key = NULL;
  ::close( fd );
  return status;
}

namespace {
struct RdbDumpGeom {
  KeyFragment * key;
  size_t        sz,    /* alloc size of message */
                start, /* where to start encoding rdb type */
                len,   /* length of data minus type, version crc */
                digs,  /* digits in length */
                trail; /* when dump, this is offset where ver crc goes */
  uint64_t      crc,   /* crc for save */
                expires;
  RdbType       type;  /* codec type */
  RdbLenEncode  rdb,   /* if save, this is the encoded key length */
                idle;  /* if save, this can be idle time */

  RdbDumpGeom( RdbType t,  size_t pack_size,  KeyFragment *kp, EvKeyCtx &ctx ) {
    this->type = t;
    this->key  = kp;
    if ( kp != NULL ) {
      this->start = 1 + this->rdb.str_size( kp->keylen - 1 );
      if ( ctx.hash1 != 0 ) { /* expires */
        this->start += 9;
        this->expires = ctx.hash1;
      }
      else
        this->expires = 0;
      if ( ctx.hash2 != 0 )   /* updated */
        this->start += 1 + this->idle.len_size( ctx.hash2 );
      else
        this->idle.init();
      this->sz    = this->start + pack_size;
      this->len   = 0; /* these are unused */
      this->digs  = 0;
      this->trail = 0;
      this->crc   = ctx.ival;
    }
    else {
      this->len     = 1 + pack_size + 2 + 8;    /* S data 9 0 crc */
      this->digs    = uint_digits( this->len ); /* $ nnn <- digits in return */
      this->sz      = 1 + this->digs + 2 + this->len + 2;/* $ nnn\r\n len \r\n*/
      this->trail   = 1 + this->digs + 2 + 1 + pack_size;
      this->start   = 1 + this->digs + 3;
      this->crc     = 0;
      this->expires = 0;
      this->rdb.init();
      this->idle.init();
    }
  }

  int64_t frame_dump_result( char *p ) noexcept;
};

inline int64_t
RdbDumpGeom::frame_dump_result( char *p ) noexcept
{
  if ( this->key != NULL ) {
    size_t off = 0;
    if ( this->expires != 0 ) {
      p[ off++ ] = RDB_EXPIRED_MS;
      le<uint64_t>( &p[ off ], this->expires );
      off += 8;
    }
    if ( this->idle.lcode != RdbLength::RDB_LEN_ERR ) {
      p[ off++ ] = RDB_IDLE;
      off += this->idle.len_encode( &p[ off ] );
    }
    p[ off++ ] = this->type;
    this->rdb.str_encode( &p[ off ], this->key->u.buf );
    this->crc = rdbparser::jones_crc64( this->crc, p, this->sz );
  }
  else {
    size_t off = this->start - 1;
    p[ 0 ] = '$';
    uint_to_str( this->len, &p[ 1 ], this->digs ); /* $ nnn */
    crlf( p, this->digs + 1 );
    p[ off ] = this->type;
    size_t i = this->trail;
    p[ i++ ] = 9;
    p[ i++ ] = 0;
    rdbparser::le<uint64_t>( (uint8_t *) &p[ i ],
      rdbparser::jones_crc64( 0, &p[ off ], i - off ) );
    crlf( p, i + 8 );
  }
  return this->crc;
}
}

ExecStatus
RedisExec::dump_string( EvKeyCtx &ctx ) noexcept
{
  void * data;
  size_t size;
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:
      if ( (ctx.kstatus = this->kctx.value( &data, size )) == KEY_OK )
        break;
      /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_STRING, rdb.str_size( size ), this->save_key, ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  rdb.str_encode( &p[ dump.start ], data );  /* key value */

  ctx.kstatus = this->kctx.validate_value(); /* check if key is valid */
  if ( ctx.kstatus != KEY_OK )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_list( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );

  switch ( list.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! list.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = list.x->count();
  ListStatus   lstatus = LIST_OK;
  ListVal      lv;
  RdbZipEncode zip;

  zip.init();
  for ( i = 0; i < count; i++ ) {
    lstatus = list.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    zip.calc_link( lv.sz + lv.sz2 );
  }
  zip.calc_end();

  if ( lstatus != LIST_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_LIST_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 0; i < count; i++ ) {
    lstatus = list.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    zip.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
  }
  zip.append_end( count );

  if ( ! list.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_hash( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );

  switch ( hash.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! hash.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = hash.x->count();
  HashStatus   hstatus = HASH_OK;
  HashVal      hv;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    hstatus = hash.x->hindex( i, hv );
    if ( hstatus != HASH_OK )
      break;
    zip.calc_link( hv.keylen );
    zip.calc_link( hv.sz + hv.sz2 );
  }
  zip.calc_end();

  if ( hstatus != HASH_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_HASH_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    hstatus = hash.x->hindex( i, hv );
    if ( hstatus != HASH_OK )
      break;
    zip.append_link( hv.key, hv.keylen );
    zip.append_link( hv.data, hv.data2, hv.sz, hv.sz2 );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! hash.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_set( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<SetData, MD_SET> set( *this, ctx );

  switch ( set.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! set.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = set.x->count();
  ListStatus   lstatus = LIST_OK;
  ListVal      lv;
  RdbLenEncode rdb;
  size_t       len = 0;

  for ( i = 1; i < count; i++ ) {
    lstatus = set.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    len += rdb.str_size( lv.sz + lv.sz2 );
  }

  if ( lstatus != LIST_OK )
    return EXEC_SEND_NIL;

  RdbDumpGeom dump( RDB_SET, len + rdb.len_size( count - 1 ), this->save_key,
                    ctx );
  size_t      off = dump.start;
  char      * p   = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  off += rdb.len_encode( &p[ off ] ); /* the count */

  for ( i = 1; i < count; i++ ) {
    lstatus = set.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    rdb.str_size( lv.sz + lv.sz2 );
    off += rdb.len_encode( &p[ off ] );
    ::memcpy( &p[ off ], lv.data, lv.sz );
    off += lv.sz;
    if ( lv.sz2 > 0 ) {
      ::memcpy( &p[ off ], lv.data2, lv.sz2 );
      off += lv.sz2;
    }
  }

  if ( ! set.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_zset( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );

  switch ( zset.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! zset.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = zset.x->count();
  ZSetStatus   zstatus = ZSET_OK;
  ZSetVal      zv;
  char         fpdata[ 64 ];
  size_t       fplen;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    zstatus = zset.x->zindex( i, zv );
    if ( zstatus != ZSET_OK )
      break;
    zip.calc_link( zv.sz + zv.sz2 );
    zip.calc_link( zv.score.to_string( fpdata ) );
  }
  zip.calc_end();

  if ( zstatus != ZSET_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_ZSET_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    zstatus = zset.x->zindex( i, zv );
    if ( zstatus != ZSET_OK )
      break;
    zip.append_link( zv.data, zv.data2, zv.sz, zv.sz2 );
    fplen = zv.score.to_string( fpdata );
    zip.append_link( fpdata, fplen );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! zset.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_geo( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );

  switch ( geo.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! geo.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = geo.x->count();
  GeoStatus    gstatus = GEO_OK;
  GeoVal       gv;
  char         fpdata[ 64 ];
  size_t       fplen;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    gstatus = geo.x->geoindex( i, gv );
    if ( gstatus != GEO_OK )
      break;
    zip.calc_link( gv.sz + gv.sz2 );
    zip.calc_link( uint_digits( gv.score ) );
  }
  zip.calc_end();

  if ( gstatus != GEO_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_ZSET_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    gstatus = geo.x->geoindex( i, gv );
    if ( gstatus != GEO_OK )
      break;
    zip.append_link( gv.data, gv.data2, gv.sz, gv.sz2 );
    fplen = uint_digits( gv.score );
    uint_to_str( gv.score, fpdata, fplen ); /* $ nnn */
    zip.append_link( fpdata, fplen );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! geo.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_hll( EvKeyCtx &ctx ) noexcept
{
  return this->dump_string( ctx );
}

ExecStatus
RedisExec::dump_stream( EvKeyCtx &ctx ) noexcept
{
  struct Consumers {
    ListVal  lv;
    uint64_t pending_cnt,
             last_seen;
  };
  ExecStreamCtx     stream( *this, ctx );
  MDMsgMem          tmp;
  ListData          ld;
  ListVal           lv, lv_grp, lv_id, lv_ns, lv_cnt;
  size_t            i,
                    j,
                    k,
                    n,
                    off,
                    scnt,
                    fcnt,
                    gcnt,
                    pcnt,
                    pending_count,
                    num_consumers,
                    max_consumers = 0;
  Consumers       * con = NULL;
  RdbListPackEncode lst;
  StreamId          sid;
  uint64_t          start_x = 0,
                    start_y = 0,
                    upd, exp,
                    last_ms,
                    last_ser;
  RdbLenEncode      rdb;

  switch ( stream.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! stream.open_readonly() )
    return ERR_KV_STATUS;

  scnt = stream.x->stream.count();
  gcnt = stream.x->group.count();
  pcnt = stream.x->pending.count();
  lst.init();
  lst.calc_immediate_int( scnt ); /* stream entry count */
  lst.calc_immediate_int( 0 );      /* deleted entry count */
  lst.calc_immediate_int( 0 );      /* master field count */
  lst.calc_immediate_int( 0 );      /* back link count */
  for ( i = 0; i < scnt; i++ ) {
    if ( stream.x->sindex( stream.x->stream, i, ld, tmp ) != STRM_OK )
      break;
    fcnt = ld.count();
    if ( fcnt < 3 || ld.lindex( 0, lv ) != LIST_OK ||
         ! sid.str_to_id( lv, tmp ) )
      break;
    /* "entries" : [
        { "id" : "1585292469991-0", "loc" : "mel", "temp" : "23" },
        { "id" : "1585292477471-0", "loc" : "sfo", "temp" : "17" } ], */
    lst.calc_immediate_int( 0 ); /* master flds not used */
    if ( i == 0 ) {
      lst.calc_immediate_int( 0 );   /* ms offset */
      lst.calc_immediate_int( 0 );   /* ser offset */
      start_x = sid.x;
      start_y = sid.y;
    }
    else {
      lst.calc_immediate_int( sid.x - start_x );   /* ms offset */
      lst.calc_immediate_int( sid.y - start_y );   /* ser offset */
    }
    fcnt -= 1;
    n = fcnt / 2;
    fcnt = n * 2; /* fcnt must be even */
    lst.calc_immediate_int( n );
    for ( k = 1; ; ) {
      if ( n-- == 0 )
        break;
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.calc_link( lv.sz + lv.sz2 );
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.calc_link( lv.sz + lv.sz2 );
    }
    lst.calc_immediate_int( 4 + fcnt ); /* back link count */
  }
  lst.calc_end();
  off = rdb.len_size( scnt ); /* count of stream recs */
  this->kctx.get_stamps( exp, upd );
  last_ms  = upd / 1000000;
  last_ser = upd % 1000000;
  /* "last_id" : "1585292477471-0",
     "num_elems" : 2,
     "num_cgroups" : 2 */
  off += rdb.len_size( last_ms );  /* last ms id used */
  off += rdb.len_size( last_ser ); /* last serial used */
  off += rdb.len_size( gcnt ); /* number of groups */
  for ( i = 0; i < gcnt; i++ ) {
    if ( stream.x->sindex( stream.x->group, i, ld, tmp ) != STRM_OK )
      break;
    if ( ld.lindex( 0, lv_grp ) != LIST_OK ||
         ld.lindex( 1, lv_id ) != LIST_OK ||
         ! sid.str_to_id( lv_id, tmp ) )
      break;
    /* "group": "strgrp",               <- rdb str
       "pending": 2,                    <- rdb len
       "last_id": "1585458175186-0",    <- rdb len * 2 */
    off += rdb.str_size( lv_grp.sz + lv_grp.sz2 ); /* group name */
    off += rdb.len_size( sid.x );            /* ms */
    off += rdb.len_size( sid.y );            /* ser */

    pending_count = 0;
    num_consumers = 0;

    for ( j = 0; j < pcnt; j++ ) {
      if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
        break;
      if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
        pending_count++;
        if ( ld.lindex( P_CON, lv ) != LIST_OK ||
             ld.lindex( P_NS, lv_ns ) != LIST_OK ||
             ld.lindex( P_CNT, lv_cnt ) != LIST_OK )
          break;
        for ( k = 0; k < num_consumers; k++ ) {
          if ( lv.cmp_key( con[ k ].lv ) == 0 )
            break;
        }
        /* add new consumer to list */
        if ( k == num_consumers ) {
          if ( k == max_consumers ) {
            tmp.extend( sizeof( con[ 0 ] ) * i,
                        sizeof( con[ 0 ] ) * ( k + 8 ), &con );
            max_consumers = k + 8;
          }
          con[ k ].lv = lv;
          con[ k ].pending_cnt = 1;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
          num_consumers++;
        }
        else {
          con[ k ].pending_cnt++;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
        }
        off += rdb.len_size( lv_cnt.u32() );
      }
    }
    /* "pel": [
         {
           "id": "1585458171466-0",     <- 16 bytes big endian
           "last_d": 1585468380773,     <-  8 bytes little endian
           "d_cnt": 1                   <-  rdb len
         },
       ],
       "consumers": [
          {
            "name": "strcons1",         <- rdb str
            "pending": 2,               <- rdb len
            "last_seen": 1585468380773, <-  8 bytes little endian
            "pel": [
              "1585458171466-0",        <- 16 bytes big endian
              "1585458175186-0"
            ]
          }
        ] */
    if ( pending_count > 0 ) {
      off += rdb.len_size( pending_count ); /* pending */
      off += pending_count * ( 16 + 8 ) ; /* stream id + last deliv */
    }
    else {
      off += rdb.len_size( 0 );
    }
    if ( num_consumers > 0 ) {
      off += rdb.len_size( num_consumers ); /* consumers */
      for ( j = 0; j < num_consumers; j++ ) {
        off += rdb.str_size( con[ j ].lv.sz + con[ j ].lv.sz2 );
        off += rdb.len_size( con[ j ].pending_cnt );
        off += con[ j ].pending_cnt * 16; /* stream id pel */
        off += 8; /* last seen */
      }
    }
    else {
      off += rdb.len_size( 0 );
    }
  }

  RdbDumpGeom dump( RDB_STREAM_LISTPACK, 1 /* Lpcnt */ + 17 /* key */ +
                    rdb.str_size( lst.off ) + off, this->save_key, ctx );
  char      * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  p[ dump.start ]     = 1; /* stream listpack count */
  p[ dump.start + 1 ] = 16;

  off = rdb.len_encode( &p[ dump.start + 18 ] );
  lst.init( &p[ dump.start + 18 + off ] );
  lst.append_immediate_int( scnt ); /* stream entry count */
  lst.append_immediate_int( 0 );      /* deleted entry count */
  lst.append_immediate_int( 0 );      /* master field count */
  lst.append_immediate_int( 0 );      /* back link count */

  for ( i = 0; i < scnt; i++ ) {
    if ( stream.x->sindex( stream.x->stream, i, ld, tmp ) != STRM_OK )
      break;
    fcnt = ld.count();
    if ( fcnt < 3 || ld.lindex( 0, lv ) != LIST_OK ||
         ! sid.str_to_id( lv, tmp ) )
      break;
    /* "entries" : [
        { "id" : "1585292469991-0", "loc" : "mel", "temp" : "23" },
        { "id" : "1585292477471-0", "loc" : "sfo", "temp" : "17" } ], */
    lst.append_immediate_int( 0 );   /* master flds not used */
    if ( i == 0 ) {
      be<uint64_t>( &p[ dump.start + 2 ], sid.x );
      be<uint64_t>( &p[ dump.start + 8 + 2 ], sid.y );
      lst.append_immediate_int( 0 );   /* ms offset */
      lst.append_immediate_int( 0 );   /* ser offset */
      start_x = sid.x;
      start_y = sid.y;
    }
    else {
      lst.append_immediate_int( sid.x - start_x );   /* ms offset */
      lst.append_immediate_int( sid.y - start_y );   /* ser offset */
    }
    fcnt -= 1;
    n = fcnt / 2;
    fcnt = n * 2; /* fcnt must be even */
    lst.append_immediate_int( n );
    for ( k = 1; ; ) {
      if ( n-- == 0 )
        break;
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
    }
    lst.append_immediate_int( 4 + fcnt ); /* back link count */
  }
  lst.append_end();

  char * q = (char *) lst.p;
  off  = lst.off;
  off += rdb.len_encode( &q[ off ], scnt );   /* count of stream recs */
  /* "last_id" : "1585292477471-0",
     "num_elems" : 2,
     "num_cgroups" : 2 */
  off += rdb.len_encode( &q[ off ], last_ms );  /* last ms id used */
  off += rdb.len_encode( &q[ off ], last_ser ); /* last serial used */
  off += rdb.len_encode( &q[ off ], gcnt );   /* number of groups */
  for ( i = 0; i < gcnt; i++ ) {
    if ( stream.x->sindex( stream.x->group, i, ld, tmp ) != STRM_OK )
      break;
    if ( ld.lindex( 0, lv_grp ) != LIST_OK ||
         ld.lindex( 1, lv_id ) != LIST_OK ||
         ! sid.str_to_id( lv_id, tmp ) )
      break;
    /* "group": "strgrp",               <- rdb str
       "pending": 2,                    <- rdb len
       "last_id": "1585458175186-0",    <- rdb len * 2 */
    off += rdb.str_encode( &q[ off ], lv_grp.data, lv_grp.data2,
                                      lv_grp.sz, lv_grp.sz2 );
    off += rdb.len_encode( &q[ off ], sid.x ); /* last ms */
    off += rdb.len_encode( &q[ off ], sid.y ); /* last ser */
    pending_count = 0;
    num_consumers = 0;

    for ( j = 0; j < pcnt; j++ ) {
      if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
        break;
      if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
        pending_count++;
        if ( ld.lindex( P_CON, lv ) != LIST_OK ||
             ld.lindex( P_NS, lv_ns ) != LIST_OK ||
             ld.lindex( P_CNT, lv_cnt ) != LIST_OK )
          break;
        for ( k = 0; k < num_consumers; k++ ) {
          if ( lv.cmp_key( con[ k ].lv ) == 0 )
            break;
        }
        /* add new consumer to list */
        if ( k == num_consumers ) {
          if ( k == max_consumers ) {
            tmp.extend( sizeof( con[ 0 ] ) * i,
                        sizeof( con[ 0 ] ) * ( k + 8 ), &con );
            max_consumers = k + 8;
          }
          con[ k ].lv = lv;
          con[ k ].pending_cnt = 1;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
          num_consumers++;
        }
        else {
          con[ k ].pending_cnt++;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
        }
      }
    }
    /* "pel": [
         {
           "id": "1585458171466-0",     <- 16 bytes big endian
           "last_d": 1585468380773,     <-  8 bytes little endian
           "d_cnt": 1                   <-  rdb len
         },
       ],
       "consumers": [
          {
            "name": "strcons1",         <- rdb str
            "pending": 2,               <- rdb len
            "last_seen": 1585468380773, <-  8 bytes little endian
            "pel": [
              "1585458171466-0",        <- 16 bytes big endian
              "1585458175186-0"
            ]
          }
        ] */
    if ( pending_count > 0 ) {
      off += rdb.len_encode( &q[ off ], pending_count ); /* pending */
      for ( j = 0; j < pcnt && pending_count > 0; j++ ) {
        if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
          break;
        if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
          pending_count -= 1;
          if ( ld.lindex( P_ID, lv_id ) != LIST_OK ||
               ld.lindex( P_NS, lv_ns ) != LIST_OK ||
               ld.lindex( P_CNT, lv_cnt ) != LIST_OK ||
               ! sid.str_to_id( lv_id, tmp ) )
            break;
          be<uint64_t>( &q[ off ], sid.x );
          be<uint64_t>( &q[ off + 8 ], sid.y );
          off += 16;
          le<uint64_t>( &q[ off ], lv_ns.u64() / 1000000 );
          off += 8;
          off += rdb.len_encode( &q[ off ], lv_cnt.u32() );
        }
      }
    }
    else {
      off += rdb.len_encode( &q[ off ], 0 );     /* pending cnt */
    }
    if ( num_consumers > 0 ) {
      off += rdb.len_encode( &q[ off ], num_consumers ); /* consumers */
      for ( j = 0; j < num_consumers; j++ ) {
        ListVal & lv_c = con[ j ].lv;
        off += rdb.str_encode( &q[ off ], lv_c.data, lv_c.data2,
                                          lv_c.sz, lv_c.sz2 );
        le<uint64_t>( &q[ off ], con[ j ].last_seen );
        off += 8;
        off += rdb.len_encode( &q[ off ], con[ j ].pending_cnt );

        pending_count = con[ j ].pending_cnt;
        for ( k = 0; k < pcnt && pending_count > 0; k++ ) {
          if ( stream.x->sindex( stream.x->pending, k, ld, tmp ) != STRM_OK )
            break;
          if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK &&
               ld.lindex_cmp_key( P_CON, lv_c ) == LIST_OK &&
               ld.lindex( P_ID, lv_id ) == LIST_OK &&
               sid.str_to_id( lv_id, tmp ) ) {
            pending_count -= 1;
            be<uint64_t>( &q[ off ], sid.x );
            be<uint64_t>( &q[ off + 8 ], sid.y );
            off += 16;
          }
        }
      }
    }
    else {
      off += rdb.len_encode( &q[ off ], 0 );
    }
  }

  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}
